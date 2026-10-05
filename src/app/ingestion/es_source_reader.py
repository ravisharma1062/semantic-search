"""SourceReader on the existing Elasticsearch document index (read only).

Reads by ``_id`` with ``get`` and ``_mget`` (no search query). Every call has a timeout, and
reads are retried with backoff because they are safe to repeat.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from elasticsearch import ApiError, AsyncElasticsearch, NotFoundError
from elasticsearch import TransportError as ClientTransportError

from app.core.errors import NonRetryableError, UpstreamUnavailableError
from app.core.retry import RetryPolicy, with_retries
from app.core.settings import ElasticsearchSettings, SourceSettings
from app.ingestion.source import SourceDocument, map_source, source_includes
from app.store.client import is_retryable, map_es_error

T = TypeVar("T")

# Per-document errors of _mget that retrying cannot fix (the request or the setup is wrong).
_PERMANENT_MGET_ERRORS = frozenset(
    {"index_not_found_exception", "security_exception", "illegal_argument_exception"}
)


class ElasticsearchSourceReader:
    """Reads documents of the existing index by ``ITEM_ID``."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        index: str,
        source: SourceSettings,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._index = index
        self._source = source
        self._timeout_s = elasticsearch.source_read_timeout_s
        self._retry = retry
        self._sleep = sleep
        self._includes = source_includes(source)

    async def _call(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run a read with retries. Client errors become our typed errors."""

        async def attempt() -> T:
            try:
                return await operation()
            except (ApiError, ClientTransportError) as exc:
                raise map_es_error(exc) from exc

        return await with_retries(
            attempt, policy=self._retry, retry_if=is_retryable, sleep=self._sleep
        )

    async def get(self, item_id: str) -> SourceDocument | None:
        """The document, or ``None`` if the index has no such ID."""
        client = self._client.options(request_timeout=self._timeout_s)

        async def fetch() -> dict[str, Any]:
            try:
                response = await client.get(
                    index=self._index, id=item_id, source_includes=self._includes
                )
            except NotFoundError as exc:
                # A missing document says found=false. A missing index is a setup error.
                if isinstance(exc.body, dict) and exc.body.get("found") is False:
                    return {}
                raise
            return dict(response.body)

        response = await self._call(fetch)
        if not response:
            return None
        return map_source(item_id, response.get("_source") or {}, self._source)

    async def get_many(self, item_ids: list[str]) -> dict[str, SourceDocument]:
        """The documents found, by ID. Missing IDs are left out. Duplicates are read once."""
        unique = list(dict.fromkeys(item_ids))
        found: dict[str, SourceDocument] = {}
        for start in range(0, len(unique), self._source.max_get_many):
            found.update(await self._get_batch(unique[start : start + self._source.max_get_many]))
        return found

    async def _get_batch(self, ids: list[str]) -> dict[str, SourceDocument]:
        client = self._client.options(request_timeout=self._timeout_s)

        async def fetch() -> list[dict[str, Any]]:
            response = await client.mget(index=self._index, ids=ids, source_includes=self._includes)
            entries: list[dict[str, Any]] = response["docs"]
            for entry in entries:
                if "error" not in entry:
                    continue
                # _mget answers 200 and reports errors per ID, for example a missing index.
                kind = entry["error"].get("type") if isinstance(entry["error"], dict) else None
                if kind in _PERMANENT_MGET_ERRORS:
                    raise NonRetryableError("Elasticsearch rejected the request")
                # Anything else, such as a failed shard, is not "missing": try the batch again.
                raise UpstreamUnavailableError("Elasticsearch unavailable")
            return entries

        documents: dict[str, SourceDocument] = {}
        for entry in await self._call(fetch):
            if entry.get("found"):
                item_id = str(entry["_id"])
                documents[item_id] = map_source(item_id, entry.get("_source") or {}, self._source)
        return documents
