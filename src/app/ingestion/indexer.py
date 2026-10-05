"""Writes chunks to the chunk index (HLD sections 4 and 5).

- ``chunk_id`` is the Elasticsearch ``_id``, so writing a chunk twice overwrites it.
- A bulk request is retried for the failed items only. Other items are not sent again.
- After a successful write, chunks of the document that are not in the new set are deleted.
- A permission change updates only the ``acl_*`` fields and the filter fields.

The delete and update by query calls here are maintenance of our own index. They return no
documents to anyone, so they are not search queries in the sense of rule 1, and they live in
``store``-level code only (decision 0006).
"""

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

import structlog
from elasticsearch import AsyncElasticsearch, ConflictError

from app.core.errors import NonRetryableError, UpstreamUnavailableError
from app.core.retry import RetryPolicy, backoff_delays
from app.core.settings import ElasticsearchSettings, StoreSettings
from app.ingestion.chunker import Chunk
from app.ingestion.source import SourceDocument
from app.store.calls import guarded

_log = structlog.get_logger(__name__)
_RETRYABLE_ITEM_STATUS = frozenset({429, 500, 502, 503, 504})
_SET_PERMISSIONS = (
    "ctx._source.acl_users = params.acl_users; ctx._source.acl_groups = params.acl_groups; "
    "ctx._source.doc_type = params.doc_type; ctx._source.tags = params.tags; "
    "ctx._source.created_at = params.created_at"
)


class Indexer(Protocol):
    """What the worker needs from the chunk index."""

    async def bulk_write(
        self,
        document: SourceDocument,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        embedding_model: str,
    ) -> None:
        """Write chunks with their vectors. The same chunk twice overwrites."""
        ...

    async def delete_stale_chunks(
        self, item_id: str, keep: Iterable[str], *, refresh_first: bool = False
    ) -> int:
        """Delete chunks of the document that are not in ``keep``."""
        ...

    async def delete_chunks(self, item_id: str, *, refresh_first: bool = False) -> int:
        """Delete all chunks of the document."""
        ...

    async def refresh_metadata(
        self, document: SourceDocument, *, refresh_first: bool = False
    ) -> int:
        """Update permissions and filter fields of the document's chunks."""
        ...


def chunk_source(
    document: SourceDocument,
    chunk: Chunk,
    vector: Sequence[float],
    embedding_model: str,
    now: datetime,
) -> dict[str, Any]:
    """The Elasticsearch document of one chunk."""
    return {
        "chunk_id": chunk.chunk_id,
        "doc_id": chunk.item_id,
        "chunk_no": chunk.chunk_no,
        "page_start": chunk.page_start,
        "page_end": chunk.page_end,
        "section_title": chunk.section_title,
        "content": chunk.content,
        "content_hash": chunk.content_hash,
        "is_table": chunk.is_table,
        "embedding": list(vector),
        "embedding_model": embedding_model,
        "chunker_version": chunk.chunker_version,
        "doc_type": document.doc_type,
        "tags": document.tags,
        "created_at": document.created_at.isoformat() if document.created_at else None,
        "acl_users": document.acl_users,
        "acl_groups": document.acl_groups,
        "indexed_at": now.isoformat(),
    }


class ElasticsearchIndexer:
    """``Indexer`` on Elasticsearch."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        index: str,
        store: StoreSettings,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._index = index
        self._store = store
        self._retry = retry
        self._sleep = sleep
        self._clock = clock
        self._bulk = client.options(request_timeout=elasticsearch.bulk_timeout_s)
        self._maintenance = client.options(request_timeout=store.maintenance_timeout_s)

    # --- writing --------------------------------------------------------------------------

    async def bulk_write(
        self,
        document: SourceDocument,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        embedding_model: str,
    ) -> None:
        """Write chunks in batches. Failed items are retried, the rest is not sent again."""
        if len(chunks) != len(vectors):
            raise NonRetryableError("Number of vectors does not match the number of chunks")
        now = self._clock()
        size = self._store.bulk_batch_size
        for start in range(0, len(chunks), size):
            items = [
                (chunk.chunk_id, chunk_source(document, chunk, vector, embedding_model, now))
                for chunk, vector in zip(
                    chunks[start : start + size], vectors[start : start + size], strict=True
                )
            ]
            await self._write_items(items)

    async def _write_items(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        pending = dict(items)
        delays = list(backoff_delays(self._retry))
        for attempt in range(self._retry.attempts):
            operations: list[dict[str, Any]] = []
            for chunk_id, source in pending.items():
                operations.append({"index": {"_index": self._index, "_id": chunk_id}})
                operations.append(source)

            async def send(ops: list[dict[str, Any]] = operations) -> dict[str, Any]:
                response = await self._bulk.bulk(operations=ops, refresh=False)
                return dict(response.body)

            body = await guarded(send, self._retry, self._sleep)
            pending = self._failed_items(body, pending)
            if not pending:
                return
            if attempt < len(delays):
                _log.warning("bulk_items_retry", failed=len(pending), attempt=attempt + 1)
                await self._sleep(delays[attempt])
        raise UpstreamUnavailableError("Elasticsearch could not take all chunks")

    @staticmethod
    def _failed_items(
        body: dict[str, Any], pending: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        """The items to send again. A permanent failure stops the write."""
        if not body.get("errors"):
            return {}
        retry: dict[str, dict[str, Any]] = {}
        permanent: list[tuple[str, str]] = []
        for entry in body["items"]:
            result = entry["index"]
            error = result.get("error")
            if error is None:
                continue
            chunk_id = str(result["_id"])
            if result.get("status") in _RETRYABLE_ITEM_STATUS:
                retry[chunk_id] = pending[chunk_id]
            else:
                permanent.append((chunk_id, str(error.get("type"))))
        if permanent:
            _log.error(
                "bulk_items_rejected",
                chunk_ids=[chunk_id for chunk_id, _ in permanent][:20],
                count=len(permanent),
                error_types=sorted({kind for _, kind in permanent}),
            )
            raise NonRetryableError("Elasticsearch rejected chunks")
        return retry

    # --- maintenance ----------------------------------------------------------------------
    #
    # Delete and update by query only see documents that are already refreshed (searchable).
    # A document written a few seconds ago would be missed, and its old chunks or old permissions
    # would stay. So the caller says "refresh_first" when the document was written recently.

    async def _refresh(self) -> None:
        async def run() -> None:
            await self._maintenance.indices.refresh(index=self._index)

        await guarded(run, self._retry, self._sleep)

    async def delete_stale_chunks(
        self, item_id: str, keep: Iterable[str], *, refresh_first: bool = False
    ) -> int:
        """Delete this document's chunks that are not in ``keep``."""
        query = {
            "bool": {
                "filter": [{"term": {"doc_id": item_id}}],
                "must_not": [{"ids": {"values": list(keep)}}],
            }
        }
        return await self._delete_by_query(query, refresh_first)

    async def delete_chunks(self, item_id: str, *, refresh_first: bool = False) -> int:
        """Delete all chunks of the document."""
        return await self._delete_by_query({"term": {"doc_id": item_id}}, refresh_first)

    async def _delete_by_query(self, query: dict[str, Any], refresh_first: bool) -> int:
        if refresh_first:
            await self._refresh()

        async def run() -> int:
            try:
                response = await self._maintenance.delete_by_query(
                    index=self._index, query=query, refresh=False
                )
            except ConflictError as exc:  # a chunk changed while deleting: try again
                raise UpstreamUnavailableError("Chunks are being changed") from exc
            return int(response["deleted"])

        return await guarded(run, self._retry, self._sleep)

    async def refresh_metadata(
        self, document: SourceDocument, *, refresh_first: bool = False
    ) -> int:
        """Set permissions and filter fields on all chunks of the document. Content is untouched."""
        if refresh_first:
            await self._refresh()
        params = {
            "acl_users": document.acl_users,
            "acl_groups": document.acl_groups,
            "doc_type": document.doc_type,
            "tags": document.tags,
            "created_at": document.created_at.isoformat() if document.created_at else None,
        }

        async def run() -> int:
            try:
                response = await self._maintenance.update_by_query(
                    index=self._index,
                    query={"term": {"doc_id": document.item_id}},
                    script={"source": _SET_PERMISSIONS, "lang": "painless", "params": params},
                    refresh=False,
                )
            except ConflictError as exc:  # never leave old permissions behind: try again
                raise UpstreamUnavailableError("Chunks are being changed") from exc
            return int(response["updated"])

        return await guarded(run, self._retry, self._sleep)
