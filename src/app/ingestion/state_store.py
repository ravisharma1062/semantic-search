"""The index state: one record per ``ITEM_ID`` (HLD section 17).

The state decides if a document is skipped (same text, model and chunker), shows backfill progress
per wave, finds failed documents, and feeds reconciliation. It lives in an Elasticsearch index.

Records are written with optimistic concurrency, because a live worker and a backfill worker can
meet on the same document. A write never moves ``doc_version`` backwards.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

import structlog
from elasticsearch import AsyncElasticsearch, ConflictError, NotFoundError
from pydantic import BaseModel

from app.core.errors import UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings
from app.store.calls import guarded

_log = structlog.get_logger(__name__)
_MAX_CONFLICT_RETRIES = 5


class IndexStatus(StrEnum):
    """Where a document stands."""

    PENDING = "PENDING"
    INDEXED = "INDEXED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    DELETED = "DELETED"


class IndexState(BaseModel):
    """The record of one document."""

    item_id: str
    status: IndexStatus
    content_hash: str | None = None
    meta_hash: str | None = None
    doc_version: int | None = None
    chunk_count: int = 0
    chunker_version: str | None = None
    embedding_model: str | None = None
    indexed_at: datetime | None = None
    attempts: int = 0  # failed attempts since the last success
    last_error: str | None = None
    reason: str | None = None  # why a document was skipped
    wave: int | None = None


class StateStore(Protocol):
    """What the worker needs from the state."""

    async def get(self, item_id: str) -> IndexState | None:
        """The record, or ``None`` if the document was never seen."""
        ...

    async def mark_indexed(
        self,
        item_id: str,
        *,
        content_hash: str,
        meta_hash: str,
        doc_version: int | None,
        chunk_count: int,
        chunker_version: str,
        embedding_model: str,
    ) -> None:
        """The chunks are written."""
        ...

    async def update_meta_hash(self, item_id: str, meta_hash: str) -> None:
        """Permissions or filter fields were refreshed on the chunks."""
        ...

    async def mark_skipped(self, item_id: str, reason: str, doc_version: int | None) -> None:
        """No usable text: not indexed, not retried."""
        ...

    async def mark_deleted(self, item_id: str, doc_version: int | None) -> None:
        """The chunks are removed."""
        ...

    async def mark_failed(self, item_id: str, error: str) -> None:
        """An attempt failed."""
        ...


Change = Callable[[IndexState | None], IndexState | None]


class ElasticsearchStateStore:
    """``StateStore`` on the state index. The document ``_id`` is the item ID."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        index: str,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client = client.options(request_timeout=elasticsearch.state_timeout_s)
        self._index = index
        self._retry = retry
        self._sleep = sleep
        self._clock = clock

    async def _read(self, item_id: str) -> tuple[IndexState | None, int | None, int | None]:
        async def fetch() -> dict[str, Any] | None:
            try:
                response = await self._client.get(index=self._index, id=item_id)
            except NotFoundError as exc:
                if isinstance(exc.body, dict) and exc.body.get("found") is False:
                    return None
                raise
            return dict(response.body)

        body = await guarded(fetch, self._retry, self._sleep)
        if body is None:
            return None, None, None
        return IndexState.model_validate(body["_source"]), body["_seq_no"], body["_primary_term"]

    async def get(self, item_id: str) -> IndexState | None:
        """The record, or ``None``."""
        state, _, _ = await self._read(item_id)
        return state

    async def _change(self, item_id: str, change: Change) -> None:
        """Read, change, write with a version check. Starts again after a conflict."""
        for _ in range(_MAX_CONFLICT_RETRIES):
            current, seq_no, term = await self._read(item_id)
            new = change(current)
            if new is None:
                return
            document = new.model_dump(mode="json", exclude_none=True)

            async def write(
                document: dict[str, Any] = document,
                seq_no: int | None = seq_no,
                term: int | None = term,
            ) -> None:
                if seq_no is None:
                    await self._client.index(
                        index=self._index, id=item_id, document=document, op_type="create"
                    )
                else:
                    await self._client.index(
                        index=self._index,
                        id=item_id,
                        document=document,
                        if_seq_no=seq_no,
                        if_primary_term=term,
                    )

            try:
                await guarded(write, self._retry, self._sleep)
            except ConflictError:
                continue  # somebody wrote in between: look again
            return
        raise UpstreamUnavailableError("State record is busy")

    @staticmethod
    def _stale(current: IndexState | None, doc_version: int | None) -> bool:
        """True if the stored record is for a newer version of the document."""
        return (
            current is not None
            and current.doc_version is not None
            and doc_version is not None
            and doc_version < current.doc_version
        )

    async def mark_indexed(
        self,
        item_id: str,
        *,
        content_hash: str,
        meta_hash: str,
        doc_version: int | None,
        chunk_count: int,
        chunker_version: str,
        embedding_model: str,
    ) -> None:
        """INDEXED, and the failure count starts again."""

        def change(current: IndexState | None) -> IndexState | None:
            if self._stale(current, doc_version):
                return None
            return IndexState(
                item_id=item_id,
                status=IndexStatus.INDEXED,
                content_hash=content_hash,
                meta_hash=meta_hash,
                doc_version=doc_version,
                chunk_count=chunk_count,
                chunker_version=chunker_version,
                embedding_model=embedding_model,
                indexed_at=self._clock(),
                wave=current.wave if current else None,
            )

        await self._change(item_id, change)

    async def update_meta_hash(self, item_id: str, meta_hash: str) -> None:
        """Only the metadata hash changes."""

        def change(current: IndexState | None) -> IndexState | None:
            if current is None:
                return None
            return current.model_copy(update={"meta_hash": meta_hash})

        await self._change(item_id, change)

    async def mark_skipped(self, item_id: str, reason: str, doc_version: int | None) -> None:
        """SKIPPED with the reason."""

        def change(current: IndexState | None) -> IndexState | None:
            if self._stale(current, doc_version):
                return None
            return IndexState(
                item_id=item_id,
                status=IndexStatus.SKIPPED,
                doc_version=doc_version,
                reason=reason,
                indexed_at=self._clock(),
                wave=current.wave if current else None,
            )

        await self._change(item_id, change)

    async def mark_deleted(self, item_id: str, doc_version: int | None) -> None:
        """DELETED."""

        def change(current: IndexState | None) -> IndexState | None:
            if self._stale(current, doc_version):
                return None
            return IndexState(
                item_id=item_id,
                status=IndexStatus.DELETED,
                doc_version=doc_version,
                indexed_at=self._clock(),
                wave=current.wave if current else None,
            )

        await self._change(item_id, change)

    async def mark_failed(self, item_id: str, error: str) -> None:
        """FAILED. The failure count goes up. Earlier hashes stay, so the record stays useful."""

        def change(current: IndexState | None) -> IndexState | None:
            base = current or IndexState(item_id=item_id, status=IndexStatus.FAILED)
            return base.model_copy(
                update={
                    "status": IndexStatus.FAILED,
                    "attempts": base.attempts + 1,
                    "last_error": error[:256],
                }
            )

        await self._change(item_id, change)
