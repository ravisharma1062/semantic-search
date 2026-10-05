"""The indexing worker: handles one event (HLD section 14, "Worker processing loop").

    DELETE            remove the chunks, state DELETED
    UPSERT, ACL_CHANGE
        read the document by ITEM_ID (missing: try again later, then treat as deleted)
        no text         -> remove old chunks, state SKIPPED
        unchanged       -> refresh permissions only if they changed
        otherwise       -> chunk, embed, write, delete stale chunks, state INDEXED

The Kafka offset is committed by the consumer loop only after this returns. Every step is safe
to repeat: chunk IDs are deterministic, so a second run overwrites the same chunks (rule 3).
"""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum

import structlog

from app.core.errors import SourceNotReadyError
from app.core.settings import ChunkingSettings, IngestionSettings, NormalizerSettings, StoreSettings
from app.embeddings.base import Embedder
from app.ingestion.chunker import Chunk, Chunker
from app.ingestion.consumer import HandlerContext
from app.ingestion.dlq import safe_error_text
from app.ingestion.events import EventType, IndexEvent
from app.ingestion.indexer import Indexer
from app.ingestion.normalizer import normalize_document
from app.ingestion.source import SourceDocument, SourceReader
from app.ingestion.state_store import IndexState, IndexStatus, StateStore

_log = structlog.get_logger(__name__)


class Outcome(StrEnum):
    """What happened to the document."""

    INDEXED = "indexed"
    UNCHANGED = "unchanged"
    METADATA_REFRESHED = "metadata_refreshed"
    SKIPPED = "skipped"
    DELETED = "deleted"
    STALE_EVENT = "stale_event"


def text_hash(document: SourceDocument) -> str:
    """Hash of the cleaned text with its page numbers."""
    digest = hashlib.sha256()
    for page in document.pages:
        digest.update(f"{page.page_no}\x1f{page.text}\x1e".encode())
    digest.update(document.text.encode())
    return "sha256:" + digest.hexdigest()


def meta_hash(document: SourceDocument) -> str:
    """Hash of permissions and filter fields: what ``refresh_metadata`` keeps up to date."""
    data = {
        "acl_users": sorted(document.acl_users),
        "acl_groups": sorted(document.acl_groups),
        "doc_type": document.doc_type,
        "tags": sorted(document.tags),
        "created_at": document.created_at.isoformat() if document.created_at else None,
    }
    return "sha256:" + hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def refresh_window_s(store: StoreSettings) -> float:
    """How long a written chunk may stay invisible to delete and update by query.

    Twice the refresh interval. ``-1`` (refresh off, during a backfill wave) means always.
    """
    value = store.refresh_interval.strip()
    if value == "-1":
        return float("inf")
    units = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
    for suffix, factor in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if value.endswith(suffix) and value[: -len(suffix)].isdigit():
            return 2 * int(value[: -len(suffix)]) * factor
    return float("inf")


class IndexingWorker:
    """Turns events into indexed chunks. ``handle`` is the handler of the consumer loop."""

    def __init__(
        self,
        *,
        source: SourceReader,
        indexer: Indexer,
        states: StateStore,
        embedder: Embedder,
        chunker: Chunker,
        chunking: ChunkingSettings,
        normalizer: NormalizerSettings,
        ingestion: IngestionSettings,
        store: StoreSettings,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._source = source
        self._indexer = indexer
        self._states = states
        self._embedder = embedder
        self._chunker = chunker
        self._chunking = chunking
        self._normalizer = normalizer
        self._ingestion = ingestion
        self._refresh_window_s = refresh_window_s(store)
        self._clock = clock

    async def handle(self, event: IndexEvent, context: HandlerContext) -> None:
        """Process one event. Raises on failure, so the consumer retries it."""
        try:
            outcome = await self.process(event, context)
        except Exception as exc:
            await self._record_failure(event.item_id, exc)
            raise
        _log.info("event_processed", item_id=event.item_id, outcome=outcome.value)

    async def _record_failure(self, item_id: str, error: Exception) -> None:
        """Best effort: a failing state store must not hide the original error."""
        try:
            await self._states.mark_failed(item_id, safe_error_text(error))
        except Exception as state_error:
            _log.warning(
                "state_not_updated", item_id=item_id, error_type=type(state_error).__name__
            )

    # --- the loop ---------------------------------------------------------------------------

    async def process(self, event: IndexEvent, context: HandlerContext) -> Outcome:
        """Do the work for one event and say what happened."""
        item_id = event.item_id
        state = await self._states.get(item_id)
        if event.event_type is EventType.DELETE:
            return await self._delete(item_id, state, event.doc_version)

        document = await self._source.get(item_id)
        if document is None:
            return await self._missing(item_id, state, event, context)
        version = document.version if document.version is not None else event.doc_version
        if self._is_stale(state, version):
            return Outcome.STALE_EVENT

        document = normalize_document(document, self._normalizer)
        if not document.has_text:
            await self._indexer.delete_chunks(item_id, refresh_first=self._recent(state))
            await self._states.mark_skipped(item_id, "no usable text", version)
            return Outcome.SKIPPED

        content_hash = text_hash(document)
        metadata_hash = meta_hash(document)
        if self._unchanged(state, content_hash):
            return await self._refresh_if_needed(document, state, metadata_hash)
        return await self._index(document, state, version, content_hash, metadata_hash)

    @staticmethod
    def _is_stale(state: IndexState | None, version: int | None) -> bool:
        return (
            state is not None
            and state.doc_version is not None
            and version is not None
            and version < state.doc_version
        )

    def _recent(self, state: IndexState | None) -> bool:
        """Was the document written so recently that its chunks may not be searchable yet?"""
        if state is None or state.indexed_at is None:
            return False
        return (self._clock() - state.indexed_at).total_seconds() < self._refresh_window_s

    def _unchanged(self, state: IndexState | None, content_hash: str) -> bool:
        return (
            state is not None
            and state.status is IndexStatus.INDEXED
            and state.content_hash == content_hash
            and state.embedding_model == self._embedder.model_name
            and state.chunker_version == self._chunking.version
        )

    # --- the outcomes -----------------------------------------------------------------------

    async def _delete(self, item_id: str, state: IndexState | None, version: int | None) -> Outcome:
        if self._is_stale(state, version):
            return Outcome.STALE_EVENT  # a newer version of the document is already indexed
        await self._indexer.delete_chunks(item_id, refresh_first=self._recent(state))
        await self._states.mark_deleted(item_id, version)
        return Outcome.DELETED

    async def _missing(
        self, item_id: str, state: IndexState | None, event: IndexEvent, context: HandlerContext
    ) -> Outcome:
        """The index has no such document. It may not be visible yet, or it was deleted."""
        if not context.retries_exhausted:
            raise SourceNotReadyError
        _log.warning("source_missing_treated_as_deleted", item_id=item_id)
        return await self._delete(item_id, state, event.doc_version)

    async def _refresh_if_needed(
        self, document: SourceDocument, state: IndexState | None, metadata_hash: str
    ) -> Outcome:
        """Same text, same model: only permissions and filter fields can have changed."""
        if state is not None and state.meta_hash == metadata_hash:
            return Outcome.UNCHANGED
        await self._indexer.refresh_metadata(document, refresh_first=self._recent(state))
        await self._states.update_meta_hash(document.item_id, metadata_hash)
        return Outcome.METADATA_REFRESHED

    async def _index(
        self,
        document: SourceDocument,
        state: IndexState | None,
        version: int | None,
        content_hash: str,
        metadata_hash: str,
    ) -> Outcome:
        item_id = document.item_id
        chunks = await self._chunker.split_async(document)
        if not chunks:
            await self._indexer.delete_chunks(item_id, refresh_first=self._recent(state))
            await self._states.mark_skipped(item_id, "no chunks", version)
            return Outcome.SKIPPED
        window = self._ingestion.window_size
        for start in range(0, len(chunks), window):
            await self._write_window(document, chunks[start : start + window])
        await self._indexer.delete_stale_chunks(
            item_id, (c.chunk_id for c in chunks), refresh_first=self._recent(state)
        )
        await self._states.mark_indexed(
            item_id,
            content_hash=content_hash,
            meta_hash=metadata_hash,
            doc_version=version,
            chunk_count=len(chunks),
            chunker_version=self._chunking.version,
            embedding_model=self._embedder.model_name,
        )
        return Outcome.INDEXED

    async def _write_window(self, document: SourceDocument, chunks: list[Chunk]) -> None:
        """Embed and write a window of chunks, so a 3,000 page document never holds all vectors."""
        vectors = await self._embedder.embed_documents([c.embedding_text for c in chunks])
        await self._indexer.bulk_write(document, chunks, vectors, self._embedder.model_name)
