"""In-memory Indexer and StateStore for testing the worker loop end to end."""

from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from app.ingestion.chunker import Chunk
from app.ingestion.source import SourceDocument
from app.ingestion.state_store import IndexState, IndexStatus


class FakeIndexer:
    """Chunks by ``chunk_id``, like the real index: the same ID overwrites."""

    def __init__(self) -> None:
        self.chunks: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, Any]] = []
        self.fail_bulk_with: Exception | None = None
        self.fail_bulk_after_windows: int | None = None
        self.fail_bulk_calls: int | None = None  # fail only this many calls, then work again
        self._windows = 0

    def doc_chunks(self, item_id: str) -> list[dict[str, Any]]:
        """Stored chunks of one document, by chunk number."""
        found = [c for c in self.chunks.values() if c["doc_id"] == item_id]
        return sorted(found, key=lambda c: c["chunk_no"])

    async def bulk_write(
        self,
        document: SourceDocument,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        embedding_model: str,
    ) -> None:
        """Store the chunks."""
        self._windows += 1
        if self.fail_bulk_with and (
            self.fail_bulk_after_windows is None or self._windows > self.fail_bulk_after_windows
        ):
            if self.fail_bulk_calls is None:
                raise self.fail_bulk_with
            if self.fail_bulk_calls > 0:
                self.fail_bulk_calls -= 1
                raise self.fail_bulk_with
        self.calls.append(("bulk_write", [c.chunk_id for c in chunks]))
        for chunk, vector in zip(chunks, vectors, strict=True):
            self.chunks[chunk.chunk_id] = {
                "chunk_id": chunk.chunk_id,
                "doc_id": chunk.item_id,
                "chunk_no": chunk.chunk_no,
                "content": chunk.content,
                "embedding": list(vector),
                "embedding_model": embedding_model,
                "acl_users": list(document.acl_users),
                "acl_groups": list(document.acl_groups),
                "doc_type": document.doc_type,
                "tags": list(document.tags),
            }

    async def delete_stale_chunks(
        self, item_id: str, keep: Iterable[str], *, refresh_first: bool = False
    ) -> int:
        """Delete this document's chunks that are not in ``keep``."""
        keep_ids = set(keep)
        self.calls.append(("delete_stale", {"item_id": item_id, "refresh_first": refresh_first}))
        stale = [i for i, c in self.chunks.items() if c["doc_id"] == item_id and i not in keep_ids]
        for chunk_id in stale:
            del self.chunks[chunk_id]
        return len(stale)

    async def delete_chunks(self, item_id: str, *, refresh_first: bool = False) -> int:
        """Delete all chunks of a document."""
        self.calls.append(("delete_chunks", {"item_id": item_id, "refresh_first": refresh_first}))
        doomed = [i for i, c in self.chunks.items() if c["doc_id"] == item_id]
        for chunk_id in doomed:
            del self.chunks[chunk_id]
        return len(doomed)

    async def refresh_metadata(
        self, document: SourceDocument, *, refresh_first: bool = False
    ) -> int:
        """Set permissions and filter fields on the document's chunks."""
        self.calls.append(
            ("refresh_metadata", {"item_id": document.item_id, "refresh_first": refresh_first})
        )
        changed = 0
        for chunk in self.chunks.values():
            if chunk["doc_id"] == document.item_id:
                chunk["acl_users"] = list(document.acl_users)
                chunk["acl_groups"] = list(document.acl_groups)
                chunk["doc_type"] = document.doc_type
                chunk["tags"] = list(document.tags)
                changed += 1
        return changed


class FakeStateStore:
    """State records in a dict. Same rules as the real store."""

    def __init__(self) -> None:
        self.states: dict[str, IndexState] = {}
        self.fail_with: Exception | None = None
        self.clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    async def get(self, item_id: str) -> IndexState | None:
        """The record, or ``None``."""
        if self.fail_with:
            raise self.fail_with
        return self.states.get(item_id)

    def _stale(self, item_id: str, version: int | None) -> bool:
        current = self.states.get(item_id)
        return bool(
            current
            and current.doc_version is not None
            and version is not None
            and version < current.doc_version
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
        """INDEXED."""
        if self.fail_with:
            raise self.fail_with
        if self._stale(item_id, doc_version):
            return
        self.states[item_id] = IndexState(
            item_id=item_id,
            status=IndexStatus.INDEXED,
            content_hash=content_hash,
            meta_hash=meta_hash,
            doc_version=doc_version,
            chunk_count=chunk_count,
            chunker_version=chunker_version,
            embedding_model=embedding_model,
            indexed_at=self.clock(),
        )

    async def update_meta_hash(self, item_id: str, meta_hash: str) -> None:
        """Only the metadata hash changes."""
        if item_id in self.states:
            self.states[item_id] = self.states[item_id].model_copy(update={"meta_hash": meta_hash})

    async def mark_skipped(self, item_id: str, reason: str, doc_version: int | None) -> None:
        """SKIPPED."""
        if not self._stale(item_id, doc_version):
            self.states[item_id] = IndexState(
                item_id=item_id,
                status=IndexStatus.SKIPPED,
                doc_version=doc_version,
                reason=reason,
                indexed_at=self.clock(),
            )

    async def mark_deleted(self, item_id: str, doc_version: int | None) -> None:
        """DELETED."""
        if not self._stale(item_id, doc_version):
            self.states[item_id] = IndexState(
                item_id=item_id,
                status=IndexStatus.DELETED,
                doc_version=doc_version,
                indexed_at=self.clock(),
            )

    async def mark_failed(self, item_id: str, error: str) -> None:
        """FAILED, with a failure count."""
        base = self.states.get(item_id) or IndexState(item_id=item_id, status=IndexStatus.FAILED)
        self.states[item_id] = base.model_copy(
            update={
                "status": IndexStatus.FAILED,
                "attempts": base.attempts + 1,
                "last_error": error,
            }
        )
