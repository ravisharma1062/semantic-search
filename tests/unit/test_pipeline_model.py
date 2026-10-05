"""A model-based test of the worker: any history of edits, permission changes, deletes,
re-creations, duplicates and late events must leave the index exactly as a fresh index of the
current document would be.

The "model" is simple: the source document as it is now. After every event the index and the
state must agree with it. This finds order and idempotency bugs that example tests miss.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from app.core.settings import (
    ChunkingSettings,
    IngestionSettings,
    NormalizerSettings,
    StoreSettings,
)
from app.ingestion.chunker import Chunker
from app.ingestion.consumer import HandlerContext
from app.ingestion.events import IndexEvent, parse_event
from app.ingestion.normalizer import normalize_document
from app.ingestion.source import SourceDocument, SourcePage
from app.ingestion.state_store import IndexStatus
from app.ingestion.tokens import WhitespaceTokenCounter
from app.ingestion.worker import IndexingWorker
from tests.fakes import FakeEmbedder, FakeSourceReader
from tests.fakes.pipeline import FakeIndexer, FakeStateStore

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
CONTEXT = HandlerContext(retry_count=0, max_retries=3)
CHUNKING = ChunkingSettings(
    version="v1",
    target_tokens=20,
    max_tokens=30,
    overlap_tokens=6,
    min_tokens=5,
    tokenizer="whitespace",
)


def _text(variant: int) -> str:
    """Text variant 0 to 3: different lengths and different words."""
    sentences = [3, 9, 15, 6][variant]
    out = []
    for n in range(sentences):
        words = [f"v{variant}w{n * 5 + i}" for i in range(5)]
        words[0] = words[0].capitalize()
        out.append(" ".join(words) + ".")
    return " ".join(out)


def _document(variant: int | None, acl: int, version: int) -> SourceDocument:
    pages = [] if variant is None else [SourcePage(page_no=1, text=_text(variant))]
    return SourceDocument(
        item_id="ITEM-1",
        pages=pages,
        doc_type="contract",
        acl_users=[f"user-{acl}"],
        acl_groups=["g1"],
        version=version,
    )


def _event(event_type: str, version: int) -> IndexEvent:
    return parse_event(
        json.dumps(
            {
                "schema_version": 1,
                "event_id": "e",
                "event_type": event_type,
                "item_id": "ITEM-1",
                "doc_version": version,
                "occurred_at": NOW.isoformat(),
                "source": "test",
            }
        ).encode()
    )


@dataclass
class World:
    """The source document now (or ``None``), and the worker with its fakes."""

    source: FakeSourceReader
    indexer: FakeIndexer
    states: FakeStateStore
    worker: IndexingWorker
    embedder: FakeEmbedder

    @classmethod
    def new(cls) -> "World":
        source = FakeSourceReader([])
        indexer = FakeIndexer()
        states = FakeStateStore()
        states.clock = lambda: NOW
        embedder = FakeEmbedder(dims=4, model_name="bge-m3@1")
        worker = IndexingWorker(
            source=source,
            indexer=indexer,
            states=states,
            embedder=embedder,
            chunker=Chunker(CHUNKING, WhitespaceTokenCounter()),
            chunking=CHUNKING,
            normalizer=NormalizerSettings(),
            ingestion=IngestionSettings(window_size=2),
            store=StoreSettings(refresh_interval="30s"),
            clock=lambda: NOW,
        )
        return cls(source, indexer, states, worker, embedder)

    async def deliver(self, event: IndexEvent) -> None:
        await self.worker.process(event, CONTEXT)

    def check_consistent(self, current: SourceDocument | None) -> None:
        """The index and the state describe exactly the current document."""
        chunks = self.indexer.doc_chunks("ITEM-1")
        state = self.states.states.get("ITEM-1")
        if current is None:
            assert chunks == []
            assert state is not None
            assert state.status is IndexStatus.DELETED
            return
        normalized = normalize_document(current, NormalizerSettings())
        if not normalized.has_text:
            assert chunks == []
            assert state is not None
            assert state.status is IndexStatus.SKIPPED
            return
        fresh = Chunker(CHUNKING, WhitespaceTokenCounter()).split(normalized)
        assert [c["chunk_id"] for c in chunks] == [c.chunk_id for c in fresh]
        assert [c["content"] for c in chunks] == [c.content for c in fresh]
        assert {tuple(c["acl_users"]) for c in chunks} == {tuple(current.acl_users)}
        assert state is not None
        assert state.status is IndexStatus.INDEXED
        assert state.chunk_count == len(fresh)
        assert len(self.indexer.chunks) == len(fresh)  # nothing else is left in the index


# One step of the history. Every step changes the source, then delivers the matching event.
_steps = st.one_of(
    st.tuples(st.just("text"), st.integers(0, 3)),
    st.tuples(st.just("acl"), st.integers(0, 2)),
    st.tuples(st.just("empty"), st.just(0)),
    st.tuples(st.just("delete"), st.just(0)),
    st.tuples(st.just("recreate"), st.integers(0, 3)),
    st.tuples(st.just("duplicate"), st.just(0)),
    st.tuples(st.just("late_delete"), st.just(0)),
    st.tuples(st.just("late_upsert"), st.just(0)),
)


async def _play(history: list[tuple[str, int]]) -> None:
    world = World.new()
    variant: int | None = None
    acl = 0
    version = 0
    present = False
    last_event: IndexEvent | None = None
    current: SourceDocument | None = None

    def put() -> None:
        nonlocal current
        current = _document(variant, acl, version)
        world.source.documents["ITEM-1"] = current

    for kind, value in history:
        event: IndexEvent | None = None
        if kind == "text" and present:
            variant, version = value, version + 1
            put()
            event = _event("UPSERT", version)
        elif kind == "acl" and present:
            acl, version = value, version + 1
            put()
            event = _event("ACL_CHANGE", version)
        elif kind == "empty" and present:
            variant, version = None, version + 1
            put()
            event = _event("UPSERT", version)
        elif kind == "delete" and present:
            version += 1
            present = False
            current = None
            world.source.documents.pop("ITEM-1", None)
            event = _event("DELETE", version)
        elif kind == "recreate" and not present:
            variant, version, present = value, version + 1, True
            put()
            event = _event("UPSERT", version)
        elif kind == "duplicate" and last_event is not None:
            event = last_event
        elif kind == "late_delete" and version > 1:
            event = _event("DELETE", version - 1)  # an old delete arriving late
        elif kind == "late_upsert" and version > 1:
            event = _event("UPSERT", version - 1)  # an old update arriving late
        if event is None:
            continue
        if event.event_type.value == "UPSERT" and not present:
            continue  # an old update of a deleted document: the real worker retries it, tested elsewhere
        last_event = event
        await world.deliver(event)
        if present or any(
            kind_ == "delete" for kind_, _ in history
        ):  # a state exists once anything was processed
            _check_after_event(world, event, current, present, version)

    if world.states.states.get("ITEM-1") is not None:
        world.check_consistent(current if present else None)


def _check_after_event(
    world: World, event: IndexEvent, current: SourceDocument | None, present: bool, version: int
) -> None:
    """After every event the index must already match the current document (or the delete)."""
    state = world.states.states.get("ITEM-1")
    if state is None:
        return
    if event.event_type.value == "DELETE" and not present and (event.doc_version or 0) == version:
        assert world.indexer.doc_chunks("ITEM-1") == []
    if present and current is not None and event.doc_version == version:
        world.check_consistent(current)


@given(st.lists(_steps, min_size=1, max_size=10))
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_any_history_leaves_the_index_equal_to_a_fresh_index_of_the_current_document(
    history: list[tuple[str, int]],
) -> None:
    asyncio.run(_play([("recreate", 1), *history]))


def test_the_model_test_really_checks_something() -> None:
    """A deliberately broken worker (it never deletes stale chunks) must be caught."""

    async def broken() -> None:
        world = World.new()

        async def keep_stale(item_id: str, keep: Any, *, refresh_first: bool = False) -> int:
            return 0

        world.indexer.delete_stale_chunks = keep_stale  # type: ignore[method-assign]
        world.source.documents["ITEM-1"] = _document(2, 0, 1)
        await world.deliver(_event("UPSERT", 1))
        world.source.documents["ITEM-1"] = _document(1, 0, 2)
        await world.deliver(_event("UPSERT", 2))
        world.check_consistent(_document(1, 0, 2))

    try:
        asyncio.run(broken())
    except AssertionError:
        return
    raise AssertionError("the consistency check did not notice stale chunks")
