"""The whole indexing path with faults: event, read, chunk, embed, index, state, commit.

Kafka, source, index and state are in-memory, but the consumer loop, the worker, the chunker, the
normalizer and the retry and DLQ paths are the real code.
"""

import asyncio
import json
import random
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from app.core.errors import NonRetryableError, UpstreamTimeoutError, UpstreamUnavailableError
from app.core.settings import (
    ChunkingSettings,
    ConsumerSettings,
    IngestionSettings,
    NormalizerSettings,
    StoreSettings,
)
from app.ingestion import dlq
from app.ingestion.chunker import Chunker
from app.ingestion.consumer import ConsumerLoop
from app.ingestion.normalizer import normalize_document
from app.ingestion.source import SourceDocument, SourcePage
from app.ingestion.state_store import IndexState, IndexStatus
from app.ingestion.tokens import WhitespaceTokenCounter
from app.ingestion.worker import IndexingWorker
from tests.fakes import FakeEmbedder, FakeSourceReader
from tests.fakes.kafka import FakeBroker, FakeConsumer, FakeProducer
from tests.fakes.pipeline import FakeIndexer, FakeStateStore

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
LIVE, RETRY, DLQ = "events", "retry", "dlq"


def _text(start: int, sentences: int = 12) -> str:
    out = []
    for n in range(start, start + sentences):
        words = [f"w{n * 5 + i}" for i in range(5)]
        words[0] = words[0].capitalize()
        out.append(" ".join(words) + ".")
    return " ".join(out)


def _doc(item_id: str, start: int = 0, version: int = 1, acl: str = "u1") -> SourceDocument:
    return SourceDocument(
        item_id=item_id,
        pages=[
            SourcePage(page_no=1, text=_text(start)),
            SourcePage(page_no=2, text=_text(start + 50)),
        ],
        doc_type="contract",
        acl_users=[acl],
        acl_groups=["g1"],
        version=version,
    )


class Pipeline:
    """Kafka loop + worker with in-memory services. ``start()`` can be called again after a stop
    or a crash, with the same broker, source, index and state: that is a restart."""

    def __init__(self, documents: list[SourceDocument] | None = None) -> None:
        self.broker = FakeBroker()
        for topic in (LIVE, RETRY, DLQ):
            self.broker.create_topic(topic)
        self.source = FakeSourceReader(documents or [])
        self.indexer = FakeIndexer()
        self.states = FakeStateStore()
        self.states.clock = lambda: NOW
        self.embedder = FakeEmbedder(dims=4, model_name="bge-m3@1")
        self.loop: ConsumerLoop | None = None
        self.task: asyncio.Task[None] | None = None
        self.consumer: FakeConsumer | None = None

    def _worker(self) -> IndexingWorker:
        chunking = ChunkingSettings(
            version="v1",
            target_tokens=20,
            max_tokens=30,
            overlap_tokens=6,
            min_tokens=5,
            tokenizer="whitespace",
        )
        return IndexingWorker(
            source=self.source,
            indexer=self.indexer,
            states=self.states,
            embedder=self.embedder,
            chunker=Chunker(chunking, WhitespaceTokenCounter()),
            chunking=chunking,
            normalizer=NormalizerSettings(),
            ingestion=IngestionSettings(window_size=3),
            store=StoreSettings(refresh_interval="30s"),
            clock=lambda: NOW,
        )

    def send(self, event_type: str, item_id: str, version: int | None = None) -> None:
        self.broker.produce(
            LIVE,
            item_id.encode(),
            json.dumps(
                {
                    "schema_version": 1,
                    "event_id": f"e-{self.broker.logs[(LIVE, 0)].__len__()}",
                    "event_type": event_type,
                    "item_id": item_id,
                    "doc_version": version,
                    "occurred_at": NOW.isoformat(),
                    "source": "test",
                }
            ).encode(),
        )

    def start(self, *, shutdown_timeout_s: float = 5.0) -> None:
        self.consumer = FakeConsumer(self.broker, "group")
        self.loop = ConsumerLoop(
            consumer=self.consumer,
            producer=FakeProducer(self.broker),
            handler=self._worker().handle,
            topics=[LIVE, RETRY],
            retry_topic=RETRY,
            dlq_topic=DLQ,
            settings=ConsumerSettings(
                poll_timeout_s=0.01,
                commit_interval_s=0.001,
                quick_retries=2,
                quick_retry_initial_delay_s=0,
                retry_delay_s=0,
                max_retries=3,
                shutdown_timeout_s=shutdown_timeout_s,
                max_in_flight=4,
            ),
            clock=lambda: 10**12,  # retry messages are always due
        )
        self.task = asyncio.create_task(self.loop.run())

    def all_committed(self) -> bool:
        for topic in (LIVE, RETRY):
            for tp in self.broker.partitions_of(topic):
                if self.broker.committed.get(("group", tp), 0) < len(self.broker.logs[tp]):
                    return False
        return True

    async def settle(self, seconds: float = 8.0) -> None:
        """Wait until every message of every topic is committed, then stop the loop."""
        deadline = asyncio.get_running_loop().time() + seconds
        stable = 0
        while stable < 5:
            assert asyncio.get_running_loop().time() < deadline, "the pipeline did not settle"
            stable = stable + 1 if self.all_committed() else 0
            await asyncio.sleep(0.01)
        await self.stop()

    async def stop(self) -> None:
        assert self.loop is not None
        assert self.task is not None
        self.loop.stop()
        await asyncio.wait_for(self.task, timeout=10)

    async def run(self, seconds: float = 8.0) -> None:
        self.start()
        await self.settle(seconds)

    def chunk_ids(self, item_id: str) -> list[str]:
        return [c["chunk_id"] for c in self.indexer.doc_chunks(item_id)]


async def _until(condition: Callable[[], bool], seconds: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + seconds
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.01)


def _expected(document: SourceDocument) -> list[str]:
    """What a clean first-time index of this document gives."""
    chunking = ChunkingSettings(
        version="v1",
        target_tokens=20,
        max_tokens=30,
        overlap_tokens=6,
        min_tokens=5,
        tokenizer="whitespace",
    )
    chunker = Chunker(chunking, WhitespaceTokenCounter())
    return [c.chunk_id for c in chunker.split(normalize_document(document, NormalizerSettings()))]


# --- the plain path -------------------------------------------------------------------------


async def test_event_read_chunk_embed_index_state_commit() -> None:
    pipeline = Pipeline([_doc("ITEM-1"), _doc("ITEM-2", start=300)])
    pipeline.send("UPSERT", "ITEM-1", 1)
    pipeline.send("UPSERT", "ITEM-2", 1)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))
    assert pipeline.chunk_ids("ITEM-2") == _expected(_doc("ITEM-2", start=300))
    assert {s.status for s in pipeline.states.states.values()} == {IndexStatus.INDEXED}
    assert pipeline.all_committed()
    assert pipeline.broker.messages(DLQ) == []
    assert pipeline.broker.messages(RETRY) == []


# --- Elasticsearch bulk errors --------------------------------------------------------------


async def test_a_bulk_error_is_retried_and_the_document_ends_complete_without_duplicates() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.indexer.fail_bulk_with = UpstreamUnavailableError()
    pipeline.indexer.fail_bulk_calls = (
        3  # fails the first quick retries, then goes through the retry topic
    )
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))
    assert len(pipeline.broker.messages(RETRY)) == 1  # one trip through the retry topic
    assert pipeline.states.states["ITEM-1"].status is IndexStatus.INDEXED
    assert pipeline.states.states["ITEM-1"].attempts == 0
    assert pipeline.broker.messages(DLQ) == []


async def test_a_bulk_failure_halfway_leaves_the_old_chunks_until_the_retry_finishes() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    old = set(pipeline.chunk_ids("ITEM-1"))

    pipeline.source.documents["ITEM-1"] = _doc("ITEM-1", start=700, version=2)
    pipeline.indexer.fail_bulk_with = UpstreamUnavailableError()
    pipeline.indexer.fail_bulk_after_windows = 1 + len(
        [c for c in pipeline.indexer.calls if c[0] == "bulk_write"]
    )
    pipeline.indexer.fail_bulk_calls = 1
    pipeline.send("UPSERT", "ITEM-1", 2)
    await pipeline.run()
    ids = pipeline.chunk_ids("ITEM-1")
    assert ids == _expected(_doc("ITEM-1", start=700, version=2))
    assert not set(ids) & old  # no old chunk is left over


async def test_a_permanent_index_error_goes_to_the_dlq_and_does_not_stop_other_documents() -> None:
    pipeline = Pipeline([_doc("BAD"), _doc("GOOD", start=400)])
    original = pipeline.indexer.bulk_write

    async def reject_bad(document: SourceDocument, chunks, vectors, model):  # type: ignore[no-untyped-def]
        if document.item_id == "BAD":
            raise NonRetryableError("Elasticsearch rejected chunks")
        await original(document, chunks, vectors, model)

    pipeline.indexer.bulk_write = reject_bad  # type: ignore[method-assign,assignment]
    pipeline.send("UPSERT", "BAD", 1)
    pipeline.send("UPSERT", "GOOD", 1)
    await pipeline.run()
    assert pipeline.chunk_ids("GOOD") == _expected(_doc("GOOD", start=400))
    assert pipeline.chunk_ids("BAD") == []
    [dead] = pipeline.broker.messages(DLQ)
    assert dead.key == b"BAD"
    assert dead.headers[dlq.REASON] == b"non-retryable error"
    assert pipeline.states.states["BAD"].status is IndexStatus.FAILED
    assert pipeline.all_committed()


# --- embedding timeouts ---------------------------------------------------------------------


async def test_embedding_timeouts_recover_through_quick_retries() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.embedder.fail_with = UpstreamTimeoutError()
    pipeline.embedder.fail_calls = 1
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))
    assert pipeline.broker.messages(RETRY) == []  # the quick retry was enough


async def test_a_long_embedding_outage_goes_through_the_retry_topic_and_recovers() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.embedder.fail_with = UpstreamTimeoutError()
    pipeline.embedder.fail_calls = 4  # more than the 2 quick attempts: the retry topic is needed
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    assert pipeline.states.states["ITEM-1"].status is IndexStatus.INDEXED
    assert len(pipeline.broker.messages(RETRY)) >= 1
    assert pipeline.broker.messages(DLQ) == []


async def test_an_outage_that_never_ends_fills_the_dlq_after_the_retry_limit() -> None:
    pipeline = Pipeline([_doc("ITEM-1"), _doc("ITEM-2", start=400)])
    pipeline.embedder.fail_with = UpstreamTimeoutError()
    pipeline.send("UPSERT", "ITEM-1", 1)
    pipeline.send("UPSERT", "ITEM-2", 1)
    await pipeline.run()
    assert sorted(m.key or b"" for m in pipeline.broker.messages(DLQ)) == [b"ITEM-1", b"ITEM-2"]
    assert all(m.headers[dlq.REASON] == b"retries exhausted" for m in pipeline.broker.messages(DLQ))
    assert pipeline.indexer.chunks == {}
    assert {s.status for s in pipeline.states.states.values()} == {IndexStatus.FAILED}
    assert pipeline.all_committed()


# --- crash and restart ----------------------------------------------------------------------


async def test_a_crash_in_the_middle_of_a_document_loses_nothing_after_the_restart() -> None:
    pipeline = Pipeline([_doc("ITEM-1"), _doc("ITEM-2", start=400)])
    pipeline.embedder.block = asyncio.Event()  # the worker hangs inside the first embedding call
    pipeline.send("UPSERT", "ITEM-1", 1)
    pipeline.send("UPSERT", "ITEM-2", 1)
    pipeline.start(shutdown_timeout_s=0.05)  # a stuck handler is cancelled at shutdown: a crash
    await _until(lambda: pipeline.embedder.entered >= 1)
    await pipeline.stop()
    assert pipeline.indexer.chunks == {}  # nothing was written
    assert not pipeline.all_committed()  # and nothing was committed

    pipeline.embedder.block = None  # the restarted worker works
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))
    assert pipeline.chunk_ids("ITEM-2") == _expected(_doc("ITEM-2", start=400))
    assert pipeline.all_committed()


async def test_a_crash_after_part_of_the_chunks_were_written_is_repaired_by_the_restart() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    old = set(pipeline.chunk_ids("ITEM-1"))

    pipeline.source.documents["ITEM-1"] = _doc("ITEM-1", start=700, version=2)
    block = asyncio.Event()
    calls = 0
    original = pipeline.embedder.embed_documents

    async def hang_on_second_window(texts: list[str]) -> list[list[float]]:
        nonlocal calls
        calls += 1
        if calls == 2:
            await block.wait()  # the first window is written, then the process "dies"
        return await original(texts)

    pipeline.embedder.embed_documents = hang_on_second_window  # type: ignore[method-assign]
    pipeline.send("UPSERT", "ITEM-1", 2)
    pipeline.start(shutdown_timeout_s=0.05)
    await _until(lambda: calls >= 2)
    await pipeline.stop()
    partial = set(pipeline.chunk_ids("ITEM-1"))
    assert old < partial  # old chunks are still there, plus the first new window
    assert pipeline.states.states["ITEM-1"].content_hash != ""  # state still describes version 1

    pipeline.embedder.embed_documents = original  # type: ignore[method-assign]
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1", start=700, version=2))


async def test_committed_events_are_not_processed_again_after_a_restart() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    embeds = len(pipeline.embedder.document_calls)
    await pipeline.run()  # restart with nothing new
    assert len(pipeline.embedder.document_calls) == embeds


# --- duplicate and out-of-order events ------------------------------------------------------


async def test_many_duplicates_give_the_same_result_as_one_event() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    for _ in range(12):
        pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))
    assert len(pipeline.indexer.chunks) == len(pipeline.chunk_ids("ITEM-1"))
    assert len(pipeline.embedder.document_calls) <= 12 * 4  # unchanged repeats do not embed again


async def test_a_duplicate_delivered_long_after_the_first_is_harmless() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    embeds = len(pipeline.embedder.document_calls)
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    assert len(pipeline.embedder.document_calls) == embeds
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))


async def test_a_late_delete_of_an_old_version_does_not_remove_the_newer_document() -> None:
    pipeline = Pipeline([_doc("ITEM-1", version=4)])
    pipeline.send("UPSERT", "ITEM-1", 4)
    pipeline.send("DELETE", "ITEM-1", 3)  # out of order: older than what is indexed
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1", version=4))
    assert pipeline.states.states["ITEM-1"].status is IndexStatus.INDEXED


async def test_an_old_upsert_after_a_newer_delete_does_not_bring_the_document_back() -> None:
    pipeline = Pipeline([])  # the document is gone from the source
    pipeline.states.states["ITEM-1"] = IndexState(
        item_id="ITEM-1", status=IndexStatus.INDEXED, doc_version=2, content_hash="sha256:x"
    )
    pipeline.send("DELETE", "ITEM-1", 3)
    pipeline.send("UPSERT", "ITEM-1", 2)  # arrives after the delete, but the document is gone
    await pipeline.run(seconds=15)
    assert pipeline.chunk_ids("ITEM-1") == []
    assert pipeline.states.states["ITEM-1"].status is IndexStatus.DELETED
    assert pipeline.all_committed()


# --- delete after update --------------------------------------------------------------------


async def test_a_delete_right_after_an_update_leaves_nothing_behind() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    pipeline.source.documents["ITEM-1"] = _doc("ITEM-1", start=700, version=2)
    pipeline.send("UPSERT", "ITEM-1", 2)
    del pipeline.source.documents["ITEM-1"]
    pipeline.send("DELETE", "ITEM-1", 3)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == []
    assert pipeline.states.states["ITEM-1"].status is IndexStatus.DELETED


async def test_the_same_update_and_delete_events_in_one_batch_are_coalesced() -> None:
    pipeline = Pipeline([])
    pipeline.send("UPSERT", "ITEM-1", 1)
    pipeline.send("DELETE", "ITEM-1", 2)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == []
    assert pipeline.embedder.document_calls == []  # the update was never worked on
    assert pipeline.states.states["ITEM-1"].status is IndexStatus.DELETED


async def test_a_document_deleted_and_created_again_is_indexed_again() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    del pipeline.source.documents["ITEM-1"]
    pipeline.send("DELETE", "ITEM-1", 2)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == []
    pipeline.source.documents["ITEM-1"] = _doc("ITEM-1", start=900, version=3)
    pipeline.send("UPSERT", "ITEM-1", 3)
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1", start=900, version=3))


# --- permission changes through the pipeline ------------------------------------------------


async def test_a_permission_change_event_updates_permissions_only() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.send("UPSERT", "ITEM-1", 1)
    await pipeline.run()
    embeds = len(pipeline.embedder.document_calls)
    pipeline.source.documents["ITEM-1"] = _doc("ITEM-1", version=2, acl="u2")
    pipeline.send("ACL_CHANGE", "ITEM-1", 2)
    await pipeline.run()
    assert len(pipeline.embedder.document_calls) == embeds
    assert {tuple(c["acl_users"]) for c in pipeline.indexer.doc_chunks("ITEM-1")} == {("u2",)}


# --- bad messages ---------------------------------------------------------------------------


async def test_poison_messages_are_set_aside_and_the_rest_continues() -> None:
    pipeline = Pipeline([_doc("ITEM-1")])
    pipeline.broker.produce(LIVE, b"ITEM-X", b"not json at all")
    pipeline.send("UPSERT", "ITEM-1", 1)
    pipeline.broker.produce(LIVE, b"ITEM-Y", b'{"schema_version": 7}')
    await pipeline.run()
    assert pipeline.chunk_ids("ITEM-1") == _expected(_doc("ITEM-1"))
    assert len(pipeline.broker.messages(DLQ)) == 2
    assert pipeline.all_committed()


@pytest.mark.parametrize("seed", range(3))
async def test_busy_mixed_traffic_ends_consistent(seed: int) -> None:
    """Many documents, duplicates and deletes together, with a flaky embedder."""
    rng = random.Random(seed)  # noqa: S311 (a test scenario, not security)
    documents = {f"D-{i:02d}": _doc(f"D-{i:02d}", start=i * 100) for i in range(12)}
    pipeline = Pipeline(list(documents.values()))
    pipeline.embedder.fail_with = UpstreamTimeoutError()
    pipeline.embedder.fail_calls = 5
    gone: set[str] = set()
    touched: set[str] = set()
    for _ in range(40):
        item = rng.choice(sorted(documents))
        touched.add(item)
        if rng.random() < 0.2 and item not in gone:
            del pipeline.source.documents[item]
            gone.add(item)
            pipeline.send("DELETE", item, 9)
        else:
            pipeline.send("UPSERT", item, 1)
    await pipeline.run(seconds=20)
    for item, document in documents.items():
        expected = [] if item in gone or item not in touched else _expected(document)
        assert pipeline.chunk_ids(item) == expected
    assert pipeline.all_committed()
