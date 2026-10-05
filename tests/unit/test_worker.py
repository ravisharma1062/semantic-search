"""The worker loop of HLD section 14 with in-memory fakes, end to end."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
import structlog

from app.core.errors import (
    NonRetryableError,
    SourceNotReadyError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.settings import (
    ChunkingSettings,
    ConsumerSettings,
    IngestionSettings,
    NormalizerSettings,
    StoreSettings,
)
from app.ingestion.chunker import Chunker
from app.ingestion.consumer import ConsumerLoop, HandlerContext
from app.ingestion.events import IndexEvent, parse_event
from app.ingestion.source import SourceDocument, SourcePage
from app.ingestion.state_store import IndexStatus
from app.ingestion.tokens import WhitespaceTokenCounter
from app.ingestion.worker import IndexingWorker, Outcome, meta_hash, refresh_window_s, text_hash
from tests.fakes import FakeEmbedder, FakeSourceReader
from tests.fakes.kafka import FakeBroker, FakeConsumer, FakeProducer
from tests.fakes.pipeline import FakeIndexer, FakeStateStore

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
FIRST_TRY = HandlerContext(retry_count=0, max_retries=3)
LAST_TRY = HandlerContext(retry_count=3, max_retries=3)


def _sentences(count: int, start: int = 0) -> str:
    out = []
    for s in range(start, start + count):
        words = [f"w{s * 5 + i}" for i in range(5)]
        words[0] = words[0].capitalize()
        out.append(" ".join(words) + ".")
    return " ".join(out)


def _doc(
    item_id: str = "ITEM-1",
    *,
    pages: int = 3,
    sentences_per_page: int = 6,
    version: int | None = 1,
    acl_users: list[str] | None = None,
    start: int = 0,
) -> SourceDocument:
    return SourceDocument(
        item_id=item_id,
        pages=[
            SourcePage(page_no=p, text=_sentences(sentences_per_page, start + p * 100))
            for p in range(1, pages + 1)
        ],
        doc_type="contract",
        tags=["vendor"],
        acl_users=acl_users if acl_users is not None else ["u1"],
        acl_groups=["g1"],
        version=version,
    )


def _event(
    event_type: str = "UPSERT", item_id: str = "ITEM-1", version: int | None = None
) -> IndexEvent:
    return parse_event(
        json.dumps(
            {
                "schema_version": 1,
                "event_id": f"evt-{event_type}-{item_id}",
                "event_type": event_type,
                "item_id": item_id,
                "doc_version": version,
                "occurred_at": NOW.isoformat(),
                "source": "test",
            }
        ).encode()
    )


class Rig:
    """The worker with all its fakes."""

    def __init__(
        self,
        documents: list[SourceDocument] | None = None,
        *,
        window: int = 100,
        version: str = "v1",
    ) -> None:
        self.source = FakeSourceReader(documents or [])
        self.indexer = FakeIndexer()
        self.states = FakeStateStore()
        self.states.clock = lambda: NOW
        self.embedder = FakeEmbedder(dims=4, model_name="bge-m3@1")
        self.now = NOW
        self.window = window
        self.version = version
        self.worker = self._build()

    def _build(self) -> IndexingWorker:
        chunking = ChunkingSettings(
            version=self.version,
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
            ingestion=IngestionSettings(window_size=self.window),
            store=StoreSettings(refresh_interval="30s"),
            clock=lambda: self.now,
        )

    def rebuild(self, *, version: str | None = None, model: str | None = None) -> None:
        if version:
            self.version = version
        if model:
            self.embedder.model_name = model
        self.worker = self._build()

    async def run(
        self, event: IndexEvent | None = None, context: HandlerContext = FIRST_TRY
    ) -> Outcome:
        return await self.worker.process(event or _event(), context)

    def chunk_ids(self, item_id: str = "ITEM-1") -> set[str]:
        return {c["chunk_id"] for c in self.indexer.doc_chunks(item_id)}


# --- indexing -------------------------------------------------------------------------------


async def test_a_new_document_is_chunked_embedded_written_and_recorded() -> None:
    rig = Rig([_doc()])
    assert await rig.run() is Outcome.INDEXED
    chunks = rig.indexer.doc_chunks("ITEM-1")
    assert len(chunks) > 3
    assert [c["chunk_no"] for c in chunks] == list(range(len(chunks)))
    assert all(c["embedding_model"] == "bge-m3@1" for c in chunks)
    assert all(c["acl_users"] == ["u1"] and c["acl_groups"] == ["g1"] for c in chunks)
    state = rig.states.states["ITEM-1"]
    assert state.status is IndexStatus.INDEXED
    assert state.chunk_count == len(chunks)
    assert state.embedding_model == "bge-m3@1"
    assert state.chunker_version == "v1"
    assert state.doc_version == 1
    assert state.content_hash is not None
    assert state.content_hash.startswith("sha256:")
    assert state.meta_hash is not None
    assert [name for name, _ in rig.indexer.calls][-1] == "delete_stale"


async def test_texts_sent_to_the_embedder_are_the_chunks_embedding_text() -> None:
    rig = Rig([_doc()])
    await rig.run()
    sent = [text for call in rig.embedder.document_calls for text in call]
    assert sent == [c["content"] for c in rig.indexer.doc_chunks("ITEM-1")]


async def test_chunks_are_embedded_and_written_in_windows() -> None:
    rig = Rig([_doc()], window=3)
    await rig.run()
    total = len(rig.indexer.doc_chunks("ITEM-1"))
    assert [len(c) for c in rig.embedder.document_calls] == [3] * (total // 3) + (
        [total % 3] if total % 3 else []
    )
    assert len([c for c in rig.indexer.calls if c[0] == "bulk_write"]) == len(
        rig.embedder.document_calls
    )


async def test_old_chunks_are_deleted_only_after_the_new_ones_are_written() -> None:
    rig = Rig([_doc()])
    await rig.run()
    first = rig.chunk_ids()
    rig.source.documents["ITEM-1"] = _doc(start=5000)  # a different text
    await rig.run(_event(version=2))
    assert rig.chunk_ids()
    assert not rig.chunk_ids() & first  # all old chunks are gone, nothing is left over
    order = [name for name, _ in rig.indexer.calls]
    assert order.index("delete_stale", order.index("bulk_write", 1)) > order.index("bulk_write", 1)


async def test_a_shorter_document_loses_its_extra_chunks() -> None:
    rig = Rig([_doc(pages=3)])
    await rig.run()
    long_count = len(rig.indexer.doc_chunks("ITEM-1"))
    rig.source.documents["ITEM-1"] = _doc(pages=1)
    await rig.run()
    assert 0 < len(rig.indexer.doc_chunks("ITEM-1")) < long_count
    assert rig.states.states["ITEM-1"].chunk_count == len(rig.indexer.doc_chunks("ITEM-1"))


# --- unchanged documents and permission changes ---------------------------------------------


async def test_a_second_event_for_an_unchanged_document_does_nothing() -> None:
    rig = Rig([_doc()])
    await rig.run()
    embeds = len(rig.embedder.document_calls)
    writes = len(rig.indexer.calls)
    assert await rig.run() is Outcome.UNCHANGED
    assert len(rig.embedder.document_calls) == embeds
    assert len(rig.indexer.calls) == writes


async def test_a_duplicate_event_is_harmless() -> None:
    rig = Rig([_doc()])
    await rig.run()
    before = rig.chunk_ids()
    await rig.run()
    assert rig.chunk_ids() == before


async def test_a_permission_change_updates_permissions_without_embedding_again() -> None:
    rig = Rig([_doc()])
    await rig.run()
    embeds = len(rig.embedder.document_calls)
    rig.source.documents["ITEM-1"] = _doc(acl_users=["u1", "u2"])
    assert await rig.run(_event("ACL_CHANGE")) is Outcome.METADATA_REFRESHED
    assert len(rig.embedder.document_calls) == embeds  # no new vectors
    assert all(c["acl_users"] == ["u1", "u2"] for c in rig.indexer.doc_chunks("ITEM-1"))
    assert rig.states.states["ITEM-1"].meta_hash == meta_hash(_doc(acl_users=["u1", "u2"]))
    assert await rig.run(_event("ACL_CHANGE")) is Outcome.UNCHANGED  # and now it is current


async def test_a_permission_change_on_an_upsert_event_is_also_applied() -> None:
    rig = Rig([_doc()])
    await rig.run()
    rig.source.documents["ITEM-1"] = _doc(acl_users=[])
    assert await rig.run(_event("UPSERT")) is Outcome.METADATA_REFRESHED
    assert all(c["acl_users"] == [] for c in rig.indexer.doc_chunks("ITEM-1"))


async def test_a_permission_event_for_an_unknown_document_indexes_it() -> None:
    rig = Rig([_doc()])
    assert await rig.run(_event("ACL_CHANGE")) is Outcome.INDEXED


async def test_a_new_model_or_chunker_version_reindexes_an_unchanged_document() -> None:
    rig = Rig([_doc()])
    await rig.run()
    rig.rebuild(model="bge-m3@2")
    assert await rig.run() is Outcome.INDEXED
    assert rig.states.states["ITEM-1"].embedding_model == "bge-m3@2"
    rig.rebuild(version="v2")
    assert await rig.run() is Outcome.INDEXED
    assert rig.states.states["ITEM-1"].chunker_version == "v2"
    assert await rig.run() is Outcome.UNCHANGED


async def test_a_failed_or_skipped_record_is_processed_again() -> None:
    rig = Rig([_doc()])
    await rig.run()
    await rig.states.mark_failed("ITEM-1", "RuntimeError")
    assert await rig.run() is Outcome.INDEXED


# --- documents without text -----------------------------------------------------------------


async def test_a_document_without_text_is_skipped_and_its_old_chunks_are_removed() -> None:
    rig = Rig([_doc()])
    await rig.run()
    rig.source.documents["ITEM-1"] = SourceDocument(
        item_id="ITEM-1", pages=[SourcePage(page_no=1, text="  ")]
    )
    assert await rig.run() is Outcome.SKIPPED
    assert rig.indexer.doc_chunks("ITEM-1") == []
    state = rig.states.states["ITEM-1"]
    assert (state.status, state.reason) == (IndexStatus.SKIPPED, "no usable text")


async def test_a_document_with_only_symbols_is_skipped() -> None:
    rig = Rig([SourceDocument(item_id="ITEM-1", text="-----")])
    assert await rig.run() is Outcome.SKIPPED
    assert rig.states.states["ITEM-1"].reason == "no chunks"


# --- deletes --------------------------------------------------------------------------------


async def test_a_delete_event_removes_the_chunks_and_records_it() -> None:
    rig = Rig([_doc()])
    await rig.run()
    assert await rig.run(_event("DELETE")) is Outcome.DELETED
    assert rig.indexer.doc_chunks("ITEM-1") == []
    assert rig.states.states["ITEM-1"].status is IndexStatus.DELETED


async def test_a_delete_for_an_unknown_document_is_fine() -> None:
    rig = Rig()
    assert await rig.run(_event("DELETE", "ITEM-X")) is Outcome.DELETED
    assert rig.states.states["ITEM-X"].status is IndexStatus.DELETED


async def test_a_document_that_comes_back_after_a_delete_is_indexed_again() -> None:
    rig = Rig([_doc()])
    await rig.run()
    await rig.run(_event("DELETE", version=2))
    rig.source.documents["ITEM-1"] = _doc(version=3)
    assert await rig.run(_event(version=3)) is Outcome.INDEXED
    assert rig.states.states["ITEM-1"].status is IndexStatus.INDEXED


async def test_a_late_delete_of_an_old_version_does_not_remove_the_new_document() -> None:
    rig = Rig([_doc(version=5)])
    await rig.run(_event(version=5))
    assert await rig.run(_event("DELETE", version=3)) is Outcome.STALE_EVENT
    assert rig.indexer.doc_chunks("ITEM-1")
    assert rig.states.states["ITEM-1"].status is IndexStatus.INDEXED


async def test_a_late_delete_is_ignored_when_the_source_has_a_newer_document_and_no_state() -> None:
    rig = Rig([_doc(version=5)])
    assert await rig.run(_event("DELETE", version=3)) is Outcome.STALE_EVENT
    assert "ITEM-1" not in rig.states.states  # nothing was recorded or deleted


async def test_a_delete_without_a_version_is_trusted() -> None:
    rig = Rig([_doc(version=5)])
    await rig.run(_event(version=5))
    assert await rig.run(_event("DELETE")) is Outcome.DELETED


async def test_a_delete_of_the_current_version_is_applied() -> None:
    rig = Rig([_doc(version=5)])
    await rig.run(_event(version=5))
    assert await rig.run(_event("DELETE", version=5)) is Outcome.DELETED


async def test_an_old_version_of_the_document_is_ignored() -> None:
    rig = Rig([_doc(version=5)])
    await rig.run(_event(version=5))
    rig.source.documents["ITEM-1"] = _doc(version=4, start=9000)  # an older read
    assert await rig.run(_event(version=4)) is Outcome.STALE_EVENT
    assert rig.states.states["ITEM-1"].doc_version == 5


# --- documents that are missing -------------------------------------------------------------


async def test_a_missing_document_is_tried_again_later() -> None:
    rig = Rig()
    with pytest.raises(SourceNotReadyError):
        await rig.worker.handle(_event(), FIRST_TRY)
    state = rig.states.states["ITEM-1"]
    assert (state.status, state.attempts) == (IndexStatus.FAILED, 1)


async def test_a_document_that_stays_missing_is_treated_as_deleted_after_the_retries() -> None:
    rig = Rig([_doc()])
    await rig.run()
    del rig.source.documents["ITEM-1"]
    assert await rig.run(context=LAST_TRY) is Outcome.DELETED
    assert rig.indexer.doc_chunks("ITEM-1") == []
    assert rig.states.states["ITEM-1"].status is IndexStatus.DELETED


# --- failures -------------------------------------------------------------------------------


async def test_an_embedding_failure_keeps_the_old_chunks_and_records_the_failure() -> None:
    rig = Rig([_doc()])
    await rig.run()
    before = rig.chunk_ids()
    rig.source.documents["ITEM-1"] = _doc(start=7000)
    rig.embedder.fail_with = UpstreamTimeoutError()
    with pytest.raises(UpstreamTimeoutError):
        await rig.worker.handle(_event(), FIRST_TRY)
    assert rig.chunk_ids() == before  # the document stays searchable
    state = rig.states.states["ITEM-1"]
    assert (state.status, state.attempts) == (IndexStatus.FAILED, 1)
    assert state.last_error == "UpstreamTimeoutError: Timeout"


async def test_a_write_failure_in_a_later_window_never_deletes_stale_chunks_or_marks_indexed() -> (
    None
):
    rig = Rig([_doc()], window=3)
    await rig.run()
    old = rig.chunk_ids()
    rig.source.documents["ITEM-1"] = _doc(start=7000)
    rig.indexer.fail_bulk_with = UpstreamUnavailableError()
    rig.indexer.fail_bulk_after_windows = 1 + len(
        [c for c in rig.indexer.calls if c[0] == "bulk_write"]
    )
    with pytest.raises(UpstreamUnavailableError):
        await rig.worker.handle(_event(), FIRST_TRY)
    assert old <= rig.chunk_ids()  # old chunks are all still there
    assert rig.states.states["ITEM-1"].status is IndexStatus.FAILED


async def test_the_retry_after_a_failure_ends_in_a_complete_and_clean_index() -> None:
    rig = Rig([_doc()], window=3)
    await rig.run()
    rig.source.documents["ITEM-1"] = _doc(start=7000)
    rig.embedder.fail_with = UpstreamTimeoutError()
    with pytest.raises(UpstreamTimeoutError):
        await rig.worker.handle(_event(), FIRST_TRY)
    rig.embedder.fail_with = None
    assert await rig.run() is Outcome.INDEXED
    fresh = Rig([_doc(start=7000)], window=3)
    await fresh.run()
    assert {c["content"] for c in rig.indexer.doc_chunks("ITEM-1")} == {
        c["content"] for c in fresh.indexer.doc_chunks("ITEM-1")
    }
    assert rig.states.states["ITEM-1"].attempts == 0


async def test_a_permanent_error_is_raised_as_it_is() -> None:
    rig = Rig([_doc()])
    rig.embedder.fail_with = NonRetryableError()
    with pytest.raises(NonRetryableError):
        await rig.worker.handle(_event(), FIRST_TRY)


async def test_a_broken_state_store_does_not_hide_the_real_error() -> None:
    rig = Rig([_doc()])
    rig.embedder.fail_with = UpstreamTimeoutError()

    async def broken(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("state down")

    rig.states.mark_failed = broken  # type: ignore[method-assign]
    with pytest.raises(UpstreamTimeoutError):
        await rig.worker.handle(_event(), FIRST_TRY)


async def test_texts_are_never_logged() -> None:
    rig = Rig([_doc()])
    with structlog.testing.capture_logs() as logs:
        await rig.worker.handle(_event(), FIRST_TRY)
    assert "w0" not in str(logs)
    assert all("content" not in entry for entry in logs)


# --- refresh before delete or update by query -----------------------------------------------


async def test_recently_written_documents_are_refreshed_before_deleting_or_updating() -> None:
    rig = Rig([_doc()])
    await rig.run()  # indexed "now"
    rig.source.documents["ITEM-1"] = _doc(start=7000)
    rig.now = NOW + timedelta(seconds=10)  # well inside the refresh window (2 x 30 s)
    await rig.run()
    stale_calls = [args for name, args in rig.indexer.calls if name == "delete_stale"]
    assert stale_calls[-1]["refresh_first"] is True


async def test_old_documents_do_not_need_a_refresh() -> None:
    rig = Rig([_doc()])
    await rig.run()
    rig.source.documents["ITEM-1"] = _doc(start=7000)
    rig.now = NOW + timedelta(minutes=10)
    await rig.run()
    stale_calls = [args for name, args in rig.indexer.calls if name == "delete_stale"]
    assert stale_calls[-1]["refresh_first"] is False


async def test_a_delete_and_a_permission_change_right_after_a_write_refresh_first() -> None:
    rig = Rig([_doc()])
    await rig.run()
    rig.now = NOW + timedelta(seconds=5)
    rig.source.documents["ITEM-1"] = _doc(acl_users=["u9"])
    await rig.run(_event("ACL_CHANGE"))
    await rig.run(_event("DELETE"))
    flags = {
        name: args["refresh_first"]
        for name, args in rig.indexer.calls
        if name in {"refresh_metadata", "delete_chunks"}
    }
    assert flags == {"refresh_metadata": True, "delete_chunks": True}


@pytest.mark.parametrize(
    ("interval", "expected"),
    [
        ("30s", 60.0),
        ("1m", 120.0),
        ("500ms", 1.0),
        ("1h", 7200.0),
        ("-1", float("inf")),
        ("nonsense", float("inf")),
    ],
)
def test_refresh_window(interval: str, expected: float) -> None:
    assert refresh_window_s(StoreSettings(refresh_interval=interval)) == expected


# --- hashes ---------------------------------------------------------------------------------


def test_text_hash_follows_the_text_and_the_page_numbers() -> None:
    base = _doc()
    assert text_hash(base) == text_hash(_doc())
    assert text_hash(base) != text_hash(_doc(start=1))
    moved = base.model_copy(
        update={"pages": [SourcePage(page_no=p.page_no + 1, text=p.text) for p in base.pages]}
    )
    assert text_hash(base) != text_hash(moved)


def test_meta_hash_ignores_order_and_follows_permissions() -> None:
    a = _doc(acl_users=["u1", "u2"])
    b = _doc(acl_users=["u2", "u1"])
    assert meta_hash(a) == meta_hash(b)
    assert meta_hash(a) != meta_hash(_doc(acl_users=["u1"]))
    assert meta_hash(a) != meta_hash(a.model_copy(update={"doc_type": "invoice"}))


# --- end to end with the consumer loop ------------------------------------------------------


async def test_event_read_chunk_embed_index_state_commit() -> None:
    rig = Rig([_doc("ITEM-1"), _doc("ITEM-2", start=500)])
    broker = FakeBroker()
    for topic in ("events", "retry", "dlq"):
        broker.create_topic(topic)
    for event_type, item in (("UPSERT", "ITEM-1"), ("UPSERT", "ITEM-2"), ("DELETE", "ITEM-3")):
        broker.produce(
            "events",
            item.encode(),
            json.dumps(
                {
                    "schema_version": 1,
                    "event_id": f"e-{item}",
                    "event_type": event_type,
                    "item_id": item,
                    "occurred_at": NOW.isoformat(),
                    "source": "test",
                }
            ).encode(),
        )
    consumer = FakeConsumer(broker, "group")
    loop = ConsumerLoop(
        consumer=consumer,
        producer=FakeProducer(broker),
        handler=rig.worker.handle,
        topics=["events", "retry"],
        retry_topic="retry",
        dlq_topic="dlq",
        settings=ConsumerSettings(
            poll_timeout_s=0.01,
            commit_interval_s=0.001,
            quick_retries=1,
            max_retries=1,
            retry_delay_s=0,
        ),
    )
    task = asyncio.create_task(loop.run())
    deadline = asyncio.get_running_loop().time() + 5
    while broker.committed.get(("group", ("events", 0)), 0) < 3:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)
    loop.stop()
    await task
    assert rig.indexer.doc_chunks("ITEM-1")
    assert rig.indexer.doc_chunks("ITEM-2")
    assert {s: v.status for s, v in rig.states.states.items()} == {
        "ITEM-1": IndexStatus.INDEXED,
        "ITEM-2": IndexStatus.INDEXED,
        "ITEM-3": IndexStatus.DELETED,
    }
    assert broker.messages("dlq") == []
