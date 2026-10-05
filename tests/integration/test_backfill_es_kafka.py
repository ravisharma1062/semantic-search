"""Scan, wave registration, pause and resume, progress and reconciliation on real services."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date

import pytest
from elasticsearch import AsyncElasticsearch

from app.core.settings import (
    BackfillSettings,
    Settings,
    SourceSettings,
    StoreSettings,
    WaveSpec,
)
from app.ingestion.events import EventType, parse_event
from app.ingestion.kafka_client import ConfluentProducer
from app.ingestion.source import map_source
from app.ingestion.state_admin import NO_WAVE, StateAdmin
from app.ingestion.state_store import ElasticsearchStateStore, IndexStatus
from app.ingestion.worker import meta_hash
from app.jobs.backfill import BackfillJob
from app.jobs.job_store import ElasticsearchJobStore, JobStatus
from app.jobs.progress import build_report, format_report
from app.jobs.rate import RateLimiter
from app.jobs.reconcile import Reconciler
from app.jobs.scan import SourceScanner
from app.store.aliases import create_state_index, install_templates
from app.store.client import create_es_client
from tests.integration.conftest import Topics
from tests.integration.kafka_helpers import read_all

pytestmark = pytest.mark.integration

MODEL = "bge-m3@1"


@dataclass
class Env:
    settings: Settings
    client: AsyncElasticsearch
    source_index: str
    backfill_topic: str
    live_topic: str
    bootstrap: str


@pytest.fixture
async def env(es_url: str, kafka_bootstrap: str, topics: Topics) -> AsyncIterator[Env]:
    suffix = uuid.uuid4().hex[:8]
    base = Settings()
    settings = base.model_copy(
        update={
            "elasticsearch": base.elasticsearch.model_copy(
                update={"hosts": [es_url], "state_index": f"state_{suffix}"}
            ),
            "store": StoreSettings(chunk_index_prefix=f"ch{suffix}", shards=1, replicas=0),
            "search": base.search.model_copy(update={"source_index": f"src_{suffix}"}),
            "backfill": BackfillSettings(
                job_index=f"jobs_{suffix}",
                scan_size=5,
                waves=[
                    WaveSpec(number=1, name="all"),
                    WaveSpec(number=2, name="contracts", doc_types=["contract"]),
                ],
            ),
        }
    )
    client = create_es_client(settings.elasticsearch)
    await install_templates(client, settings)
    await create_state_index(client, settings)
    await client.indices.create(
        index=f"src_{suffix}",
        settings={"number_of_shards": 1, "number_of_replicas": 0},
        mappings={
            "properties": {
                "item_id": {"type": "keyword"},
                "doc_type": {"type": "keyword"},
                "created_at": {"type": "date"},
                "acl_users": {"type": "keyword"},
                "version": {"type": "long"},
            }
        },
    )
    yield Env(settings, client, f"src_{suffix}", topics.dlq, topics.live, kafka_bootstrap)
    await client.close()


async def _add_documents(env: Env, count: int, *, start: int = 0) -> list[str]:
    ids = []
    for i in range(start, start + count):
        item_id = f"ITEM-{i:03d}"
        ids.append(item_id)
        await env.client.index(
            index=env.source_index,
            id=item_id,
            document={
                "item_id": item_id,
                "doc_type": "contract" if i % 2 == 0 else "invoice",
                "created_at": "2024-05-01T00:00:00Z",
                "acl_users": ["u1"],
                "version": 5,
                "ocr_text": "synthetic secret document text",
                "pages": [{"page_no": 1, "text": "synthetic secret document text"}],
            },
        )
    await env.client.indices.refresh(index=env.source_index)
    return ids


def _scanner(env: Env) -> SourceScanner:
    s = env.settings
    return SourceScanner(
        env.client, env.source_index, s.source, s.backfill, s.elasticsearch, s.retry
    )


def _admin(env: Env) -> StateAdmin:
    s = env.settings
    return StateAdmin(env.client, s.elasticsearch.state_index, s.elasticsearch, s.retry)


def _jobs(env: Env) -> ElasticsearchJobStore:
    s = env.settings
    return ElasticsearchJobStore(env.client, s.backfill.job_index, s.elasticsearch, s.retry)


# --- the scan -------------------------------------------------------------------------------


async def test_the_scan_is_stable_pages_in_order_and_can_continue_from_a_cursor(env: Env) -> None:
    ids = await _add_documents(env, 23)
    scanner = _scanner(env)
    pages = [page async for page in scanner.pages()]
    assert [len(p.items) for p in pages] == [5, 5, 5, 5, 3]
    assert [i.item_id for p in pages for i in p.items] == ids
    assert pages[1].cursor == ids[9]
    rest = [i.item_id async for page in scanner.pages(after=pages[1].cursor) for i in page.items]
    assert rest == ids[10:]  # continues exactly after the cursor


async def test_the_scan_follows_the_wave_and_the_limit(env: Env) -> None:
    await _add_documents(env, 20)
    scanner = _scanner(env)
    contracts = [
        i.item_id
        async for page in scanner.pages(WaveSpec(number=2, doc_types=["contract"]))
        for i in page.items
    ]
    assert len(contracts) == 10
    assert all(int(c[-3:]) % 2 == 0 for c in contracts)
    dated = WaveSpec(number=3, created_from=date(2024, 1, 1), created_to=date(2024, 12, 31))
    assert sum([len(p.items) async for p in scanner.pages(dated)]) == 20
    too_old = WaveSpec(number=4, created_to=date(2020, 1, 1))
    assert [p async for p in scanner.pages(too_old)] == []
    limited = sum([len(p.items) async for p in scanner.pages(max_items=7)])
    assert limited == 7


async def test_the_scan_reads_only_filter_fields_never_the_text(env: Env) -> None:
    await _add_documents(env, 3)
    page = await anext(_scanner(env).pages(include_meta=True))
    source = page.items[0].source
    assert source["acl_users"] == ["u1"]
    assert source["version"] == 5
    assert "ocr_text" not in source
    assert "pages" not in source
    bare = await anext(_scanner(env).pages())
    assert bare.items[0].source == {}


async def test_existing_ids(env: Env) -> None:
    await _add_documents(env, 3)
    assert await _scanner(env).existing(["ITEM-000", "ITEM-002", "NOPE"]) == {
        "ITEM-000",
        "ITEM-002",
    }


# --- state admin and job store --------------------------------------------------------------


async def test_waves_are_registered_without_changing_the_status_of_known_documents(
    env: Env,
) -> None:
    admin = _admin(env)
    states = ElasticsearchStateStore(
        env.client,
        env.settings.elasticsearch.state_index,
        env.settings.elasticsearch,
        env.settings.retry,
    )
    await states.mark_indexed(
        "KNOWN",
        content_hash="sha256:a",
        meta_hash="sha256:m",
        doc_version=1,
        chunk_count=2,
        chunker_version="v1",
        embedding_model=MODEL,
    )
    await admin.register_wave(["KNOWN", "NEW-1", "NEW-2"], 1)
    got = await admin.get_many(["KNOWN", "NEW-1", "NEW-2", "NONE"])
    assert set(got) == {"KNOWN", "NEW-1", "NEW-2"}
    assert got["KNOWN"].status is IndexStatus.INDEXED
    assert got["KNOWN"].wave == 1
    assert got["KNOWN"].content_hash == "sha256:a"  # untouched
    assert got["NEW-1"].status is IndexStatus.PENDING
    await env.client.indices.refresh(index=env.settings.elasticsearch.state_index)
    assert await admin.status_counts() == {1: {"INDEXED": 1, "PENDING": 2}}


async def test_counts_per_wave_and_status_include_records_outside_any_wave(env: Env) -> None:
    admin = _admin(env)
    states = ElasticsearchStateStore(
        env.client,
        env.settings.elasticsearch.state_index,
        env.settings.elasticsearch,
        env.settings.retry,
    )
    await states.mark_failed("LIVE-1", "RuntimeError")  # no wave
    await admin.register_wave(["W-1", "W-2"], 2)
    await env.client.indices.refresh(index=env.settings.elasticsearch.state_index)
    assert await admin.status_counts() == {NO_WAVE: {"FAILED": 1}, 2: {"PENDING": 2}}


async def test_state_scan_is_ordered_and_can_filter_by_wave(env: Env) -> None:
    admin = _admin(env)
    await admin.register_wave([f"S-{i:02d}" for i in range(7)], 1)
    await admin.register_wave(["T-1"], 2)
    await env.client.indices.refresh(index=env.settings.elasticsearch.state_index)
    first, cursor = await admin.scan(after=None, size=4, wave=1)
    assert [s.item_id for s in first] == ["S-00", "S-01", "S-02", "S-03"]
    second, _ = await admin.scan(after=cursor, size=4, wave=1)
    assert [s.item_id for s in second] == ["S-04", "S-05", "S-06"]
    everything, _ = await admin.scan(after=None, size=50)
    assert len(everything) == 8


async def test_job_records_pause_request_and_restart(env: Env) -> None:
    jobs = _jobs(env)
    await jobs.ensure_index()
    await jobs.ensure_index()  # safe to repeat
    started = await jobs.start("j1", "backfill", 1)
    assert started.status is JobStatus.RUNNING
    await jobs.save_progress("j1", cursor="ITEM-009", scanned=10, published=8, skipped_up_to_date=2)
    assert await jobs.desired("j1") == "RUNNING"
    assert await jobs.request("j1", "PAUSED") is True
    assert await jobs.desired("j1") == "PAUSED"
    assert await jobs.request("nope", "PAUSED") is False
    await jobs.finish("j1", JobStatus.PAUSED)
    resumed = await jobs.start("j1", "backfill", 1)  # resume: cursor kept, request cleared
    assert (resumed.cursor, resumed.scanned, resumed.desired) == ("ITEM-009", 10, "RUNNING")
    assert await jobs.desired("j1") == "RUNNING"
    restarted = await jobs.start("j1", "backfill", 1, restart=True)
    assert (restarted.cursor, restarted.scanned) == (None, 0)
    stored = await jobs.get("j1")
    assert stored is not None
    assert stored.cursor is None
    await jobs.finish("j1", JobStatus.FAILED, "x" * 500)
    failed = await jobs.get("j1")
    assert failed is not None
    assert (failed.status, len(failed.last_error or "")) == (JobStatus.FAILED, 256)
    await env.client.indices.refresh(index=env.settings.backfill.job_index)
    assert [j.job_id for j in await jobs.list_jobs()] == ["j1"]
    assert await jobs.get("missing") is None


# --- the backfill with real Kafka -----------------------------------------------------------


def _job(env: Env, producer: ConfluentProducer, *, rate: float = 1_000_000) -> BackfillJob:
    s = env.settings
    return BackfillJob(
        scanner=_scanner(env),
        jobs=_jobs(env),
        states=_admin(env),
        producer=producer,
        topic=env.backfill_topic,
        limiter=RateLimiter(rate),
        settings=s.backfill,
        embedding_model=MODEL,
        chunker_version="v1",
    )


async def test_a_wave_is_published_to_kafka_and_registered_in_the_state_index(env: Env) -> None:
    ids = await _add_documents(env, 23)
    await _jobs(env).ensure_index()
    producer = ConfluentProducer(
        env.settings.kafka.model_copy(update={"bootstrap_servers": env.bootstrap})
    )
    try:
        record = await _job(env, producer).run(
            "backfill-wave1", WaveSpec(number=1), asyncio.Event()
        )
    finally:
        await producer.close()
    assert (record.status, record.scanned, record.published) == (JobStatus.COMPLETED, 23, 23)

    messages = await asyncio.to_thread(read_all, env.bootstrap, env.backfill_topic, 23)
    events = [parse_event(value) for value, _ in messages]
    assert sorted(e.item_id for e in events) == ids
    assert {(e.event_type, e.wave, e.priority) for e in events} == {
        (EventType.UPSERT, 1, "backfill")
    }
    assert all(b"synthetic secret" not in value for value, _ in messages)  # no text in events

    await env.client.indices.refresh(index=env.settings.elasticsearch.state_index)
    assert await _admin(env).status_counts() == {1: {"PENDING": 23}}
    report = format_report(await build_report(_admin(env), _jobs(env)))
    assert "backfill-wave1: COMPLETED" in report


async def test_pause_and_resume_lose_no_document_and_repeat_none(env: Env) -> None:
    ids = await _add_documents(env, 40)
    await _jobs(env).ensure_index()
    producer = ConfluentProducer(
        env.settings.kafka.model_copy(update={"bootstrap_servers": env.bootstrap})
    )
    jobs = _jobs(env)
    try:
        slow = _job(env, producer, rate=40)  # 5 documents per page: about 0.125 s per page
        task = asyncio.create_task(slow.run("j", WaveSpec(number=1), asyncio.Event()))
        deadline = asyncio.get_running_loop().time() + 30
        while True:
            record = await jobs.get("j")
            if record and record.scanned >= 10:
                break
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.02)
        await jobs.request("j", "PAUSED")
        paused = await asyncio.wait_for(task, timeout=30)
        assert paused.status is JobStatus.PAUSED
        assert 10 <= paused.scanned < 40
        assert paused.cursor is not None

        resumed = await _job(env, producer).run("j", WaveSpec(number=1), asyncio.Event())
    finally:
        await producer.close()
    assert (resumed.status, resumed.scanned, resumed.published) == (JobStatus.COMPLETED, 40, 40)
    messages = await asyncio.to_thread(read_all, env.bootstrap, env.backfill_topic, 40)
    published = sorted(parse_event(value).item_id for value, _ in messages)
    assert published == ids  # every document exactly once


# --- reconciliation -------------------------------------------------------------------------


async def test_reconciliation_republishes_exactly_the_differences(env: Env) -> None:
    await _add_documents(env, 6)  # ITEM-000 .. ITEM-005
    s = env.settings
    states = ElasticsearchStateStore(
        env.client, s.elasticsearch.state_index, s.elasticsearch, s.retry
    )
    document = map_source(
        "x",
        {
            "doc_type": "contract",
            "created_at": "2024-05-01T00:00:00Z",
            "acl_users": ["u1"],
            "version": 5,
        },
        SourceSettings(),
    )

    async def indexed(item_id: str, acl: list[str] | None = None) -> None:
        doc = document if acl is None else document.model_copy(update={"acl_users": acl})
        await states.mark_indexed(
            item_id,
            content_hash="sha256:a",
            meta_hash=meta_hash(doc),
            doc_version=5,
            chunk_count=1,
            chunker_version="v1",
            embedding_model=MODEL,
        )

    # sources: 000 doc_type contract, 001 invoice, ... the state must describe the same fields
    for item_id in ("ITEM-000", "ITEM-002", "ITEM-004"):
        await indexed(item_id)  # contracts: consistent
    for item_id in ("ITEM-001",):
        invoice = document.model_copy(update={"doc_type": "invoice"})
        await states.mark_indexed(
            item_id, content_hash="sha256:a", meta_hash=meta_hash(invoice), doc_version=5,
            chunk_count=1, chunker_version="v1", embedding_model=MODEL,
        )  # fmt: skip
    await indexed("ITEM-003", acl=["somebody-else"])  # permissions differ from the source (invoice)
    # ITEM-005: no record at all. And one record without a document:
    await indexed("GONE-1")
    await env.client.indices.refresh(index=s.elasticsearch.state_index)

    producer = ConfluentProducer(s.kafka.model_copy(update={"bootstrap_servers": env.bootstrap}))
    reconciler = Reconciler(
        scanner=_scanner(env),
        states=_admin(env),
        producer=producer,
        live_topic=env.live_topic,
        backfill_topic=env.backfill_topic,
        limiter=RateLimiter(1_000_000),
        backfill=s.backfill,
        source=s.source,
        embedding_model=MODEL,
        chunker_version="v1",
    )
    try:
        result = await reconciler.run()
    finally:
        await producer.close()

    assert result.scanned == 6
    assert result.missing_state == 1
    assert result.permission_mismatch == 1
    assert result.orphans_deleted == 1
    backfill = await asyncio.to_thread(read_all, env.bootstrap, env.backfill_topic, 1)
    live = await asyncio.to_thread(read_all, env.bootstrap, env.live_topic, 2)
    assert [(parse_event(v).item_id, parse_event(v).event_type) for v, _ in backfill] == [
        ("ITEM-005", EventType.UPSERT)
    ]
    assert sorted((parse_event(v).item_id, parse_event(v).event_type.value) for v, _ in live) == [
        ("GONE-1", "DELETE"),
        ("ITEM-003", "ACL_CHANGE"),
    ]
