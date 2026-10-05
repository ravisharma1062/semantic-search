import asyncio
import json
from datetime import date
from typing import Any, cast

import pytest

from app.core.errors import UpstreamUnavailableError
from app.core.settings import BackfillSettings, WaveSpec
from app.ingestion.events import EventType, parse_event
from app.ingestion.state_admin import StateAdmin
from app.ingestion.state_store import IndexState, IndexStatus
from app.jobs.backfill import BackfillJob
from app.jobs.events import build_event
from app.jobs.job_store import ElasticsearchJobStore, JobRecord, JobStatus
from app.jobs.rate import RateLimiter
from app.jobs.scan import SourceScanner
from tests.fakes.jobs import FakeJobStore, FakeScanner, FakeStateAdmin
from tests.fakes.kafka import FakeBroker, FakeProducer

TOPIC = "backfill"
MODEL = "bge-m3@1"
WAVE_1 = WaveSpec(number=1, name="pilot")


async def _no_sleep(_seconds: float) -> None:
    return None


def _docs(count: int, **extra: Any) -> dict[str, dict[str, Any]]:
    return {
        f"ITEM-{i:03d}": {"doc_type": "contract", "created_at": "2024-05-01", **extra}
        for i in range(count)
    }


class Rig:
    def __init__(
        self,
        documents: dict[str, dict[str, Any]] | None = None,
        *,
        page_size: int = 10,
        skip_up_to_date: bool = True,
        rate: float = 1_000_000,
    ) -> None:
        self.broker = FakeBroker()
        self.broker.create_topic(TOPIC)
        self.producer = FakeProducer(self.broker)
        self.scanner = FakeScanner(documents if documents is not None else _docs(25), page_size)
        self.jobs = FakeJobStore()
        self.states = FakeStateAdmin()
        self.sleeps: list[float] = []
        self.clock_now = 0.0

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            self.clock_now += seconds

        self.limiter = RateLimiter(rate, clock=lambda: self.clock_now, sleep=sleep)
        self.job = BackfillJob(
            scanner=cast(SourceScanner, self.scanner),
            jobs=cast(ElasticsearchJobStore, self.jobs),
            states=cast(StateAdmin, self.states),
            producer=self.producer,
            topic=TOPIC,
            limiter=self.limiter,
            settings=BackfillSettings(skip_up_to_date=skip_up_to_date),
            embedding_model=MODEL,
            chunker_version="v1",
        )

    def published(self) -> list[str]:
        return [m.key.decode() for m in self.broker.messages(TOPIC) if m.key]

    async def run(
        self,
        job_id: str = "job-1",
        wave: WaveSpec = WAVE_1,
        *,
        stop: asyncio.Event | None = None,
        restart: bool = False,
    ) -> JobRecord:
        return await self.job.run(job_id, wave, stop or asyncio.Event(), restart=restart)


# --- events ---------------------------------------------------------------------------------


def test_event_is_valid_schema_v1_with_the_item_id_as_key() -> None:
    key, value = build_event(
        "ITEM-1", EventType.UPSERT, source="backfill", priority="backfill", wave=3
    )
    event = parse_event(value)
    assert key == b"ITEM-1"
    assert (event.event_type, event.item_id, event.source, event.priority, event.wave) == (
        EventType.UPSERT,
        "ITEM-1",
        "backfill",
        "backfill",
        3,
    )
    data = json.loads(value)
    assert set(data) <= {
        "schema_version",
        "event_id",
        "event_type",
        "item_id",
        "doc_version",
        "occurred_at",
        "source",
        "priority",
        "wave",
    }
    assert "text" not in data  # an event never carries text or permissions


def test_events_have_unique_ids() -> None:
    ids = {
        json.loads(build_event("I", EventType.DELETE, source="s", priority="live")[1])["event_id"]
        for _ in range(20)
    }
    assert len(ids) == 20


# --- the producer ---------------------------------------------------------------------------


async def test_every_document_is_published_once_with_its_wave() -> None:
    rig = Rig()
    record = await rig.run()
    assert record.status is JobStatus.COMPLETED
    assert rig.published() == sorted(rig.scanner.documents)
    event = parse_event(rig.broker.messages(TOPIC)[0].value)
    assert (event.event_type, event.wave, event.priority, event.source) == (
        EventType.UPSERT,
        1,
        "backfill",
        "backfill",
    )
    assert (record.scanned, record.published, record.skipped_up_to_date) == (25, 25, 0)


async def test_documents_join_the_wave_as_pending_before_they_are_published() -> None:
    rig = Rig()
    await rig.run()
    assert {s.status for s in rig.states.states.values()} == {IndexStatus.PENDING}
    assert {s.wave for s in rig.states.states.values()} == {1}
    assert len(rig.states.states) == 25


async def test_progress_is_saved_after_every_page() -> None:
    rig = Rig()
    cursors: list[str | None] = []

    async def note(record: Any) -> None:
        cursors.append(record.cursor)

    rig.jobs.after_save = note
    await rig.run()
    assert cursors == ["ITEM-009", "ITEM-019", "ITEM-024"]


async def test_an_empty_source_completes_at_once() -> None:
    rig = Rig({})
    record = await rig.run()
    assert (record.status, record.scanned, record.published) == (JobStatus.COMPLETED, 0, 0)


async def test_a_wave_only_covers_its_own_documents() -> None:
    docs = {
        "A-1": {"doc_type": "contract", "created_at": "2024-01-10"},
        "A-2": {"doc_type": "invoice", "created_at": "2024-01-10"},
        "A-3": {"doc_type": "contract", "created_at": "2021-01-10"},
    }
    rig = Rig(docs)
    wave = WaveSpec(number=2, doc_types=["contract"], created_from=date(2023, 1, 1))
    await rig.run(wave=wave)
    assert rig.published() == ["A-1"]


# --- documents that are indexed already -----------------------------------------------------


def _indexed(item_id: str, model: str = MODEL, chunker: str = "v1") -> IndexState:
    return IndexState(
        item_id=item_id,
        status=IndexStatus.INDEXED,
        embedding_model=model,
        chunker_version=chunker,
        content_hash="sha256:x",
    )


async def test_documents_that_are_up_to_date_are_not_published_but_still_join_the_wave() -> None:
    rig = Rig(_docs(5))
    rig.states.put(_indexed("ITEM-000"))
    rig.states.put(_indexed("ITEM-001"))
    record = await rig.run()
    assert rig.published() == ["ITEM-002", "ITEM-003", "ITEM-004"]
    assert (record.scanned, record.published, record.skipped_up_to_date) == (5, 3, 2)
    assert rig.states.states["ITEM-000"].wave == 1  # counted in the wave report
    assert rig.states.states["ITEM-000"].status is IndexStatus.INDEXED  # status untouched


@pytest.mark.parametrize(
    "state",
    [
        _indexed("ITEM-000", model="bge-m3@2"),
        _indexed("ITEM-000", chunker="v2"),
        IndexState(item_id="ITEM-000", status=IndexStatus.FAILED),
    ],
)
async def test_documents_with_another_model_a_new_chunker_or_a_failure_are_published(
    state: IndexState,
) -> None:
    rig = Rig(_docs(1))
    rig.states.put(state)
    await rig.run()
    assert rig.published() == ["ITEM-000"]


async def test_everything_is_published_when_skipping_is_off() -> None:
    rig = Rig(_docs(3), skip_up_to_date=False)
    rig.states.put(_indexed("ITEM-000"))
    await rig.run()
    assert len(rig.published()) == 3


# --- speed ----------------------------------------------------------------------------------


async def test_the_rate_limit_slows_the_scan_down() -> None:
    rig = Rig(_docs(100), page_size=10, rate=10)  # 10 per second, 100 documents
    await rig.run()
    assert sum(rig.sleeps) == pytest.approx(9.0)  # the first 10 are the burst, 90 more take 9 s
    assert max(rig.sleeps) == pytest.approx(1.0)  # one page at a time


async def test_only_published_documents_count_against_nothing_else() -> None:
    rig = Rig(_docs(10), rate=1_000)
    await rig.run()
    assert rig.sleeps == []  # far below the limit: no waiting


# --- pause and resume -----------------------------------------------------------------------


async def test_a_pause_request_stops_the_job_between_pages_and_resume_continues() -> None:
    rig = Rig()

    async def pause_after_first_page(record: Any) -> None:
        if record.scanned == 10:
            await rig.jobs.request("job-1", "PAUSED")

    rig.jobs.after_save = pause_after_first_page
    first = await rig.run()
    assert first.status is JobStatus.PAUSED
    assert first.cursor == "ITEM-009"
    assert len(rig.published()) == 10

    rig.jobs.after_save = None
    second = await rig.run()  # resume: the same job ID
    assert second.status is JobStatus.COMPLETED
    assert rig.published() == sorted(rig.scanner.documents)  # nothing lost, nothing twice
    assert (second.scanned, second.published) == (25, 25)


async def test_sigterm_pauses_the_job_and_saves_the_cursor() -> None:
    rig = Rig()
    stop = asyncio.Event()

    async def stop_after_first_page(record: Any) -> None:
        stop.set()

    rig.jobs.after_save = stop_after_first_page
    record = await rig.run(stop=stop)
    assert record.status is JobStatus.PAUSED
    assert record.cursor == "ITEM-009"


async def test_a_job_can_be_paused_before_it_starts_a_page() -> None:
    rig = Rig()
    await rig.jobs.start("job-1", "backfill", 1)
    await rig.jobs.request("job-1", "PAUSED")
    # starting the job clears an old request, as a resume does
    record = await rig.run()
    assert record.status is JobStatus.COMPLETED


async def test_restart_scans_the_wave_from_the_beginning() -> None:
    rig = Rig()
    await rig.run()
    again = await rig.run(restart=True)
    assert again.published == 25
    assert len(rig.published()) == 50


async def test_a_finished_job_started_again_publishes_nothing() -> None:
    rig = Rig()
    await rig.run()
    await rig.run()
    assert len(rig.published()) == 25


# --- failures -------------------------------------------------------------------------------


async def test_a_kafka_outage_fails_the_job_and_resume_loses_nothing() -> None:
    rig = Rig()
    rig.producer.fail_times = 1
    # first page published, second page cannot be published
    sent = {"n": 0}
    original = rig.producer.send_batch

    async def flaky(topic: str, items: Any) -> None:
        sent["n"] += 1
        if sent["n"] == 2:
            raise UpstreamUnavailableError()
        await original(topic, items)

    rig.producer.fail_times = 0
    rig.producer.send_batch = flaky  # type: ignore[method-assign]
    with pytest.raises(UpstreamUnavailableError):
        await rig.run()
    record = rig.jobs.records["job-1"]
    assert record.status is JobStatus.FAILED
    assert record.last_error == "UpstreamUnavailableError: Upstream service unavailable"
    assert record.cursor == "ITEM-009"  # only the first page is checkpointed

    rig.producer.send_batch = original  # type: ignore[method-assign]
    done = await rig.run()
    assert done.status is JobStatus.COMPLETED
    assert set(rig.published()) == set(rig.scanner.documents)


async def test_a_crash_between_publishing_and_saving_publishes_that_page_again_not_never() -> None:
    rig = Rig()
    rig.jobs.fail_save_times = 1  # the first checkpoint cannot be saved
    with pytest.raises(RuntimeError):
        await rig.run()
    assert len(rig.published()) == 10  # published, but not checkpointed
    await rig.run()
    published = rig.published()
    assert set(published) == set(rig.scanner.documents)  # nothing lost
    assert len(published) == 35  # the first page twice, which is harmless


async def test_a_scan_error_fails_the_job() -> None:
    rig = Rig()
    rig.scanner.fail_after_pages = 1
    with pytest.raises(RuntimeError):
        await rig.run()
    assert rig.jobs.records["job-1"].status is JobStatus.FAILED
    assert rig.jobs.records["job-1"].last_error == "RuntimeError"  # the type only


async def test_job_status_never_holds_document_text() -> None:
    rig = Rig(_docs(3, ocr_text="synthetic secret text"))
    await rig.run()
    assert "synthetic secret text" not in rig.jobs.records["job-1"].model_dump_json()
    assert all(b"synthetic secret text" not in (m.value or b"") for m in rig.broker.messages(TOPIC))
