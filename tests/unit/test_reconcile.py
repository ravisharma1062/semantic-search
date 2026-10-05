import json
from typing import Any, cast

import pytest

from app.core.settings import BackfillSettings, SourceSettings, WaveSpec
from app.ingestion.events import EventType, parse_event
from app.ingestion.source import SourceDocument
from app.ingestion.state_admin import NO_WAVE, StateAdmin
from app.ingestion.state_store import IndexState, IndexStatus
from app.ingestion.worker import meta_hash
from app.jobs.job_store import ElasticsearchJobStore, JobRecord
from app.jobs.progress import ProgressReport, WaveProgress, build_report, format_report
from app.jobs.rate import RateLimiter
from app.jobs.reconcile import Reconciler
from app.jobs.scan import SourceScanner
from tests.fakes.jobs import NOW, FakeJobStore, FakeScanner, FakeStateAdmin
from tests.fakes.kafka import FakeBroker, FakeProducer

LIVE, BACKFILL = "live", "backfill"
MODEL = "bge-m3@1"


def _source(version: int | None = 5, acl: str = "u1", doc_type: str = "contract") -> dict[str, Any]:
    return {
        "doc_type": doc_type,
        "tags": ["vendor"],
        "created_at": "2024-05-01",
        "acl_users": [acl],
        "acl_groups": ["g1"],
        "version": version,
    }


def _state(item_id: str, status: IndexStatus = IndexStatus.INDEXED, **changes: Any) -> IndexState:
    values: dict[str, Any] = {
        "item_id": item_id,
        "status": status,
        "content_hash": "sha256:x",
        "meta_hash": meta_hash(_doc(item_id)),
        "doc_version": 5,
        "embedding_model": MODEL,
        "chunker_version": "v1",
    }
    return IndexState(**{**values, **changes})


def _doc(item_id: str, **changes: Any) -> SourceDocument:
    from app.ingestion.source import map_source

    return map_source(item_id, _source(**changes), SourceSettings())


class Rig:
    def __init__(self, documents: dict[str, dict[str, Any]]) -> None:
        self.broker = FakeBroker()
        for topic in (LIVE, BACKFILL):
            self.broker.create_topic(topic)
        self.producer = FakeProducer(self.broker)
        self.scanner = FakeScanner(documents)
        self.states = FakeStateAdmin()
        self.reconciler = Reconciler(
            scanner=cast(SourceScanner, self.scanner),
            states=cast(StateAdmin, self.states),
            producer=self.producer,
            live_topic=LIVE,
            backfill_topic=BACKFILL,
            limiter=RateLimiter(1_000_000),
            backfill=BackfillSettings(),
            source=SourceSettings(),
            embedding_model=MODEL,
            chunker_version="v1",
        )

    def events(self, topic: str) -> list[tuple[str, EventType]]:
        out = []
        for message in self.broker.messages(topic):
            event = parse_event(message.value)
            out.append((event.item_id, event.event_type))
        return sorted(out)


async def test_a_consistent_index_publishes_nothing() -> None:
    rig = Rig({"A": _source(), "B": _source()})
    rig.states.put(_state("A"))
    rig.states.put(_state("B", IndexStatus.SKIPPED))
    result = await rig.reconciler.run()
    assert result.scanned == 2
    assert result.published == 0
    assert rig.events(LIVE) == [] and rig.events(BACKFILL) == []


async def test_a_document_without_a_record_is_republished() -> None:
    rig = Rig({"A": _source()})
    result = await rig.reconciler.run()
    assert result.missing_state == 1
    assert rig.events(BACKFILL) == [("A", EventType.UPSERT)]


@pytest.mark.parametrize(
    ("state", "counter"),
    [
        (IndexStatus.FAILED, "failed"),
        (IndexStatus.PENDING, "pending"),
        (IndexStatus.DELETED, "resurrected"),
    ],
)
async def test_failed_pending_and_wrongly_deleted_documents_are_republished(
    state: IndexStatus, counter: str
) -> None:
    rig = Rig({"A": _source()})
    rig.states.put(_state("A", state))
    result = await rig.reconciler.run()
    assert getattr(result, counter) == 1
    assert rig.events(BACKFILL) == [("A", EventType.UPSERT)]


async def test_a_changed_version_is_republished() -> None:
    rig = Rig({"A": _source(version=6)})
    rig.states.put(_state("A", doc_version=5))
    result = await rig.reconciler.run()
    assert result.version_mismatch == 1
    assert rig.events(BACKFILL) == [("A", EventType.UPSERT)]


async def test_unknown_versions_are_not_a_difference() -> None:
    rig = Rig({"A": _source(version=None)})
    rig.states.put(_state("A", doc_version=5))
    assert (await rig.reconciler.run()).published == 0


async def test_another_model_or_chunker_is_republished() -> None:
    rig = Rig({"A": _source(), "B": _source()})
    rig.states.put(_state("A", embedding_model="bge-m3@0"))
    rig.states.put(_state("B", chunker_version="v0"))
    result = await rig.reconciler.run()
    assert result.stale_model == 2
    assert rig.events(BACKFILL) == [("A", EventType.UPSERT), ("B", EventType.UPSERT)]


async def test_a_permission_difference_goes_to_the_live_topic_as_a_permission_change() -> None:
    rig = Rig({"A": _source(acl="u2")})  # the source now says u2
    rig.states.put(_state("A"))  # the index was built for u1
    result = await rig.reconciler.run()
    assert result.permission_mismatch == 1
    assert rig.events(LIVE) == [("A", EventType.ACL_CHANGE)]
    assert rig.events(BACKFILL) == []
    event = parse_event(rig.broker.messages(LIVE)[0].value)
    assert (event.priority, event.source, event.doc_version) == ("live", "reconcile", 5)


async def test_a_changed_document_type_or_tags_also_counts_as_a_difference() -> None:
    rig = Rig({"A": _source(doc_type="invoice")})
    rig.states.put(_state("A"))
    assert (await rig.reconciler.run()).permission_mismatch == 1


async def test_a_record_without_a_document_gets_a_delete_event() -> None:
    rig = Rig({"A": _source()})
    rig.states.put(_state("A"))
    rig.states.put(_state("GONE"))
    rig.states.put(_state("OLD", IndexStatus.DELETED))  # already deleted: nothing to do
    result = await rig.reconciler.run()
    assert result.orphans_deleted == 1
    assert rig.events(LIVE) == [("GONE", EventType.DELETE)]


async def test_the_orphan_check_can_be_switched_off() -> None:
    rig = Rig({})
    rig.states.put(_state("GONE"))
    assert (await rig.reconciler.run(check_orphans=False)).orphans_deleted == 0
    assert rig.events(LIVE) == []


async def test_a_run_can_be_limited_to_a_sample() -> None:
    rig = Rig({f"D-{i:02d}": _source() for i in range(30)})
    result = await rig.reconciler.run(max_items=12)
    assert result.scanned == 12
    assert len(rig.events(BACKFILL)) == 12


async def test_the_limit_comes_from_settings_when_not_given() -> None:
    rig = Rig({f"D-{i:02d}": _source() for i in range(30)})
    rig.reconciler._cfg = BackfillSettings(reconcile_max_items=5)
    assert (await rig.reconciler.run()).scanned == 5


async def test_a_wave_limits_both_directions() -> None:
    rig = Rig({"A": _source(doc_type="contract"), "B": _source(doc_type="invoice")})
    rig.states.put(_state("GONE", wave=1))
    rig.states.put(_state("GONE-OTHER-WAVE", wave=2))
    result = await rig.reconciler.run(WaveSpec(number=1, doc_types=["contract"]))
    assert result.scanned == 1  # only the contract
    assert result.orphans_deleted == 1  # only the orphan of wave 1
    assert rig.events(LIVE) == [("GONE", EventType.DELETE)]


async def test_a_second_run_after_the_fix_finds_nothing() -> None:
    rig = Rig({"A": _source(), "B": _source(acl="u2")})
    rig.states.put(_state("B"))
    first = await rig.reconciler.run()
    assert first.published == 2
    rig.states.put(_state("A"))
    rig.states.put(_state("B", meta_hash=meta_hash(_doc("B", acl="u2"))))
    assert (await rig.reconciler.run()).published == 0


# --- progress report ------------------------------------------------------------------------


async def test_the_report_shows_counts_per_wave_and_status() -> None:
    states = FakeStateAdmin()
    for i in range(6):
        states.put(IndexState(item_id=f"A{i}", status=IndexStatus.INDEXED, wave=1))
    states.put(IndexState(item_id="P", status=IndexStatus.PENDING, wave=1))
    states.put(IndexState(item_id="F", status=IndexStatus.FAILED, wave=1))
    states.put(IndexState(item_id="W2", status=IndexStatus.PENDING, wave=2))
    states.put(IndexState(item_id="LIVE", status=IndexStatus.INDEXED))  # no wave: live event
    jobs = FakeJobStore()
    await jobs.start("backfill-wave1", "backfill", 1)
    report = await build_report(cast(StateAdmin, states), cast(ElasticsearchJobStore, jobs))
    by_wave = {w.wave: w for w in report.waves}
    assert by_wave[1].counts == {"INDEXED": 6, "PENDING": 1, "FAILED": 1}
    assert by_wave[1].total == 8
    assert by_wave[1].percent_done == 75.0
    assert by_wave[2].counts == {"PENDING": 1}
    assert by_wave[None].counts == {"INDEXED": 1}
    assert NO_WAVE not in by_wave
    text = format_report(report)
    assert "wave" in text.splitlines()[0]
    assert "75.0" in text
    assert "backfill-wave1: RUNNING" in text


def test_an_empty_report() -> None:
    text = format_report(ProgressReport(waves=[], jobs=[]))
    assert "(none)" in text
    assert WaveProgress(wave=1, counts={}).percent_done == 0.0


def test_the_report_lists_the_job_counters() -> None:
    job = JobRecord(
        job_id="j",
        wave=2,
        started_at=NOW,
        updated_at=NOW,
        scanned=10,
        published=7,
        skipped_up_to_date=3,
    )
    assert "scanned 10, published 7, up to date 3" in format_report(
        ProgressReport(waves=[], jobs=[job])
    )


def test_state_without_text_json_check() -> None:
    assert "text" not in json.dumps(_state("A").model_dump(mode="json"))
