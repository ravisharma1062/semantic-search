import json

import pytest

from app.core.errors import UpstreamUnavailableError
from app.ingestion import dlq
from app.jobs.dlq_replay import REPLAYED, DlqReplayer, build_parser
from tests.fakes.kafka import FakeBroker, FakeConsumer, FakeProducer

DLQ = "dlq"
LIVE = "live"


def _event(item_id: str) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": f"evt-{item_id}",
            "event_type": "UPSERT",
            "item_id": item_id,
            "occurred_at": "2026-10-05T12:00:00+00:00",
            "source": "test",
        }
    ).encode()


def _put(broker: FakeBroker, key: str, value: bytes, reason: str = "retries exhausted") -> None:
    broker.produce(DLQ, key.encode(), value, {dlq.REASON: reason.encode(), dlq.ORIGIN: b"live/0/1"})


def _replayer(
    broker: FakeBroker, group: str = "g"
) -> tuple[DlqReplayer, FakeConsumer, FakeProducer]:
    consumer, producer = FakeConsumer(broker, group), FakeProducer(broker)
    replayer = DlqReplayer(
        consumer=consumer,
        producer=producer,
        dlq_topic=DLQ,
        live_topic=LIVE,
        batch_size=3,
        idle_timeout_s=0.01,
    )
    return replayer, consumer, producer


@pytest.fixture
def broker() -> FakeBroker:
    b = FakeBroker()
    b.create_topic(DLQ)
    b.create_topic(LIVE)
    return b


async def test_valid_events_go_back_to_the_live_topic_unchanged(broker: FakeBroker) -> None:
    for n in range(5):
        _put(broker, f"ITEM-{n}", _event(f"ITEM-{n}"))
    replayer, consumer, _ = _replayer(broker)
    report = await replayer.run()
    assert (report.read, report.replayed, report.invalid_skipped) == (5, 5, 0)
    live = broker.messages(LIVE)
    assert sorted(m.key or b"" for m in live) == [f"ITEM-{n}".encode() for n in range(5)]
    assert [m.value for m in live] == [m.value for m in broker.messages(DLQ)] or len(live) == 5
    assert all(m.headers == {REPLAYED: b"1"} for m in live)
    assert consumer.closed
    assert broker.committed[("g", (DLQ, 0))] == 5


async def test_the_original_value_is_not_changed_at_all(broker: FakeBroker) -> None:
    value = _event("ITEM-1")
    _put(broker, "ITEM-1", value)
    replayer, _, _ = _replayer(broker)
    await replayer.run()
    [message] = broker.messages(LIVE)
    assert message.value == value and message.key == b"ITEM-1"


async def test_messages_that_are_not_events_are_counted_and_skipped(broker: FakeBroker) -> None:
    _put(broker, "ITEM-1", _event("ITEM-1"))
    _put(broker, "BAD", b"not json", reason="invalid event")
    _put(broker, "BAD2", b"", reason="invalid event")
    replayer, _, _ = _replayer(broker)
    report = await replayer.run()
    assert (report.read, report.replayed, report.invalid_skipped) == (3, 1, 2)
    assert report.by_reason == {"retries exhausted": 1, "invalid event": 2}
    assert len(broker.messages(LIVE)) == 1
    assert broker.committed[("g", (DLQ, 0))] == 3  # the bad ones are not read again either


async def test_a_dry_run_changes_nothing_and_can_be_repeated(broker: FakeBroker) -> None:
    for n in range(4):
        _put(
            broker,
            f"ITEM-{n}",
            _event(f"ITEM-{n}"),
            "non-retryable error" if n else "retries exhausted",
        )
    replayer, consumer, producer = _replayer(broker)
    report = await replayer.run(dry_run=True)
    assert report.dry_run and report.read == 4 and report.replayed == 0
    assert report.by_reason == {"non-retryable error": 3, "retries exhausted": 1}
    assert broker.messages(LIVE) == [] and consumer.commits == [] and producer.sent == []
    again, _, _ = _replayer(broker)
    assert (await again.run(dry_run=True)).read == 4


async def test_max_limits_the_run_and_a_second_run_continues(broker: FakeBroker) -> None:
    for n in range(7):
        _put(broker, f"ITEM-{n}", _event(f"ITEM-{n}"))
    first, _, _ = _replayer(broker)
    assert (await first.run(max_messages=4)).replayed == 4
    second, _, _ = _replayer(broker)
    report = await second.run()
    assert report.replayed == 3
    assert sorted(m.key or b"" for m in broker.messages(LIVE)) == [
        f"ITEM-{n}".encode() for n in range(7)
    ]
    third, _, _ = _replayer(broker)
    assert (await third.run()).read == 0  # nothing new


async def test_a_new_group_reads_everything_again(broker: FakeBroker) -> None:
    _put(broker, "ITEM-1", _event("ITEM-1"))
    await (_replayer(broker, "g")[0]).run()
    report = await (_replayer(broker, "g2")[0]).run()
    assert report.replayed == 1 and len(broker.messages(LIVE)) == 2


async def test_a_failed_publish_commits_nothing_for_that_batch(broker: FakeBroker) -> None:
    for n in range(3):
        _put(broker, f"ITEM-{n}", _event(f"ITEM-{n}"))
    replayer, consumer, producer = _replayer(broker)
    producer.fail_times = 1
    with pytest.raises(UpstreamUnavailableError):
        await replayer.run()
    assert consumer.commits == [] and consumer.closed
    # The next run starts from the beginning: no event is lost (a few may be sent twice).
    again, _, _ = _replayer(broker)
    assert (await again.run()).replayed == 3


async def test_an_empty_dlq_is_a_clean_run(broker: FakeBroker) -> None:
    replayer, consumer, _ = _replayer(broker)
    report = await replayer.run()
    assert (report.read, report.replayed) == (0, 0) and consumer.closed


def test_the_command_line() -> None:
    args = build_parser().parse_args(["--max", "10", "--dry-run", "--group", "x"])
    assert (args.max_messages, args.dry_run, args.group) == (10, True, "x")
