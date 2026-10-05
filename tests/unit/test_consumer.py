"""The consumer loop against the in-memory Kafka."""

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta

import pytest
import structlog

from app.core.errors import NonRetryableError
from app.core.settings import ConsumerSettings
from app.ingestion import dlq
from app.ingestion.consumer import ConsumerLoop, Tracked, coalesce
from app.ingestion.events import EventType, IndexEvent, parse_event
from app.ingestion.kafka_io import KafkaMessage
from tests.fakes.kafka import FakeBroker, FakeConsumer, FakeProducer

LIVE, RETRY, DLQ = "events", "retry", "dlq"
BASE_TIME = datetime(2026, 10, 5, 10, 0, tzinfo=UTC)

Handler = Callable[[IndexEvent], Awaitable[None]]


def _settings(**changes: object) -> ConsumerSettings:
    values: dict[str, object] = {
        "max_in_flight": 4,
        "poll_batch_size": 50,
        "poll_timeout_s": 0.01,
        "commit_interval_s": 0.001,
        "quick_retries": 3,
        "quick_retry_initial_delay_s": 0,
        "retry_delay_s": 60,
        "max_retries": 2,
        "shutdown_timeout_s": 2,
        "rebalance_timeout_s": 2,
    }
    return ConsumerSettings(**{**values, **changes})


def _event_bytes(
    item_id: str, event_type: str = "UPSERT", version: int = 1, seconds: int = 0
) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": f"evt-{item_id}-{event_type}-{seconds}",
            "event_type": event_type,
            "item_id": item_id,
            "doc_version": version,
            "occurred_at": (BASE_TIME + timedelta(seconds=seconds)).isoformat(),
            "source": "test",
        }
    ).encode()


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


class Harness:
    def __init__(self, handler: Handler | None = None, partitions: int = 1, **settings: object):
        self.broker = FakeBroker(partitions)
        for topic in (LIVE, RETRY, DLQ):
            self.broker.create_topic(topic)
        self.consumer = FakeConsumer(self.broker, "group")
        self.producer = FakeProducer(self.broker)
        self.clock = _Clock()
        self.handled: list[IndexEvent] = []
        self._handler = handler
        self.loop = ConsumerLoop(
            consumer=self.consumer,
            producer=self.producer,
            handler=self._handle,
            topics=[LIVE, RETRY],
            retry_topic=RETRY,
            dlq_topic=DLQ,
            settings=_settings(**settings),
            clock=self.clock,
        )
        self.task: asyncio.Task[None] | None = None

    async def _handle(self, event: IndexEvent) -> None:
        self.handled.append(event)
        if self._handler:
            await self._handler(event)

    def add(
        self,
        item_id: str,
        event_type: str = "UPSERT",
        *,
        version: int = 1,
        seconds: int = 0,
        topic: str = LIVE,
        headers: Mapping[str, bytes] | None = None,
    ) -> KafkaMessage:
        return self.broker.produce(
            topic, item_id.encode(), _event_bytes(item_id, event_type, version, seconds), headers
        )

    def start(self) -> None:
        self.task = asyncio.create_task(self.loop.run())

    async def stop(self) -> None:
        self.loop.stop()
        assert self.task is not None
        await asyncio.wait_for(self.task, timeout=5)

    def committed(self, topic: str = LIVE, partition: int = 0) -> int | None:
        return self.broker.committed.get(("group", (topic, partition)))

    async def until(self, condition: Callable[[], bool], seconds: float = 3.0) -> None:
        deadline = asyncio.get_running_loop().time() + seconds
        while not condition():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("condition not reached in time")
            await asyncio.sleep(0.005)


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    h = Harness()
    yield h
    if h.task and not h.task.done():
        await h.stop()


async def _fail(_event: IndexEvent) -> None:
    raise RuntimeError("synthetic failure")


# --- the basics -----------------------------------------------------------------------------


async def test_events_are_handled_and_committed(harness: Harness) -> None:
    for i in range(3):
        harness.add(f"ITEM-{i}")
    harness.start()
    await harness.until(lambda: harness.committed() == 3)
    assert sorted(e.item_id for e in harness.handled) == ["ITEM-0", "ITEM-1", "ITEM-2"]


async def test_offset_is_committed_only_after_the_handler_succeeded() -> None:
    gate = asyncio.Event()

    async def wait(_event: IndexEvent) -> None:
        await gate.wait()

    h = Harness(wait)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: len(h.handled) == 1)
    await asyncio.sleep(0.05)
    assert h.committed() is None  # the handler has not finished
    gate.set()
    await h.until(lambda: h.committed() == 1)
    await h.stop()


async def test_duplicate_messages_are_both_handled_and_committed(harness: Harness) -> None:
    harness.add("ITEM-1")
    harness.add("ITEM-1")
    harness.start()
    await harness.until(lambda: harness.committed() == 2)
    assert harness.handled  # handling twice is safe: the handler is idempotent (task T1.6)


async def test_empty_topic_just_waits(harness: Harness) -> None:
    harness.start()
    await asyncio.sleep(0.05)
    assert harness.committed() is None
    await harness.stop()


# --- bad messages ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "value", "reason"),
    [
        (b"ITEM-1", b"not json", "not valid JSON"),
        (b"ITEM-1", None, "empty message value"),
        (b"ITEM-1", b'{"schema_version": 9}', "schema v1 violation"),
        (b"OTHER", _event_bytes("ITEM-1"), "key does not match"),
        (None, _event_bytes("ITEM-1"), "key does not match"),
    ],
)
async def test_bad_messages_go_to_the_dlq_and_the_loop_continues(
    harness: Harness, key: bytes | None, value: bytes | None, reason: str
) -> None:
    harness.broker.produce(LIVE, key, value)
    harness.add("ITEM-GOOD")
    harness.start()
    await harness.until(lambda: harness.committed() == 2)
    [dead] = harness.broker.messages(DLQ)
    assert dead.value == value  # the original value, untouched
    assert reason in dead.headers[dlq.REASON].decode()
    assert dead.headers[dlq.ORIGIN] == f"{LIVE}/0/0".encode()
    assert [e.item_id for e in harness.handled] == ["ITEM-GOOD"]


async def test_dlq_headers_and_logs_never_hold_message_content() -> None:
    h = Harness()
    h.broker.produce(LIVE, b"ITEM-1", b'{"event_type": "synthetic-secret-value"}')
    h.start()
    with structlog.testing.capture_logs() as logs:
        await h.until(lambda: h.committed() == 1)
        await h.stop()
    assert "synthetic-secret-value" not in str(logs)
    [dead] = h.broker.messages(DLQ)
    assert b"synthetic-secret-value" not in b"".join(dead.headers.values())


# --- errors: quick retries, retry topic, DLQ ------------------------------------------------


async def test_quick_retries_fix_a_short_failure_without_the_retry_topic() -> None:
    attempts = 0

    async def flaky(_event: IndexEvent) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("synthetic failure")

    h = Harness(flaky)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    assert attempts == 3
    assert h.broker.messages(RETRY) == []
    await h.stop()


async def test_failure_after_quick_retries_goes_to_the_retry_topic_with_a_delay() -> None:
    h = Harness(_fail)
    original = h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    assert len(h.handled) == 3  # three quick attempts
    [retry] = h.broker.messages(RETRY)
    assert retry.key == b"ITEM-1"
    assert retry.value == original.value
    assert dlq.retry_count_of(retry.headers) == 1
    assert dlq.not_before_of(retry.headers) == h.clock.now + 60
    assert retry.headers[dlq.ERROR] == b"RuntimeError"
    assert h.broker.messages(DLQ) == []
    await h.stop()


async def test_retry_message_waits_for_its_delay() -> None:
    h = Harness()
    h.add("ITEM-1", topic=RETRY, headers={dlq.RETRY_COUNT: b"1", dlq.NOT_BEFORE: b"1030"})
    h.start()
    await asyncio.sleep(0.1)
    assert h.handled == []
    assert RETRY_TP in h.consumer.paused
    h.clock.now = 1031
    await h.until(lambda: h.committed(RETRY) == 1)
    assert [e.item_id for e in h.handled] == ["ITEM-1"]
    assert RETRY_TP not in h.consumer.paused
    await h.stop()


RETRY_TP = (RETRY, 0)


async def test_after_the_retry_limit_the_message_goes_to_the_dlq() -> None:
    h = Harness(_fail, max_retries=2)
    h.add("ITEM-1", topic=RETRY, headers={dlq.RETRY_COUNT: b"2", dlq.NOT_BEFORE: b"0"})
    h.start()
    await h.until(lambda: h.committed(RETRY) == 1)
    [dead] = h.broker.messages(DLQ)
    assert dead.headers[dlq.REASON] == b"retries exhausted"
    assert dead.headers[dlq.ERROR] == b"RuntimeError"
    assert h.broker.messages(RETRY) == [h.broker.messages(RETRY)[0]]  # only the input
    await h.stop()


async def test_non_retryable_error_goes_straight_to_the_dlq() -> None:
    async def permanent(_event: IndexEvent) -> None:
        raise NonRetryableError

    h = Harness(permanent)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    assert len(h.handled) == 1  # no quick retries
    [dead] = h.broker.messages(DLQ)
    assert dead.headers[dlq.REASON] == b"non-retryable error"
    assert b"NonRetryableError" in dead.headers[dlq.ERROR]
    assert h.broker.messages(RETRY) == []
    await h.stop()


async def test_handler_error_text_is_not_copied_into_headers() -> None:
    async def leaky(_event: IndexEvent) -> None:
        raise RuntimeError("synthetic document text")

    h = Harness(leaky, quick_retries=1)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    [retry] = h.broker.messages(RETRY)
    assert b"synthetic document text" not in b"".join(retry.headers.values())
    await h.stop()


async def test_offset_stays_uncommitted_when_the_retry_topic_is_unreachable() -> None:
    h = Harness(_fail)
    h.producer.fail_times = 1000
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.producer.fail_times < 1000)
    await asyncio.sleep(0.1)
    assert h.committed() is None  # not safe yet: it is read again after a restart
    assert h.broker.messages(RETRY) == []
    await h.stop()


async def test_publish_is_retried_before_giving_up() -> None:
    h = Harness(_fail)
    h.producer.fail_times = 2  # the third publish attempt works
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    assert len(h.broker.messages(RETRY)) == 1
    await h.stop()


# --- coalescing and order -------------------------------------------------------------------


def _tracked(event_type: str, arrival: int, seconds: int, version: int = 1) -> Tracked:
    value = _event_bytes("ITEM-1", event_type, version, seconds)
    message = KafkaMessage(LIVE, 0, arrival, b"ITEM-1", value)
    return Tracked(message, parse_event(value), 0, arrival)


def test_coalesce_prefers_the_last_upsert() -> None:
    work = coalesce([_tracked("UPSERT", 1, 0, 1), _tracked("UPSERT", 2, 1, 2)])
    assert work.winner.message.offset == 2
    assert len(work.members) == 2


def test_coalesce_last_delete_wins() -> None:
    work = coalesce([_tracked("UPSERT", 1, 0), _tracked("DELETE", 2, 1)])
    assert work.event.event_type is EventType.DELETE


def test_coalesce_upsert_after_delete_wins() -> None:
    work = coalesce([_tracked("DELETE", 1, 0), _tracked("UPSERT", 2, 1)])
    assert work.event.event_type is EventType.UPSERT


def test_coalesce_upsert_includes_a_permission_refresh() -> None:
    work = coalesce([_tracked("UPSERT", 1, 0), _tracked("ACL_CHANGE", 2, 1)])
    assert work.event.event_type is EventType.UPSERT


def test_coalesce_only_permission_changes_stay_a_permission_change() -> None:
    work = coalesce([_tracked("ACL_CHANGE", 1, 0), _tracked("ACL_CHANGE", 2, 1)])
    assert work.event.event_type is EventType.ACL_CHANGE


def test_coalesce_orders_by_event_time_not_by_arrival() -> None:
    # a delayed retry copy of an old UPSERT arrives after the newer DELETE
    work = coalesce([_tracked("DELETE", 1, 10), _tracked("UPSERT", 2, 0)])
    assert work.event.event_type is EventType.DELETE


async def test_waiting_events_of_one_item_are_handled_once() -> None:
    h = Harness()
    h.add("ITEM-1", version=1, seconds=0)
    h.add("ITEM-1", version=2, seconds=1)
    h.add("ITEM-1", version=3, seconds=2)
    h.start()
    await h.until(lambda: h.committed() == 3)  # all three are finished
    assert [e.doc_version for e in h.handled] == [3]
    await h.stop()


async def test_deletes_and_permission_changes_go_before_updates() -> None:
    h = Harness(max_in_flight=1)
    h.add("ITEM-A", "UPSERT")
    h.add("ITEM-B", "ACL_CHANGE")
    h.add("ITEM-C", "DELETE")
    h.start()
    await h.until(lambda: h.committed() == 3)
    assert [e.event_type for e in h.handled] == [
        EventType.DELETE,
        EventType.ACL_CHANGE,
        EventType.UPSERT,
    ]
    await h.stop()


async def test_one_item_is_never_handled_twice_at_the_same_time() -> None:
    gate = asyncio.Event()
    running = 0
    peak = 0

    async def slow(_event: IndexEvent) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await gate.wait()
        running -= 1

    h = Harness(slow)
    h.add("ITEM-1", version=1, seconds=0)
    h.start()
    await h.until(lambda: len(h.handled) == 1)
    h.add("ITEM-1", version=2, seconds=1)  # arrives in a later poll
    await asyncio.sleep(0.1)
    assert len(h.handled) == 1  # waits for the first
    gate.set()
    await h.until(lambda: h.committed() == 2)
    assert peak == 1
    await h.stop()


async def test_parallel_handling_is_bounded() -> None:
    running = 0
    peak = 0

    async def slow(_event: IndexEvent) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1

    h = Harness(slow, max_in_flight=2)
    for i in range(8):
        h.add(f"ITEM-{i}")
    h.start()
    await h.until(lambda: h.committed() == 8)
    assert peak == 2
    await h.stop()


# --- offsets with out-of-order completion ---------------------------------------------------


async def test_a_fast_message_does_not_commit_past_a_slow_one() -> None:
    gate = asyncio.Event()

    async def handler(event: IndexEvent) -> None:
        if event.item_id == "SLOW":
            await gate.wait()

    h = Harness(handler)
    h.add("SLOW")  # offset 0
    h.add("FAST")  # offset 1
    h.start()
    await h.until(lambda: len(h.handled) == 2)
    await asyncio.sleep(0.1)
    assert h.committed() is None  # offset 0 is still running, so nothing may be committed
    gate.set()
    await h.until(lambda: h.committed() == 2)
    await h.stop()


# --- shutdown, rebalance, outages -----------------------------------------------------------


async def test_shutdown_finishes_the_message_being_handled() -> None:
    gate = asyncio.Event()

    async def wait(_event: IndexEvent) -> None:
        await gate.wait()

    h = Harness(wait)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: len(h.handled) == 1)
    h.loop.stop()
    await asyncio.sleep(0.1)
    assert h.task is not None
    assert not h.task.done()  # waits for the handler
    gate.set()
    await asyncio.wait_for(h.task, timeout=3)
    assert h.committed() == 1
    assert h.consumer.closed
    assert h.producer.closed


async def test_shutdown_gives_up_on_a_stuck_handler_and_leaves_it_uncommitted() -> None:
    async def stuck(_event: IndexEvent) -> None:
        await asyncio.Event().wait()

    h = Harness(stuck, shutdown_timeout_s=0.05)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: len(h.handled) == 1)
    await h.stop()
    assert h.committed() is None  # read again after a restart
    assert h.consumer.closed


async def test_restart_continues_from_the_committed_offset() -> None:
    h = Harness()
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    await h.stop()
    h.add("ITEM-2")
    second = Harness()
    second.broker = h.broker
    second.consumer = FakeConsumer(h.broker, "group")
    second.loop = ConsumerLoop(
        consumer=second.consumer,
        producer=FakeProducer(h.broker),
        handler=second._handle,
        topics=[LIVE, RETRY],
        retry_topic=RETRY,
        dlq_topic=DLQ,
        settings=_settings(),
    )
    second.start()
    await second.until(lambda: h.committed() == 2)
    assert [e.item_id for e in second.handled] == ["ITEM-2"]  # ITEM-1 is not seen again
    await second.stop()


async def test_rebalance_finishes_running_work_and_commits_before_giving_partitions_up() -> None:
    gate = asyncio.Event()

    async def handler(event: IndexEvent) -> None:
        if event.item_id == "SLOW":
            await gate.wait()

    h = Harness(handler)
    h.add("FAST")
    h.add("SLOW")
    h.start()
    await h.until(lambda: len(h.handled) == 2)
    await h.until(lambda: h.committed() == 1)  # FAST is done, SLOW is running
    asyncio.get_running_loop().call_later(0.05, gate.set)
    await h.consumer.rebalance()
    assert h.committed() == 2  # SLOW finished before the partitions were released
    await asyncio.sleep(0.05)
    assert len(h.handled) == 2  # nothing was read again
    await h.stop()


async def test_rebalance_cancels_work_that_does_not_finish_and_it_is_read_again() -> None:
    first_call = True

    async def handler(_event: IndexEvent) -> None:
        nonlocal first_call
        if first_call:
            first_call = False
            await asyncio.Event().wait()

    h = Harness(handler, rebalance_timeout_s=0.05)
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: len(h.handled) == 1)
    await h.consumer.rebalance()
    assert h.committed() is None  # not finished, so not committed
    await h.until(lambda: h.committed() == 1)  # the new owner handles it
    assert len(h.handled) == 2
    await h.stop()


async def test_the_loop_survives_a_kafka_outage() -> None:
    h = Harness()
    h.consumer.fail_poll_times = 3
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    await h.stop()


async def test_a_failed_commit_is_tried_again() -> None:
    h = Harness()
    h.consumer.fail_commit_times = 3
    h.add("ITEM-1")
    h.start()
    await h.until(lambda: h.committed() == 1)
    await h.stop()


# --- dlq helpers ----------------------------------------------------------------------------


def test_error_text_hides_messages_except_our_own() -> None:
    assert dlq.safe_error_text(RuntimeError("synthetic secret")) == "RuntimeError"
    assert dlq.safe_error_text(NonRetryableError()) == (
        "NonRetryableError: Non-retryable processing error"
    )


@pytest.mark.parametrize(("raw", "expected"), [(b"3", 3), (b"-1", 0), (b"x", 0)])
def test_retry_count_from_headers(raw: bytes, expected: int) -> None:
    assert dlq.retry_count_of({dlq.RETRY_COUNT: raw}) == expected
    assert dlq.retry_count_of({}) == 0


def test_origin_is_kept_across_retries() -> None:
    first = KafkaMessage(LIVE, 2, 7, b"k", b"v")
    again = KafkaMessage(RETRY, 0, 1, b"k", b"v", {dlq.ORIGIN: dlq.origin_of(first).encode()})
    assert dlq.origin_of(again) == "events/2/7"
