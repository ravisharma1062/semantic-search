"""The consumer loop against a real Kafka broker."""

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import cast

import pytest
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka import TopicPartition as KafkaTopicPartition

from app.core.settings import ConsumerSettings, KafkaSettings
from app.ingestion import dlq
from app.ingestion.consumer import ConsumerLoop
from app.ingestion.events import IndexEvent
from app.ingestion.kafka_client import ConfluentConsumer, ConfluentProducer
from tests.integration.conftest import Topics

pytestmark = pytest.mark.integration

Handler = Callable[[IndexEvent], Awaitable[None]]


def _settings(**changes: object) -> ConsumerSettings:
    values: dict[str, object] = {
        "poll_timeout_s": 0.2,
        "commit_interval_s": 0.1,
        "quick_retries": 1,
        "quick_retry_initial_delay_s": 0,
        "retry_delay_s": 0.5,
        "max_retries": 2,
        "shutdown_timeout_s": 5,
        "rebalance_timeout_s": 10,
    }
    return ConsumerSettings(**{**values, **changes})


def _event(item_id: str, version: int = 1) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": uuid.uuid4().hex,
            "event_type": "UPSERT",
            "item_id": item_id,
            "doc_version": version,
            "occurred_at": datetime.now(UTC).isoformat(),
            "source": "integration-test",
        }
    ).encode()


def _produce(bootstrap: str, topic: str, messages: list[tuple[str, bytes]]) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    for key, value in messages:
        producer.produce(topic, key=key.encode(), value=value)
    assert producer.flush(30) == 0


def _read_all(
    bootstrap: str, topic: str, expected: int, wait_s: float = 20
) -> list[tuple[bytes, dict[str, bytes]]]:
    """Read a topic from the start with a throw-away consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"reader-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    found: list[tuple[bytes, dict[str, bytes]]] = []
    deadline = time.monotonic() + wait_s
    while len(found) < expected and time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        raw_headers = cast(list[tuple[str, bytes | str | None]], message.headers() or [])
        headers = {k: v for k, v in raw_headers if isinstance(v, bytes)}
        found.append((message.value() or b"", headers))
    consumer.close()
    return found


def _committed_total(bootstrap: str, group: str, topic: str, partitions: int = 4) -> int:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group})
    try:
        offsets = consumer.committed(
            [KafkaTopicPartition(topic, p) for p in range(partitions)], timeout=10
        )
    except KafkaException:
        return 0
    finally:
        consumer.close()
    return sum(max(0, o.offset) for o in offsets)


def _loop(
    kafka: KafkaSettings, topics: Topics, handler: Handler, **settings: object
) -> ConsumerLoop:
    config = _settings(**settings)
    return ConsumerLoop(
        consumer=ConfluentConsumer(kafka, topics.group, config.rebalance_timeout_s),
        producer=ConfluentProducer(kafka),
        handler=handler,
        topics=[topics.live, topics.retry],
        retry_topic=topics.retry,
        dlq_topic=topics.dlq,
        settings=config,
    )


async def _until(condition: Callable[[], bool], seconds: float = 30) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.1)


async def test_events_are_handled_and_offsets_committed_after_success(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    handled: list[str] = []

    async def handler(event: IndexEvent) -> None:
        handled.append(event.item_id)

    _produce(kafka_bootstrap, topics.live, [(f"ITEM-{i}", _event(f"ITEM-{i}")) for i in range(20)])
    loop = _loop(kafka_settings, topics, handler)
    task = asyncio.create_task(loop.run())
    await _until(lambda: len(handled) == 20)
    loop.stop()
    await asyncio.wait_for(task, timeout=30)
    assert sorted(handled) == sorted(f"ITEM-{i}" for i in range(20))
    assert _committed_total(kafka_bootstrap, topics.group, topics.live) == 20


async def test_duplicate_messages_are_handled_idempotently(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    indexed: dict[str, int] = {}

    async def handler(event: IndexEvent) -> None:  # idempotent: a set of results, not a counter
        indexed[event.item_id] = event.doc_version or 0

    message = _event("ITEM-1", version=3)
    _produce(kafka_bootstrap, topics.live, [("ITEM-1", message), ("ITEM-1", message)])
    loop = _loop(kafka_settings, topics, handler)
    task = asyncio.create_task(loop.run())
    await _until(lambda: _committed_total(kafka_bootstrap, topics.group, topics.live) == 2)
    loop.stop()
    await asyncio.wait_for(task, timeout=30)
    assert indexed == {"ITEM-1": 3}


async def test_poison_message_goes_to_the_dlq_and_does_not_block_the_partition(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    handled: list[str] = []

    async def handler(event: IndexEvent) -> None:
        handled.append(event.item_id)

    _produce(
        kafka_bootstrap,
        topics.live,
        [
            ("ITEM-1", b"this is not json"),
            ("ITEM-1", _event("ITEM-1")),
            ("ITEM-2", _event("ITEM-2")),
        ],
    )
    loop = _loop(kafka_settings, topics, handler)
    task = asyncio.create_task(loop.run())
    await _until(lambda: sorted(handled) == ["ITEM-1", "ITEM-2"])
    loop.stop()
    await asyncio.wait_for(task, timeout=30)
    [(value, headers)] = await asyncio.to_thread(_read_all, kafka_bootstrap, topics.dlq, 1)
    assert value == b"this is not json"
    assert b"not valid JSON" in headers[dlq.REASON]
    assert _committed_total(kafka_bootstrap, topics.group, topics.live) == 3


async def test_failure_goes_through_the_retry_topic_and_succeeds_later(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    calls = 0

    async def flaky(_event: IndexEvent) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("synthetic failure")

    _produce(kafka_bootstrap, topics.live, [("ITEM-1", _event("ITEM-1"))])
    loop = _loop(kafka_settings, topics, flaky)
    task = asyncio.create_task(loop.run())
    await _until(lambda: calls == 2)  # first attempt, then the copy from the retry topic
    await _until(lambda: _committed_total(kafka_bootstrap, topics.group, topics.retry) == 1)
    loop.stop()
    await asyncio.wait_for(task, timeout=30)
    [(_, headers)] = await asyncio.to_thread(_read_all, kafka_bootstrap, topics.retry, 1)
    assert dlq.retry_count_of(headers) == 1
    assert await asyncio.to_thread(_read_all, kafka_bootstrap, topics.dlq, 1, 2) == []


async def test_message_that_always_fails_ends_in_the_dlq_after_the_retry_limit(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    calls = 0

    async def always_fails(_event: IndexEvent) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("synthetic failure")

    _produce(kafka_bootstrap, topics.live, [("ITEM-1", _event("ITEM-1"))])
    loop = _loop(kafka_settings, topics, always_fails, max_retries=2, retry_delay_s=0.2)
    task = asyncio.create_task(loop.run())
    [(_, headers)] = await asyncio.to_thread(_read_all, kafka_bootstrap, topics.dlq, 1, 40)
    loop.stop()
    await asyncio.wait_for(task, timeout=30)
    assert calls == 3  # first try and two retries
    assert headers[dlq.REASON] == b"retries exhausted"
    assert headers[dlq.ERROR] == b"RuntimeError"
    assert dlq.retry_count_of(headers) == 2


async def test_unfinished_work_is_read_again_after_a_restart(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    first_run: list[str] = []

    async def stuck(event: IndexEvent) -> None:  # the worker "crashes" while handling this
        first_run.append(event.item_id)
        await asyncio.Event().wait()

    _produce(kafka_bootstrap, topics.live, [("ITEM-1", _event("ITEM-1"))])
    loop = _loop(kafka_settings, topics, stuck, shutdown_timeout_s=0.2)
    task = asyncio.create_task(loop.run())
    await _until(lambda: first_run == ["ITEM-1"])
    loop.stop()
    await asyncio.wait_for(task, timeout=30)
    assert _committed_total(kafka_bootstrap, topics.group, topics.live) == 0  # nothing was lost

    second_run: list[str] = []

    async def works(event: IndexEvent) -> None:
        second_run.append(event.item_id)

    loop = _loop(kafka_settings, topics, works)
    task = asyncio.create_task(loop.run())
    await _until(lambda: second_run == ["ITEM-1"])
    await _until(lambda: _committed_total(kafka_bootstrap, topics.group, topics.live) == 1)
    loop.stop()
    await asyncio.wait_for(task, timeout=30)


async def test_rebalance_loses_no_message(
    kafka_bootstrap: str, kafka_settings: KafkaSettings, topics: Topics
) -> None:
    seen: set[str] = set()

    async def handler(event: IndexEvent) -> None:
        await asyncio.sleep(0.01)
        seen.add(event.item_id)

    first_batch = [f"ITEM-{i}" for i in range(60)]
    _produce(kafka_bootstrap, topics.live, [(i, _event(i)) for i in first_batch])
    loop_a = _loop(kafka_settings, topics, handler)
    task_a = asyncio.create_task(loop_a.run())
    await _until(lambda: len(seen) >= 5)

    loop_b = _loop(kafka_settings, topics, handler)  # joins the group: partitions move
    task_b = asyncio.create_task(loop_b.run())
    second_batch = [f"ITEM-{i}" for i in range(60, 120)]
    _produce(kafka_bootstrap, topics.live, [(i, _event(i)) for i in second_batch])

    await _until(lambda: len(seen) == 120, seconds=60)
    await _until(lambda: _committed_total(kafka_bootstrap, topics.group, topics.live) == 120)
    loop_a.stop()
    loop_b.stop()
    await asyncio.wait_for(asyncio.gather(task_a, task_b), timeout=40)
    assert seen == set(first_batch + second_batch)
