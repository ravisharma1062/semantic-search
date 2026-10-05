"""confluent-kafka implementations of ``MessageConsumer`` and ``MessageProducer``.

The client is blocking. Every call runs on one dedicated thread, so the event loop is never
blocked and the (not thread-safe) client is only used from that thread. The rebalance callback
runs on that thread inside ``poll`` and hands over to the event loop.
"""

import asyncio
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, TypeVar, cast

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka import TopicPartition as KafkaTopicPartition

from app.core.errors import UpstreamUnavailableError
from app.core.settings import KafkaSettings
from app.ingestion.kafka_io import KafkaMessage, RevokeCallback, TopicPartition

T = TypeVar("T")

_log = structlog.get_logger(__name__)


class ConfluentConsumer:
    """A consumer-group member with manual commits."""

    def __init__(self, settings: KafkaSettings, group_id: str, rebalance_timeout_s: float) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "socket.timeout.ms": int(settings.request_timeout_s * 1000),
            }
        )
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-consumer")
        self._rebalance_timeout_s = rebalance_timeout_s
        self._loop: asyncio.AbstractEventLoop | None = None

    async def _run(self, function: Callable[..., T], *args: Any) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, partial(function, *args))

    async def subscribe(self, topics: Sequence[str], on_revoke: RevokeCallback) -> None:
        """Join the group. ``on_revoke`` runs before partitions are released."""
        loop = asyncio.get_running_loop()
        self._loop = loop

        def revoked(consumer: Consumer, partitions: list[KafkaTopicPartition]) -> None:
            tps = [(p.topic, p.partition) for p in partitions]

            async def release() -> Mapping[TopicPartition, int]:
                return await on_revoke(tps)

            future = asyncio.run_coroutine_threadsafe(release(), loop)
            try:
                offsets = future.result(timeout=self._rebalance_timeout_s + 5)
            except Exception as exc:  # the group moves on, the work is read again by the new owner
                _log.error("revoke_failed", error_type=type(exc).__name__)
                return
            if offsets:
                self._commit_now(consumer, offsets)

        await self._run(partial(self._consumer.subscribe, list(topics), on_revoke=revoked))

    @staticmethod
    def _commit_now(consumer: Consumer, offsets: Mapping[TopicPartition, int]) -> None:
        try:
            consumer.commit(
                offsets=[KafkaTopicPartition(t, p, o) for (t, p), o in offsets.items()],
                asynchronous=False,
            )
        except KafkaException as exc:
            raise UpstreamUnavailableError("Kafka commit failed") from exc

    def _poll(self, max_messages: int, timeout_s: float) -> list[KafkaMessage]:
        result: list[KafkaMessage] = []
        for message in self._consumer.consume(num_messages=max_messages, timeout=timeout_s):
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                if error.fatal():
                    raise UpstreamUnavailableError("Kafka consumer failed")
                _log.warning("consume_error", code=str(error.code()))
                continue
            raw_headers = cast(Sequence[tuple[str, str | bytes | None]], message.headers() or [])
            headers = {
                name: value if isinstance(value, bytes) else value.encode()
                for name, value in raw_headers
                if value is not None
            }
            result.append(
                KafkaMessage(
                    topic=cast(str, message.topic()),
                    partition=cast(int, message.partition()),
                    offset=cast(int, message.offset()),
                    key=message.key(),
                    value=message.value(),
                    headers=headers,
                )
            )
        return result

    async def poll(self, max_messages: int, timeout_s: float) -> list[KafkaMessage]:
        """Fetch a batch of messages."""
        try:
            return await self._run(self._poll, max_messages, timeout_s)
        except KafkaException as exc:
            raise UpstreamUnavailableError("Kafka poll failed") from exc

    async def commit(self, offsets: Mapping[TopicPartition, int]) -> None:
        """Commit the next offset to read per partition."""
        await self._run(self._commit_now, self._consumer, offsets)

    async def pause(self, partitions: Sequence[TopicPartition]) -> None:
        """Stop fetching from the partitions."""
        await self._run(self._set_paused, partitions, True)

    async def resume(self, partitions: Sequence[TopicPartition]) -> None:
        """Fetch from the partitions again."""
        await self._run(self._set_paused, partitions, False)

    def _set_paused(self, partitions: Sequence[TopicPartition], paused: bool) -> None:
        targets = [KafkaTopicPartition(t, p) for t, p in partitions]
        try:
            (self._consumer.pause if paused else self._consumer.resume)(targets)
        except KafkaException:
            _log.warning("pause_resume_failed", paused=paused)  # partition no longer assigned

    async def close(self) -> None:
        """Leave the group and stop the thread."""
        await self._run(self._consumer.close)
        self._executor.shutdown(wait=False)


class ConfluentProducer:
    """An idempotent producer that waits for the broker's acknowledgement."""

    def __init__(self, settings: KafkaSettings) -> None:
        self._timeout_s = settings.producer_timeout_s
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": int(settings.producer_timeout_s * 1000),
                "socket.timeout.ms": int(settings.request_timeout_s * 1000),
            }
        )
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-producer")

    def _send(
        self, topic: str, key: bytes | None, value: bytes | None, headers: Mapping[str, bytes]
    ) -> None:
        failure: list[object] = []

        def delivered(error: object, _message: object) -> None:
            if error is not None:
                failure.append(error)

        try:
            self._producer.produce(
                topic,
                key=key,
                value=value,
                headers=list(headers.items()),
                on_delivery=delivered,
            )
            remaining = self._producer.flush(self._timeout_s)
        except (BufferError, KafkaException) as exc:
            raise UpstreamUnavailableError("Kafka publish failed") from exc
        if remaining or failure:
            raise UpstreamUnavailableError("Kafka publish failed")

    async def send(
        self, topic: str, key: bytes | None, value: bytes | None, headers: Mapping[str, bytes]
    ) -> None:
        """Publish one message and wait for the acknowledgement."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._send, topic, key, value, headers)

    async def close(self) -> None:
        """Flush what is left and stop the thread."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, partial(self._producer.flush, self._timeout_s))
        self._executor.shutdown(wait=False)
