"""In-memory Kafka: a broker, a consumer-group member and a producer.

Behaves like the real thing where the consumer loop depends on it: per-partition order,
manual commits, redelivery from the committed offset, pause and resume, and a revoke callback.
"""

import asyncio
import zlib
from collections.abc import Mapping, Sequence

from app.core.errors import UpstreamUnavailableError
from app.ingestion.kafka_io import KafkaMessage, RevokeCallback, TopicPartition


class FakeBroker:
    """Topics, partitions and committed offsets per group."""

    def __init__(self, partitions: int = 1) -> None:
        self._default_partitions = partitions
        self.logs: dict[TopicPartition, list[KafkaMessage]] = {}
        self.committed: dict[tuple[str, TopicPartition], int] = {}
        self._topics: dict[str, int] = {}

    def create_topic(self, topic: str, partitions: int | None = None) -> None:
        """Create the topic with its partitions."""
        count = partitions or self._default_partitions
        self._topics[topic] = count
        for partition in range(count):
            self.logs.setdefault((topic, partition), [])

    def partitions_of(self, topic: str) -> list[TopicPartition]:
        """All partitions of a topic."""
        return [(topic, p) for p in range(self._topics[topic])]

    def produce(
        self,
        topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: Mapping[str, bytes] | None = None,
    ) -> KafkaMessage:
        """Append a message. The partition comes from the key, as with the real client."""
        if topic not in self._topics:
            self.create_topic(topic)
        partition = zlib.crc32(key or b"") % self._topics[topic]
        log = self.logs[(topic, partition)]
        message = KafkaMessage(topic, partition, len(log), key, value, dict(headers or {}))
        log.append(message)
        return message

    def messages(self, topic: str) -> list[KafkaMessage]:
        """All messages of a topic, by partition and offset."""
        return [m for tp in self.partitions_of(topic) for m in self.logs[tp]]


class FakeConsumer:
    """One member of a group that owns all partitions of its topics."""

    def __init__(self, broker: FakeBroker, group: str) -> None:
        self.broker = broker
        self.group = group
        self.closed = False
        self.fail_poll_times = 0
        self.fail_commit_times = 0
        self.commits: list[dict[TopicPartition, int]] = []
        self.paused: set[TopicPartition] = set()
        self._positions: dict[TopicPartition, int] = {}
        self._on_revoke: RevokeCallback | None = None

    @property
    def assigned(self) -> list[TopicPartition]:
        """Partitions this member reads."""
        return list(self._positions)

    async def subscribe(self, topics: Sequence[str], on_revoke: RevokeCallback) -> None:
        """Take all partitions of the topics, starting at the committed offsets."""
        self._on_revoke = on_revoke
        for topic in topics:
            for tp in self.broker.partitions_of(topic):
                self._positions[tp] = self.broker.committed.get((self.group, tp), 0)

    async def poll(self, max_messages: int, timeout_s: float) -> list[KafkaMessage]:
        """Messages after the current positions."""
        if self.fail_poll_times:
            self.fail_poll_times -= 1
            raise UpstreamUnavailableError("poll failed")
        batch: list[KafkaMessage] = []
        for tp, position in self._positions.items():
            if tp in self.paused:
                continue
            log = self.broker.logs[tp]
            take = log[position : position + max(0, max_messages - len(batch))]
            batch.extend(take)
            self._positions[tp] = position + len(take)
        if not batch:
            await asyncio.sleep(min(timeout_s, 0.002))
        return batch

    async def commit(self, offsets: Mapping[TopicPartition, int]) -> None:
        """Store the offsets for the group."""
        if self.fail_commit_times:
            self.fail_commit_times -= 1
            raise UpstreamUnavailableError("commit failed")
        self.commits.append(dict(offsets))
        for tp, offset in offsets.items():
            self.broker.committed[(self.group, tp)] = offset

    async def pause(self, partitions: Sequence[TopicPartition]) -> None:
        """Stop returning messages of these partitions."""
        self.paused.update(partitions)

    async def resume(self, partitions: Sequence[TopicPartition]) -> None:
        """Return messages of these partitions again."""
        self.paused.difference_update(partitions)

    async def close(self) -> None:
        """Leave the group."""
        self.closed = True

    async def rebalance(self) -> None:
        """Take all partitions away, then give them back from the committed offsets.

        Like the real client: the revoke callback returns offsets, they are committed, and
        everything that was not committed is delivered again.
        """
        assert self._on_revoke is not None
        offsets = await self._on_revoke(self.assigned)
        if offsets:
            await self.commit(offsets)
        for tp in self._positions:
            self._positions[tp] = self.broker.committed.get((self.group, tp), 0)
        self.paused.clear()


class FakeProducer:
    """Appends to the broker. Can be told to fail a number of times."""

    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.closed = False
        self.fail_times = 0
        self.sent: list[KafkaMessage] = []

    async def send(
        self, topic: str, key: bytes | None, value: bytes | None, headers: Mapping[str, bytes]
    ) -> None:
        """Publish one message."""
        if self.fail_times:
            self.fail_times -= 1
            raise UpstreamUnavailableError("publish failed")
        self.sent.append(self.broker.produce(topic, key, value, headers))

    async def send_batch(
        self, topic: str, items: Sequence[tuple[bytes | None, bytes | None]]
    ) -> None:
        """Publish many messages. All or nothing, like one acknowledged batch."""
        if self.fail_times:
            self.fail_times -= 1
            raise UpstreamUnavailableError("publish failed")
        for key, value in items:
            self.sent.append(self.broker.produce(topic, key, value))

    async def close(self) -> None:
        """Nothing to flush."""
        self.closed = True
