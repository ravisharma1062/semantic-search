"""The small Kafka surface the consumer loop needs.

The loop talks only to these protocols. ``kafka_client.py`` implements them with
confluent-kafka, and ``tests/fakes/kafka.py`` implements them in memory.
"""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

TopicPartition = tuple[str, int]

# Called when partitions are taken away. It returns the offsets to commit for them
# (the next offset to read per partition) before the partitions are released.
RevokeCallback = Callable[[Sequence[TopicPartition]], Awaitable[Mapping[TopicPartition, int]]]


@dataclass(frozen=True)
class KafkaMessage:
    """One consumed message."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Mapping[str, bytes] = field(default_factory=dict)

    @property
    def tp(self) -> TopicPartition:
        """Topic and partition."""
        return (self.topic, self.partition)


class MessageConsumer(Protocol):
    """A consumer-group member. Offsets are committed by hand only."""

    async def subscribe(self, topics: Sequence[str], on_revoke: RevokeCallback) -> None:
        """Join the group for ``topics``."""
        ...

    async def poll(self, max_messages: int, timeout_s: float) -> list[KafkaMessage]:
        """Return up to ``max_messages`` messages, or an empty list after ``timeout_s``."""
        ...

    async def commit(self, offsets: Mapping[TopicPartition, int]) -> None:
        """Commit the next offset to read per partition."""
        ...

    async def pause(self, partitions: Sequence[TopicPartition]) -> None:
        """Stop fetching from these partitions."""
        ...

    async def resume(self, partitions: Sequence[TopicPartition]) -> None:
        """Start fetching from these partitions again."""
        ...

    async def close(self) -> None:
        """Leave the group."""
        ...


class MessageProducer(Protocol):
    """Publishes messages and waits until the broker has them."""

    async def send(
        self, topic: str, key: bytes | None, value: bytes | None, headers: Mapping[str, bytes]
    ) -> None:
        """Publish one message. Raises ``UpstreamUnavailableError`` if it was not delivered."""
        ...

    async def close(self) -> None:
        """Flush and close."""
        ...
