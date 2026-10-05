"""Offset tracking for out-of-order completion.

Messages finish in any order, but an offset may be committed only when every earlier message
of the partition has finished. Otherwise a crash would lose the slow message.
"""

from collections import deque
from collections.abc import Collection, Mapping

from app.ingestion.kafka_io import TopicPartition


class OffsetTracker:
    """Tracks received and finished offsets per partition."""

    def __init__(self) -> None:
        self._received: dict[TopicPartition, deque[int]] = {}
        self._done: dict[TopicPartition, set[int]] = {}
        self._next_commit: dict[TopicPartition, int] = {}
        self._dirty: set[TopicPartition] = set()

    def register(self, tp: TopicPartition, offset: int) -> None:
        """A message was received and is now being handled."""
        self._received.setdefault(tp, deque()).append(offset)
        self._done.setdefault(tp, set())

    def complete(self, tp: TopicPartition, offset: int) -> None:
        """The message is finished (handled, or safely moved to the retry topic or DLQ)."""
        received = self._received.get(tp)
        if received is None:
            return  # the partition was revoked meanwhile
        self._done[tp].add(offset)
        while received and received[0] in self._done[tp]:
            finished = received.popleft()
            self._done[tp].discard(finished)
            self._next_commit[tp] = finished + 1
            self._dirty.add(tp)

    def committable(self) -> dict[TopicPartition, int]:
        """The next offset to read, for partitions that moved since the last commit."""
        return {tp: self._next_commit[tp] for tp in self._dirty}

    def mark_committed(self, offsets: Mapping[TopicPartition, int]) -> None:
        """These offsets were committed. Forget them unless the partition moved on again."""
        for tp, offset in offsets.items():
            if self._next_commit.get(tp) == offset:
                self._dirty.discard(tp)

    def pending(self, tp: TopicPartition) -> int:
        """Number of received messages that are not finished."""
        return len(self._received.get(tp, ()))

    def drop(self, partitions: Collection[TopicPartition]) -> dict[TopicPartition, int]:
        """Forget revoked partitions. Returns the offsets that still had to be committed."""
        final = {tp: self._next_commit[tp] for tp in partitions if tp in self._dirty}
        for tp in partitions:
            self._received.pop(tp, None)
            self._done.pop(tp, None)
            self._next_commit.pop(tp, None)
            self._dirty.discard(tp)
        return final
