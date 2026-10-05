"""The indexing consumer loop (HLD section 15, Kafka design).

- An offset is committed only after the handler succeeded, or the message was safely handed to
  the retry topic or the DLQ (rule 3). Offsets move up only when all earlier messages finished.
- Handler errors: quick retries in process, then the retry topic with a delay, then the DLQ.
- Events for one ``item_id`` that wait together are coalesced and handled once.
- Delete and permission events go first when work is queued.
- On shutdown, and on a rebalance, the messages being handled are finished before the offsets
  are committed.

Logs carry IDs and counts only (rule 2).
"""

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

import structlog

from app.core.errors import NonRetryableError, UpstreamUnavailableError
from app.core.retry import RetryPolicy, with_retries
from app.core.settings import ConsumerSettings
from app.ingestion import dlq
from app.ingestion.events import EventType, IndexEvent, InvalidEventError, parse_event
from app.ingestion.kafka_io import (
    KafkaMessage,
    MessageConsumer,
    MessageProducer,
    TopicPartition,
)
from app.ingestion.offsets import OffsetTracker


@dataclass(frozen=True)
class HandlerContext:
    """What the handler may need to know about this attempt."""

    retry_count: int  # how often the event already went through the retry topic
    max_retries: int

    @property
    def retries_exhausted(self) -> bool:
        """True when a failure now would send the event to the DLQ."""
        return self.retry_count >= self.max_retries


EventHandler = Callable[[IndexEvent, HandlerContext], Awaitable[None]]

_log = structlog.get_logger(__name__)

# Deletes and permission changes go before normal updates (HLD section 17).
_PRIORITY = {EventType.DELETE: 0, EventType.ACL_CHANGE: 1, EventType.UPSERT: 2}


@dataclass(frozen=True)
class Tracked:
    """A valid message with its parsed event."""

    message: KafkaMessage
    event: IndexEvent
    retry_count: int
    arrival: int


@dataclass(frozen=True)
class Work:
    """One unit of work: the winning event and all messages it stands for."""

    winner: Tracked
    members: tuple[Tracked, ...]

    @property
    def event(self) -> IndexEvent:
        """The event the handler gets."""
        return self.winner.event

    @property
    def retry_count(self) -> int:
        """The highest retry count of the members."""
        return max(m.retry_count for m in self.members)

    @property
    def partitions(self) -> set[TopicPartition]:
        """Partitions the members came from."""
        return {m.message.tp for m in self.members}


def coalesce(group: Sequence[Tracked]) -> Work:
    """Merge events of one ``item_id`` into one unit of work.

    Order is by ``doc_version`` when all events have one (a late DELETE of an old version must not
    beat a newer UPSERT), otherwise by event time, then arrival. A last DELETE wins. Otherwise
    the events after the last DELETE count: an UPSERT includes a permission refresh, so it wins
    over ACL_CHANGE.
    """
    versioned = all(t.event.doc_version is not None for t in group)
    ordered = sorted(
        group,
        key=lambda t: (
            t.event.doc_version if versioned and t.event.doc_version is not None else 0,
            t.event.occurred_at,
            t.arrival,
        ),
    )
    last_delete = max(
        (i for i, t in enumerate(ordered) if t.event.event_type is EventType.DELETE), default=-1
    )
    if last_delete == len(ordered) - 1:
        return Work(ordered[-1], tuple(ordered))
    tail = ordered[last_delete + 1 :]
    upserts = [t for t in tail if t.event.event_type is EventType.UPSERT]
    winner = upserts[-1] if upserts else tail[-1]
    return Work(winner, tuple(ordered))


class _ItemLocks:
    """One lock per ``item_id``, so a document is never handled twice at the same time."""

    def __init__(self) -> None:
        self._locks: dict[str, tuple[asyncio.Lock, int]] = {}

    async def run(self, item_id: str, work: Callable[[], Awaitable[bool]]) -> bool:
        lock, users = self._locks.get(item_id, (asyncio.Lock(), 0))
        self._locks[item_id] = (lock, users + 1)
        try:
            async with lock:
                return await work()
        finally:
            lock, users = self._locks[item_id]
            if users <= 1:
                del self._locks[item_id]
            else:
                self._locks[item_id] = (lock, users - 1)


class ConsumerLoop:
    """Consumes events and calls the handler. Call ``run()``, and ``stop()`` to end it."""

    def __init__(
        self,
        *,
        consumer: MessageConsumer,
        producer: MessageProducer,
        handler: EventHandler,
        topics: Sequence[str],
        retry_topic: str,
        dlq_topic: str,
        settings: ConsumerSettings,
        clock: Callable[[], float] = time.time,  # wall time, for the not-before header
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._handler = handler
        self._topics = list(topics)
        self._retry_topic = retry_topic
        self._dlq_topic = dlq_topic
        self._settings = settings
        self._clock = clock
        self._sleep = sleep
        self._policy = RetryPolicy(
            attempts=settings.quick_retries,
            initial_delay_s=settings.quick_retry_initial_delay_s,
            max_delay_s=settings.quick_retry_initial_delay_s * 20,
            jitter_s=settings.quick_retry_initial_delay_s,
        )
        self._tracker = OffsetTracker()
        self._locks = _ItemLocks()
        self._semaphore = asyncio.Semaphore(settings.max_in_flight)
        self._tasks: dict[asyncio.Task[None], Work] = {}
        self._delayed: dict[TopicPartition, deque[Tracked]] = {}
        self._stop = asyncio.Event()
        self._arrival = 0
        self._last_commit = time.monotonic()

    def stop(self) -> None:
        """Ask the loop to finish the messages it is handling and exit."""
        self._stop.set()

    async def run(self) -> None:
        """Consume until ``stop()`` is called."""
        await self._consumer.subscribe(self._topics, self._on_revoke)
        _log.info("consumer_started", topics=self._topics)
        try:
            while not self._stop.is_set():
                await self._cycle()
        finally:
            await self._shutdown()

    # --- one pass of the loop -------------------------------------------------------------

    async def _cycle(self) -> None:
        await self._wait_for_capacity()
        if self._stop.is_set():
            return
        try:
            messages = await self._consumer.poll(
                self._settings.poll_batch_size, self._settings.poll_timeout_s
            )
        except UpstreamUnavailableError:
            _log.warning("poll_failed")
            await self._sleep(self._settings.poll_timeout_s)
            messages = []
        ready = await self._ingest(messages)
        ready += await self._release_due()
        self._dispatch(ready)
        await self._maybe_commit()

    async def _wait_for_capacity(self) -> None:
        """Do not fetch more while all handler slots are busy."""
        while len(self._tasks) >= self._settings.max_in_flight and not self._stop.is_set():
            await asyncio.wait(
                list(self._tasks),
                timeout=self._settings.poll_timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            await self._maybe_commit()

    async def _ingest(self, messages: Sequence[KafkaMessage]) -> list[Tracked]:
        ready: list[Tracked] = []
        for message in messages:
            self._tracker.register(message.tp, message.offset)
            try:
                event = parse_event(message.value)
                self._check_key(message, event)
            except InvalidEventError as exc:
                await self._dead_letter_invalid(message, str(exc))
                continue
            self._arrival += 1
            tracked = Tracked(message, event, dlq.retry_count_of(message.headers), self._arrival)
            held = self._delayed.get(message.tp)
            if held or self._not_due(tracked):
                self._delayed.setdefault(message.tp, deque()).append(tracked)
                await self._consumer.pause([message.tp])
            else:
                ready.append(tracked)
        return ready

    @staticmethod
    def _check_key(message: KafkaMessage, event: IndexEvent) -> None:
        """The Kafka key is the ITEM_ID: ordering per document depends on it."""
        if message.key is None or message.key.decode(errors="replace") != event.item_id:
            raise InvalidEventError("message key does not match item_id")

    def _not_due(self, tracked: Tracked) -> bool:
        if tracked.message.topic != self._retry_topic:
            return False
        return dlq.not_before_of(tracked.message.headers) > self._clock()

    async def _release_due(self) -> list[Tracked]:
        """Messages of the retry topic whose delay is over."""
        ready: list[Tracked] = []
        for tp in list(self._delayed):
            queue = self._delayed[tp]
            while queue and not self._not_due(queue[0]):
                ready.append(queue.popleft())
            if not queue:
                del self._delayed[tp]
                await self._consumer.resume([tp])
        return ready

    def _dispatch(self, ready: Sequence[Tracked]) -> None:
        groups: dict[str, list[Tracked]] = {}
        for tracked in ready:
            groups.setdefault(tracked.event.item_id, []).append(tracked)
        works = [coalesce(group) for group in groups.values()]
        works.sort(key=lambda w: _PRIORITY[w.event.event_type])
        for work in works:
            if len(work.members) > 1:
                _log.debug("events_coalesced", item_id=work.event.item_id, count=len(work.members))
            task = asyncio.create_task(self._run_work(work))
            self._tasks[task] = work
            task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.pop(task, None)
        if not task.cancelled() and task.exception() is not None:
            # Offsets of this work stay uncommitted, so it is read again after a restart.
            _log.error("work_crashed", error_type=type(task.exception()).__name__)

    # --- handling one unit of work ----------------------------------------------------------

    async def _run_work(self, work: Work) -> None:
        async def attempt() -> bool:
            async with self._semaphore:
                return await self._execute(work)

        routed = await self._locks.run(work.event.item_id, attempt)
        if routed:
            for member in work.members:
                self._tracker.complete(member.message.tp, member.message.offset)

    async def _execute(self, work: Work) -> bool:
        """Run the handler. True if the work is finished or safely handed on."""
        event = work.event
        context = HandlerContext(work.retry_count, self._settings.max_retries)
        try:
            await with_retries(
                lambda: self._handler(event, context),
                policy=self._policy,
                retry_if=lambda exc: not isinstance(exc, NonRetryableError),
                sleep=self._sleep,
            )
        except NonRetryableError as exc:
            return await self._dead_letter(work, "non-retryable error", exc)
        except Exception as exc:
            return await self._retry_or_dead_letter(work, exc)
        _log.debug("event_handled", item_id=event.item_id, event_type=event.event_type.value)
        return True

    async def _retry_or_dead_letter(self, work: Work, error: Exception) -> bool:
        if work.retry_count >= self._settings.max_retries:
            return await self._dead_letter(work, "retries exhausted", error)
        outgoing = dlq.build_retry(
            work.winner.message,
            self._retry_topic,
            work.retry_count + 1,
            self._settings.retry_delay_s,
            error,
            self._clock(),
        )
        _log.warning(
            "event_sent_to_retry",
            item_id=work.event.item_id,
            retry_count=work.retry_count + 1,
            error=dlq.safe_error_text(error),
        )
        return await self._publish(outgoing)

    async def _dead_letter(self, work: Work, reason: str, error: BaseException) -> bool:
        outgoing = dlq.build_dlq(
            work.winner.message, self._dlq_topic, work.retry_count, reason, error, self._clock()
        )
        _log.error(
            "event_sent_to_dlq",
            item_id=work.event.item_id,
            reason=reason,
            error=dlq.safe_error_text(error),
        )
        return await self._publish(outgoing)

    async def _dead_letter_invalid(self, message: KafkaMessage, reason: str) -> None:
        """A message that is not a valid event. It is finished once it is in the DLQ."""
        outgoing = dlq.build_dlq(message, self._dlq_topic, 0, reason, None, self._clock())
        _log.error(
            "invalid_event_sent_to_dlq",
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            reason=reason,
        )
        if await self._publish(outgoing):
            self._tracker.complete(message.tp, message.offset)

    async def _publish(self, outgoing: dlq.Outgoing) -> bool:
        """Send to the retry topic or the DLQ. False if the broker did not take it."""
        try:
            await with_retries(
                lambda: self._producer.send(
                    outgoing.topic, outgoing.key, outgoing.value, outgoing.headers
                ),
                policy=self._policy,
                retry_if=lambda exc: isinstance(exc, UpstreamUnavailableError),
                sleep=self._sleep,
            )
        except UpstreamUnavailableError:
            # Not finished: the offset stays uncommitted and the message is read again.
            _log.error("publish_failed", topic=outgoing.topic)
            return False
        return True

    # --- commit, rebalance, shutdown ----------------------------------------------------------

    async def _maybe_commit(self, *, force: bool = False) -> None:
        if not force and time.monotonic() - self._last_commit < self._settings.commit_interval_s:
            return
        offsets = self._tracker.committable()
        if not offsets:
            return
        try:
            await self._consumer.commit(offsets)
        except UpstreamUnavailableError:
            _log.warning("commit_failed")  # tried again on the next pass
            return
        self._tracker.mark_committed(offsets)
        self._last_commit = time.monotonic()

    async def _on_revoke(self, partitions: Sequence[TopicPartition]) -> dict[TopicPartition, int]:
        """Finish what is being handled for these partitions, then hand back their offsets."""
        revoked = set(partitions)
        for tp in revoked:
            self._delayed.pop(tp, None)
        affected = [task for task, work in self._tasks.items() if work.partitions & revoked]
        if affected:
            _, pending = await asyncio.wait(affected, timeout=self._settings.rebalance_timeout_s)
            for task in pending:
                task.cancel()  # not finished in time: the new owner reads it again
            await asyncio.gather(*pending, return_exceptions=True)
        offsets = self._tracker.drop(revoked)
        _log.info("partitions_revoked", count=len(revoked))
        return offsets

    async def _shutdown(self) -> None:
        """Finish the messages being handled, commit, and leave the group."""
        tasks = list(self._tasks)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=self._settings.shutdown_timeout_s)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        await self._maybe_commit(force=True)
        await self._consumer.close()
        await self._producer.close()
        _log.info("consumer_stopped")
