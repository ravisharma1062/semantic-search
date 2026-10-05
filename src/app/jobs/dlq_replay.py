"""Replay the dead-letter topic after the cause was fixed (runbook 3).

    python -m app.jobs.dlq_replay --dry-run            # what is in the DLQ, by reason
    python -m app.jobs.dlq_replay --max 1000           # republish up to 1000 events
    python -m app.jobs.dlq_replay --group dlq-replay-2 # read everything again with a new group

Each event is published again to the live topic with its original key and value (the event is only
an ``ITEM_ID`` and metadata, so it can be replayed any number of times: processing is idempotent).
An offset is committed only after the republish worked (rule 3), so a crash replays a few events
twice, never loses one. Messages that are not a valid event would only fail again: they are counted
and skipped. Nothing is deleted from the DLQ topic. Which events were replayed is remembered by the
consumer group, so a second run continues where the first one stopped.
"""

import argparse
import asyncio
import sys
from collections import Counter
from collections.abc import Sequence

import structlog
from pydantic import BaseModel

from app.core.errors import AppError
from app.core.logging import configure_logging
from app.core.settings import Settings, get_settings
from app.ingestion import dlq
from app.ingestion.events import InvalidEventError, parse_event
from app.ingestion.kafka_client import ConfluentConsumer, ConfluentProducer
from app.ingestion.kafka_io import MessageConsumer, MessageProducer, TopicPartition

_log = structlog.get_logger(__name__)
REPLAYED = "x-replayed"


class ReplayReport(BaseModel):
    """What a replay run found and did."""

    read: int = 0
    replayed: int = 0
    invalid_skipped: int = 0
    by_reason: dict[str, int] = {}
    dry_run: bool = False


class DlqReplayer:
    """Reads the DLQ and republishes valid events."""

    def __init__(
        self,
        *,
        consumer: MessageConsumer,
        producer: MessageProducer,
        dlq_topic: str,
        live_topic: str,
        batch_size: int = 100,
        idle_timeout_s: float = 3.0,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dlq_topic = dlq_topic
        self._live_topic = live_topic
        self._batch = batch_size
        self._idle_s = idle_timeout_s

    async def run(self, *, max_messages: int | None = None, dry_run: bool = False) -> ReplayReport:
        """Read until the DLQ is empty (no message for ``idle_timeout_s``) or ``max_messages``."""

        async def on_revoke(_: Sequence[TopicPartition]) -> dict[TopicPartition, int]:
            return {}  # offsets are committed after every batch

        await self._consumer.subscribe([self._dlq_topic], on_revoke)
        report = ReplayReport(dry_run=dry_run)
        reasons: Counter[str] = Counter()
        try:
            while max_messages is None or report.read < max_messages:
                want = (
                    self._batch
                    if max_messages is None
                    else min(self._batch, max_messages - report.read)
                )
                messages = await self._consumer.poll(want, self._idle_s)
                if not messages:
                    break
                offsets: dict[TopicPartition, int] = {}
                for message in messages:
                    report.read += 1
                    reasons[
                        message.headers.get(dlq.REASON, b"unknown").decode(errors="replace")
                    ] += 1
                    try:
                        parse_event(message.value or b"")
                    except InvalidEventError:
                        report.invalid_skipped += 1
                    else:
                        if not dry_run:
                            await self._producer.send(
                                self._live_topic, message.key, message.value, {REPLAYED: b"1"}
                            )
                            report.replayed += 1
                    offsets[message.tp] = max(offsets.get(message.tp, 0), message.offset + 1)
                if dry_run:
                    # Look but do not move the group: a dry run can be repeated.
                    continue
                await self._consumer.commit(offsets)
        finally:
            await self._consumer.close()
        report.by_reason = dict(reasons)
        _log.info(
            "dlq_replay_done",
            read=report.read,
            replayed=report.replayed,
            invalid_skipped=report.invalid_skipped,
            dry_run=dry_run,
        )
        return report


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(prog="dlq_replay", description=__doc__)
    parser.add_argument("--max", type=int, dest="max_messages", help="stop after this many")
    parser.add_argument("--dry-run", action="store_true", help="count by reason, change nothing")
    parser.add_argument("--group", help="consumer group (default: <consumer_group>-dlq-replay)")
    return parser


async def _main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)
    settings: Settings = get_settings()
    configure_logging(settings.service.log_level, settings.service.log_json)
    kafka = settings.kafka
    group = args.group or f"{kafka.consumer_group}-dlq-replay"
    # A dry run uses a group of its own so that it never moves the real replay group.
    if args.dry_run:
        group = f"{group}-dry-run"
    producer = ConfluentProducer(kafka)
    try:
        replayer = DlqReplayer(
            consumer=ConfluentConsumer(kafka, group, settings.consumer.rebalance_timeout_s),
            producer=producer,
            dlq_topic=kafka.dlq_topic,
            live_topic=kafka.live_topic,
        )
        report = await replayer.run(max_messages=args.max_messages, dry_run=args.dry_run)
        print(report.model_dump_json(indent=2))
    except AppError as error:
        print(f"error: {error.message}", file=sys.stderr)
        return 1
    finally:
        await producer.close()
    return 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
