"""Backfill and reconciliation from the command line (HLD runbook: pause, resume, throttle):

    python -m app.jobs.cli backfill start --wave 1 [--job-id ID] [--restart]
    python -m app.jobs.cli backfill pause --job-id ID
    python -m app.jobs.cli backfill resume --job-id ID
    python -m app.jobs.cli backfill status
    python -m app.jobs.cli reconcile [--wave 1] [--max-items 100000] [--no-orphans]

``start`` and ``resume`` run in this process until the wave is done. SIGTERM (or Ctrl+C) pauses
the job and saves its cursor. ``pause`` asks a running job to pause, from anywhere.
"""

import argparse
import asyncio
import sys
from collections.abc import Sequence

from app.core.errors import AppError, NonRetryableError
from app.core.logging import configure_logging
from app.core.settings import Settings, WaveSpec, get_settings
from app.ingestion.kafka_client import ConfluentProducer
from app.ingestion.state_admin import StateAdmin
from app.jobs.backfill import BackfillJob
from app.jobs.job_store import ElasticsearchJobStore, JobRecord
from app.jobs.progress import build_report, format_report
from app.jobs.rate import RateLimiter
from app.jobs.reconcile import Reconciler
from app.jobs.scan import SourceScanner
from app.modes import install_signal_handlers
from app.store.client import create_es_client


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(prog="jobs", description=__doc__)
    top = parser.add_subparsers(dest="group", required=True)
    backfill = top.add_parser("backfill").add_subparsers(dest="command", required=True)
    start = backfill.add_parser("start", help="scan a wave and publish events")
    start.add_argument("--wave", type=int, required=True)
    start.add_argument("--job-id")
    start.add_argument("--restart", action="store_true", help="scan the wave from the beginning")
    for name in ("pause", "resume"):
        sub = backfill.add_parser(name)
        sub.add_argument("--job-id", required=True)
    backfill.add_parser("status", help="progress per wave and status, and the jobs")
    backfill.add_parser(
        "run-requested", help="run the jobs that were requested through the admin API"
    )
    reconcile = top.add_parser("reconcile", help="republish differences between state and source")
    reconcile.add_argument("--wave", type=int)
    reconcile.add_argument("--max-items", type=int)
    reconcile.add_argument("--no-orphans", action="store_true")
    return parser


def find_wave(settings: Settings, number: int) -> WaveSpec:
    """The wave definition from the settings."""
    for wave in settings.backfill.waves:
        if wave.number == number:
            return wave
    raise NonRetryableError(f"Wave {number} is not defined in backfill.waves")


def _model(settings: Settings) -> str:
    return f"{settings.embedding.model}@{settings.embedding.model_version}"


NEWLINE = chr(10)


def _backfill_job(
    settings: Settings,
    scanner: SourceScanner,
    jobs: ElasticsearchJobStore,
    states: StateAdmin,
    producer: ConfluentProducer,
    limiter: RateLimiter,
) -> BackfillJob:
    return BackfillJob(
        scanner=scanner,
        jobs=jobs,
        states=states,
        producer=producer,
        topic=settings.kafka.backfill_topic,
        limiter=limiter,
        settings=settings.backfill,
        embedding_model=_model(settings),
        chunker_version=settings.chunking.version,
    )


def _summary(record: JobRecord) -> str:
    return (
        f"{record.job_id}: {record.status.value}, scanned {record.scanned}, "
        f"published {record.published}, up to date {record.skipped_up_to_date}"
    )


async def run(args: argparse.Namespace, settings: Settings, stop: asyncio.Event) -> str:
    """Run one command and return what to print."""
    client = create_es_client(settings.elasticsearch)
    producer = ConfluentProducer(settings.kafka)
    try:
        jobs = ElasticsearchJobStore(
            client, settings.backfill.job_index, settings.elasticsearch, settings.retry
        )
        await jobs.ensure_index()
        states = StateAdmin(
            client, settings.elasticsearch.state_index, settings.elasticsearch, settings.retry
        )
        scanner = SourceScanner(
            client,
            settings.search.source_index,
            settings.source,
            settings.backfill,
            settings.elasticsearch,
            settings.retry,
        )
        limiter = RateLimiter(settings.backfill.rate_per_second)
        if args.group == "reconcile":
            reconciler = Reconciler(
                scanner=scanner,
                states=states,
                producer=producer,
                live_topic=settings.kafka.live_topic,
                backfill_topic=settings.kafka.backfill_topic,
                limiter=limiter,
                backfill=settings.backfill,
                source=settings.source,
                embedding_model=_model(settings),
                chunker_version=settings.chunking.version,
            )
            scope = find_wave(settings, args.wave) if args.wave else None
            result = await reconciler.run(
                scope, max_items=args.max_items, check_orphans=not args.no_orphans
            )
            return result.model_dump_json(indent=2)
        match args.command:
            case "status":
                return format_report(await build_report(states, jobs))
            case "pause":
                found = await jobs.request(args.job_id, "PAUSED")
                return "pause requested" if found else "no such job"
            case "run-requested":
                done: list[str] = []
                for requested in await jobs.list_requested():
                    if stop.is_set() or requested.wave is None:
                        break
                    job = _backfill_job(settings, scanner, jobs, states, producer, limiter)
                    wave = find_wave(settings, requested.wave)
                    done.append(_summary(await job.run(requested.job_id, wave, stop)))
                return NEWLINE.join(done) or "no requested jobs"
            case "start" | "resume":
                job_id = args.job_id or f"backfill-wave{args.wave}"
                if args.command == "resume":
                    record = await jobs.get(job_id)
                    if record is None or record.wave is None:
                        raise NonRetryableError("No such job to resume")
                    wave = find_wave(settings, record.wave)
                else:
                    wave = find_wave(settings, args.wave)
                job = _backfill_job(settings, scanner, jobs, states, producer, limiter)
                record = await job.run(job_id, wave, stop, restart=getattr(args, "restart", False))
                return _summary(record)
        raise AssertionError(args.command)
    finally:
        await producer.close()
        await client.close()


async def _main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(settings.service.log_level, settings.service.log_json)
    stop = asyncio.Event()
    install_signal_handlers(asyncio.get_running_loop(), stop)
    try:
        print(await run(args, settings, stop))
    except AppError as error:
        print(f"error: {error.message}", file=sys.stderr)
        return 1
    return 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
