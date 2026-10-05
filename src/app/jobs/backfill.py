"""The backfill producer (HLD section 4, "Batch re-index").

It scans the existing index in a stable order and publishes ``ITEM_ID`` events to the backfill
topic with a wave number. The same workers handle them, in a separate consumer group, so live
updates are never stuck behind it.

- Speed is limited by ``backfill.rate_per_second``.
- After every page the cursor is saved. The job can pause (the operator asks, or SIGTERM) and
  continue from the cursor. A crash between publishing and saving publishes one page twice,
  which is harmless: processing is idempotent.
- Documents that are already indexed with the current model and chunker are not published again
  (``backfill.skip_up_to_date``). They still join the wave, so the progress report is complete.
"""

import asyncio
from datetime import UTC, datetime

import structlog

from app.core.errors import NonRetryableError
from app.core.settings import BackfillSettings, WaveSpec
from app.ingestion.dlq import safe_error_text
from app.ingestion.events import EventType
from app.ingestion.kafka_io import MessageProducer
from app.ingestion.state_admin import StateAdmin
from app.ingestion.state_store import IndexState, IndexStatus
from app.jobs.events import build_event
from app.jobs.job_store import ElasticsearchJobStore, JobRecord, JobStatus
from app.jobs.rate import RateLimiter
from app.jobs.scan import ScanItem, SourceScanner

_log = structlog.get_logger(__name__)


class BackfillJob:
    """One backfill wave."""

    def __init__(
        self,
        *,
        scanner: SourceScanner,
        jobs: ElasticsearchJobStore,
        states: StateAdmin,
        producer: MessageProducer,
        topic: str,
        limiter: RateLimiter,
        settings: BackfillSettings,
        embedding_model: str,
        chunker_version: str,
    ) -> None:
        self._scanner = scanner
        self._jobs = jobs
        self._states = states
        self._producer = producer
        self._topic = topic
        self._limiter = limiter
        self._cfg = settings
        self._model = embedding_model
        self._chunker_version = chunker_version

    async def run(
        self, job_id: str, wave: WaveSpec, stop: asyncio.Event, *, restart: bool = False
    ) -> JobRecord:
        """Scan and publish until the wave is done, a pause is asked for, or ``stop`` is set."""
        record = await self._jobs.start(job_id, "backfill", wave.number, restart=restart)
        scanned, published, skipped = record.scanned, record.published, record.skipped_up_to_date
        try:
            async for page in self._scanner.pages(wave, after=record.cursor):
                if stop.is_set() or await self._jobs.desired(job_id) == "PAUSED":
                    await self._jobs.finish(job_id, JobStatus.PAUSED)
                    _log.info("backfill_paused", job_id=job_id, cursor_saved=True)
                    return await self._current(job_id)
                to_publish, up_to_date = await self._split(page.items)
                await self._limiter.acquire(len(page.items))
                await self._states.register_wave([i.item_id for i in page.items], wave.number)
                await self._producer.send_batch(
                    self._topic, [self._message(item.item_id, wave.number) for item in to_publish]
                )
                scanned += len(page.items)
                published += len(to_publish)
                skipped += up_to_date
                await self._jobs.save_progress(
                    job_id,
                    cursor=page.cursor,
                    scanned=scanned,
                    published=published,
                    skipped_up_to_date=skipped,
                )
        except Exception as exc:
            await self._jobs.finish(job_id, JobStatus.FAILED, safe_error_text(exc))
            raise
        await self._jobs.finish(job_id, JobStatus.COMPLETED)
        _log.info("backfill_completed", job_id=job_id, scanned=scanned, published=published)
        return await self._current(job_id)

    async def _current(self, job_id: str) -> JobRecord:
        record = await self._jobs.get(job_id)
        if record is None:
            raise NonRetryableError("The job record is gone")
        return record

    async def _split(self, items: list[ScanItem]) -> tuple[list[ScanItem], int]:
        """The items that need an event, and how many are up to date already."""
        if not self._cfg.skip_up_to_date:
            return items, 0
        states = await self._states.get_many([i.item_id for i in items])
        todo = [i for i in items if not self._up_to_date(states.get(i.item_id))]
        return todo, len(items) - len(todo)

    def _up_to_date(self, state: IndexState | None) -> bool:
        return (
            state is not None
            and state.status is IndexStatus.INDEXED
            and state.embedding_model == self._model
            and state.chunker_version == self._chunker_version
        )

    @staticmethod
    def _message(item_id: str, wave: int) -> tuple[bytes, bytes]:
        return build_event(
            item_id,
            EventType.UPSERT,
            source="backfill",
            priority="backfill",
            wave=wave,
            now=datetime.now(UTC),
        )
