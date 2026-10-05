"""What the admin API does (HLD section 8): manual indexing, deleting, and re-index jobs.

Indexing and deleting a document publish an ``ITEM_ID`` event to the live topic, so the same worker
code path and the same idempotency rules apply as for events from the Java app. The API does not
touch the chunk index itself. A re-index job is only *requested* here: a job process
(``python -m app.jobs.cli backfill run-requested``, a Kubernetes Job) starts it, so a long scan is
never run inside an API pod.
"""

import json
import re
from typing import Any, Protocol

from app.core.errors import InvalidRequestError
from app.core.settings import Settings, WaveSpec
from app.ingestion.events import EventType
from app.ingestion.kafka_io import MessageProducer
from app.jobs.events import build_event
from app.jobs.job_store import JobRecord

_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def valid_item_id(value: str) -> str:
    """The ID, or ``InvalidRequestError``. IDs go into Kafka keys, job IDs and logs."""
    if not _ID.match(value):
        raise InvalidRequestError("Invalid document ID")
    return value


class JobsPort(Protocol):
    """The part of the job store that the admin use cases need."""

    async def request_job(
        self, job_id: str, kind: str, wave: int | None, *, restart: bool = False
    ) -> JobRecord:
        """Queue a job."""
        ...

    async def get(self, job_id: str) -> JobRecord | None:
        """A job record, or ``None``."""
        ...


class AdminService:
    """The admin use cases."""

    def __init__(self, *, producer: MessageProducer, jobs: JobsPort, settings: Settings) -> None:
        self._producer = producer
        self._jobs = jobs
        self._settings = settings

    async def _publish(self, item_id: str, event_type: EventType) -> str:
        key, value = build_event(item_id, event_type, source="admin-api", priority="live")
        await self._producer.send(self._settings.kafka.live_topic, key, value, {})
        return str(json.loads(value)["event_id"])

    async def index_document(self, item_id: str) -> str:
        """Ask for one document to be (re)indexed. Returns the event ID."""
        return await self._publish(valid_item_id(item_id), EventType.UPSERT)

    async def delete_document(self, doc_id: str) -> str:
        """Ask for all chunks of a document to be removed. Returns the event ID."""
        return await self._publish(valid_item_id(doc_id), EventType.DELETE)

    def wave(self, number: int) -> WaveSpec:
        """The wave definition, or ``InvalidRequestError`` if it is not configured."""
        for wave in self._settings.backfill.waves:
            if wave.number == number:
                return wave
        raise InvalidRequestError("Unknown wave")

    async def request_reindex(self, wave_number: int, *, restart: bool) -> JobRecord:
        """Queue a re-index job for a wave. Asking again for a job that is queued or running
        returns that job."""
        wave = self.wave(wave_number)
        return await self._jobs.request_job(
            f"backfill-wave{wave.number}", "backfill", wave.number, restart=restart
        )

    async def job(self, job_id: str) -> JobRecord | None:
        """A job record, or ``None``."""
        return await self._jobs.get(valid_item_id(job_id))


def job_view(record: JobRecord) -> dict[str, Any]:
    """The fields of a job that the API returns."""
    return record.model_dump(mode="json", exclude={"cursor"}, exclude_none=True)
