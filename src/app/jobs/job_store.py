"""Job records: where a backfill stands, so it can pause and resume (task T1.7).

One record per job in the jobs index. The job process saves progress after every page. An
operator asks for a pause by setting ``desired`` to PAUSED. The job notices it between pages,
saves its cursor and stops. Resume starts the job again from the saved cursor.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from elasticsearch import AsyncElasticsearch, ConflictError, NotFoundError
from pydantic import BaseModel

from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings
from app.store.calls import guarded


class JobStatus(StrEnum):
    """Where the job process stands."""

    REQUESTED = "REQUESTED"  # asked for through the admin API, not started yet
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobRecord(BaseModel):
    """One backfill job."""

    job_id: str
    kind: str = "backfill"
    wave: int | None = None
    status: JobStatus = JobStatus.RUNNING
    desired: Literal["RUNNING", "PAUSED"] = "RUNNING"
    cursor: str | None = None
    scanned: int = 0
    published: int = 0
    skipped_up_to_date: int = 0
    started_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None
    last_error: str | None = None


def job_mappings() -> dict[str, Any]:
    """Mapping of the jobs index. The cursor is stored, not searched."""
    return {
        "dynamic": "strict",
        "properties": {
            "job_id": {"type": "keyword"},
            "kind": {"type": "keyword"},
            "wave": {"type": "integer"},
            "status": {"type": "keyword"},
            "desired": {"type": "keyword"},
            "cursor": {"type": "keyword", "index": False, "doc_values": False},
            "scanned": {"type": "long"},
            "published": {"type": "long"},
            "skipped_up_to_date": {"type": "long"},
            "started_at": {"type": "date"},
            "updated_at": {"type": "date"},
            "finished_at": {"type": "date"},
            "last_error": {"type": "keyword", "ignore_above": 256},
        },
    }


class ElasticsearchJobStore:
    """Job records in Elasticsearch."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        index: str,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client = client.options(request_timeout=elasticsearch.state_timeout_s * 5)
        self._index = index
        self._retry = retry
        self._sleep = sleep
        self._clock = clock

    async def ensure_index(self) -> None:
        """Create the jobs index if it is missing."""

        async def run() -> None:
            if not await self._client.indices.exists(index=self._index):
                await self._client.indices.create(
                    index=self._index,
                    mappings=job_mappings(),
                    settings={"number_of_shards": 1, "number_of_replicas": 1},
                )

        await guarded(run, self._retry, self._sleep)

    async def get(self, job_id: str) -> JobRecord | None:
        """The record, or ``None``."""

        async def fetch() -> dict[str, Any] | None:
            try:
                response = await self._client.get(index=self._index, id=job_id)
            except NotFoundError as exc:
                if isinstance(exc.body, dict) and exc.body.get("found") is False:
                    return None
                raise
            return dict(response["_source"])

        source = await guarded(fetch, self._retry, self._sleep)
        return JobRecord.model_validate(source) if source else None

    async def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        """Newest jobs first."""

        async def fetch() -> list[dict[str, Any]]:
            response = await self._client.search(
                index=self._index,
                query={"match_all": {}},
                sort=[{"started_at": {"order": "desc"}}],
                size=limit,
                track_total_hits=False,
            )
            return list(response["hits"]["hits"])

        return [
            JobRecord.model_validate(h["_source"])
            for h in await guarded(fetch, self._retry, self._sleep)
        ]

    async def start(
        self, job_id: str, kind: str, wave: int | None, *, restart: bool = False
    ) -> JobRecord:
        """Create the job, or take the existing one up again from its cursor.

        ``restart`` clears the cursor and the counters, to scan the wave from the beginning.
        """
        existing = await self.get(job_id)
        now = self._clock()
        if existing is None:
            record = JobRecord(job_id=job_id, kind=kind, wave=wave, started_at=now, updated_at=now)

            async def create() -> None:
                await self._client.index(
                    index=self._index,
                    id=job_id,
                    document=record.model_dump(mode="json", exclude_none=True),
                    op_type="create",
                )

            try:
                await guarded(create, self._retry, self._sleep)
            except ConflictError:
                return await self.start(
                    job_id, kind, wave, restart=restart
                )  # created by another process
            return record
        fields: dict[str, Any] = {
            "status": "RUNNING",
            "desired": "RUNNING",
            "last_error": None,
            "finished_at": None,
        }
        changes: dict[str, Any] = {"status": JobStatus.RUNNING, "desired": "RUNNING"}
        if restart:
            fields |= {"cursor": None, "scanned": 0, "published": 0, "skipped_up_to_date": 0}
            changes |= {"cursor": None, "scanned": 0, "published": 0, "skipped_up_to_date": 0}
        await self._update(job_id, fields)
        return existing.model_copy(update=changes)

    async def _update(self, job_id: str, fields: dict[str, Any]) -> None:
        body = {**fields, "updated_at": self._clock().isoformat()}

        async def run() -> None:
            await self._client.update(index=self._index, id=job_id, doc=body)

        await guarded(run, self._retry, self._sleep)

    async def save_progress(
        self, job_id: str, *, cursor: str, scanned: int, published: int, skipped_up_to_date: int
    ) -> None:
        """Checkpoint after a page."""
        await self._update(
            job_id,
            {
                "cursor": cursor,
                "scanned": scanned,
                "published": published,
                "skipped_up_to_date": skipped_up_to_date,
            },
        )

    async def finish(self, job_id: str, status: JobStatus, error: str | None = None) -> None:
        """PAUSED, COMPLETED or FAILED."""
        fields: dict[str, Any] = {"status": status.value}
        if status is not JobStatus.PAUSED:
            fields["finished_at"] = self._clock().isoformat()
        if error:
            fields["last_error"] = error[:256]
        await self._update(job_id, fields)

    async def request_job(
        self, job_id: str, kind: str, wave: int | None, *, restart: bool = False
    ) -> JobRecord:
        """Ask for a job to run. A job process (``backfill run-requested``) picks it up.

        A job that is already REQUESTED or RUNNING is returned as it is (asking twice is
        harmless). A finished or paused job is queued again, from its cursor unless ``restart``.
        """
        existing = await self.get(job_id)
        now = self._clock()
        if existing is None:
            record = JobRecord(
                job_id=job_id,
                kind=kind,
                wave=wave,
                status=JobStatus.REQUESTED,
                started_at=now,
                updated_at=now,
            )

            async def create() -> None:
                await self._client.index(
                    index=self._index,
                    id=job_id,
                    document=record.model_dump(mode="json", exclude_none=True),
                    op_type="create",
                )

            try:
                await guarded(create, self._retry, self._sleep)
            except ConflictError:
                return await self.request_job(job_id, kind, wave, restart=restart)
            return record
        if existing.status in (JobStatus.REQUESTED, JobStatus.RUNNING):
            return existing
        fields: dict[str, Any] = {
            "status": JobStatus.REQUESTED.value,
            "desired": "RUNNING",
            "last_error": None,
            "finished_at": None,
        }
        changes: dict[str, Any] = {"status": JobStatus.REQUESTED, "desired": "RUNNING"}
        if restart:
            cleared = {"cursor": None, "scanned": 0, "published": 0, "skipped_up_to_date": 0}
            fields |= cleared
            changes |= cleared
        await self._update(job_id, fields)
        return existing.model_copy(update=changes)

    async def list_requested(self) -> list[JobRecord]:
        """Jobs that wait for a process to start them, oldest first."""

        async def fetch() -> list[dict[str, Any]]:
            response = await self._client.search(
                index=self._index,
                query={"term": {"status": JobStatus.REQUESTED.value}},
                sort=[{"started_at": {"order": "asc"}}],
                size=100,
                track_total_hits=False,
            )
            return list(response["hits"]["hits"])

        return [
            JobRecord.model_validate(h["_source"])
            for h in await guarded(fetch, self._retry, self._sleep)
        ]

    async def request(self, job_id: str, desired: Literal["RUNNING", "PAUSED"]) -> bool:
        """Ask a running job to pause (or clear the request). False if there is no such job."""
        if await self.get(job_id) is None:
            return False
        await self._update(job_id, {"desired": desired})
        return True

    async def desired(self, job_id: str) -> str:
        """What the operator wants. A job without a record keeps running."""
        record = await self.get(job_id)
        return record.desired if record else "RUNNING"
