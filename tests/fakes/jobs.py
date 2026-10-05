"""In-memory scanner, state admin and job store for testing the backfill and reconciliation."""

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

from app.core.settings import WaveSpec
from app.ingestion.state_admin import NO_WAVE
from app.ingestion.state_store import IndexState, IndexStatus
from app.jobs.job_store import JobRecord, JobStatus
from app.jobs.scan import ScanItem, ScanPage

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


class FakeScanner:
    """The source index as a dict: item ID to source fields. Sorted by item ID."""

    def __init__(self, documents: dict[str, dict[str, Any]], page_size: int = 10) -> None:
        self.documents = documents
        self.page_size = page_size
        self.fail_after_pages: int | None = None
        self.pages_served = 0

    def _matches(self, source: dict[str, Any], scope: WaveSpec | None) -> bool:
        if scope is None:
            return True
        if scope.doc_types and source.get("doc_type") not in scope.doc_types:
            return False
        created = str(source.get("created_at", ""))[:10]
        if scope.created_from and created < scope.created_from.isoformat():
            return False
        return not (scope.created_to and created > scope.created_to.isoformat())

    async def pages(
        self,
        scope: WaveSpec | None = None,
        *,
        after: str | None = None,
        include_meta: bool = False,
        max_items: int | None = None,
    ) -> AsyncIterator[ScanPage]:
        """Pages after the cursor, in order."""
        ids = sorted(i for i, s in self.documents.items() if self._matches(s, scope))
        if after is not None:
            ids = [i for i in ids if i > after]
        if max_items is not None:
            ids = ids[:max_items]
        for start in range(0, len(ids), self.page_size):
            if self.fail_after_pages is not None and self.pages_served >= self.fail_after_pages:
                raise RuntimeError("scan failed")
            self.pages_served += 1
            chunk = ids[start : start + self.page_size]
            items = [ScanItem(i, dict(self.documents[i]) if include_meta else {}) for i in chunk]
            yield ScanPage(items, chunk[-1])

    async def existing(self, item_ids: list[str]) -> set[str]:
        """The IDs that exist."""
        return {i for i in item_ids if i in self.documents}


class FakeStateAdmin:
    """State records in a dict."""

    def __init__(self) -> None:
        self.states: dict[str, IndexState] = {}
        self.registered: list[tuple[list[str], int]] = []

    def put(self, state: IndexState) -> None:
        """Add a record."""
        self.states[state.item_id] = state

    async def get_many(self, item_ids: Sequence[str]) -> dict[str, IndexState]:
        """The records that exist."""
        return {i: self.states[i] for i in item_ids if i in self.states}

    async def register_wave(self, item_ids: Sequence[str], wave: int) -> None:
        """PENDING record for new documents. The wave number for existing ones."""
        self.registered.append((list(item_ids), wave))
        for item_id in item_ids:
            current = self.states.get(item_id)
            if current is None:
                self.states[item_id] = IndexState(
                    item_id=item_id, status=IndexStatus.PENDING, wave=wave
                )
            else:
                self.states[item_id] = current.model_copy(update={"wave": wave})

    async def status_counts(self) -> dict[int, dict[str, int]]:
        """Counts per wave and status."""
        counts: dict[int, dict[str, int]] = {}
        for state in self.states.values():
            wave = NO_WAVE if state.wave is None else state.wave
            by_status = counts.setdefault(wave, {})
            by_status[state.status.value] = by_status.get(state.status.value, 0) + 1
        return counts

    async def scan(
        self, *, after: str | None, size: int, wave: int | None = None
    ) -> tuple[list[IndexState], str | None]:
        """Records in item order after the cursor."""
        records = sorted(
            (s for s in self.states.values() if wave is None or s.wave == wave),
            key=lambda s: s.item_id,
        )
        if after is not None:
            records = [s for s in records if s.item_id > after]
        page = records[:size]
        return page, (page[-1].item_id if page else None)


class FakeJobStore:
    """Job records in a dict."""

    def __init__(self) -> None:
        self.records: dict[str, JobRecord] = {}
        self.after_save: Any = None  # a hook that runs after each saved page
        self.fail_save_times = 0

    async def start(
        self, job_id: str, kind: str, wave: int | None, *, restart: bool = False
    ) -> JobRecord:
        """New record, or the existing one again (from its cursor unless ``restart``)."""
        record = self.records.get(job_id)
        if record is None:
            record = JobRecord(job_id=job_id, kind=kind, wave=wave, started_at=NOW, updated_at=NOW)
        update: dict[str, Any] = {"status": JobStatus.RUNNING, "desired": "RUNNING"}
        if restart:
            update |= {"cursor": None, "scanned": 0, "published": 0, "skipped_up_to_date": 0}
        self.records[job_id] = record.model_copy(update=update)
        return self.records[job_id]

    async def get(self, job_id: str) -> JobRecord | None:
        """The record, or ``None``."""
        return self.records.get(job_id)

    async def request_job(
        self, job_id: str, kind: str, wave: int | None, *, restart: bool = False
    ) -> JobRecord:
        """Queue a job. One that is queued or running stays as it is."""
        existing = self.records.get(job_id)
        if existing is not None and existing.status in (JobStatus.REQUESTED, JobStatus.RUNNING):
            return existing
        update: dict[str, Any] = {"status": JobStatus.REQUESTED, "desired": "RUNNING"}
        if restart:
            update |= {"cursor": None, "scanned": 0, "published": 0, "skipped_up_to_date": 0}
        record = existing or JobRecord(
            job_id=job_id, kind=kind, wave=wave, started_at=NOW, updated_at=NOW
        )
        self.records[job_id] = record.model_copy(update=update)
        return self.records[job_id]

    async def list_requested(self) -> list[JobRecord]:
        """Queued jobs, oldest first."""
        queued = [r for r in self.records.values() if r.status is JobStatus.REQUESTED]
        return sorted(queued, key=lambda r: r.started_at)

    async def list_jobs(self, limit: int = 100) -> list[JobRecord]:
        """All records."""
        return list(self.records.values())[:limit]

    async def save_progress(
        self, job_id: str, *, cursor: str, scanned: int, published: int, skipped_up_to_date: int
    ) -> None:
        """Checkpoint."""
        if self.fail_save_times:
            self.fail_save_times -= 1
            raise RuntimeError("could not save")
        self.records[job_id] = self.records[job_id].model_copy(
            update={
                "cursor": cursor,
                "scanned": scanned,
                "published": published,
                "skipped_up_to_date": skipped_up_to_date,
            }
        )
        if self.after_save:
            await self.after_save(self.records[job_id])

    async def finish(self, job_id: str, status: JobStatus, error: str | None = None) -> None:
        """PAUSED, COMPLETED or FAILED."""
        self.records[job_id] = self.records[job_id].model_copy(
            update={"status": status, "last_error": error}
        )

    async def request(self, job_id: str, desired: str) -> bool:
        """Ask for a pause."""
        if job_id not in self.records:
            return False
        self.records[job_id] = self.records[job_id].model_copy(update={"desired": desired})
        return True

    async def desired(self, job_id: str) -> str:
        """What the operator wants."""
        record = self.records.get(job_id)
        return record.desired if record else "RUNNING"
