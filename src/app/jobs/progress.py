"""The backfill progress report: counts per wave and status from the state index, and the jobs."""

from pydantic import BaseModel

from app.ingestion.state_admin import NO_WAVE, StateAdmin
from app.jobs.job_store import ElasticsearchJobStore, JobRecord

_STATUS_ORDER = ["PENDING", "INDEXED", "SKIPPED", "FAILED", "DELETED"]


class WaveProgress(BaseModel):
    """Counts of one wave. ``wave`` is ``None`` for records outside any wave."""

    wave: int | None
    counts: dict[str, int]

    @property
    def total(self) -> int:
        """All documents of the wave."""
        return sum(self.counts.values())

    @property
    def percent_done(self) -> float:
        """Share of documents that are no longer waiting or failed."""
        if not self.total:
            return 0.0
        done = sum(self.counts.get(s, 0) for s in ("INDEXED", "SKIPPED", "DELETED"))
        return round(100 * done / self.total, 1)


class ProgressReport(BaseModel):
    """Everything the report shows."""

    waves: list[WaveProgress]
    jobs: list[JobRecord]


async def build_report(states: StateAdmin, jobs: ElasticsearchJobStore) -> ProgressReport:
    """Read the counts and the jobs."""
    counts = await states.status_counts()
    waves = [
        WaveProgress(wave=None if wave == NO_WAVE else wave, counts=by_status)
        for wave, by_status in sorted(counts.items())
    ]
    return ProgressReport(waves=waves, jobs=await jobs.list_jobs())


def format_report(report: ProgressReport) -> str:
    """The report as plain text."""
    header = ["wave", *_STATUS_ORDER, "total", "done %"]
    rows = [header]
    for wave in report.waves:
        rows.append(
            [
                "-" if wave.wave is None else str(wave.wave),
                *(str(wave.counts.get(status, 0)) for status in _STATUS_ORDER),
                str(wave.total),
                str(wave.percent_done),
            ]
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    lines = ["  ".join(cell.rjust(widths[i]) for i, cell in enumerate(row)) for row in rows]
    lines.append("")
    lines.append("jobs:")
    for job in report.jobs:
        lines.append(
            f"  {job.job_id}: {job.status.value} (wanted: {job.desired}), wave {job.wave}, "
            f"scanned {job.scanned}, published {job.published}, "
            f"up to date {job.skipped_up_to_date}"
        )
    if not report.jobs:
        lines.append("  (none)")
    return "\n".join(lines)
