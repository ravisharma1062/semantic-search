"""Retrieval experiments (task T3.2): which settings give the best results, with evidence.

An experiment is a search configuration: the search mode, reranking on or off, and a chunk
variant (an index built with other chunker settings). ``run_matrix`` runs all of them on the
same questions and ``format_comparison`` shows the metrics side by side with the gain over the
baseline. Every report keeps the configuration, so a decision note can point at the runs it is
based on.

The runner does not build indices. The caller supplies a ``factory`` that returns the search
function for an experiment, so the same code works in process (tests) and against real services.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from eval.dataset import EvalQuestion
from eval.runner import RetrievalReport, SearchFn, evaluate

METRICS = ("recall@10", "recall@50", "mrr", "ndcg@10")


@dataclass(frozen=True)
class Experiment:
    """One search configuration."""

    mode: str  # "bm25", "vector" or "hybrid"
    rerank: bool = False
    chunks: str = "default"  # name of the chunk variant (an index built with other settings)

    @property
    def name(self) -> str:
        """A short name for tables and file names."""
        return f"{self.mode}{'+rerank' if self.rerank else ''}@{self.chunks}"


def build_matrix(chunk_variants: list[str] | None = None) -> list[Experiment]:
    """The comparison of HLD section 10: BM25 only, kNN only, hybrid, with and without rerank, and
    at least two chunk variants."""
    variants = chunk_variants or ["default"]
    return [
        Experiment(mode, rerank, chunks)
        for chunks in variants
        for mode, rerank in (
            ("bm25", False),
            ("vector", False),
            ("hybrid", False),
            ("hybrid", True),
        )
    ]


async def run_matrix(
    questions: list[EvalQuestion],
    experiments: list[Experiment],
    factory: Callable[[Experiment], SearchFn],
    *,
    config: dict[str, Any] | None = None,
) -> list[RetrievalReport]:
    """Run every experiment on the same questions."""
    reports = []
    for experiment in experiments:
        reports.append(
            await evaluate(
                questions,
                factory(experiment),
                name=experiment.name,
                config={
                    **(config or {}),
                    "experiment": experiment.name,
                    "chunks": experiment.chunks,
                },
            )
        )
    return reports


def format_comparison(reports: list[RetrievalReport], baseline: str | None = None) -> str:
    """A Markdown table with the metrics and the change of each metric against the baseline run.
    The baseline is the first report unless a name is given."""
    if not reports:
        return ""
    base = next((r for r in reports if r.name == baseline), reports[0])
    lines = [
        "| run | " + " | ".join(METRICS) + " |",
        "| --- | " + " | ".join("---" for _ in METRICS) + " |",
    ]
    for report in reports:
        cells = []
        for metric in METRICS:
            value = report.metrics.get(metric, 0.0)
            delta = value - base.metrics.get(metric, 0.0)
            cells.append(f"{value:.3f}" + ("" if report is base else f" ({delta:+.3f})"))
        marker = " (baseline)" if report is base else ""
        lines.append(f"| {report.name}{marker} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def best_by(reports: list[RetrievalReport], metric: str = "ndcg@10") -> RetrievalReport:
    """The run with the highest value of a metric. Ties go to the earlier run (the simpler one)."""
    return max(reports, key=lambda r: (r.metrics.get(metric, 0.0), -reports.index(r)))
