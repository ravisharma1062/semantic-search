"""The retrieval evaluation runner (task T2.5).

    python -m eval.runner --set eval/smoke_set.jsonl --api http://localhost:8080
        --token ... --mode hybrid

It reads the evaluation file, runs every question as its user, and reports recall@10, recall@50, MRR
and nDCG@10. The report is saved with the configuration that produced it (models, chunker version,
search settings), so runs can be compared later. A small smoke set runs in CI. The full set runs on
demand and before any model, prompt or index change goes live.
"""

import argparse
import asyncio
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

import httpx
from pydantic import BaseModel

from app.core.security import Identity
from app.core.settings import Settings
from app.retrieval.service import RequestMode, SearchService
from eval.dataset import EvalQuestion, load_questions
from eval.metrics import RankedHit, ndcg_at_k, recall_at_k, reciprocal_rank

DEFAULT_KS = (10, 50)


class SearchFn(Protocol):
    """Runs one question. Returns the ranked hits and the mode that really ran."""

    async def __call__(self, question: EvalQuestion, top_k: int) -> tuple[list[RankedHit], str]:
        """Search as the question's user."""
        ...


class QuestionResult(BaseModel):
    """What one question got."""

    id: str
    answerable: bool
    mode_used: str
    first_correct_rank: int | None
    reciprocal_rank: float
    recall: dict[str, float]
    ndcg_at_10: float
    returned: int


class RetrievalReport(BaseModel):
    """All results of one run, with the configuration that made them."""

    name: str
    created_at: str
    config: dict[str, Any]
    questions: int
    answerable: int
    metrics: dict[str, float]
    modes_used: dict[str, int]
    results: list[QuestionResult]


def _first_rank(hits: list[RankedHit], question: EvalQuestion) -> int | None:
    from eval.metrics import matches

    for rank, hit in enumerate(hits, start=1):
        if any(matches(hit, item) for item in question.relevant):
            return rank
    return None


async def evaluate(
    questions: list[EvalQuestion],
    search: SearchFn,
    *,
    name: str,
    config: dict[str, Any],
    ks: tuple[int, ...] = DEFAULT_KS,
    clock: Any = lambda: datetime.now(UTC),
) -> RetrievalReport:
    """Run all questions and compute the metrics over the answerable ones."""
    top_k = max(ks)
    results: list[QuestionResult] = []
    modes: Counter[str] = Counter()
    for question in questions:
        hits, mode_used = await search(question, top_k)
        modes[mode_used] += 1
        results.append(
            QuestionResult(
                id=question.id,
                answerable=question.answerable,
                mode_used=mode_used,
                first_correct_rank=_first_rank(hits, question) if question.answerable else None,
                reciprocal_rank=reciprocal_rank(hits, question.relevant),
                recall={str(k): recall_at_k(hits, question.relevant, k) for k in ks},
                ndcg_at_10=ndcg_at_k(hits, question.relevant, 10),
                returned=len(hits),
            )
        )
    scored = [r for r in results if r.answerable]
    count = len(scored) or 1
    metrics = {f"recall@{k}": sum(r.recall[str(k)] for r in scored) / count for k in ks}
    metrics["mrr"] = sum(r.reciprocal_rank for r in scored) / count
    metrics["ndcg@10"] = sum(r.ndcg_at_10 for r in scored) / count
    return RetrievalReport(
        name=name,
        created_at=clock().isoformat(),
        config=config,
        questions=len(results),
        answerable=len(scored),
        metrics={k: round(v, 4) for k, v in metrics.items()},
        modes_used=dict(modes),
        results=results,
    )


def save_report(report: RetrievalReport, directory: Path) -> Path:
    """Write the report as JSON. The file name has the time and the run name."""
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report.created_at).strftime("%Y%m%dT%H%M%S")
    path = directory / f"{stamp}-{report.name}.json"
    path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return path


def config_from_settings(settings: Settings, **extra: Any) -> dict[str, Any]:
    """The settings that decide retrieval quality."""
    return {
        "embedding_model": f"{settings.embedding.model}@{settings.embedding.model_version}",
        "reranker_model": settings.reranker.model,
        "reranker_enabled": settings.reranker.enabled,
        "chunker": settings.chunking.model_dump(exclude={"tokenizer_file"}),
        "search": settings.search.model_dump(),
        **extra,
    }


# --- adapters -------------------------------------------------------------------------------


class ServiceSearch:
    """Searches in process through the ``SearchService``."""

    def __init__(self, service: SearchService, mode: RequestMode = "hybrid") -> None:
        self._service = service
        self._mode = mode

    async def __call__(self, question: EvalQuestion, top_k: int) -> tuple[list[RankedHit], str]:
        identity = Identity(question.as_user, tuple(question.groups), "eval")
        result = await self._service.search(
            query=question.question, identity=identity, top_k=top_k, mode=self._mode
        )
        hits = [RankedHit(h.doc_id, h.chunk_id, tuple(h.pages)) for h in result.hits]
        return hits, result.mode_used


class HttpSearch:
    """Searches through the HTTP API like the Java app does."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        token: str,
        mode: Literal["hybrid", "bm25", "vector"] = "hybrid",
        rerank: bool | None = None,
    ) -> None:
        self._client = client
        self._url = f"{base_url.rstrip('/')}/v1/search"
        self._token = token
        self._mode = mode
        self._rerank = rerank

    async def __call__(self, question: EvalQuestion, top_k: int) -> tuple[list[RankedHit], str]:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-User-Id": question.as_user,
            "X-User-Groups": ",".join(question.groups),
        }
        body: dict[str, Any] = {"query": question.question, "top_k": top_k, "mode": self._mode}
        if self._rerank is not None:
            body["rerank"] = self._rerank
        response = await self._client.post(self._url, json=body, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        hits = [RankedHit(r["doc_id"], r["chunk_id"], tuple(r["pages"])) for r in data["results"]]
        return hits, str(data["mode_used"])


# --- command line ---------------------------------------------------------------------------


def below_thresholds(report: RetrievalReport, minimums: dict[str, float]) -> list[str]:
    """Which metrics are under their minimum. Used as a quality gate."""
    return [
        f"{name}: {report.metrics.get(name, 0.0)} < {minimum}"
        for name, minimum in minimums.items()
        if report.metrics.get(name, 0.0) < minimum
    ]


async def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="eval.runner", description=__doc__)
    parser.add_argument("--set", required=True, type=Path)
    parser.add_argument("--api", required=True, help="base URL of the service")
    parser.add_argument("--token", required=True)
    parser.add_argument("--mode", default="hybrid", choices=["hybrid", "bm25", "vector"])
    parser.add_argument("--rerank", choices=["on", "off"])
    parser.add_argument("--name", default="run")
    parser.add_argument("--out", type=Path, default=Path("eval/results"))
    parser.add_argument("--slice", dest="slice_name", default="dev", choices=["dev", "test"])
    parser.add_argument(
        "--final", action="store_true", help="allow the test slice (final check only)"
    )
    parser.add_argument("--config", type=Path, help="JSON file with the configuration to record")
    parser.add_argument("--min-recall10", type=float, default=0.0)
    parser.add_argument("--min-mrr", type=float, default=0.0)
    args = parser.parse_args(argv)
    questions = load_questions(args.set, slice_name=args.slice_name, allow_test_slice=args.final)
    config: dict[str, Any] = (
        json.loads(args.config.read_text(encoding="utf-8")) if args.config else {}
    )
    config.update({"api": args.api, "mode": args.mode, "rerank": args.rerank, "set": str(args.set)})
    rerank = None if args.rerank is None else args.rerank == "on"
    async with httpx.AsyncClient() as client:
        search = HttpSearch(client, args.api, args.token, args.mode, rerank)
        report = await evaluate(questions, search, name=args.name, config=config)
    path = save_report(report, args.out)
    print(json.dumps(report.metrics, indent=2))
    print(f"saved: {path}")
    failed = below_thresholds(report, {"recall@10": args.min_recall10, "mrr": args.min_mrr})
    for line in failed:
        print(f"below threshold: {line}", file=sys.stderr)
    return 1 if failed else 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
