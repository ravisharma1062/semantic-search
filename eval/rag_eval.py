"""RAG evaluation (task T4.4): is the answer right, supported, and correctly cited?

    python -m eval.rag_eval --set eval/smoke_set.jsonl --api http://localhost:8080 \
        --token ... --judge

Four measures (HLD section 10):

* faithfulness: the share of the answer's claims that the sources support (LLM judge)
* answer relevance: how well the answer fits the question and the reference answer (LLM judge)
* citation accuracy: the share of cited sources that are correct documents (and pages, when the set
  names pages). No judge needed.
* "not found" accuracy: for questions that have no answer, the share that were refused. The report
  also gives the false refusal rate: answerable questions that were refused.

The judge is the same ``LLMClient`` interface as the service uses, with versioned prompts
(``prompts/judge_*.v1.txt``). LLM scores are rough: spot-check them by hand.
``compare_prompts`` runs the same questions with several prompt versions so the choice is based on
numbers.
"""

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, Field

from app.core.security import Identity
from app.llm.base import LLMClient, LLMOptions
from app.rag.prompts import PromptTemplate, load_prompt
from app.rag.service import AnswerService
from eval.dataset import EvalQuestion, load_questions

METRICS = ("faithfulness", "relevance", "citation_accuracy", "not_found_accuracy", "false_refusal")
_JSON = re.compile(r"\{.*?\}", re.DOTALL)


class CitedSource(BaseModel):
    """A source the answer cited. ``text`` is the passage when known, else the snippet."""

    doc_id: str
    pages: list[int] = Field(default_factory=list)
    text: str = ""


class AnswerRecord(BaseModel):
    """What the system answered to one question."""

    found: bool
    reason: str = ""
    answer: str = ""
    citations: list[CitedSource] = Field(default_factory=list)


class AnswerFn(Protocol):
    """Runs one question as its user."""

    async def __call__(self, question: EvalQuestion) -> AnswerRecord:
        """Answer one question."""
        ...


class JudgeScores(BaseModel):
    """Scores of the judge for one answer. ``None`` means the judge gave no usable reply."""

    faithfulness: float | None = None
    relevance: float | None = None


class QuestionOutcome(BaseModel):
    """The scores of one question."""

    id: str
    answerable: bool
    found: bool
    reason: str
    cited: int
    correct_citations: int
    faithfulness: float | None = None
    relevance: float | None = None


class RagReport(BaseModel):
    """All outcomes of one run with the configuration that produced them."""

    name: str
    created_at: str
    config: dict[str, Any]
    questions: int
    metrics: dict[str, float | None]
    judge_errors: int
    outcomes: list[QuestionOutcome]


# --- the judge ------------------------------------------------------------------------------


def _json_object(text: str) -> dict[str, Any] | None:
    for match in _JSON.finditer(text):
        try:
            value = json.loads(match.group(0))
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return None


def parse_faithfulness(text: str) -> float | None:
    """``{"supported": 3, "total": 4}`` becomes 0.75. A reply that does not fit gives ``None``."""
    data = _json_object(text)
    try:
        supported, total = int(data["supported"]), int(data["total"])  # type: ignore[index]
    except (TypeError, KeyError, ValueError):
        return None
    if total <= 0 or not 0 <= supported <= total:
        return None
    return supported / total


def parse_relevance(text: str) -> float | None:
    """``{"score": 4}`` (1 to 5) becomes 0.75."""
    data = _json_object(text)
    try:
        score = int(data["score"])  # type: ignore[index]
    except (TypeError, KeyError, ValueError):
        return None
    return (score - 1) / 4 if 1 <= score <= 5 else None


class LlmJudge:
    """Scores answers with an LLM and the judge prompts."""

    def __init__(self, llm: LLMClient, faithfulness: PromptTemplate, relevance: PromptTemplate):
        self._llm = llm
        self._faithfulness = faithfulness
        self._relevance = relevance
        self._options = LLMOptions(max_output_tokens=60, temperature=0.0)

    @classmethod
    def from_prompts(cls, llm: LLMClient, directory: Path, version: str = "v1") -> "LlmJudge":
        """A judge with the prompt files of one version."""
        return cls(
            llm,
            load_prompt(directory, "judge_faithfulness", version),
            load_prompt(directory, "judge_relevance", version),
        )

    async def score(self, question: EvalQuestion, record: AnswerRecord) -> JudgeScores:
        """Faithfulness against the cited passages, and relevance against the reference."""
        context = "\n\n".join(f"[{i}] {c.text}" for i, c in enumerate(record.citations, start=1))
        faith = rel = None
        if context:
            reply = await self._llm.generate(
                self._faithfulness.render(context=context, answer=record.answer), self._options
            )
            faith = parse_faithfulness(reply)
        if question.reference_answer:
            reply = await self._llm.generate(
                self._relevance.render(
                    question=question.question,
                    reference=question.reference_answer,
                    answer=record.answer,
                ),
                self._options,
            )
            rel = parse_relevance(reply)
        return JudgeScores(faithfulness=faith, relevance=rel)


class Judge(Protocol):
    """Anything that can score an answer."""

    async def score(self, question: EvalQuestion, record: AnswerRecord) -> JudgeScores:
        """Scores for one answer."""
        ...


# --- measures -------------------------------------------------------------------------------


def correct_citations(question: EvalQuestion, record: AnswerRecord) -> int:
    """How many cited sources are a correct document, on a correct page if the set names pages."""
    count = 0
    for cited in record.citations:
        for item in question.relevant:
            if cited.doc_id != item.doc_id:
                continue
            if item.pages and cited.pages and not set(item.pages) & set(cited.pages):
                continue
            count += 1
            break
    return count


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def summarise(outcomes: list[QuestionOutcome]) -> dict[str, float | None]:
    """The measures over all outcomes. A measure with nothing to measure is ``None``."""
    answered = [o for o in outcomes if o.answerable and o.found]
    answerable = [o for o in outcomes if o.answerable]
    unanswerable = [o for o in outcomes if not o.answerable]
    cited = sum(o.cited for o in answered)
    return {
        "faithfulness": _mean([o.faithfulness for o in answered if o.faithfulness is not None]),
        "relevance": _mean([o.relevance for o in answered if o.relevance is not None]),
        "citation_accuracy": round(sum(o.correct_citations for o in answered) / cited, 4)
        if cited
        else None,
        "not_found_accuracy": _mean([0.0 if o.found else 1.0 for o in unanswerable]),
        "false_refusal": _mean([0.0 if o.found else 1.0 for o in answerable]),
    }


async def evaluate_rag(
    questions: list[EvalQuestion],
    answer: AnswerFn,
    judge: Judge | None,
    *,
    name: str,
    config: dict[str, Any],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RagReport:
    """Run all questions, score them, and summarise."""
    outcomes: list[QuestionOutcome] = []
    judge_errors = 0
    for question in questions:
        record = await answer(question)
        scores = JudgeScores()
        if judge is not None and record.found and question.answerable:
            scores = await judge.score(question, record)
            judge_errors += int(scores.faithfulness is None and bool(record.citations))
            judge_errors += int(scores.relevance is None and bool(question.reference_answer))
        outcomes.append(
            QuestionOutcome(
                id=question.id,
                answerable=question.answerable,
                found=record.found,
                reason=record.reason,
                cited=len(record.citations),
                correct_citations=correct_citations(question, record),
                faithfulness=scores.faithfulness,
                relevance=scores.relevance,
            )
        )
    return RagReport(
        name=name,
        created_at=clock().isoformat(),
        config=config,
        questions=len(questions),
        metrics=summarise(outcomes),
        judge_errors=judge_errors,
        outcomes=outcomes,
    )


async def compare_prompts(
    questions: list[EvalQuestion],
    factories: Mapping[str, AnswerFn],
    judge: Judge | None,
    *,
    config: dict[str, Any] | None = None,
) -> list[RagReport]:
    """Run the same questions once per prompt version."""
    return [
        await evaluate_rag(
            questions,
            fn,
            judge,
            name=f"prompt-{version}",
            config={**(config or {}), "prompt_version": version},
        )
        for version, fn in factories.items()
    ]


def format_comparison(reports: list[RagReport]) -> str:
    """A Markdown table of the measures, one row per run."""
    if not reports:
        return ""
    lines = [
        "| run | " + " | ".join(METRICS) + " |",
        "| --- | " + " | ".join("---" for _ in METRICS) + " |",
    ]
    for report in reports:
        cells = [
            "n/a" if report.metrics[m] is None else f"{report.metrics[m]:.3f}" for m in METRICS
        ]
        lines.append(f"| {report.name} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def below_thresholds(report: RagReport, minimums: dict[str, float]) -> list[str]:
    """Measures under their minimum (a quality gate). A measure that could not be computed fails."""
    failed = []
    for name, minimum in minimums.items():
        value = report.metrics.get(name)
        if value is None or value < minimum:
            failed.append(f"{name}: {value} < {minimum}")
    return failed


# --- adapters -------------------------------------------------------------------------------


class ServiceAnswer:
    """Answers in process through the ``AnswerService``. The judge sees the real passages."""

    def __init__(self, service: AnswerService) -> None:
        self._service = service

    async def __call__(self, question: EvalQuestion) -> AnswerRecord:
        identity = Identity(question.as_user, tuple(question.groups), "eval")
        result, context = await self._service.answer_with_context(
            question=question.question, identity=identity
        )
        text = {c.chunk_id: c.text for c in context}
        return AnswerRecord(
            found=result.found,
            reason=result.reason,
            answer=result.answer,
            citations=[
                CitedSource(doc_id=c.doc_id, pages=c.pages, text=text.get(c.chunk_id, c.snippet))
                for c in result.citations
            ],
        )


class HttpAnswer:
    """Answers through the HTTP API like the Java app does. The API returns snippets, not whole
    passages, so the judge sees the snippets."""

    def __init__(self, client: httpx.AsyncClient, base_url: str, token: str) -> None:
        self._client = client
        self._url = f"{base_url.rstrip('/')}/v1/answer"
        self._token = token

    async def __call__(self, question: EvalQuestion) -> AnswerRecord:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-User-Id": question.as_user,
            "X-User-Groups": ",".join(question.groups),
        }
        response = await self._client.post(
            self._url, json={"question": question.question}, headers=headers, timeout=60
        )
        response.raise_for_status()
        data = response.json()
        return AnswerRecord(
            found=bool(data["found"]),
            reason=str(data["reason"]),
            answer=str(data["answer"]),
            citations=[
                CitedSource(doc_id=c["doc_id"], pages=c["pages"], text=c["snippet"])
                for c in data["citations"]
            ],
        )


# --- command line ---------------------------------------------------------------------------


async def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="eval.rag_eval", description=__doc__)
    parser.add_argument("--set", required=True, type=Path)
    parser.add_argument("--api", required=True, help="base URL of the service")
    parser.add_argument("--token", required=True)
    parser.add_argument("--judge", action="store_true", help="score with the configured LLM")
    parser.add_argument("--prompts-dir", type=Path, default=Path("prompts"))
    parser.add_argument("--name", default="run")
    parser.add_argument("--out", type=Path, default=Path("eval/results"))
    parser.add_argument("--slice", dest="slice_name", default="dev", choices=["dev", "test"])
    parser.add_argument("--final", action="store_true", help="allow the test slice")
    parser.add_argument("--min-citation-accuracy", type=float, default=0.0)
    parser.add_argument("--min-not-found-accuracy", type=float, default=0.0)
    args = parser.parse_args(argv)
    questions = load_questions(args.set, slice_name=args.slice_name, allow_test_slice=args.final)
    async with httpx.AsyncClient() as client:
        judge: Judge | None = None
        if args.judge:
            from app.core.settings import get_settings
            from app.llm.factory import create_llm

            settings = get_settings()
            llm = create_llm(settings.llm, client, settings.api.request_retry)
            judge = LlmJudge.from_prompts(llm, args.prompts_dir)
        report = await evaluate_rag(
            questions,
            HttpAnswer(client, args.api, args.token),
            judge,
            name=args.name,
            config={"api": args.api, "set": str(args.set), "judge": args.judge},
        )
    args.out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(report.created_at).strftime("%Y%m%dT%H%M%S")
    path = args.out / f"{stamp}-rag-{report.name}.json"
    path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    print(json.dumps(report.metrics, indent=2))
    print(f"saved: {path}")
    failed = below_thresholds(
        report,
        {
            **(
                {"citation_accuracy": args.min_citation_accuracy}
                if args.min_citation_accuracy
                else {}
            ),
            **(
                {"not_found_accuracy": args.min_not_found_accuracy}
                if args.min_not_found_accuracy
                else {}
            ),
        },
    )
    for line in failed:
        print(f"below threshold: {line}", file=sys.stderr)
    return 1 if failed else 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
