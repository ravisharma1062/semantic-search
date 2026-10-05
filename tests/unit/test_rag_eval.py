import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from eval.dataset import EvalQuestion, Relevant
from eval.rag_eval import (
    AnswerRecord,
    CitedSource,
    JudgeScores,
    LlmJudge,
    ServiceAnswer,
    below_thresholds,
    compare_prompts,
    correct_citations,
    evaluate_rag,
    format_comparison,
    parse_faithfulness,
    parse_relevance,
)

from app.core.security import Identity
from app.core.settings import Settings
from tests.fakes import FakeLLMClient
from tests.unit.test_answer_service import ALICE, make_service

PROMPTS = Path(__file__).resolve().parents[2] / "prompts"
CLOCK = lambda: datetime(2025, 1, 1, tzinfo=UTC)  # noqa: E731


def _q(
    qid: str,
    *,
    doc: str | None = "D1",
    pages: list[int] | None = None,
    reference: str | None = "ref",
) -> EvalQuestion:
    relevant = [Relevant(doc_id=doc, pages=pages or [])] if doc else []
    return EvalQuestion(
        id=qid,
        question=f"question {qid}",
        relevant=relevant,
        answerable=bool(doc),
        reference_answer=reference,
    )


def _found(*cited: tuple[str, list[int]]) -> AnswerRecord:
    return AnswerRecord(
        found=True,
        reason="answered",
        answer="an answer",
        citations=[CitedSource(doc_id=d, pages=p, text=f"text of {d}") for d, p in cited],
    )


NOT_FOUND = AnswerRecord(found=False, reason="not_found")


class Scripted:
    """An AnswerFn with one record per question ID."""

    def __init__(self, records: dict[str, AnswerRecord]) -> None:
        self.records = records

    async def __call__(self, question: EvalQuestion) -> AnswerRecord:
        return self.records[question.id]


class FixedJudge:
    def __init__(self, faith: float | None, rel: float | None) -> None:
        self.scores = JudgeScores(faithfulness=faith, relevance=rel)
        self.seen: list[str] = []

    async def score(self, question: EvalQuestion, record: AnswerRecord) -> JudgeScores:
        self.seen.append(question.id)
        return self.scores


# --- parsing the judge's reply --------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ('{"supported": 3, "total": 4}', 0.75),
        ('Sure! {"supported": 0, "total": 2} done', 0.0),
        ('{"supported": 5, "total": 4}', None),
        ('{"supported": 1, "total": 0}', None),
        ("no json", None),
        ('{"supported": "x", "total": 2}', None),
        ("[1, 2]", None),
    ],
)
def test_faithfulness_reply(text: str, expected: float | None) -> None:
    assert parse_faithfulness(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ('{"score": 5}', 1.0),
        ('{"score": 1}', 0.0),
        ('{"score": 3}', 0.5),
        ('{"score": 9}', None),
        ("x", None),
    ],
)
def test_relevance_reply(text: str, expected: float | None) -> None:
    assert parse_relevance(text) == expected


# --- citation accuracy ----------------------------------------------------------------------


def test_citations_must_name_a_correct_document() -> None:
    q = _q("a", doc="D1")
    assert correct_citations(q, _found(("D1", [1]), ("D9", [1]))) == 1


def test_pages_count_when_the_set_names_them() -> None:
    q = _q("a", doc="D1", pages=[4])
    assert correct_citations(q, _found(("D1", [4, 5]))) == 1
    assert correct_citations(q, _found(("D1", [9]))) == 0
    assert correct_citations(_q("b", doc="D1"), _found(("D1", [9]))) == 1  # no pages named


# --- the run --------------------------------------------------------------------------------


async def test_all_four_measures_and_the_false_refusal_rate() -> None:
    questions = [
        _q("a", doc="D1"),
        _q("b", doc="D2"),
        _q("c", doc="D3"),  # the system wrongly refuses
        _q("n1", doc=None, reference=None),
        _q("n2", doc=None, reference=None),  # the system wrongly answers
    ]
    answers = Scripted(
        {
            "a": _found(("D1", [1])),
            "b": _found(("D2", [1]), ("D9", [1])),
            "c": NOT_FOUND,
            "n1": NOT_FOUND,
            "n2": _found(("D5", [1])),
        }
    )
    judge = FixedJudge(0.5, 0.75)
    report = await evaluate_rag(questions, answers, judge, name="t", config={"k": 1}, clock=CLOCK)
    m = report.metrics
    assert m["citation_accuracy"] == round(2 / 3, 4)  # a:1/1, b:1/2 -> 2 of 3 cited sources
    assert m["not_found_accuracy"] == 0.5
    assert m["false_refusal"] == round(1 / 3, 4)
    assert m["faithfulness"] == 0.5 and m["relevance"] == 0.75
    assert judge.seen == ["a", "b"]  # only answered, answerable questions are judged
    assert report.config == {"k": 1} and report.questions == 5 and report.judge_errors == 0
    json.loads(report.model_dump_json())


async def test_without_a_judge_only_the_citation_and_refusal_measures_exist() -> None:
    report = await evaluate_rag(
        [_q("a")], Scripted({"a": _found(("D1", [1]))}), None, name="t", config={}, clock=CLOCK
    )
    assert report.metrics["faithfulness"] is None and report.metrics["relevance"] is None
    assert report.metrics["citation_accuracy"] == 1.0
    assert report.metrics["not_found_accuracy"] is None  # no unanswerable question in the set


async def test_judge_failures_are_counted_not_scored_as_zero() -> None:
    report = await evaluate_rag(
        [_q("a")],
        Scripted({"a": _found(("D1", [1]))}),
        FixedJudge(None, None),
        name="t",
        config={},
        clock=CLOCK,
    )
    assert report.metrics["faithfulness"] is None
    assert report.judge_errors == 2


def test_quality_gate() -> None:
    from eval.rag_eval import RagReport

    report = RagReport(
        name="t",
        created_at="x",
        config={},
        questions=1,
        metrics={"citation_accuracy": 0.8, "not_found_accuracy": None},
        judge_errors=0,
        outcomes=[],
    )
    assert below_thresholds(report, {"citation_accuracy": 0.7}) == []
    assert below_thresholds(report, {"citation_accuracy": 0.9}) == ["citation_accuracy: 0.8 < 0.9"]
    assert below_thresholds(report, {"not_found_accuracy": 0.5})  # not computable fails the gate


# --- the LLM judge --------------------------------------------------------------------------


async def test_the_llm_judge_uses_the_judge_prompts_and_the_cited_text() -> None:
    llm = FakeLLMClient(['{"supported": 1, "total": 2}', '{"score": 5}'])
    judge = LlmJudge.from_prompts(llm, PROMPTS)
    scores = await judge.score(_q("a"), _found(("D1", [1])))
    assert (scores.faithfulness, scores.relevance) == (0.5, 1.0)
    faith_prompt, rel_prompt = llm.calls
    assert "text of D1" in faith_prompt[1].content and "an answer" in faith_prompt[1].content
    assert "ref" in rel_prompt[1].content and "question a" in rel_prompt[1].content


async def test_the_judge_skips_what_it_cannot_judge() -> None:
    llm = FakeLLMClient(['{"score": 4}'])
    judge = LlmJudge.from_prompts(llm, PROMPTS)
    scores = await judge.score(_q("a"), AnswerRecord(found=True, answer="x"))  # no citations
    assert scores.faithfulness is None and scores.relevance == 0.75
    assert len(llm.calls) == 1
    scores = await judge.score(_q("a", reference=None), _found(("D1", [1])))
    assert scores.relevance is None


# --- prompt versions ------------------------------------------------------------------------


async def test_prompt_versions_are_compared_on_the_same_questions() -> None:
    questions = [_q("a"), _q("n", doc=None, reference=None)]
    good = Scripted({"a": _found(("D1", [1])), "n": NOT_FOUND})
    bad = Scripted({"a": _found(("D9", [1])), "n": _found(("D9", [1]))})
    reports = await compare_prompts(questions, {"v1": good, "v2": bad}, None)
    assert [r.name for r in reports] == ["prompt-v1", "prompt-v2"]
    assert reports[0].config["prompt_version"] == "v1"
    table = format_comparison(reports).splitlines()
    assert table[0].startswith("| run | faithfulness")
    assert "prompt-v1 | n/a | n/a | 1.000 | 1.000 | 0.000 |" in table[2]
    assert "prompt-v2 | n/a | n/a | 0.000 | 0.000 | 0.000 |" in table[3]
    assert format_comparison([]) == ""


# --- in process -----------------------------------------------------------------------------


async def test_the_service_adapter_gives_the_judge_the_real_passage(settings: Settings) -> None:
    llm = FakeLLMClient(["The vendor pays one percent [1]."])
    service, _ = make_service(settings, llm)
    question = EvalQuestion(
        id="q",
        question="penalty for late delivery",
        relevant=[Relevant(doc_id="D1")],
        as_user=ALICE.user_id,
        groups=list(ALICE.groups),
    )
    record = await ServiceAnswer(service)(question)
    assert record.found and [c.doc_id for c in record.citations] == ["D1"]
    assert "one percent of the order value per week of late delivery" in record.citations[0].text
    assert isinstance(ALICE, Identity)
