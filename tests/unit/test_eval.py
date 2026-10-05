import json
import math
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx
from eval.dataset import EvalQuestion, Relevant, load_questions
from eval.metrics import RankedHit, matches, ndcg_at_k, recall_at_k, reciprocal_rank
from eval.runner import (
    HttpSearch,
    below_thresholds,
    config_from_settings,
    evaluate,
    save_report,
)

from app.core.errors import InvalidRequestError
from app.core.settings import Settings

ROOT = Path(__file__).resolve().parents[2]


def _hit(doc: str, *pages: int) -> RankedHit:
    return RankedHit(doc_id=doc, chunk_id=f"{doc}:0", pages=pages)


# --- metrics --------------------------------------------------------------------------------


def test_a_hit_must_match_the_document_and_overlap_the_pages_when_pages_are_given() -> None:
    assert matches(_hit("A", 4, 5), Relevant(doc_id="A", pages=[5]))
    assert not matches(_hit("A", 1), Relevant(doc_id="A", pages=[5]))
    assert matches(_hit("A", 1), Relevant(doc_id="A"))  # no pages given: the document is enough
    assert not matches(_hit("B", 5), Relevant(doc_id="A", pages=[5]))
    assert not matches(_hit("A"), Relevant(doc_id="A", pages=[5]))  # a hit without pages


def test_recall_counts_found_correct_items_in_the_top_k() -> None:
    relevant = [Relevant(doc_id="A"), Relevant(doc_id="B")]
    ranked = [_hit("X"), _hit("A"), _hit("Y"), _hit("B")]
    assert recall_at_k(ranked, relevant, 1) == 0.0
    assert recall_at_k(ranked, relevant, 2) == 0.5
    assert recall_at_k(ranked, relevant, 4) == 1.0
    assert recall_at_k([], relevant, 10) == 0.0
    assert recall_at_k(ranked, [], 10) == 0.0


def test_reciprocal_rank_is_one_over_the_first_correct_rank() -> None:
    relevant = [Relevant(doc_id="A")]
    assert reciprocal_rank([_hit("A")], relevant) == 1.0
    assert reciprocal_rank([_hit("X"), _hit("Y"), _hit("A")], relevant) == pytest.approx(1 / 3)
    assert reciprocal_rank([_hit("X")], relevant) == 0.0
    assert reciprocal_rank([], relevant) == 0.0


def test_ndcg_is_one_for_the_ideal_order_and_lower_for_a_late_hit() -> None:
    relevant = [Relevant(doc_id="A"), Relevant(doc_id="B")]
    assert ndcg_at_k([_hit("A"), _hit("B"), _hit("X")], relevant, 10) == pytest.approx(1.0)
    late = ndcg_at_k([_hit("X"), _hit("A"), _hit("B")], relevant, 10)
    expected = (1 / math.log2(3) + 1 / math.log2(4)) / (1 + 1 / math.log2(3))
    assert late == pytest.approx(expected)
    assert ndcg_at_k([_hit("X")], relevant, 10) == 0.0


def test_ndcg_counts_a_document_once_and_respects_k() -> None:
    relevant = [Relevant(doc_id="A")]
    twice = ndcg_at_k([_hit("A"), _hit("A"), _hit("A")], relevant, 10)
    assert twice == pytest.approx(1.0)  # more chunks of the same document do not add up
    assert ndcg_at_k([_hit("X"), _hit("A")], relevant, 1) == 0.0


# --- the evaluation set ---------------------------------------------------------------------


def test_the_committed_smoke_set_loads_and_has_dev_and_test_slices() -> None:
    dev = load_questions(ROOT / "eval/smoke_set.jsonl")
    assert len(dev) == 11
    assert {q.slice for q in dev} == {"dev"}
    final = load_questions(ROOT / "eval/smoke_set.jsonl", slice_name="test", allow_test_slice=True)
    assert [q.id for q in final] == ["t01", "t02"]


def test_the_test_slice_needs_an_explicit_allowance() -> None:
    with pytest.raises(InvalidRequestError, match="test slice"):
        load_questions(ROOT / "eval/smoke_set.jsonl", slice_name="test")


@pytest.mark.parametrize(
    "line",
    [
        '{"id": "a", "question": "q"}',  # answerable but nothing correct
        '{"id": "a", "question": "q", "answerable": false, "relevant": [{"doc_id": "D"}]}',
        "not json",
        '{"question": "no id"}',
    ],
)
def test_bad_lines_are_refused_with_the_line_number(tmp_path: Path, line: str) -> None:
    path = tmp_path / "set.jsonl"
    path.write_text(line + "\n", encoding="utf-8")
    with pytest.raises(InvalidRequestError, match="line 1"):
        load_questions(path)


def test_duplicate_ids_are_refused(tmp_path: Path) -> None:
    line = '{"id": "a", "question": "q", "relevant": [{"doc_id": "D"}]}'
    path = tmp_path / "set.jsonl"
    path.write_text(f"{line}\n{line}\n", encoding="utf-8")
    with pytest.raises(InvalidRequestError, match="unique"):
        load_questions(path)


# --- the runner -----------------------------------------------------------------------------


class _Fake:
    """Answers from a table: question ID to the ranked documents."""

    def __init__(self, table: dict[str, list[str]], mode: str = "hybrid") -> None:
        self.table = table
        self.mode = mode
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, question: EvalQuestion, top_k: int) -> tuple[list[RankedHit], str]:
        self.calls.append((question.id, top_k))
        return [_hit(d, 1) for d in self.table.get(question.id, [])], self.mode


def _questions() -> list[EvalQuestion]:
    return [
        EvalQuestion(id="a", question="qa", relevant=[Relevant(doc_id="A")]),
        EvalQuestion(id="b", question="qb", relevant=[Relevant(doc_id="B")]),
        EvalQuestion(id="n", question="no answer", answerable=False),
    ]


async def test_the_report_has_the_metrics_over_answerable_questions_only() -> None:
    fake = _Fake({"a": ["A", "X"], "b": ["X", "B"], "n": ["Z"]})
    report = await evaluate(_questions(), fake, name="t", config={"k": 1}, ks=(1, 10))
    assert report.questions == 3
    assert report.answerable == 2
    assert report.metrics["recall@1"] == 0.5
    assert report.metrics["recall@10"] == 1.0
    assert report.metrics["mrr"] == 0.75
    assert 0.8 < report.metrics["ndcg@10"] < 0.9
    assert [r.first_correct_rank for r in report.results] == [1, 2, None]
    assert report.modes_used == {"hybrid": 3}
    assert report.config == {"k": 1}


async def test_the_search_is_asked_for_the_largest_k() -> None:
    fake = _Fake({})
    await evaluate(_questions(), fake, name="t", config={}, ks=(10, 50))
    assert {k for _, k in fake.calls} == {50}


async def test_fallbacks_are_counted() -> None:
    report = await evaluate(_questions(), _Fake({}, mode="bm25"), name="t", config={})
    assert report.modes_used == {"bm25": 3}


async def test_a_run_without_answerable_questions_does_not_divide_by_zero() -> None:
    only_none = [EvalQuestion(id="n", question="q", answerable=False)]
    report = await evaluate(only_none, _Fake({}), name="t", config={})
    assert report.metrics["mrr"] == 0.0


async def test_the_report_is_saved_with_its_config_and_can_be_read_back(tmp_path: Path) -> None:
    report = await evaluate(
        _questions(),
        _Fake({"a": ["A"]}),
        name="baseline",
        config={"embedding_model": "bge-m3@1"},
        clock=lambda: datetime(2026, 10, 5, 12, 30, 15, tzinfo=UTC),
    )
    path = save_report(report, tmp_path / "results")
    assert path.name == "20261005T123015-baseline.json"
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["config"] == {"embedding_model": "bge-m3@1"}
    assert saved["metrics"]["recall@10"] == 0.5


def test_the_config_records_what_decides_quality(settings: Settings) -> None:
    config = config_from_settings(settings, run="x")
    assert config["embedding_model"] == "bge-m3@1"
    assert config["chunker"]["target_tokens"] == 400
    assert config["search"]["rrf_rank_constant"] == 60
    assert config["run"] == "x"
    assert "tokenizer_file" not in config["chunker"]


def test_thresholds_make_a_quality_gate() -> None:
    from eval.runner import RetrievalReport

    report = RetrievalReport(
        name="t", created_at="2026-01-01T00:00:00+00:00", config={}, questions=1, answerable=1,
        metrics={"recall@10": 0.8, "mrr": 0.4}, modes_used={}, results=[],
    )  # fmt: skip
    assert below_thresholds(report, {"recall@10": 0.7, "mrr": 0.5}) == ["mrr: 0.4 < 0.5"]
    assert below_thresholds(report, {"recall@10": 0.7}) == []


# --- the HTTP adapter -----------------------------------------------------------------------


async def test_the_http_adapter_calls_the_api_like_the_java_app() -> None:
    answer = {
        "request_id": "r",
        "mode_used": "hybrid+rerank",
        "results": [
            {
                "doc_id": "A",
                "chunk_id": "A:0",
                "score": 1,
                "pages": [3, 4],
                "snippet": "s",
                "highlights": [],
            }
        ],
        "took_ms": 5,
    }
    question = EvalQuestion(
        id="a",
        question="qa",
        relevant=[Relevant(doc_id="A")],
        as_user="lawyer",
        groups=["g1", "g2"],
    )
    async with httpx.AsyncClient() as client:
        with respx.mock() as router:
            route = router.post("http://api.test/v1/search").respond(json=answer)
            hits, mode = await HttpSearch(client, "http://api.test/", "tok", "hybrid", rerank=True)(
                question, 20
            )
    request = route.calls[0].request
    assert request.headers["authorization"] == "Bearer tok"
    assert request.headers["x-user-id"] == "lawyer"
    assert request.headers["x-user-groups"] == "g1,g2"
    assert json.loads(request.content) == {
        "query": "qa",
        "top_k": 20,
        "mode": "hybrid",
        "rerank": True,
    }
    assert (hits, mode) == ([RankedHit("A", "A:0", (3, 4))], "hybrid+rerank")
