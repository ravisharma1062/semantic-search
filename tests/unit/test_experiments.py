from collections.abc import Callable

from eval.dataset import EvalQuestion, Relevant
from eval.experiments import Experiment, best_by, build_matrix, format_comparison, run_matrix
from eval.metrics import RankedHit
from eval.runner import SearchFn


def _questions() -> list[EvalQuestion]:
    return [
        EvalQuestion(id="a", question="qa", relevant=[Relevant(doc_id="A")]),
        EvalQuestion(id="b", question="qb", relevant=[Relevant(doc_id="B")]),
    ]


def test_the_matrix_covers_the_comparison_of_the_design() -> None:
    names = [e.name for e in build_matrix(["small", "large"])]
    assert names == [
        "bm25@small",
        "vector@small",
        "hybrid@small",
        "hybrid+rerank@small",
        "bm25@large",
        "vector@large",
        "hybrid@large",
        "hybrid+rerank@large",
    ]
    assert next(e.name for e in build_matrix()) == "bm25@default"


def _factory(table: dict[str, dict[str, list[str]]]) -> Callable[[Experiment], SearchFn]:
    def make(experiment: Experiment) -> SearchFn:
        async def search(question: EvalQuestion, top_k: int) -> tuple[list[RankedHit], str]:
            docs = table[experiment.name].get(question.id, [])
            return [RankedHit(d, f"{d}:0", (1,)) for d in docs], experiment.mode

        return search

    return make


async def test_all_experiments_run_on_the_same_questions_and_keep_their_config() -> None:
    table = {
        "bm25@default": {"a": ["A"], "b": ["X", "B"]},
        "hybrid@default": {"a": ["A"], "b": ["B"]},
    }
    experiments = [Experiment("bm25"), Experiment("hybrid")]
    reports = await run_matrix(
        _questions(), experiments, _factory(table), config={"embedding_model": "m@1"}
    )
    assert [r.name for r in reports] == ["bm25@default", "hybrid@default"]
    assert reports[0].config["embedding_model"] == "m@1"
    assert reports[0].config["experiment"] == "bm25@default"
    assert reports[0].metrics["mrr"] == 0.75
    assert reports[1].metrics["mrr"] == 1.0


async def test_the_comparison_shows_gain_over_the_baseline() -> None:
    table = {
        "bm25@default": {"a": ["A"], "b": ["X", "B"]},
        "hybrid@default": {"a": ["A"], "b": ["B"]},
    }
    reports = await run_matrix(
        _questions(), [Experiment("bm25"), Experiment("hybrid")], _factory(table)
    )
    text = format_comparison(reports)
    lines = text.splitlines()
    assert lines[0].startswith("| run |")
    assert "bm25@default (baseline)" in lines[2]
    assert "(+0.250)" in lines[3]  # mrr gain of hybrid over bm25
    assert format_comparison([]) == ""


async def test_the_best_run_is_chosen_by_a_metric_and_ties_go_to_the_simpler_one() -> None:
    table = {
        "bm25@default": {"a": ["A"], "b": ["B"]},
        "hybrid@default": {"a": ["A"], "b": ["B"]},
    }
    reports = await run_matrix(
        _questions(), [Experiment("bm25"), Experiment("hybrid")], _factory(table)
    )
    assert best_by(reports).name == "bm25@default"
