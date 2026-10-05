"""What the Java app sees when a dependency of the service fails (HLD sections 6 and 18).

The rule (CLAUDE.md rule 10): a failure either gives a clear error code that makes the Java app fall
back to keyword search (503 UPSTREAM_UNAVAILABLE, 504 TIMEOUT), or a successful answer whose
``mode_used`` says which simpler search ran. Never a silent partial result.
"""

import asyncio
from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core.breaker import CircuitBreaker
from app.core.errors import (
    NonRetryableError,
    UpstreamOverloadedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.retry import RetryPolicy
from app.core.settings import ApiSettings, Settings
from app.main import create_app
from app.rerank.guarded import GuardedReranker
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import SearchService
from app.services import Services
from tests.fakes import FakeEmbedder, FakeLLMClient, FakeReranker
from tests.fakes.search_es import FakeSearchEs, chunk_doc
from tests.unit.test_answer_service import make_service

HEADERS = {"Authorization": "Bearer tok", "X-User-Id": "alice"}
SEARCH = {"query": "late delivery", "top_k": 5}
ANSWER = {"question": "late delivery penalty"}


class Rig:
    def __init__(self, settings: Settings, *, reranker: Any = None, **search: Any) -> None:
        self.settings = settings.model_copy(
            update={
                "api": ApiSettings(
                    service_tokens={"svc": SecretStr("tok")}, identity_services=["svc"]
                ),
                "search": settings.search.model_copy(update=search),
            }
        )
        self.es = FakeSearchEs(
            [chunk_doc("D1:0", "D1", "late delivery penalty is one percent", users=["alice"])]
        )
        self.embedder = FakeEmbedder(dims=4)
        self.llm = FakeLLMClient(["One percent [1]."])
        self.reranker = reranker
        cfg = self.settings.search
        searcher = HybridSearcher(
            cast(AsyncElasticsearch, self.es),
            cfg.index_alias,
            QueryBuilder(cfg),
            cfg,
            self.settings.elasticsearch,
            RetryPolicy(attempts=1),
        )
        self.search = SearchService(
            embedder=self.embedder, searcher=searcher, settings=self.settings, reranker=reranker
        )
        answers, _ = make_service(self.settings, self.llm, reranker=reranker)
        # The answer service must use this rig's search service, so the same fakes fail.
        answers._search = self.search
        self.app = create_app(self.settings, Services(search=self.search, answer=answers))


@pytest.fixture
def make(settings: Settings) -> Iterator[Callable[..., tuple[Rig, TestClient]]]:
    clients: list[TestClient] = []

    def build(**kwargs: Any) -> tuple[Rig, TestClient]:
        rig = Rig(settings, **kwargs)
        client = TestClient(rig.app, raise_server_exceptions=False)
        client.__enter__()
        clients.append(client)
        return rig, client

    yield build
    for client in clients:
        client.__exit__(None, None, None)


def _error(response: Any) -> tuple[int, str]:
    assert response.json()["error"]["request_id"]
    return response.status_code, response.json()["error"]["code"]


# --- search: simpler mode, said out loud ----------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        UpstreamUnavailableError(),
        UpstreamTimeoutError(),
        UpstreamOverloadedError(),
        NonRetryableError(),
    ],
)
def test_a_dead_embedding_server_means_keyword_search_and_says_so(
    make: Any, error: Exception
) -> None:
    rig, client = make()
    rig.embedder.fail_with = error
    body = client.post("/v1/search", json=SEARCH, headers=HEADERS).json()
    assert body["mode_used"] == "bm25"
    assert [r["doc_id"] for r in body["results"]] == ["D1"]


def test_a_failing_vector_leg_means_keyword_search(make: Any) -> None:
    rig, client = make()
    rig.es.fail["knn"] = UpstreamUnavailableError()
    assert client.post("/v1/search", json=SEARCH, headers=HEADERS).json()["mode_used"] == "bm25"


def test_a_failing_keyword_leg_means_vector_search(make: Any) -> None:
    rig, client = make()
    rig.es.fail["bm25"] = UpstreamUnavailableError()
    assert client.post("/v1/search", json=SEARCH, headers=HEADERS).json()["mode_used"] == "knn"


def test_a_dead_reranker_gives_the_rrf_order_without_rerank_in_the_mode(make: Any) -> None:
    reranker = FakeReranker()
    reranker.fail_with = UpstreamUnavailableError()
    _, client = make(reranker=reranker)
    body = client.post("/v1/search", json={**SEARCH, "rerank": True}, headers=HEADERS).json()
    assert body["mode_used"] == "hybrid" and body["results"]


def test_an_open_breaker_skips_the_reranker_at_once(make: Any) -> None:
    inner = FakeReranker()
    inner.fail_with = UpstreamUnavailableError()
    breaker = CircuitBreaker(2, 60)
    guarded = GuardedReranker(inner, breaker, timeout_s=1)
    _, client = make(reranker=guarded)
    for _ in range(3):
        body = client.post("/v1/search", json={**SEARCH, "rerank": True}, headers=HEADERS).json()
        assert body["mode_used"] == "hybrid"
    assert breaker.is_open
    inner.fail_with = None
    inner.calls.clear()
    client.post("/v1/search", json={**SEARCH, "rerank": True}, headers=HEADERS)
    assert inner.calls == []  # not even tried while the breaker is open


def test_a_slow_reranker_is_cut_off(make: Any) -> None:
    class Slow:
        model_name = "slow"

        async def rerank(
            self, query: str, passages: list[str], top_n: int
        ) -> list[tuple[int, float]]:
            await asyncio.sleep(5)
            return []

    guarded = GuardedReranker(Slow(), CircuitBreaker(5, 60), timeout_s=0.05)
    _, client = make(reranker=guarded)
    body = client.post("/v1/search", json={**SEARCH, "rerank": True}, headers=HEADERS).json()
    assert body["mode_used"] == "hybrid"


# --- search: an error code for the fallback -------------------------------------------------


def test_both_legs_down_is_a_503_for_the_java_fallback(make: Any) -> None:
    rig, client = make()
    rig.es.fail["bm25"] = UpstreamUnavailableError()
    rig.es.fail["knn"] = UpstreamUnavailableError()
    assert _error(client.post("/v1/search", json=SEARCH, headers=HEADERS)) == (
        503,
        "UPSTREAM_UNAVAILABLE",
    )


def test_keyword_only_with_elasticsearch_down_is_a_503(make: Any) -> None:
    rig, client = make()
    rig.es.fail["bm25"] = UpstreamUnavailableError()
    response = client.post("/v1/search", json={**SEARCH, "mode": "bm25"}, headers=HEADERS)
    assert _error(response) == (503, "UPSTREAM_UNAVAILABLE")


def test_vector_only_with_a_dead_embedding_server_is_a_503(make: Any) -> None:
    rig, client = make()
    rig.embedder.fail_with = UpstreamUnavailableError()
    response = client.post("/v1/search", json={**SEARCH, "mode": "vector"}, headers=HEADERS)
    assert response.status_code in (503, 504)
    assert response.json()["error"]["code"] in {"UPSTREAM_UNAVAILABLE", "TIMEOUT"}


def test_a_search_over_its_time_budget_is_a_504(make: Any) -> None:
    rig, client = make(timeout_ms=50)

    async def slow(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(1)

    rig.es.search = slow
    assert _error(client.post("/v1/search", json=SEARCH, headers=HEADERS)) == (504, "TIMEOUT")


def test_the_feature_flag_off_is_a_503(make: Any) -> None:
    rig, client = make()
    rig.search._settings = rig.settings.model_copy(
        update={
            "feature_flags": rig.settings.feature_flags.model_copy(
                update={"semantic_search": False}
            )
        }
    )
    assert _error(client.post("/v1/search", json=SEARCH, headers=HEADERS)) == (
        503,
        "UPSTREAM_UNAVAILABLE",
    )


def test_an_unexpected_bug_is_a_500_internal_error_not_a_stack_trace(make: Any) -> None:
    rig, client = make()

    async def broken(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("synthetic secret detail")

    rig.search._find = broken
    response = client.post("/v1/search", json=SEARCH, headers=HEADERS)
    assert _error(response) == (500, "INTERNAL_ERROR")
    assert "synthetic secret detail" not in response.text and "Traceback" not in response.text


def test_an_unexpected_error_from_one_leg_is_a_503_that_hides_the_detail(make: Any) -> None:
    rig, client = make()
    rig.es.fail["bm25"] = RuntimeError("synthetic secret detail")
    rig.es.fail["knn"] = RuntimeError("synthetic secret detail")
    response = client.post("/v1/search", json=SEARCH, headers=HEADERS)
    assert _error(response) == (503, "UPSTREAM_UNAVAILABLE")
    assert "synthetic secret detail" not in response.text


# --- answers --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error,status,code",
    [
        (UpstreamUnavailableError(), 503, "UPSTREAM_UNAVAILABLE"),
        (UpstreamOverloadedError(), 503, "UPSTREAM_UNAVAILABLE"),
        (UpstreamTimeoutError(), 504, "TIMEOUT"),
        (NonRetryableError("the model sent junk"), 503, "UPSTREAM_UNAVAILABLE"),
    ],
)
def test_model_failures_are_fallback_errors_for_answers(
    make: Any, error: Exception, status: int, code: str
) -> None:
    rig, client = make()
    rig.llm.fail_with = error
    response = client.post("/v1/answer", json=ANSWER, headers=HEADERS)
    assert _error(response) == (status, code)
    assert "junk" not in response.text


def test_search_failure_inside_an_answer_is_a_fallback_error(make: Any) -> None:
    rig, client = make()
    rig.es.fail["bm25"] = UpstreamUnavailableError()
    rig.es.fail["knn"] = UpstreamUnavailableError()
    assert _error(client.post("/v1/answer", json=ANSWER, headers=HEADERS)) == (
        503,
        "UPSTREAM_UNAVAILABLE",
    )


def test_a_dead_model_before_the_stream_starts_is_a_normal_error(make: Any) -> None:
    rig, client = make()
    rig.es.fail["bm25"] = UpstreamUnavailableError()
    rig.es.fail["knn"] = UpstreamUnavailableError()
    response = client.post("/v1/answer/stream", json=ANSWER, headers=HEADERS)
    assert _error(response) == (503, "UPSTREAM_UNAVAILABLE")


def test_a_model_that_breaks_during_the_stream_ends_with_an_error_event(make: Any) -> None:
    rig, client = make()
    rig.llm.replies = ["one two three four five"]
    rig.llm.fail_stream_after = 2
    text = client.post("/v1/answer/stream", json=ANSWER, headers=HEADERS).text
    assert "event: error" in text and "UPSTREAM_UNAVAILABLE" in text and "event: done" not in text


def test_an_unusable_stream_start_is_a_normal_error_event(make: Any) -> None:
    rig, client = make()
    rig.llm.fail_with = NonRetryableError()
    text = client.post("/v1/answer/stream", json=ANSWER, headers=HEADERS).text
    assert "event: error" in text and "UPSTREAM_UNAVAILABLE" in text


# --- the edges ------------------------------------------------------------------------------


def test_every_failure_response_has_a_request_id_and_the_error_shape(make: Any) -> None:
    _, client = make()
    for path, body, headers in [
        ("/v1/search", {"query": ""}, HEADERS),
        ("/v1/search", SEARCH, {}),
        ("/v1/search", SEARCH, {"Authorization": "Bearer wrong", "X-User-Id": "a"}),
        ("/v1/answer", {"nope": 1}, HEADERS),
    ]:
        response = client.post(path, json=body, headers=headers)
        assert response.status_code in (400, 401, 403)
        error = response.json()["error"]
        assert set(error) == {"code", "message", "request_id"} and error["request_id"]
        assert response.headers["x-request-id"] == error["request_id"]
