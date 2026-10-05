import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any, cast

import httpx
import pytest
import respx
from elasticsearch import AsyncElasticsearch

from app.core.breaker import CircuitBreaker
from app.core.errors import (
    NonRetryableError,
    UpstreamOverloadedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.security import Identity
from app.core.settings import RerankerSettings, Settings
from app.rerank.base import Reranker
from app.rerank.factory import create_reranker
from app.rerank.guarded import GuardedReranker
from app.rerank.inhouse import InHouseReranker
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import SearchService
from tests.fakes import FakeEmbedder, FakeReranker
from tests.fakes.search_es import FakeSearchEs, chunk_doc

URL = "http://rerank.test/rerank"
IDENTITY = Identity("alice", ("g1",), "svc")


def _cfg(**changes: Any) -> RerankerSettings:
    values: dict[str, Any] = {
        "model": "bge-reranker",
        "endpoint": "http://rerank.test",
        "timeout_s": 0.5,
    }
    return RerankerSettings(**{**values, **changes})


@pytest.fixture
async def http() -> AsyncIterator[JsonHttpClient]:
    async with httpx.AsyncClient() as client:
        yield JsonHttpClient(
            client, RetryPolicy(attempts=2, initial_delay_s=0, max_delay_s=0, jitter_s=0)
        )


# --- the client -----------------------------------------------------------------------------


async def test_request_and_answer(http: JsonHttpClient) -> None:
    reranker = InHouseReranker(_cfg(truncate=True), http)
    assert isinstance(reranker, Reranker)
    with respx.mock() as router:
        route = router.post(URL).respond(
            json=[
                {"index": 1, "score": 0.9},
                {"index": 0, "score": 0.2},
                {"index": 2, "score": 0.5},
            ]
        )
        result = await reranker.rerank("late delivery", ["a", "b", "c"], top_n=2)
    assert json.loads(route.calls[0].request.content) == {
        "query": "late delivery",
        "texts": ["a", "b", "c"],
        "truncate": True,
    }
    assert result == [(1, 0.9), (2, 0.5)]
    assert route.calls[0].request.extensions["timeout"]["read"] == 0.5


async def test_no_passages_means_no_call(http: JsonHttpClient) -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.post(URL).respond(json=[])
        assert await InHouseReranker(_cfg(), http).rerank("q", [], 5) == []
    assert route.call_count == 0


@pytest.mark.parametrize(
    "answer",
    [
        {"error": "x"},
        [{"index": 5, "score": 1}],
        [{"index": -1, "score": 1}],
        [{"index": 0}],
        [{"index": "a", "score": 1}],
        [{"index": 0, "score": "nan"}],
    ],
)
async def test_odd_answers_are_rejected(http: JsonHttpClient, answer: object) -> None:
    with respx.mock() as router:
        router.post(URL).respond(json=answer)
        with pytest.raises(NonRetryableError):
            await InHouseReranker(_cfg(), http).rerank("q", ["a", "b"], 2)


async def test_failures_are_typed_and_the_body_is_not_copied(http: JsonHttpClient) -> None:
    with respx.mock() as router:
        router.post(URL).respond(500, json={"echo": "synthetic secret passage"})
        with pytest.raises(UpstreamUnavailableError) as error:
            await InHouseReranker(_cfg(), http).rerank("q", ["a"], 1)
    assert "synthetic secret passage" not in str(error.value)
    with respx.mock() as router:
        router.post(URL).respond(429)
        with pytest.raises(UpstreamOverloadedError):
            await InHouseReranker(_cfg(), http).rerank("q", ["a"], 1)
    with respx.mock() as router:
        router.post(URL).mock(side_effect=httpx.ReadTimeout("slow"))
        with pytest.raises(UpstreamTimeoutError):
            await InHouseReranker(_cfg(), http).rerank("q", ["a"], 1)


# --- guarded --------------------------------------------------------------------------------


async def test_a_slow_reranker_is_cut_off_and_opens_the_breaker() -> None:
    fake = FakeReranker()
    original = fake.rerank

    async def slow(query: str, passages: list[str], top_n: int) -> list[tuple[int, float]]:
        await asyncio.sleep(1)
        return await original(query, passages, top_n)

    fake.rerank = slow  # type: ignore[method-assign]
    guarded = GuardedReranker(fake, CircuitBreaker(2, 10), timeout_s=0.02)
    for _ in range(2):
        with pytest.raises(TimeoutError):
            await guarded.rerank("q", ["a"], 1)
    from app.core.breaker import BreakerOpenError

    with pytest.raises(BreakerOpenError):
        await guarded.rerank("q", ["a"], 1)


async def test_the_factory_builds_a_guarded_inhouse_reranker() -> None:
    async with httpx.AsyncClient() as client:
        reranker = create_reranker(_cfg(), client, RetryPolicy(attempts=1))
    assert isinstance(reranker, GuardedReranker)
    assert reranker.model_name == "bge-reranker"


# --- in the search service ------------------------------------------------------------------


def _docs() -> list[dict[str, Any]]:
    return [
        chunk_doc("D1:0", "D1", "late payment interest", users=["alice"]),
        chunk_doc("D2:0", "D2", "penalty for late delivery of goods", users=["alice"]),
        chunk_doc("D3:0", "D3", "delivery schedule late", users=["alice"]),
        chunk_doc("SECRET:0", "SECRET", "late delivery penalty secret plan", users=["bob"]),
    ]


def _service(
    settings: Settings, reranker: Reranker | None, **reranker_cfg: Any
) -> tuple[SearchService, FakeSearchEs]:
    s = settings.model_copy(update={"reranker": settings.reranker.model_copy(update=reranker_cfg)})
    es = FakeSearchEs(_docs())
    cfg = s.search
    searcher = HybridSearcher(
        cast(AsyncElasticsearch, es),
        cfg.index_alias,
        QueryBuilder(cfg),
        cfg,
        s.elasticsearch,
        RetryPolicy(attempts=1),
    )
    return SearchService(
        embedder=FakeEmbedder(dims=4), searcher=searcher, settings=s, reranker=reranker
    ), es


async def test_the_reranker_reorders_and_the_mode_says_so(settings: Settings) -> None:
    fake = FakeReranker()
    service, _ = _service(settings, fake)
    result = await service.search(
        query="penalty for late delivery", identity=IDENTITY, mode="bm25", rerank=True
    )
    assert result.mode_used == "bm25+rerank"
    assert result.hits[0].doc_id == "D2"
    assert result.hits[0].score == 1.0  # the reranker's score replaces the retrieval score


async def test_it_asks_for_more_candidates_than_the_final_size(settings: Settings) -> None:
    service, es = _service(settings, FakeReranker())
    await service.search(query="late", identity=IDENTITY, mode="bm25", top_k=2, rerank=True)
    assert es.requests[0]["size"] == settings.search.rerank_top_n
    es.requests.clear()
    await service.search(query="late", identity=IDENTITY, mode="bm25", top_k=2, rerank=False)
    assert es.requests[0]["size"] == 2


async def test_the_final_size_is_applied_after_reranking(settings: Settings) -> None:
    service, _ = _service(settings, FakeReranker())
    result = await service.search(
        query="late delivery", identity=IDENTITY, mode="bm25", top_k=2, rerank=True
    )
    assert len(result.hits) == 2


async def test_reranking_can_be_switched_off_per_request_and_by_configuration(
    settings: Settings,
) -> None:
    fake = FakeReranker()
    service, _ = _service(settings, fake)
    off = await service.search(query="late", identity=IDENTITY, mode="bm25", rerank=False)
    assert off.mode_used == "bm25"
    assert fake.calls == []
    disabled, _ = _service(settings, fake, enabled=False)
    default = await disabled.search(query="late", identity=IDENTITY, mode="bm25")
    assert default.mode_used == "bm25"
    forced = await disabled.search(query="late", identity=IDENTITY, mode="bm25", rerank=True)
    assert forced.mode_used == "bm25+rerank"


async def test_the_setting_decides_when_the_request_does_not(settings: Settings) -> None:
    service, _ = _service(settings, FakeReranker(), enabled=True)
    assert (
        await service.search(query="late", identity=IDENTITY, mode="bm25")
    ).mode_used == "bm25+rerank"


@pytest.mark.parametrize(
    "error", [UpstreamUnavailableError(), UpstreamTimeoutError(), TimeoutError()]
)
async def test_a_failing_or_slow_reranker_gives_the_rrf_order_and_says_so(
    settings: Settings, error: Exception
) -> None:
    fake = FakeReranker()
    fake.fail_with = error
    service, _ = _service(settings, fake)
    plain = await service.search(
        query="late delivery", identity=IDENTITY, mode="bm25", rerank=False
    )
    result = await service.search(
        query="late delivery", identity=IDENTITY, mode="bm25", rerank=True
    )
    assert result.mode_used == "bm25"  # no "+rerank"
    assert [h.chunk_id for h in result.hits] == [h.chunk_id for h in plain.hits]


async def test_a_reranker_that_answers_with_a_bad_index_is_ignored(settings: Settings) -> None:
    class Broken:
        model_name = "broken"

        async def rerank(
            self, query: str, passages: list[str], top_n: int
        ) -> list[tuple[int, float]]:
            return [(99, 1.0)]

    service, _ = _service(settings, Broken())
    result = await service.search(query="late", identity=IDENTITY, mode="bm25", rerank=True)
    assert result.mode_used == "bm25"


async def test_no_hits_means_no_rerank_call(settings: Settings) -> None:
    fake = FakeReranker()
    service, _ = _service(settings, fake)
    result = await service.search(query="zzzzzz", identity=IDENTITY, mode="bm25", rerank=True)
    assert result.hits == []
    assert fake.calls == []


async def test_without_a_reranker_the_request_flag_does_nothing(settings: Settings) -> None:
    service, _ = _service(settings, None)
    assert (
        await service.search(query="late", identity=IDENTITY, mode="bm25", rerank=True)
    ).mode_used == "bm25"


async def test_the_reranker_only_ever_sees_passages_the_user_may_read(settings: Settings) -> None:
    fake = FakeReranker()
    service, _ = _service(settings, fake)
    await service.search(query="late delivery penalty", identity=IDENTITY, mode="bm25", rerank=True)
    seen = " ".join(" ".join(passages) for _, passages, _ in fake.calls)
    assert "secret plan" not in seen
    assert "penalty for late delivery" in seen


async def test_passages_are_never_logged(settings: Settings) -> None:
    import structlog

    fake = FakeReranker()
    fake.fail_with = UpstreamUnavailableError()
    service, _ = _service(settings, fake)
    with structlog.testing.capture_logs() as logs:
        await service.search(
            query="synthetic secret question", identity=IDENTITY, mode="bm25", rerank=True
        )
    assert "synthetic secret question" not in str(logs)
    assert "penalty for late delivery" not in str(logs)
