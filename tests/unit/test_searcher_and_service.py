import asyncio
from typing import Any, cast

import pytest
from elastic_transport import ConnectionTimeout
from elasticsearch import AsyncElasticsearch

from app.core.errors import (
    ForbiddenError,
    InvalidRequestError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.retry import RetryPolicy
from app.core.security import Identity
from app.core.settings import ElasticsearchSettings, SearchSettings, Settings
from app.retrieval.acl import AclFilter
from app.retrieval.filters import SearchFilters
from app.retrieval.models import SearchHit, parse_hit, rrf_merge
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import SearchService, best_chunk_per_document
from tests.fakes import FakeEmbedder
from tests.fakes.es import api_error
from tests.fakes.search_es import FakeSearchEs, chunk_doc

ACL = AclFilter.from_identity(Identity(user_id="u1", groups=("g1",), service="s"))
IDENTITY = Identity(user_id="u1", groups=("g1",), service="s")
V_ALPHA = [1.0, 0.0, 0.0, 0.0]
V_BETA = [0.0, 1.0, 0.0, 0.0]


def _docs() -> list[dict[str, Any]]:
    return [
        chunk_doc(
            "D1:0",
            "D1",
            "penalty for late delivery of goods",
            users=["u1"],
            embedding=V_BETA,
            pages=(4, 5),
        ),
        chunk_doc("D2:0", "D2", "late payment interest rules", groups=["g1"], embedding=V_ALPHA),
        chunk_doc(
            "D3:0", "D3", "delivery schedule appendix", users=["u1"], embedding=[0.7, 0.7, 0, 0]
        ),
        chunk_doc("D4:0", "D4", "late delivery penalty secret", users=["other"], embedding=V_ALPHA),
    ]


def _searcher(es: FakeSearchEs, **search: Any) -> HybridSearcher:
    cfg = SearchSettings(index_alias="alias", source_index="src", **search)
    return HybridSearcher(
        cast(AsyncElasticsearch, es),
        "alias",
        QueryBuilder(cfg),
        cfg,
        ElasticsearchSettings(hosts=["http://es.test:9200"], state_index="s", search_timeout_s=1.5),
        RetryPolicy(attempts=1),
    )


def _ids(hits: list[SearchHit]) -> list[str]:
    return [h.chunk_id for h in hits]


# --- hits and merging -----------------------------------------------------------------------


def test_a_hit_with_a_highlight_gets_a_plain_snippet_and_the_matched_terms() -> None:
    hit = parse_hit(
        {
            "_score": 2.5,
            "_source": {
                "chunk_id": "c",
                "doc_id": "d",
                "content": "full text",
                "page_start": 4,
                "page_end": 6,
            },
            "highlight": {
                "content": ["If <em>delivery</em> is &lt;late&gt; the <em>Penalty</em> applies"]
            },
        },
        300,
    )
    assert hit.snippet == "If delivery is <late> the Penalty applies"
    assert hit.highlights == ["delivery", "penalty"]
    assert hit.pages == [4, 5, 6]
    assert hit.score == 2.5


def test_a_hit_without_a_highlight_gets_the_start_of_the_content() -> None:
    hit = parse_hit({"_source": {"chunk_id": "c", "doc_id": "d", "content": "x" * 500}}, 100)
    assert hit.snippet == "x" * 100 + "..."
    assert hit.highlights == []
    assert hit.pages == []  # document-level text has no pages


def test_rrf_scores_follow_the_formula_and_keep_the_highlight() -> None:
    a = SearchHit(chunk_id="a", doc_id="d", score=9, content="a", snippet="a")
    b = SearchHit(chunk_id="b", doc_id="d", score=9, content="b", snippet="b", highlights=["x"])
    b_plain = b.model_copy(update={"highlights": []})
    merged = rrf_merge([[a, b], [b_plain, a]], rank_constant=60, size=10)
    assert [h.chunk_id for h in merged] == ["a", "b"]  # equal scores: ordered by ID
    assert merged[0].score == pytest.approx(1 / 61 + 1 / 62)
    assert merged[1].highlights == ["x"]


def test_a_chunk_found_by_both_beats_chunks_found_by_one() -> None:
    both = SearchHit(chunk_id="both", doc_id="d", score=0, content="c", snippet="c")
    only_a = SearchHit(chunk_id="only-a", doc_id="d", score=0, content="c", snippet="c")
    only_b = SearchHit(chunk_id="only-b", doc_id="d", score=0, content="c", snippet="c")
    merged = rrf_merge([[only_a, both], [only_b, both]], 60, 10)
    assert merged[0].chunk_id == "both"


def test_the_size_limits_the_merged_list() -> None:
    hits = [
        SearchHit(chunk_id=f"c{i}", doc_id="d", score=0, content="c", snippet="c")
        for i in range(10)
    ]
    assert len(rrf_merge([hits], 60, 3)) == 3


def test_best_chunk_per_document() -> None:
    hits = [
        SearchHit(chunk_id=c, doc_id=d, score=0, content="x", snippet="x")
        for c, d in [("1", "A"), ("2", "A"), ("3", "B")]
    ]
    assert [h.chunk_id for h in best_chunk_per_document(hits)] == ["1", "3"]


# --- the searcher ---------------------------------------------------------------------------


async def test_bm25_search_returns_only_chunks_the_user_may_see() -> None:
    es = FakeSearchEs(_docs())
    outcome = await _searcher(es).search("late delivery penalty", None, ACL, SearchFilters(), 10)
    assert outcome.mode == "bm25"
    assert _ids(outcome.hits) == ["D1:0", "D2:0", "D3:0"]
    assert "D4:0" not in _ids(outcome.hits)  # the secret of another user
    assert outcome.hits[0].highlights == ["penalty", "late", "delivery"]  # in text order
    assert outcome.hits[0].pages == [4, 5]


async def test_hybrid_search_merges_both_legs_with_rrf() -> None:
    es = FakeSearchEs(_docs())
    outcome = await _searcher(es).search("late delivery penalty", V_BETA, ACL, SearchFilters(), 10)
    assert outcome.mode == "hybrid"
    assert _ids(outcome.hits)[0] == "D1:0"  # first in both lists
    assert set(_ids(outcome.hits)) == {"D1:0", "D2:0", "D3:0"}
    assert len(es.requests) == 2
    assert {bool(r["knn"]) for r in es.requests} == {True, False}


async def test_every_request_that_leaves_the_searcher_carries_the_access_filter() -> None:
    es = FakeSearchEs(_docs())
    searcher = _searcher(es)
    await searcher.search("late", V_ALPHA, ACL, SearchFilters(doc_type=["contract"]), 5)
    await searcher.search("late", None, ACL, SearchFilters(), 5)
    await searcher.knn_only("late", V_ALPHA, ACL, SearchFilters(), 5)
    for request in es.requests:
        QueryBuilder.verify(
            {k: v for k, v in request.items() if v is not None and k != "index"}, ACL
        )


async def test_filters_narrow_the_result_inside_the_access_filter() -> None:
    docs = [
        *_docs(),
        chunk_doc("D5:0", "D5", "late delivery invoice", users=["u1"], doc_type="invoice"),
    ]
    outcome = await _searcher(FakeSearchEs(docs)).search(
        "late delivery", None, ACL, SearchFilters(doc_type=["invoice"]), 10
    )
    assert _ids(outcome.hits) == ["D5:0"]


async def test_the_search_timeout_is_set_on_the_client() -> None:
    es = FakeSearchEs(_docs())
    await _searcher(es).search("late", None, ACL, SearchFilters(), 5)
    assert es.timeouts == [1.5]


async def test_if_the_vector_leg_fails_keyword_search_answers_and_says_so() -> None:
    es = FakeSearchEs(_docs())
    es.fail["knn"] = ConnectionTimeout("slow")
    outcome = await _searcher(es).search("late delivery", V_ALPHA, ACL, SearchFilters(), 10)
    assert outcome.mode == "bm25"
    assert _ids(outcome.hits) == ["D1:0", "D2:0", "D3:0"]


async def test_if_the_keyword_leg_fails_the_vector_leg_answers_and_says_so() -> None:
    es = FakeSearchEs(_docs())
    es.fail["bm25"] = api_error(503)
    outcome = await _searcher(es).search("late delivery", V_ALPHA, ACL, SearchFilters(), 10)
    assert outcome.mode == "knn"
    assert "D4:0" not in _ids(outcome.hits)


async def test_if_both_legs_fail_the_error_is_raised_for_the_fallback() -> None:
    es = FakeSearchEs(_docs())
    es.fail["bm25"] = ConnectionTimeout("slow")
    es.fail["knn"] = ConnectionTimeout("slow")
    with pytest.raises(UpstreamTimeoutError):
        await _searcher(es).search("late", V_ALPHA, ACL, SearchFilters(), 10)


async def test_the_retriever_mode_sends_one_request() -> None:
    es = FakeSearchEs(_docs())
    outcome = await _searcher(es, rrf_mode="retriever").search(
        "late delivery", V_BETA, ACL, SearchFilters(), 10
    )
    assert outcome.mode == "hybrid"
    assert len(es.requests) == 1
    assert es.requests[0]["retriever"] is not None
    assert _ids(outcome.hits)[0] == "D1:0"


async def test_a_cluster_without_the_retriever_falls_back_to_the_python_merge_and_remembers() -> (
    None
):
    es = FakeSearchEs(_docs())
    es.fail["retriever"] = api_error(400)  # for example "license" or "unknown retriever"
    searcher = _searcher(es, rrf_mode="retriever")
    first = await searcher.search("late delivery", V_BETA, ACL, SearchFilters(), 10)
    assert first.mode == "hybrid"
    assert [bool(r["retriever"]) for r in es.requests] == [True, False, False]
    es.requests.clear()
    await searcher.search("late delivery", V_BETA, ACL, SearchFilters(), 10)
    assert not any(r["retriever"] for r in es.requests)  # not tried again


async def test_a_busy_retriever_falls_back_to_the_two_legs() -> None:
    es = FakeSearchEs(_docs())
    es.fail["retriever"] = ConnectionTimeout("slow")
    outcome = await _searcher(es, rrf_mode="retriever").search(
        "late", V_ALPHA, ACL, SearchFilters(), 10
    )
    assert outcome.mode == "hybrid"


async def test_no_hits_is_an_empty_result_not_an_error() -> None:
    outcome = await _searcher(FakeSearchEs(_docs())).search(
        "nonexistentword", V_ALPHA, ACL, SearchFilters(), 10
    )
    assert outcome.hits[:0] == []


# --- the service ----------------------------------------------------------------------------


def _service(
    settings: Settings, es: FakeSearchEs, embedder: FakeEmbedder | None = None, **search: Any
) -> SearchService:
    settings = settings.model_copy(update={"search": settings.search.model_copy(update=search)})
    cfg = settings.search
    searcher = HybridSearcher(
        cast(AsyncElasticsearch, es),
        cfg.index_alias,
        QueryBuilder(cfg),
        cfg,
        settings.elasticsearch,
        RetryPolicy(attempts=1),
    )
    return SearchService(
        embedder=embedder or FakeEmbedder(dims=4), searcher=searcher, settings=settings
    )


async def test_hybrid_is_the_default_and_the_result_says_so(settings: Settings) -> None:
    result = await _service(settings, FakeSearchEs(_docs())).search(
        query="late delivery", identity=IDENTITY
    )
    assert result.mode_used == "hybrid"
    assert result.took_ms >= 0
    assert result.hits


async def test_bm25_mode_does_not_embed_the_query(settings: Settings) -> None:
    embedder = FakeEmbedder(dims=4)
    result = await _service(settings, FakeSearchEs(_docs()), embedder).search(
        query="late", identity=IDENTITY, mode="bm25"
    )
    assert result.mode_used == "bm25"
    assert embedder.query_calls == []


async def test_vector_mode_is_knn_only(settings: Settings) -> None:
    result = await _service(settings, FakeSearchEs(_docs())).search(
        query="late", identity=IDENTITY, mode="vector"
    )
    assert result.mode_used == "knn"


async def test_a_failed_query_embedding_falls_back_to_keyword_search_visibly(
    settings: Settings,
) -> None:
    embedder = FakeEmbedder(dims=4)
    embedder.fail_with = UpstreamUnavailableError()
    result = await _service(settings, FakeSearchEs(_docs()), embedder).search(
        query="late delivery", identity=IDENTITY
    )
    assert result.mode_used == "bm25"
    assert result.hits


async def test_a_slow_query_embedding_falls_back_too(settings: Settings) -> None:
    embedder = FakeEmbedder(dims=4)
    embedder.block = asyncio.Event()  # never answers
    result = await _service(settings, FakeSearchEs(_docs()), embedder).search(
        query="late delivery", identity=IDENTITY
    )
    assert result.mode_used == "bm25"


async def test_vector_mode_cannot_fall_back(settings: Settings) -> None:
    embedder = FakeEmbedder(dims=4)
    embedder.fail_with = UpstreamUnavailableError()
    with pytest.raises(UpstreamUnavailableError):
        await _service(settings, FakeSearchEs(_docs()), embedder).search(
            query="late", identity=IDENTITY, mode="vector"
        )


async def test_the_time_budget_ends_in_a_timeout_error(settings: Settings) -> None:
    es = FakeSearchEs(_docs())
    es.delay_s = 1.0
    service = _service(settings, es, timeout_ms=50)
    with pytest.raises(UpstreamTimeoutError):
        await service.search(query="late", identity=IDENTITY)


async def test_top_k_is_limited_by_the_setting(settings: Settings) -> None:
    docs = [chunk_doc(f"C{i}:0", f"C{i}", "late delivery", users=["u1"]) for i in range(10)]
    result = await _service(settings, FakeSearchEs(docs), max_top_k=3).search(
        query="late", identity=IDENTITY, top_k=50, mode="bm25"
    )
    assert len(result.hits) == 3


async def test_the_default_top_k_comes_from_settings(settings: Settings) -> None:
    docs = [chunk_doc(f"C{i}:0", f"C{i}", "late delivery", users=["u1"]) for i in range(10)]
    result = await _service(settings, FakeSearchEs(docs), top_k_default=4).search(
        query="late", identity=IDENTITY, mode="bm25"
    )
    assert len(result.hits) == 4


async def test_group_by_document_keeps_the_best_chunk_of_each(settings: Settings) -> None:
    docs = [
        chunk_doc("A:0", "A", "late delivery late", users=["u1"]),
        chunk_doc("A:1", "A", "late delivery", users=["u1"]),
        chunk_doc("B:0", "B", "late", users=["u1"]),
    ]
    result = await _service(settings, FakeSearchEs(docs)).search(
        query="late delivery", identity=IDENTITY, mode="bm25", group_by_document=True
    )
    assert [h.chunk_id for h in result.hits] == ["A:0", "B:0"]


@pytest.mark.parametrize("query", ["", "   "])
async def test_an_empty_query_is_invalid(settings: Settings, query: str) -> None:
    with pytest.raises(InvalidRequestError):
        await _service(settings, FakeSearchEs(_docs())).search(query=query, identity=IDENTITY)


async def test_no_user_means_no_search(settings: Settings) -> None:
    es = FakeSearchEs(_docs())
    with pytest.raises(ForbiddenError):
        await _service(settings, es).search(
            query="late", identity=Identity(user_id=" ", groups=(), service="s")
        )
    assert es.requests == []  # nothing was sent


async def test_the_feature_flag_switches_search_off_with_a_fallback_error(
    settings: Settings,
) -> None:
    off = settings.model_copy(
        update={
            "feature_flags": settings.feature_flags.model_copy(update={"semantic_search": False})
        }
    )
    with pytest.raises(UpstreamUnavailableError):
        await _service(off, FakeSearchEs(_docs())).search(query="late", identity=IDENTITY)


# --- local snippets (hits that Elasticsearch did not highlight) ------------------------------


def test_a_hit_without_a_highlight_gets_a_snippet_around_the_first_query_word() -> None:
    content = "intro " * 50 + "the penalty for late delivery applies here " + "tail " * 50
    hit = parse_hit(
        {"_source": {"chunk_id": "c", "doc_id": "d", "content": content}},
        80,
        "late delivery penalty",
    )
    assert "penalty" in hit.snippet
    assert hit.snippet.startswith("...") and hit.snippet.endswith("...")
    assert hit.highlights == ["penalty", "late", "delivery"]  # in text order
    assert len(hit.snippet) <= 80 + 6


def test_short_words_and_missing_words_are_not_highlights() -> None:
    hit = parse_hit(
        {"_source": {"chunk_id": "c", "doc_id": "d", "content": "an of to nothing relevant"}},
        100,
        "an of zebra",
    )
    assert hit.highlights == []
    assert hit.snippet == "an of to nothing relevant"


def test_the_rrf_retriever_request_has_no_highlight() -> None:
    from app.retrieval.query import QueryBuilder as Builder

    request = Builder(SearchSettings(index_alias="a", source_index="s")).rrf(
        "q", V_ALPHA, ACL, SearchFilters(), 5
    )
    assert (
        "highlight" not in request
    )  # Elasticsearch 8.15: [rank] cannot be used with [highlighter]


async def test_vector_only_results_still_get_highlight_terms() -> None:
    es = FakeSearchEs(_docs())
    outcome = await _searcher(es).knn_only("late delivery", V_ALPHA, ACL, SearchFilters(), 10)
    assert outcome.mode == "knn"
    assert any(h.highlights for h in outcome.hits)
