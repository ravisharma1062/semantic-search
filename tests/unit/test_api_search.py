"""POST /v1/search through the real app, with fake backends."""

import json
from collections.abc import Iterator
from typing import Any, cast

import pytest
import structlog
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core.errors import UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ApiSettings, Settings
from app.main import create_app
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import SearchService
from app.services import Services
from tests.fakes import FakeEmbedder
from tests.fakes.search_es import FakeSearchEs, chunk_doc

AUTH = {"Authorization": "Bearer token-java"}
USER = {"X-User-Id": "alice", "X-User-Groups": "g1"}
HEADERS = {**AUTH, **USER}


def _docs() -> list[dict[str, Any]]:
    return [
        chunk_doc(
            "D1:0", "D1", "penalty for late delivery of goods", users=["alice"], pages=(4, 5)
        ),
        chunk_doc("D2:0", "D2", "late payment interest rules", groups=["g1"]),
        chunk_doc("D3:0", "D3", "the secret merger plan", users=["bob"]),
    ]


def _settings(settings: Settings, **api: Any) -> Settings:
    values: dict[str, Any] = {
        "service_tokens": {
            "java-search": SecretStr("token-java"),
            "other": SecretStr("token-other"),
        },
        "identity_services": ["java-search"],
    }
    return settings.model_copy(update={"api": ApiSettings(**{**values, **api})})


def _services(
    settings: Settings, es: FakeSearchEs, embedder: FakeEmbedder | None = None
) -> Services:
    cfg = settings.search
    searcher = HybridSearcher(
        cast(AsyncElasticsearch, es),
        cfg.index_alias,
        QueryBuilder(cfg),
        cfg,
        settings.elasticsearch,
        RetryPolicy(attempts=1),
    )
    return Services(
        search=SearchService(
            embedder=embedder or FakeEmbedder(dims=4), searcher=searcher, settings=settings
        )
    )


class Rig:
    def __init__(self, settings: Settings, **api: Any) -> None:
        self.settings = _settings(settings, **api)
        self.es = FakeSearchEs(_docs())
        self.embedder = FakeEmbedder(dims=4)
        self.app = create_app(self.settings, _services(self.settings, self.es, self.embedder))


@pytest.fixture
def rig(settings: Settings) -> Iterator[tuple[Rig, TestClient]]:
    r = Rig(settings)
    with TestClient(r.app, raise_server_exceptions=False) as client:
        yield r, client


def _post(client: TestClient, body: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
    return client.post("/v1/search", json=body, headers=HEADERS if headers is None else headers)


# --- the answer -----------------------------------------------------------------------------


def test_search_returns_the_documented_shape(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    response = _post(
        client,
        {"query": "late delivery", "top_k": 5, "mode": "bm25"},
        {**HEADERS, "X-Request-Id": "req-1"},
    )
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"request_id", "mode_used", "results", "took_ms"}
    assert body["request_id"] == "req-1"
    assert body["mode_used"] == "bm25"
    first = body["results"][0]
    assert set(first) == {"doc_id", "chunk_id", "score", "pages", "snippet", "highlights"}
    assert (first["doc_id"], first["chunk_id"], first["pages"]) == ("D1", "D1:0", [4, 5])
    assert first["highlights"]
    assert "content" not in first  # no chunk text beyond the snippet


def test_the_default_is_hybrid(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    assert _post(client, {"query": "late delivery"}).json()["mode_used"] == "hybrid"


def test_the_user_only_sees_what_the_headers_allow(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    mine = _post(client, {"query": "late merger secret", "mode": "bm25"}).json()
    assert {r["doc_id"] for r in mine["results"]} == {"D1", "D2"}
    bob = _post(
        client, {"query": "late merger secret", "mode": "bm25"}, {**AUTH, "X-User-Id": "bob"}
    ).json()
    assert {r["doc_id"] for r in bob["results"]} == {"D3"}


def test_the_request_to_elasticsearch_carries_the_users_rights(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    _post(
        client,
        {"query": "late", "mode": "hybrid"},
        {**AUTH, "X-User-Id": "alice", "X-User-Groups": "g1,g2"},
    )
    for request in r.es.requests:
        text = json.dumps(request, default=str)
        assert '"acl_users": "alice"' in text
        assert '"acl_groups": ["g1", "g2"]' in text


def test_filters_are_accepted(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    body = {"query": "late", "mode": "bm25", "filters": {"doc_type": ["invoice"]}}
    assert _post(client, body).json()["results"] == []


def test_group_by_document(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    assert (
        _post(client, {"query": "late", "mode": "bm25", "group_by_document": True}).status_code
        == 200
    )


# --- fallbacks are visible ------------------------------------------------------------------


def test_a_failing_embedding_server_gives_keyword_search_and_says_so(
    rig: tuple[Rig, TestClient],
) -> None:
    r, client = rig
    r.embedder.fail_with = UpstreamUnavailableError()
    body = _post(client, {"query": "late delivery"}).json()
    assert body["mode_used"] == "bm25"
    assert body["results"]


def test_an_elasticsearch_outage_is_an_error_that_makes_java_fall_back(
    rig: tuple[Rig, TestClient],
) -> None:
    from elastic_transport import ConnectionError as TransportConnectionError

    r, client = rig
    r.es.fail["bm25"] = TransportConnectionError("down")
    r.es.fail["knn"] = TransportConnectionError("down")
    response = _post(client, {"query": "late"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "UPSTREAM_UNAVAILABLE"


def test_the_feature_flag_makes_java_fall_back(settings: Settings) -> None:
    off = _settings(settings).model_copy(
        update={
            "feature_flags": settings.feature_flags.model_copy(update={"semantic_search": False})
        }
    )
    app = create_app(off, _services(off, FakeSearchEs(_docs())))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = _post(client, {"query": "late"})
    assert (response.status_code, response.json()["error"]["code"]) == (503, "UPSTREAM_UNAVAILABLE")


def test_the_time_budget_ends_in_504(settings: Settings) -> None:
    s = _settings(settings)
    s = s.model_copy(update={"search": s.search.model_copy(update={"timeout_ms": 30})})
    es = FakeSearchEs(_docs())
    es.delay_s = 1.0
    with TestClient(create_app(s, _services(s, es)), raise_server_exceptions=False) as client:
        response = _post(client, {"query": "late", "mode": "bm25"})
    assert (response.status_code, response.json()["error"]["code"]) == (504, "TIMEOUT")


# --- authentication and identity ------------------------------------------------------------


def test_no_token_is_401(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    response = _post(client, {"query": "late"}, USER)
    assert (response.status_code, response.json()["error"]["code"]) == (401, "UNAUTHORIZED")
    assert r.es.requests == []


def test_a_wrong_token_is_401(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    assert (
        _post(client, {"query": "late"}, {"Authorization": "Bearer nope", **USER}).status_code
        == 401
    )


def test_authentication_comes_before_validation(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    assert _post(client, {"nonsense": 1}, USER).status_code == 401


def test_a_service_that_may_not_send_users_is_403(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    response = _post(client, {"query": "late"}, {"Authorization": "Bearer token-other", **USER})
    assert response.status_code == 403
    assert r.es.requests == []


def test_a_missing_user_is_400_and_nothing_is_searched(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    assert _post(client, {"query": "late"}, AUTH).status_code == 400
    assert r.es.requests == []


def test_a_strange_user_id_is_400(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    assert _post(client, {"query": "late"}, {**AUTH, "X-User-Id": "a*"}).status_code == 400


def test_identity_headers_cannot_be_forged_through_the_body(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    response = _post(client, {"query": "late", "user_id": "bob"})
    assert response.status_code == 400  # unknown fields are refused


# --- validation -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"query": ""},
        {"query": "x", "top_k": 0},
        {"query": "x", "top_k": 1000},
        {"query": "x", "mode": "fuzzy"},
        {"query": "x", "filters": {"created_from": "2025-01-01", "created_to": "2024-01-01"}},
        {"query": "x", "filters": {"acl_users": ["bob"]}},
        {"query": "y" * 5000},
    ],
)
def test_bad_requests_are_400_invalid_request(
    rig: tuple[Rig, TestClient], body: dict[str, Any]
) -> None:
    _, client = rig
    response = _post(client, body)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_REQUEST"


def test_a_query_over_the_configured_length_is_400(settings: Settings) -> None:
    r = Rig(settings, max_query_chars=20)
    with TestClient(r.app, raise_server_exceptions=False) as client:
        assert _post(client, {"query": "x" * 21}).status_code == 400
        assert _post(client, {"query": "x" * 20}).status_code == 200


def test_the_error_does_not_echo_the_question(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    response = _post(client, {"query": "synthetic secret question", "top_k": "abc"})
    assert "synthetic secret question" not in response.text


# --- rate limits ----------------------------------------------------------------------------


def test_a_user_over_the_limit_gets_429_with_retry_after(settings: Settings) -> None:
    r = Rig(settings, rate_limit_user_per_min=2)
    with TestClient(r.app, raise_server_exceptions=False) as client:
        assert _post(client, {"query": "late"}).status_code == 200
        assert _post(client, {"query": "late"}).status_code == 200
        limited = _post(client, {"query": "late"})
        assert limited.status_code == 429
        assert limited.json()["error"]["code"] == "RATE_LIMITED"
        assert int(limited.headers["Retry-After"]) >= 1
        assert _post(client, {"query": "late"}, {**AUTH, "X-User-Id": "bob"}).status_code == 200


def test_a_service_over_the_limit_gets_429(settings: Settings) -> None:
    r = Rig(settings, rate_limit_service_per_min=1)
    with TestClient(r.app, raise_server_exceptions=False) as client:
        assert _post(client, {"query": "late"}).status_code == 200
        assert (
            _post(
                client, {"query": "late", "mode": "bm25"}, {**AUTH, "X-User-Id": "bob"}
            ).status_code
            == 429
        )


# --- logs -----------------------------------------------------------------------------------


def test_the_log_has_ids_and_never_the_question(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    with structlog.testing.capture_logs() as logs:
        _post(client, {"query": "synthetic secret question about penalty", "mode": "bm25"})
    text = str(logs)
    assert "synthetic secret question" not in text
    assert "D1:0" in text
    assert "alice" in text
