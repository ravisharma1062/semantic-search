"""Access isolation (task T2.6): users see only what their rights allow.

Several users run the same queries in every search mode. The results must differ as expected, and no
snippet, highlight, ID or page of a document the user may not open may appear anywhere in an answer.
This suite blocks the merge (CI job "Access isolation").
"""

import json
from collections.abc import Iterator
from typing import Any, cast

import pytest
from elastic_transport import ConnectionTimeout
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core.retry import RetryPolicy
from app.core.security import Identity
from app.core.settings import ApiSettings, Settings
from app.main import create_app
from app.retrieval.filters import SearchFilters
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import RequestMode, SearchService
from app.services import Services
from tests.fakes import FakeEmbedder
from tests.fakes.search_es import FakeSearchEs, chunk_doc

pytestmark = pytest.mark.access

# document -> (who may open it, a word that only this document contains)
CORPUS: dict[str, tuple[dict[str, Any], str]] = {
    "LEGAL": ({"groups": ["g-legal"]}, "legalsecret"),
    "HR": ({"groups": ["g-hr"]}, "hrsecret"),
    "CAROL": ({"users": ["carol"]}, "carolsecret"),
    "ALICE": ({"users": ["alice"]}, "alicesecret"),
    "NOBODY": ({}, "nobodysecret"),  # nobody can open it
    "SHARED": ({"groups": ["g-legal", "g-hr"], "users": ["carol"]}, "sharedsecret"),
    "ALICE2": ({"users": ["alice2"]}, "alicetwosecret"),  # similar user name
}

USERS: dict[str, Identity] = {
    "alice": Identity("alice", ("g-legal",), "svc"),
    "bob": Identity("bob", ("g-hr",), "svc"),
    "carol": Identity("carol", (), "svc"),
    "dave": Identity("dave", ("g-legal", "g-hr"), "svc"),
    "eve": Identity("eve", (), "svc"),
    "alic": Identity("alic", (), "svc"),  # a prefix of alice
}

VISIBLE: dict[str, set[str]] = {
    "alice": {"LEGAL", "ALICE", "SHARED"},
    "bob": {"HR", "SHARED"},
    "carol": {"CAROL", "SHARED"},
    "dave": {"LEGAL", "HR", "SHARED"},
    "eve": set(),
    "alic": set(),
}

MODES: list[RequestMode] = ["bm25", "hybrid", "vector"]


def _docs() -> list[dict[str, Any]]:
    return [
        chunk_doc(
            f"{name}:0",
            name,
            f"quarterly report {secret} appendix",
            users=acl.get("users"),
            groups=acl.get("groups"),
        )
        for name, (acl, secret) in CORPUS.items()
    ]


def _service(settings: Settings) -> SearchService:
    cfg = settings.search
    searcher = HybridSearcher(
        cast(AsyncElasticsearch, FakeSearchEs(_docs())),
        cfg.index_alias,
        QueryBuilder(cfg),
        cfg,
        settings.elasticsearch,
        RetryPolicy(attempts=1),
    )
    return SearchService(embedder=FakeEmbedder(dims=4), searcher=searcher, settings=settings)


def _forbidden_secrets(user: str) -> set[str]:
    return {secret for name, (_, secret) in CORPUS.items() if name not in VISIBLE[user]}


@pytest.fixture
def service(settings: Settings) -> SearchService:
    return _service(settings)


# --- the same query, different users --------------------------------------------------------


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("user", sorted(USERS))
async def test_each_user_gets_exactly_the_documents_they_may_open(
    service: SearchService, user: str, mode: RequestMode
) -> None:
    result = await service.search(
        query="quarterly report", identity=USERS[user], mode=mode, top_k=50
    )
    assert {h.doc_id for h in result.hits} == VISIBLE[user]


@pytest.mark.parametrize("mode", MODES)
async def test_the_results_really_differ_between_users(
    service: SearchService, mode: RequestMode
) -> None:
    results = {
        user: frozenset(
            h.doc_id
            for h in (
                await service.search(query="quarterly report", identity=ident, mode=mode, top_k=50)
            ).hits
        )
        for user, ident in USERS.items()
    }
    assert len(set(results.values())) >= 4  # at least four different views of the same corpus
    assert results["dave"] > results["bob"]  # dave has all of bob's rights and more
    assert results["alice"] != results["dave"]  # alice has a personal document that dave lacks
    assert results["eve"] == frozenset()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("user", sorted(USERS))
async def test_nothing_of_a_forbidden_document_appears_anywhere_in_an_answer(
    service: SearchService, user: str, mode: RequestMode
) -> None:
    result = await service.search(
        query="quarterly report appendix", identity=USERS[user], mode=mode, top_k=50
    )
    answer = json.dumps(
        [
            {
                "snippet": h.snippet,
                "highlights": h.highlights,
                "ids": [h.doc_id, h.chunk_id],
                "pages": h.pages,
                "content": h.content,
            }
            for h in result.hits
        ]
    ).lower()
    for secret in _forbidden_secrets(user):
        assert secret not in answer
    forbidden_docs = set(CORPUS) - VISIBLE[user]
    assert not any(
        f'"{doc.lower()}"' in answer or f'"{doc.lower()}:0"' in answer for doc in forbidden_docs
    )


@pytest.mark.parametrize("mode", ["bm25", "hybrid"])
@pytest.mark.parametrize("user", sorted(USERS))
async def test_asking_for_another_users_secret_finds_nothing(
    service: SearchService, user: str, mode: RequestMode
) -> None:
    for secret in _forbidden_secrets(user):
        result = await service.search(
            query=secret, identity=USERS[user], mode="bm25" if mode == "bm25" else "hybrid"
        )
        found_secrets = {h.doc_id for h in result.hits if secret in h.content}
        assert found_secrets == set(), f"{user} found {secret}"


async def test_a_user_can_find_their_own_secret(service: SearchService) -> None:
    result = await service.search(query="alicesecret", identity=USERS["alice"], mode="bm25")
    assert [h.doc_id for h in result.hits] == ["ALICE"]
    assert result.hits[0].highlights == ["alicesecret"]


# --- ways to try to get more ----------------------------------------------------------------


async def test_a_user_id_that_looks_like_a_group_name_gets_nothing(service: SearchService) -> None:
    sneaky = Identity("g-legal", (), "svc")
    result = await service.search(query="quarterly", identity=sneaky, mode="bm25", top_k=50)
    assert result.hits == []


async def test_a_group_name_does_not_match_as_a_user(service: SearchService) -> None:
    sneaky = Identity("carol", (), "svc")
    result = await service.search(query="quarterly", identity=sneaky, mode="bm25", top_k=50)
    assert {h.doc_id for h in result.hits} == {"CAROL", "SHARED"}  # as a user, no group rights


async def test_a_similar_user_name_does_not_match(service: SearchService) -> None:
    for name in ("alice2", "alice ", "Alice", "alic"):
        result = await service.search(
            query="alicesecret alicetwosecret",
            identity=Identity(name, (), "svc"),
            mode="bm25",
            top_k=50,
        )
        if name == "alice2":
            assert {h.doc_id for h in result.hits} == {"ALICE2"}
        else:
            assert result.hits == [], name


async def test_filters_never_widen_access(service: SearchService) -> None:
    for filters in (
        SearchFilters(doc_type=["contract"]),
        SearchFilters(tags=[]),
        SearchFilters(created_from="2000-01-01", created_to="2100-01-01"),
    ):
        result = await service.search(
            query="quarterly", identity=USERS["bob"], mode="bm25", filters=filters, top_k=50
        )
        assert {h.doc_id for h in result.hits} <= VISIBLE["bob"]


@pytest.mark.parametrize("mode", MODES)
async def test_grouping_by_document_and_a_huge_top_k_do_not_leak(
    service: SearchService, mode: RequestMode
) -> None:
    result = await service.search(
        query="quarterly", identity=USERS["carol"], mode=mode, top_k=10_000, group_by_document=True
    )
    assert {h.doc_id for h in result.hits} <= VISIBLE["carol"]


async def test_the_wide_fallback_modes_keep_the_filter(settings: Settings) -> None:
    """When the vector leg fails the keyword leg answers, and still only with allowed documents."""
    es = FakeSearchEs(_docs())
    es.fail["knn"] = ConnectionTimeout("slow")
    cfg = settings.search
    searcher = HybridSearcher(
        cast(AsyncElasticsearch, es),
        cfg.index_alias,
        QueryBuilder(cfg),
        cfg,
        settings.elasticsearch,
        RetryPolicy(attempts=1),
    )
    service = SearchService(embedder=FakeEmbedder(dims=4), searcher=searcher, settings=settings)
    result = await service.search(query="quarterly", identity=USERS["bob"], top_k=50)
    assert result.mode_used == "bm25"
    assert {h.doc_id for h in result.hits} == VISIBLE["bob"]


# --- the same through the HTTP API ----------------------------------------------------------


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    api = ApiSettings(
        service_tokens={"java-search": SecretStr("token-java")},
        identity_services=["java-search"],
        rate_limit_user_per_min=0,
        rate_limit_service_per_min=0,
    )
    s = settings.model_copy(update={"api": api})
    with TestClient(
        create_app(s, Services(search=_service(s))), raise_server_exceptions=False
    ) as c:
        yield c


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("user", sorted(USERS))
def test_http_answers_follow_the_identity_headers(client: TestClient, user: str, mode: str) -> None:
    ident = USERS[user]
    headers = {
        "Authorization": "Bearer token-java",
        "X-User-Id": ident.user_id,
        "X-User-Groups": ",".join(ident.groups),
    }
    response = client.post(
        "/v1/search", json={"query": "quarterly report", "mode": mode, "top_k": 50}, headers=headers
    )
    assert response.status_code == 200
    body = response.json()
    assert {r["doc_id"] for r in body["results"]} == VISIBLE[user]
    for secret in _forbidden_secrets(user):
        assert secret not in response.text.lower()


def test_http_extra_groups_in_the_body_or_query_string_change_nothing(client: TestClient) -> None:
    headers = {"Authorization": "Bearer token-java", "X-User-Id": "eve"}
    response = client.post(
        "/v1/search?groups=g-legal&user_id=alice",
        json={"query": "quarterly", "mode": "bm25"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["results"] == []
