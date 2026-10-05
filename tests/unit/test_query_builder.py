"""The access filter and the query builder: every request carries the filter (rule 1)."""

import copy
from datetime import date
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from app.core.errors import ForbiddenError
from app.core.security import Identity
from app.core.settings import SearchSettings
from app.retrieval.acl import AclFilter
from app.retrieval.filters import SearchFilters
from app.retrieval.query import AccessFilterMissingError, QueryBuilder

CFG = SearchSettings(index_alias="alias", source_index="src")
BUILDER = QueryBuilder(CFG)
VECTOR = [0.1, 0.2, 0.3, 0.4]


def _acl(user: str = "u1", groups: tuple[str, ...] = ("g1", "g2")) -> AclFilter:
    return AclFilter.from_identity(Identity(user_id=user, groups=groups, service="svc"))


# --- the access filter ----------------------------------------------------------------------


def test_filter_is_user_or_any_group() -> None:
    assert _acl().to_query() == {
        "bool": {
            "should": [{"term": {"acl_users": "u1"}}, {"terms": {"acl_groups": ["g1", "g2"]}}],
            "minimum_should_match": 1,
        }
    }


def test_filter_without_groups_has_only_the_user_clause() -> None:
    assert _acl(groups=()).to_query()["bool"]["should"] == [{"term": {"acl_users": "u1"}}]


def test_groups_are_sorted_and_unique() -> None:
    acl = AclFilter.from_identity(Identity(user_id="u1", groups=("b", "a", "b"), service="s"))
    assert acl.groups == ("a", "b")


@pytest.mark.parametrize("user", ["", "   "])
def test_no_user_means_no_access(user: str) -> None:
    with pytest.raises(ForbiddenError):
        AclFilter.from_identity(Identity(user_id=user, groups=("g",), service="s"))


def test_scope_key_depends_on_the_rights_and_nothing_else() -> None:
    assert _acl().scope_key() == _acl().scope_key()
    assert _acl().scope_key() != _acl(user="u2").scope_key()
    assert _acl().scope_key() != _acl(groups=("g1",)).scope_key()
    same = AclFilter.from_identity(
        Identity(user_id="u1", groups=("g2", "g1"), service="other-service")
    )
    assert same.scope_key() == _acl().scope_key()  # order and service do not matter


def test_a_user_and_a_group_with_the_same_text_are_different_scopes() -> None:
    assert _acl(user="a", groups=("b",)).scope_key() != _acl(user="a,b", groups=()).scope_key()


# --- filters --------------------------------------------------------------------------------


def test_filters_make_clauses() -> None:
    filters = SearchFilters(
        doc_type=["contract"],
        tags=["vendor", "2024"],
        created_from=date(2024, 1, 1),
        created_to=date(2024, 12, 31),
    )
    assert filters.clauses() == [
        {"terms": {"doc_type": ["contract"]}},
        {"term": {"tags": "vendor"}},
        {"term": {"tags": "2024"}},
        {"range": {"created_at": {"gte": "2024-01-01", "lte": "2024-12-31"}}},
    ]


def test_no_filters_no_clauses() -> None:
    assert SearchFilters().clauses() == []


@pytest.mark.parametrize(
    "data",
    [
        {"doc_type": [""]},
        {"tags": ["x" * 129]},
        {"created_from": "2024-05-01", "created_to": "2024-01-01"},
        {"unknown": 1},
        {"doc_type": [f"t{i}" for i in range(51)]},
    ],
)
def test_bad_filters_are_rejected(data: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        SearchFilters.model_validate(data)


# --- the requests ---------------------------------------------------------------------------


def _collect_filters(node: Any, found: list[list[Any]]) -> None:
    if isinstance(node, dict):
        if "filter" in node:
            found.append(node["filter"])
        for value in node.values():
            _collect_filters(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_filters(item, found)


def test_bm25_has_the_access_filter_next_to_the_user_filters() -> None:
    request = BUILDER.bm25("late delivery", _acl(), SearchFilters(doc_type=["contract"]), 10)
    assert request["query"]["bool"]["filter"] == [
        _acl().to_query(),
        {"terms": {"doc_type": ["contract"]}},
    ]
    assert request["query"]["bool"]["must"][0]["multi_match"]["query"] == "late delivery"
    assert request["size"] == 10
    assert "content" in request["source"]
    assert request["highlight"]["fields"]["content"]["encoder"] == "html"


def test_knn_has_the_access_filter_inside_the_knn_part() -> None:
    request = BUILDER.knn(VECTOR, _acl(), SearchFilters(tags=["vendor"]), 10)
    knn = request["knn"]
    assert knn["filter"] == [_acl().to_query(), {"term": {"tags": "vendor"}}]
    assert knn["field"] == "embedding"
    assert knn["query_vector"] == VECTOR
    assert knn["k"] == 100
    assert knn["num_candidates"] == 300


def test_k_is_at_least_the_requested_size() -> None:
    assert BUILDER.knn(VECTOR, _acl(), SearchFilters(), 150)["knn"]["k"] == 150


def test_the_rrf_retriever_has_the_filter_in_both_legs() -> None:
    request = BUILDER.rrf("late delivery", VECTOR, _acl(), SearchFilters(), 10)
    rrf = request["retriever"]["rrf"]
    standard, knn = rrf["retrievers"]
    assert _acl().to_query() in standard["standard"]["query"]["bool"]["filter"]
    assert _acl().to_query() in knn["knn"]["filter"]
    assert (rrf["rank_window_size"], rrf["rank_constant"]) == (100, 60)


def test_settings_decide_candidates_and_the_rank_constant() -> None:
    builder = QueryBuilder(
        SearchSettings(
            index_alias="a",
            source_index="s",
            candidates=20,
            rrf_rank_constant=10,
            num_candidates_factor=5,
        )
    )
    rrf = builder.rrf("q", VECTOR, _acl(), SearchFilters(), 5)["retriever"]["rrf"]
    assert (rrf["rank_window_size"], rrf["rank_constant"]) == (20, 10)
    assert rrf["retrievers"][1]["knn"]["num_candidates"] == 100


# --- verify refuses requests without the filter ---------------------------------------------


def _good() -> dict[str, Any]:
    return BUILDER.rrf("q", VECTOR, _acl(), SearchFilters(), 10)


def test_verify_accepts_what_the_builder_makes() -> None:
    for request in (
        BUILDER.bm25("q", _acl(), SearchFilters(), 5),
        BUILDER.knn(VECTOR, _acl(), SearchFilters(), 5),
        _good(),
    ):
        QueryBuilder.verify(request, _acl())


def test_verify_refuses_a_request_made_for_another_user() -> None:
    with pytest.raises(AccessFilterMissingError):
        QueryBuilder.verify(BUILDER.bm25("q", _acl("u1"), SearchFilters(), 5), _acl("u2"))


def test_verify_refuses_a_text_query_without_the_filter() -> None:
    request = _good()
    request["retriever"]["rrf"]["retrievers"][0]["standard"]["query"]["bool"]["filter"] = []
    with pytest.raises(AccessFilterMissingError):
        QueryBuilder.verify(request, _acl())


def test_verify_refuses_a_knn_without_the_filter() -> None:
    request = _good()
    del request["retriever"]["rrf"]["retrievers"][1]["knn"]["filter"]
    with pytest.raises(AccessFilterMissingError):
        QueryBuilder.verify(request, _acl())


def test_verify_refuses_a_knn_with_only_user_filters() -> None:
    request = BUILDER.knn(VECTOR, _acl(), SearchFilters(tags=["a"]), 5)
    request["knn"]["filter"] = [{"term": {"tags": "a"}}]
    with pytest.raises(AccessFilterMissingError):
        QueryBuilder.verify(request, _acl())


@pytest.mark.parametrize(
    "request_body",
    [
        {"query": {"match_all": {}}, "size": 10},
        {"query": {"query_string": {"query": "*"}}, "size": 10},
        {"size": 10},
        {},
    ],
)
def test_verify_refuses_unfiltered_query_types_and_empty_requests(
    request_body: dict[str, Any],
) -> None:
    with pytest.raises(AccessFilterMissingError):
        QueryBuilder.verify(request_body, _acl())


def test_a_weakened_filter_is_not_accepted() -> None:
    request = BUILDER.bm25("q", _acl(), SearchFilters(), 5)
    weaker = copy.deepcopy(request)
    weaker["query"]["bool"]["filter"][0]["bool"]["minimum_should_match"] = 0
    with pytest.raises(AccessFilterMissingError):
        QueryBuilder.verify(weaker, _acl())


# --- properties -----------------------------------------------------------------------------

_IDS = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789._-", min_size=1, max_size=12)


@given(
    user=_IDS,
    groups=st.lists(_IDS, max_size=6),
    text=st.text(min_size=1, max_size=40),
    types=st.lists(_IDS, max_size=3),
    size=st.integers(1, 100),
)
@settings(max_examples=150, deadline=None)
def test_every_request_the_builder_makes_carries_the_access_filter(
    user: str, groups: list[str], text: str, types: list[str], size: int
) -> None:
    acl = AclFilter.from_identity(Identity(user_id=user, groups=tuple(groups), service="s"))
    filters = SearchFilters(doc_type=types)
    for request in (
        BUILDER.bm25(text, acl, filters, size),
        BUILDER.knn(VECTOR, acl, filters, size),
        BUILDER.rrf(text, VECTOR, acl, filters, size),
    ):
        QueryBuilder.verify(request, acl)
        found: list[list[Any]] = []
        _collect_filters(request, found)
        assert found
        assert all(acl.to_query() in clauses for clauses in found)
