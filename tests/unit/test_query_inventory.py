"""Every place in the code that sends an Elasticsearch search or query request (rule 1).

New places must be added here on purpose, with a reason. ``retrieval`` is the only package that may serve
users. The others are internal jobs and maintenance (decisions 0006 and 0007). No router may call them.
"""

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "app"
# Names of client calls that send a query to Elasticsearch.
QUERY_CALLS = {"search", "count", "delete_by_query", "update_by_query", "msearch", "knn_search"}

ALLOWED = {
    # user searches: the access filter is built and verified by the QueryBuilder
    "retrieval/searcher.py": "user search, verified by QueryBuilder.verify",
    # internal jobs and maintenance: no user, no document text (decisions 0006 and 0007)
    "jobs/scan.py": "backfill scan of the source index (IDs and filter fields only)",
    "jobs/job_store.py": "list job records",
    "ingestion/state_admin.py": "state counts and ordered scan of our state index",
    "ingestion/indexer.py": "delete and update by query on our own chunk index",
    "store/aliases.py": "document count of an index before the alias moves",
}


def _receiver(node: ast.expr) -> str:
    """The last name of what the method is called on: ``self._client.search`` gives ``_client``."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _is_elasticsearch_client(name: str) -> bool:
    return "client" in name or name in {"es", "_es", "_maintenance", "_bulk"}


def _calls(path: Path) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in QUERY_CALLS
            and _is_elasticsearch_client(_receiver(node.func.value))
        ):
            found.add(node.func.attr)
    return found


def _files_with_query_calls() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for path in SRC.rglob("*.py"):
        calls = _calls(path)
        if calls:
            out[path.relative_to(SRC).as_posix()] = calls
    return out


def test_only_known_places_send_queries() -> None:
    found = _files_with_query_calls()
    unknown = sorted(set(found) - set(ALLOWED))
    assert not unknown, (
        f"new places that send Elasticsearch queries, review them for rule 1: {unknown}"
    )


def test_the_allow_list_has_no_stale_entries() -> None:
    found = _files_with_query_calls()
    stale = sorted(set(ALLOWED) - set(found))
    assert not stale, f"no query call any more in: {stale}"


@pytest.mark.parametrize("name", ["api", "rag"])
def test_routers_and_rag_never_talk_to_elasticsearch_directly(name: str) -> None:
    offenders = [p for p in _files_with_query_calls() if p.startswith(f"{name}/")]
    assert offenders == []


def test_only_retrieval_serves_user_searches() -> None:
    user_facing = {p for p in _files_with_query_calls() if p.startswith("retrieval/")}
    assert user_facing == {"retrieval/searcher.py"}
