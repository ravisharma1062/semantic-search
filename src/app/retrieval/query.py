"""The only place that builds Elasticsearch search requests (rule 1).

Every request has the access filter: in the text query, inside the kNN part, and in both legs of the
rrf retriever. ``verify`` walks a finished request and refuses it if any query part is without the
filter. Callers (``HybridSearcher``) always verify before they send, so a mistake here fails closed.

Keys follow the Python client: ``query``, ``knn``, ``retriever``, ``size``, ``source``,
``highlight``.
"""

from collections.abc import Iterator, Sequence
from typing import Any

from app.core.errors import NonRetryableError
from app.core.settings import SearchSettings
from app.retrieval.acl import AclFilter
from app.retrieval.filters import SearchFilters

SOURCE_FIELDS = [
    "doc_id",
    "chunk_id",
    "chunk_no",
    "page_start",
    "page_end",
    "section_title",
    "content",
    "doc_type",
]


class AccessFilterMissingError(NonRetryableError):
    """A request was built without the access filter. It is never sent."""

    default_message = "Search request without access filter"


def _violations(node: Any, acl_query: dict[str, Any]) -> Iterator[str]:
    """Query parts that do not carry the access filter."""
    if isinstance(node, dict):
        knn: Any = node.get("knn")
        if (
            isinstance(knn, dict)
            and "query_vector" in knn
            and acl_query not in _as_list(knn.get("filter"))
        ):
            yield "knn without access filter"
        bool_node: Any = node.get("bool")
        if (
            isinstance(bool_node, dict)
            and "must" in bool_node
            and acl_query not in _as_list(bool_node.get("filter"))
        ):
            yield "text query without access filter"
        if "match_all" in node or "query_string" in node:
            yield "unfiltered query type"
        for value in node.values():
            yield from _violations(value, acl_query)
    elif isinstance(node, list):
        for item in node:
            yield from _violations(item, acl_query)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _has_query_part(node: Any) -> bool:
    if isinstance(node, dict):
        if "query_vector" in node:
            return True
        bool_node = node.get("bool")
        if isinstance(bool_node, dict) and "must" in bool_node:
            return True
        return any(_has_query_part(v) for v in node.values())
    if isinstance(node, list):
        return any(_has_query_part(i) for i in node)
    return False


class QueryBuilder:
    """Builds search requests. Needs the access filter for every one of them."""

    def __init__(self, settings: SearchSettings) -> None:
        self._cfg = settings

    # --- the parts ------------------------------------------------------------------------

    def _text_query(self, text: str, acl: AclFilter, filters: SearchFilters) -> dict[str, Any]:
        return {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": text,
                            "fields": ["content", "section_title^2"],
                            "type": "best_fields",
                        }
                    }
                ],
                "filter": [acl.to_query(), *filters.clauses()],
            }
        }

    def _knn(
        self, vector: Sequence[float], acl: AclFilter, filters: SearchFilters, size: int
    ) -> dict[str, Any]:
        k = max(self._cfg.candidates, size)
        return {
            "field": "embedding",
            "query_vector": list(vector),
            "k": k,
            "num_candidates": k * self._cfg.num_candidates_factor,
            "filter": [acl.to_query(), *filters.clauses()],
        }

    def _highlight(self) -> dict[str, Any]:
        return {
            "fields": {
                "content": {
                    "fragment_size": self._cfg.snippet_chars,
                    "number_of_fragments": 1,
                    "pre_tags": ["<em>"],
                    "post_tags": ["</em>"],
                    "encoder": "html",
                }
            }
        }

    # --- the requests ---------------------------------------------------------------------

    def bm25(self, text: str, acl: AclFilter, filters: SearchFilters, size: int) -> dict[str, Any]:
        """Keyword search with highlights."""
        request = {
            "query": self._text_query(text, acl, filters),
            "size": size,
            "source": SOURCE_FIELDS,
            "highlight": self._highlight(),
        }
        self.verify(request, acl)
        return request

    def knn(
        self, vector: Sequence[float], acl: AclFilter, filters: SearchFilters, size: int
    ) -> dict[str, Any]:
        """Vector search."""
        request = {
            "knn": self._knn(vector, acl, filters, size),
            "size": size,
            "source": SOURCE_FIELDS,
        }
        self.verify(request, acl)
        return request

    def rrf(
        self,
        text: str,
        vector: Sequence[float],
        acl: AclFilter,
        filters: SearchFilters,
        size: int,
    ) -> dict[str, Any]:
        """Both searches merged by Elasticsearch (the rrf retriever).

        Elasticsearch cannot highlight with a retriever, so snippets are made here.
        """
        request = {
            "retriever": {
                "rrf": {
                    "retrievers": [
                        {"standard": {"query": self._text_query(text, acl, filters)}},
                        {"knn": self._knn(vector, acl, filters, size)},
                    ],
                    "rank_window_size": max(self._cfg.candidates, size),
                    "rank_constant": self._cfg.rrf_rank_constant,
                }
            },
            "size": size,
            "source": SOURCE_FIELDS,
        }
        self.verify(request, acl)
        return request

    # --- the guard ------------------------------------------------------------------------

    @staticmethod
    def verify(request: dict[str, Any], acl: AclFilter) -> None:
        """Raise unless every query part of the request carries the access filter."""
        acl_query = acl.to_query()
        problems = list(_violations(request, acl_query))
        if problems or not _has_query_part(request):
            raise AccessFilterMissingError
