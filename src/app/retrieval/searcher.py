"""Hybrid search: keyword and vector search merged with RRF (HLD section 6).

Two ways to merge, chosen by ``search.rrf_mode``:
- ``python``: two searches run at the same time and are merged here. Works with every license.
- ``retriever``: one request with the Elasticsearch rrf retriever. If the cluster does not support
  it (license, version), the searcher says so once in the log and uses ``python``.

If one leg fails, the other one answers and ``mode`` says which. If both fail, the error is raised,
and the Java app falls back to its own keyword search (rule 10). Every request is verified by the
``QueryBuilder`` to carry the access filter before it is sent.
"""

import asyncio
from collections.abc import Sequence
from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch

from app.core.errors import AppError, NonRetryableError, UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings, SearchSettings
from app.retrieval.acl import AclFilter
from app.retrieval.filters import SearchFilters
from app.retrieval.models import SearchHit, SearchOutcome, parse_hit, rrf_merge
from app.retrieval.query import QueryBuilder
from app.store.calls import guarded

_log = structlog.get_logger(__name__)


class HybridSearcher:
    """Runs searches against the chunk index alias."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        alias: str,
        builder: QueryBuilder,
        settings: SearchSettings,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
    ) -> None:
        self._client = client.options(request_timeout=elasticsearch.search_timeout_s)
        self._alias = alias
        self._builder = builder
        self._cfg = settings
        self._retry = retry
        self._retriever_unsupported = False

    async def _run(self, request: dict[str, Any], query: str) -> list[SearchHit]:
        async def call() -> list[dict[str, Any]]:
            response = await self._client.search(index=self._alias, **request)
            return list(response["hits"]["hits"])

        hits = await guarded(call, self._retry)
        return [parse_hit(h, self._cfg.snippet_chars, query) for h in hits]

    async def search(
        self,
        text: str,
        vector: Sequence[float] | None,
        acl: AclFilter,
        filters: SearchFilters,
        size: int,
    ) -> SearchOutcome:
        """The best ``size`` chunks the user may see."""
        if vector is None:
            return SearchOutcome(hits=await self._bm25(text, acl, filters, size), mode="bm25")
        if self._cfg.rrf_mode == "retriever" and not self._retriever_unsupported:
            outcome = await self._retriever(text, vector, acl, filters, size)
            if outcome is not None:
                return outcome
        return await self._python_merge(text, vector, acl, filters, size)

    async def _bm25(
        self, text: str, acl: AclFilter, filters: SearchFilters, size: int
    ) -> list[SearchHit]:
        return await self._run(self._builder.bm25(text, acl, filters, size), text)

    async def _retriever(
        self,
        text: str,
        vector: Sequence[float],
        acl: AclFilter,
        filters: SearchFilters,
        size: int,
    ) -> SearchOutcome | None:
        try:
            hits = await self._run(self._builder.rrf(text, vector, acl, filters, size), text)
        except NonRetryableError:
            _log.warning("rrf_retriever_not_supported_using_python_merge")
            self._retriever_unsupported = True
            return None
        except AppError:
            return None  # the python path tries both legs and reports the one that works
        return SearchOutcome(hits=hits, mode="hybrid")

    async def _python_merge(
        self,
        text: str,
        vector: Sequence[float],
        acl: AclFilter,
        filters: SearchFilters,
        size: int,
    ) -> SearchOutcome:
        leg = max(self._cfg.candidates, size)
        keyword, vectors = await asyncio.gather(
            self._bm25(text, acl, filters, leg),
            self._run(self._builder.knn(vector, acl, filters, leg), text),
            return_exceptions=True,
        )
        if isinstance(keyword, BaseException) and isinstance(vectors, BaseException):
            raise keyword if isinstance(keyword, AppError) else UpstreamUnavailableError()
        if isinstance(keyword, BaseException):
            _log.warning("bm25_leg_failed", error_type=type(keyword).__name__)
            return SearchOutcome(hits=vectors[:size], mode="knn")  # type: ignore[index]
        if isinstance(vectors, BaseException):
            _log.warning("knn_leg_failed", error_type=type(vectors).__name__)
            return SearchOutcome(hits=keyword[:size], mode="bm25")
        merged = rrf_merge([keyword, vectors], self._cfg.rrf_rank_constant, size)
        return SearchOutcome(hits=merged, mode="hybrid")

    async def knn_only(
        self, text: str, vector: Sequence[float], acl: AclFilter, filters: SearchFilters, size: int
    ) -> SearchOutcome:
        """Vector search alone (used for experiments and as a request mode)."""
        hits = await self._run(self._builder.knn(vector, acl, filters, size), text)
        return SearchOutcome(hits=hits[:size], mode="knn")
