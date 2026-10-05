"""The search use case: flags, query embedding, search, result shaping (HLD section 6).

Fallbacks, in order, each one visible in ``mode_used`` (rule 10):
    query embedding fails or is slow      -> keyword search only
    one leg of the hybrid search fails    -> the other leg
    both fail, or the time budget is over -> an error, and the Java app uses its own search
"""

import asyncio
import time
from typing import Literal

import structlog
from pydantic import BaseModel

from app.core.errors import (
    AppError,
    InvalidRequestError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.security import Identity
from app.core.settings import Settings
from app.embeddings.base import Embedder
from app.rerank.base import Reranker
from app.retrieval.acl import AclFilter
from app.retrieval.filters import SearchFilters
from app.retrieval.models import SearchHit, SearchOutcome
from app.retrieval.searcher import HybridSearcher

_log = structlog.get_logger(__name__)

RequestMode = Literal["hybrid", "bm25", "vector"]


class SearchResult(BaseModel):
    """The answer of the search use case."""

    mode_used: str
    hits: list[SearchHit]
    took_ms: int


def best_chunk_per_document(hits: list[SearchHit]) -> list[SearchHit]:
    """Keep the first (best) chunk of every document, in order."""
    seen: set[str] = set()
    out: list[SearchHit] = []
    for hit in hits:
        if hit.doc_id not in seen:
            seen.add(hit.doc_id)
            out.append(hit)
    return out


class SearchService:
    """Turns a query and an identity into ranked chunks the user may see."""

    def __init__(
        self,
        *,
        embedder: Embedder,
        searcher: HybridSearcher,
        settings: Settings,
        reranker: Reranker | None = None,
    ) -> None:
        self._embedder = embedder
        self._searcher = searcher
        self._settings = settings
        self._reranker = reranker

    async def search(
        self,
        *,
        query: str,
        identity: Identity,
        top_k: int | None = None,
        mode: RequestMode = "hybrid",
        filters: SearchFilters | None = None,
        group_by_document: bool = False,
        rerank: bool | None = None,
    ) -> SearchResult:
        """Search within the user's rights and within the time budget.

        ``rerank`` switches the reranker on or off for this request. ``None`` follows the
        setting ``reranker.enabled``. If the reranker fails or is slow, the RRF order is
        returned and ``mode_used`` does not say ``+rerank``.
        """
        cfg = self._settings.search
        if not self._settings.feature_flags.semantic_search:
            raise UpstreamUnavailableError("Semantic search is switched off")
        size = min(top_k or cfg.top_k_default, cfg.max_top_k)
        if size < 1 or not query.strip():
            raise InvalidRequestError("Empty query")
        acl = AclFilter.from_identity(identity)
        wanted = self._settings.reranker.enabled if rerank is None else rerank
        use_rerank = wanted and self._reranker is not None
        fetch = max(size, cfg.rerank_top_n) if use_rerank else size
        started = time.perf_counter()
        reranked = False
        try:
            async with asyncio.timeout(cfg.timeout_ms / 1000):
                outcome = await self._find(query, acl, filters or SearchFilters(), fetch, mode)
                hits = outcome.hits
                if use_rerank and hits:
                    hits, reranked = await self._rerank(query, hits)
        except TimeoutError as exc:
            raise UpstreamTimeoutError("Search budget exceeded") from exc
        if group_by_document:
            hits = best_chunk_per_document(hits)
        return SearchResult(
            mode_used=self._mode_used(outcome) + ("+rerank" if reranked else ""),
            hits=hits[:size],
            took_ms=round((time.perf_counter() - started) * 1000),
        )

    async def _rerank(self, query: str, hits: list[SearchHit]) -> tuple[list[SearchHit], bool]:
        """Reorder the best candidates. Any problem keeps the RRF order."""
        reranker = self._reranker
        if reranker is None:
            return hits, False
        limit = self._settings.search.rerank_top_n
        candidates, rest = hits[:limit], hits[limit:]
        passages = [
            f"{h.section_title}\n{h.content}" if h.section_title else h.content for h in candidates
        ]
        try:
            ordering = await reranker.rerank(query, passages, len(candidates))
            reordered = [candidates[i].model_copy(update={"score": s}) for i, s in ordering]
        except (AppError, TimeoutError, IndexError) as exc:
            _log.warning("rerank_skipped", error_type=type(exc).__name__)
            return hits, False
        return [*reordered, *rest], True

    @staticmethod
    def _mode_used(outcome: SearchOutcome) -> str:
        return {"hybrid": "hybrid", "bm25": "bm25", "knn": "knn"}[outcome.mode]

    async def _query_vector(self, query: str, *, required: bool) -> list[float] | None:
        """The query embedding, or ``None`` if it failed (then keyword search runs alone)."""
        budget = self._settings.embedding.timeout_s + 0.25
        try:
            return await asyncio.wait_for(self._embedder.embed_query(query), budget)
        except (AppError, TimeoutError) as exc:
            if required:
                raise
            _log.warning("query_embedding_failed", error_type=type(exc).__name__)
            return None

    async def _find(
        self,
        query: str,
        acl: AclFilter,
        filters: SearchFilters,
        size: int,
        mode: RequestMode,
    ) -> SearchOutcome:
        if mode == "bm25":
            return await self._searcher.search(query, None, acl, filters, size)
        vector = await self._query_vector(query, required=mode == "vector")
        if mode == "vector":
            if vector is None:
                raise UpstreamUnavailableError("Query embedding failed")
            return await self._searcher.knn_only(query, vector, acl, filters, size)
        return await self._searcher.search(query, vector, acl, filters, size)
