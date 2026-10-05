"""In-house reranker: an HTTP server in the TEI style running a cross-encoder (bge-reranker).

``POST {endpoint}/rerank`` with ``{"query": "...", "texts": [...], "truncate": false}`` answers with
``[{"index": 3, "score": 0.91}, ...]``. The passages are chunk texts the user may see: the search
filters by access before anything reaches this class.
"""

import math
from typing import Any

from app.core.errors import NonRetryableError
from app.core.http import JsonHttpClient
from app.core.limiter import AdaptiveLimiter
from app.core.settings import RerankerSettings


class InHouseReranker:
    """``Reranker`` for the in-house model server."""

    def __init__(self, settings: RerankerSettings, http: JsonHttpClient) -> None:
        self._cfg = settings
        self._http = http
        self.model_name = settings.model
        self._url = f"{settings.endpoint.rstrip('/')}/rerank"
        self._limiter = AdaptiveLimiter(settings.max_concurrency)

    async def rerank(self, query: str, passages: list[str], top_n: int) -> list[tuple[int, float]]:
        """The best ``top_n`` passages as (index into ``passages``, score), best first."""
        if not passages:
            return []
        payload = {"query": query, "texts": passages, "truncate": self._cfg.truncate}
        async with self._limiter:
            raw = await self._http.post_json(
                self._url,
                payload,
                timeout_s=self._cfg.timeout_s,
                on_overload=self._limiter.on_overload,
                on_success=self._limiter.on_success,
            )
        return self._parse(raw, len(passages), top_n)

    @staticmethod
    def _parse(raw: Any, count: int, top_n: int) -> list[tuple[int, float]]:
        if not isinstance(raw, list):
            raise NonRetryableError("Reranker sent an unexpected answer")
        scored: dict[int, float] = {}
        for item in raw:
            try:
                index, score = int(item["index"]), float(item["score"])
            except (KeyError, TypeError, ValueError) as exc:
                raise NonRetryableError("Reranker sent an unexpected answer") from exc
            if not 0 <= index < count or not math.isfinite(score):
                raise NonRetryableError("Reranker sent an unexpected answer")
            scored[index] = score
        ordered = sorted(scored.items(), key=lambda pair: (-pair[1], pair[0]))
        return ordered[:top_n]
