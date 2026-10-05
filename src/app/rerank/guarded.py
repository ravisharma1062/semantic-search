"""A reranker with a time limit and a circuit breaker (HLD section 11).

When the reranker server is slow or down, the breaker opens after a few failures and calls fail at
once, so the search returns the RRF order without waiting. The search service reports that in
``mode_used``.
"""

import asyncio

from app.core.breaker import CircuitBreaker
from app.rerank.base import Reranker


class GuardedReranker:
    """Wraps a ``Reranker`` with a timeout and a circuit breaker."""

    def __init__(self, inner: Reranker, breaker: CircuitBreaker, timeout_s: float) -> None:
        self._inner = inner
        self._breaker = breaker
        self._timeout_s = timeout_s
        self.model_name = inner.model_name

    async def rerank(self, query: str, passages: list[str], top_n: int) -> list[tuple[int, float]]:
        """Rerank, or raise quickly if the server is slow or the breaker is open."""
        return await self._breaker.call(
            lambda: asyncio.wait_for(self._inner.rerank(query, passages, top_n), self._timeout_s)
        )
