"""Reranker interface."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class Reranker(Protocol):
    """Re-orders passages by relevance to a query."""

    model_name: str

    async def rerank(self, query: str, passages: list[str], top_n: int) -> list[tuple[int, float]]:
        """Return up to ``top_n`` pairs of (passage index, score), best first."""
        ...
