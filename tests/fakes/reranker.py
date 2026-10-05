"""Fake Reranker: scores by the share of query words found in the passage."""


class FakeReranker:
    """Deterministic ordering. Records calls. Can be told to fail."""

    def __init__(self, model_name: str = "fake-reranker") -> None:
        self.model_name = model_name
        self.fail_with: Exception | None = None
        self.calls: list[tuple[str, list[str], int]] = []

    async def rerank(self, query: str, passages: list[str], top_n: int) -> list[tuple[int, float]]:
        """Best passages first. Ties keep the input order."""
        if self.fail_with:
            raise self.fail_with
        self.calls.append((query, list(passages), top_n))
        words = set(query.lower().split())
        scored = [
            (index, len(words & set(passage.lower().split())) / (len(words) or 1))
            for index, passage in enumerate(passages)
        ]
        scored.sort(key=lambda pair: (-pair[1], pair[0]))
        return scored[:top_n]
