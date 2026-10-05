"""Fake Embedder: deterministic, normalized vectors from a hash of the text."""

import hashlib
import math


class FakeEmbedder:
    """Same text gives the same vector. Records calls. Can be told to fail."""

    def __init__(self, dims: int = 8, model_name: str = "fake-embedder") -> None:
        self.model_name = model_name
        self.dims = dims
        self.fail_with: Exception | None = None
        self.document_calls: list[list[str]] = []
        self.query_calls: list[str] = []

    def _vector(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode()).digest()
        raw = [digest[i % len(digest)] - 127.5 for i in range(self.dims)]
        norm = math.sqrt(sum(x * x for x in raw)) or 1.0
        return [x / norm for x in raw]

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """One vector per text."""
        if self.fail_with:
            raise self.fail_with
        self.document_calls.append(list(texts))
        return [self._vector(t) for t in texts]

    async def embed_query(self, text: str) -> list[float]:
        """One vector for the query."""
        if self.fail_with:
            raise self.fail_with
        self.query_calls.append(text)
        return self._vector(text)
