"""Embedder interface. Pipeline code talks only to this protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class Embedder(Protocol):
    """Turns text into vectors."""

    model_name: str
    dims: int

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed chunk texts, one vector per text, in the same order."""
        ...

    async def embed_query(self, text: str) -> list[float]:
        """Embed one search query."""
        ...
