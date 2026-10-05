"""In-house embedding provider: an HTTP server in the TEI style (Hugging Face Text Embeddings
Inference, or an approved equivalent) running on our own GPUs.

``POST {endpoint}/embed`` with ``{"inputs": [...], "normalize": true, "truncate": false}``
answers with one vector per input. Chunks go in batches, several batches at a time, within a
concurrency limit that drops when the server says it is overloaded.
"""

from app.core.http import JsonHttpClient
from app.core.limiter import AdaptiveLimiter
from app.core.settings import EmbeddingSettings
from app.embeddings.batching import embed_in_batches
from app.embeddings.vectors import clean_vectors


class InHouseEmbedder:
    """Embedder for the in-house model server."""

    def __init__(self, settings: EmbeddingSettings, http: JsonHttpClient) -> None:
        self._cfg = settings
        self._http = http
        self.model_name = f"{settings.model}@{settings.model_version}"
        self.dims = settings.dims
        self._url = f"{settings.endpoint.rstrip('/')}/embed"
        self._limiter = AdaptiveLimiter(settings.max_concurrency)

    async def _embed_batch(self, texts: list[str], timeout_s: float) -> list[list[float]]:
        payload = {"inputs": texts, "normalize": True, "truncate": self._cfg.truncate}
        async with self._limiter:
            raw = await self._http.post_json(
                self._url,
                payload,
                timeout_s=timeout_s,
                on_overload=self._limiter.on_overload,
                on_success=self._limiter.on_success,
            )
        return clean_vectors(raw, len(texts), self.dims)

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """One vector per text, in order. Texts are sent in batches, several at a time."""

        async def one(batch: list[str]) -> list[list[float]]:
            return await self._embed_batch(batch, self._cfg.document_timeout_s)

        return await embed_in_batches(one, texts, self._cfg.batch_size)

    async def embed_query(self, text: str) -> list[float]:
        """One vector for a search query, with the short query timeout."""
        [vector] = await self._embed_batch([text], self._cfg.timeout_s)
        return vector
