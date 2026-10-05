"""OpenAI embedding provider. Optional, and off by default.

It is used only when ``embedding.provider`` is ``openai`` AND ``embedding.allow_external`` is
true, and only for data that security has approved (HLD section 9). Traffic leaves through the
approved egress proxy (``embedding.proxy``). Plain HTTP, no SDK.
"""

from app.core.errors import NonRetryableError
from app.core.http import JsonHttpClient
from app.core.limiter import AdaptiveLimiter
from app.core.settings import EmbeddingSettings
from app.embeddings.batching import embed_in_batches
from app.embeddings.vectors import clean_vectors


class OpenAIEmbedder:
    """Embedder for the OpenAI embeddings API."""

    def __init__(self, settings: EmbeddingSettings, http: JsonHttpClient) -> None:
        if settings.openai_api_key is None:
            raise NonRetryableError("OpenAI embeddings need an API key")
        self._cfg = settings
        self._http = http
        self._headers = {"Authorization": f"Bearer {settings.openai_api_key.get_secret_value()}"}
        self.model_name = f"{settings.model}@{settings.model_version}"
        self.dims = settings.dims
        self._url = f"{settings.openai_base_url.rstrip('/')}/embeddings"
        self._limiter = AdaptiveLimiter(settings.max_concurrency)

    async def _embed_batch(self, texts: list[str], timeout_s: float) -> list[list[float]]:
        payload = {"model": self._cfg.model, "input": texts, "dimensions": self.dims}
        async with self._limiter:
            raw = await self._http.post_json(
                self._url,
                payload,
                timeout_s=timeout_s,
                headers=self._headers,
                on_overload=self._limiter.on_overload,
                on_success=self._limiter.on_success,
            )
        try:
            ordered = sorted(raw["data"], key=lambda entry: entry["index"])
            vectors = [entry["embedding"] for entry in ordered]
        except (KeyError, TypeError) as exc:
            raise NonRetryableError("OpenAI sent an invalid answer") from exc
        return clean_vectors(vectors, len(texts), self.dims)

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """One vector per text, in order. Texts are sent in batches, several at a time."""

        async def one(batch: list[str]) -> list[list[float]]:
            return await self._embed_batch(batch, self._cfg.document_timeout_s)

        return await embed_in_batches(one, texts, self._cfg.batch_size)

    async def embed_query(self, text: str) -> list[float]:
        """One vector for a search query."""
        [vector] = await self._embed_batch([text], self._cfg.timeout_s)
        return vector
