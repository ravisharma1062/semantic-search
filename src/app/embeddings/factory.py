"""Builds the embedder chosen by settings (rule 5: pipeline code never picks a provider)."""

import httpx

from app.core.errors import NonRetryableError
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import EmbeddingSettings, RedisSettings
from app.embeddings.base import Embedder
from app.embeddings.cache import CachedEmbedder, RedisLike, create_query_cache
from app.embeddings.inhouse import InHouseEmbedder
from app.embeddings.openai import OpenAIEmbedder


def create_http_client(settings: EmbeddingSettings) -> httpx.AsyncClient:
    """The HTTP client for the embedding provider. The egress proxy applies to OpenAI only."""
    proxy = settings.proxy if settings.provider == "openai" else None
    return httpx.AsyncClient(proxy=proxy)


def create_embedder(
    settings: EmbeddingSettings,
    http_client: httpx.AsyncClient,
    retry: RetryPolicy,
    *,
    redis: RedisLike | None = None,
    redis_settings: RedisSettings | None = None,
) -> Embedder:
    """The embedder for ``embedding.provider``, with the query cache when Redis is given."""
    http = JsonHttpClient(http_client, retry)
    embedder: Embedder
    if settings.provider == "openai":
        if not settings.allow_external:
            raise NonRetryableError(
                "OpenAI embeddings are off: set embedding.allow_external after security approval"
            )
        embedder = OpenAIEmbedder(settings, http)
    else:
        embedder = InHouseEmbedder(settings, http)
    if redis is not None and redis_settings is not None:
        cache = create_query_cache(redis, settings, redis_settings, embedder.model_name)
        return CachedEmbedder(embedder, cache)
    return embedder
