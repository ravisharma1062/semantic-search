"""Redis cache for query embeddings.

Redis only makes repeated queries faster. An outage must never fail a request, so every Redis
call has a short timeout and any error counts as "not cached". After a few failures in a row the
cache is skipped for a while (a circuit breaker), so a dead Redis does not add its timeout to
every request.

- The key holds the model name and version, so vectors of different models are never mixed
  (HLD sections 4 and 11), and a hash of the query, never the query text itself.
- A cached vector has the same classification as the text: it stays in the approved Redis.
- Query embeddings do not depend on who asks, so they are shared between users. Answers are
  different: they are cached only per access scope (task T4.x).
"""

import asyncio
import hashlib
import struct
import time
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import structlog

from app.core.settings import EmbeddingSettings, RedisSettings
from app.embeddings.base import Embedder

_log = structlog.get_logger(__name__)


class RedisLike(Protocol):
    """The two Redis calls the cache uses. ``redis.asyncio.Redis`` fits."""

    def get(self, name: str) -> Awaitable[Any]:
        """The stored bytes, or ``None``."""
        ...

    def set(self, name: str, value: bytes, ex: int | None = None) -> Awaitable[Any]:
        """Store a value that expires after ``ex`` seconds."""
        ...


class QueryEmbeddingCache:
    """Gets and stores query vectors. Never raises."""

    def __init__(
        self,
        redis: RedisLike,
        *,
        model_name: str,
        dims: int,
        ttl_s: int,
        timeout_s: float,
        breaker_failures: int,
        breaker_cooldown_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._redis = redis
        self._model_name = model_name
        self._dims = dims
        self._ttl_s = ttl_s
        self._timeout_s = timeout_s
        self._breaker_failures = breaker_failures
        self._breaker_cooldown_s = breaker_cooldown_s
        self._clock = clock
        self._failures = 0
        self._open_until = 0.0

    def key(self, text: str) -> str:
        """Cache key: model name and version, and a hash of the query."""
        digest = hashlib.sha256(text.encode()).hexdigest()
        return f"emb:q:{self._model_name}:{self._dims}:{digest}"

    def _skip(self) -> bool:
        return self._clock() < self._open_until

    def _failed(self) -> None:
        self._failures += 1
        if self._failures >= self._breaker_failures:
            self._open_until = self._clock() + self._breaker_cooldown_s
            self._failures = 0
            _log.warning("embedding_cache_paused", seconds=self._breaker_cooldown_s)

    def _worked(self) -> None:
        self._failures = 0

    async def get(self, text: str) -> list[float] | None:
        """The cached vector, or ``None`` on a miss or any problem."""
        if self._skip():
            return None
        try:
            raw = await asyncio.wait_for(self._redis.get(self.key(text)), self._timeout_s)
        except Exception:
            self._failed()
            return None
        self._worked()
        if not isinstance(raw, bytes) or len(raw) != 4 * self._dims:
            return None  # a miss, or damaged, or from an older layout
        return list(struct.unpack(f"<{self._dims}f", raw))

    async def set(self, text: str, vector: list[float]) -> None:
        """Store the vector. Problems are ignored."""
        if self._skip() or len(vector) != self._dims:
            return
        try:
            packed = struct.pack(f"<{self._dims}f", *vector)
            await asyncio.wait_for(
                self._redis.set(self.key(text), packed, ex=self._ttl_s), self._timeout_s
            )
        except Exception:
            self._failed()
            return
        self._worked()


class CachedEmbedder:
    """An ``Embedder`` that answers repeated queries from Redis."""

    def __init__(self, inner: Embedder, cache: QueryEmbeddingCache) -> None:
        self._inner = inner
        self._cache = cache
        self.model_name = inner.model_name
        self.dims = inner.dims

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Chunks are not cached: each is embedded once."""
        return await self._inner.embed_documents(texts)

    async def embed_query(self, text: str) -> list[float]:
        """From the cache when possible, otherwise from the model server."""
        cached = await self._cache.get(text)
        if cached is not None:
            return cached
        vector = await self._inner.embed_query(text)
        await self._cache.set(text, vector)
        return vector


def create_query_cache(
    redis: RedisLike, embedding: EmbeddingSettings, settings: RedisSettings, model_name: str
) -> QueryEmbeddingCache:
    """The cache configured from settings."""
    return QueryEmbeddingCache(
        redis,
        model_name=model_name,
        dims=embedding.dims,
        ttl_s=embedding.cache_ttl_s,
        timeout_s=settings.timeout_ms / 1000,
        breaker_failures=settings.breaker_failures,
        breaker_cooldown_s=settings.breaker_cooldown_s,
    )
