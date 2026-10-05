"""The objects the API needs, built once at startup.

Tests build ``Services`` from fakes and pass it to ``create_app``. Production builds the real ones
from the settings in ``build_services``. Nothing here keeps state that must survive a restart.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import httpx
from redis.asyncio import Redis

from app.core.settings import Settings
from app.embeddings.factory import create_embedder, create_http_client
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import SearchService
from app.store.client import create_es_client


@dataclass
class Services:
    """The use cases the routers call."""

    search: SearchService
    closers: list[Callable[[], Awaitable[None]]] = field(default_factory=list)

    async def close(self) -> None:
        """Close clients, last opened first."""
        for closer in reversed(self.closers):
            await closer()


async def build_services(settings: Settings) -> Services:
    """The real services: Elasticsearch, the embedding server and Redis."""
    es = create_es_client(settings.elasticsearch)
    http: httpx.AsyncClient = create_http_client(settings.embedding)
    redis = Redis.from_url(
        settings.redis.url,
        socket_timeout=settings.redis.timeout_ms / 1000,
        socket_connect_timeout=settings.redis.timeout_ms / 1000,
    )
    embedder = create_embedder(
        settings.embedding,
        http,
        settings.api.request_retry,
        redis=redis,
        redis_settings=settings.redis,
    )
    searcher = HybridSearcher(
        es,
        settings.search.index_alias,
        QueryBuilder(settings.search),
        settings.search,
        settings.elasticsearch,
        settings.api.request_retry,
    )
    search = SearchService(embedder=embedder, searcher=searcher, settings=settings)
    return Services(search=search, closers=[es.close, http.aclose, redis.aclose])
