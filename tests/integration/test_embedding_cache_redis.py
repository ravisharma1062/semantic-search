"""The query cache against a real Redis, and what happens when Redis is gone."""

import time
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from app.core.settings import EmbeddingSettings, RedisSettings
from app.embeddings.cache import CachedEmbedder, create_query_cache
from tests.fakes import FakeEmbedder

pytestmark = pytest.mark.integration

DIMS = 8


def _embedder(redis: Redis, *, model: str = "bge-m3@1") -> tuple[CachedEmbedder, FakeEmbedder]:
    inner = FakeEmbedder(dims=DIMS, model_name=model)
    settings = EmbeddingSettings(model="bge-m3", endpoint="unused", dims=DIMS, cache_ttl_s=300)
    cache = create_query_cache(redis, settings, RedisSettings(url="unused", timeout_ms=200), model)
    return CachedEmbedder(inner, cache), inner


@pytest.fixture
async def redis(redis_url: str) -> AsyncIterator[Redis]:
    client = Redis.from_url(redis_url, socket_timeout=0.5, socket_connect_timeout=0.5)
    await client.flushdb()
    yield client
    await client.aclose()


async def test_a_repeated_query_is_served_from_redis_with_a_ttl(redis: Redis) -> None:
    embedder, inner = _embedder(redis)
    first = await embedder.embed_query("what is the penalty")
    second = await embedder.embed_query("what is the penalty")
    assert inner.query_calls == ["what is the penalty"]
    assert second == pytest.approx(first, abs=1e-6)
    [key] = [k async for k in redis.scan_iter("emb:q:*")]
    assert 0 < await redis.ttl(key) <= 300
    assert b"penalty" not in key  # the key holds a hash, not the question


async def test_a_new_model_version_uses_its_own_entries(redis: Redis) -> None:
    old, _ = _embedder(redis, model="bge-m3@1")
    new, new_inner = _embedder(redis, model="bge-m3@2")
    await old.embed_query("q")
    await new.embed_query("q")
    assert new_inner.query_calls == ["q"]  # not served from the old version
    assert len([k async for k in redis.scan_iter("emb:q:*")]) == 2


async def test_when_redis_is_down_requests_still_work_and_stay_fast() -> None:
    dead = Redis.from_url("redis://127.0.0.1:1/0", socket_timeout=0.2, socket_connect_timeout=0.2)
    try:
        embedder, inner = _embedder(dead)
        started = time.monotonic()
        for _ in range(10):
            await embedder.embed_query("q")
        elapsed = time.monotonic() - started
    finally:
        await dead.aclose()
    assert len(inner.query_calls) == 10  # every call got its vector
    assert elapsed < 5  # the breaker stops Redis from slowing every request
