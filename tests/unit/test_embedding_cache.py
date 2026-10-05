import struct

import pytest
import structlog

from app.embeddings.cache import CachedEmbedder, QueryEmbeddingCache
from tests.fakes import FakeEmbedder
from tests.fakes.redis import FakeRedis

DIMS = 8


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _cache(
    redis: FakeRedis,
    *,
    model: str = "bge-m3@1",
    clock: _Clock | None = None,
    failures: int = 3,
    timeout_s: float = 0.05,
) -> QueryEmbeddingCache:
    return QueryEmbeddingCache(
        redis,
        model_name=model,
        dims=DIMS,
        ttl_s=600,
        timeout_s=timeout_s,
        breaker_failures=failures,
        breaker_cooldown_s=10,
        clock=clock or _Clock(),
    )


def _setup(
    redis: FakeRedis | None = None, **cache_options: object
) -> tuple[CachedEmbedder, FakeEmbedder, FakeRedis]:
    redis = redis or FakeRedis()
    inner = FakeEmbedder(dims=DIMS, model_name="bge-m3@1")
    return CachedEmbedder(inner, _cache(redis, **cache_options)), inner, redis  # type: ignore[arg-type]


# --- hits and misses ------------------------------------------------------------------------


async def test_second_identical_query_comes_from_the_cache() -> None:
    embedder, inner, redis = _setup()
    first = await embedder.embed_query("what is the penalty")
    second = await embedder.embed_query("what is the penalty")
    assert inner.query_calls == ["what is the penalty"]  # the model was asked once
    assert second == pytest.approx(first, abs=1e-6)  # stored as float32
    assert redis.get_calls == 2


async def test_different_queries_are_cached_separately() -> None:
    embedder, inner, _ = _setup()
    await embedder.embed_query("one")
    await embedder.embed_query("two")
    assert inner.query_calls == ["one", "two"]


async def test_the_entry_gets_the_ttl_from_settings() -> None:
    embedder, _, redis = _setup()
    await embedder.embed_query("q")
    assert list(redis.ttls.values()) == [600]


async def test_documents_are_not_cached() -> None:
    embedder, inner, redis = _setup()
    await embedder.embed_documents(["a", "b"])
    await embedder.embed_documents(["a", "b"])
    assert len(inner.document_calls) == 2
    assert redis.set_calls == 0


async def test_the_wrapper_has_the_same_name_and_size() -> None:
    embedder, inner, _ = _setup()
    assert embedder.model_name == inner.model_name
    assert embedder.dims == DIMS


# --- keys -----------------------------------------------------------------------------------


def test_the_key_contains_the_model_version_and_a_hash_not_the_text() -> None:
    cache = _cache(FakeRedis(), model="bge-m3@1")
    key = cache.key("a very private question")
    assert "bge-m3@1" in key
    assert "private" not in key
    assert key == cache.key("a very private question")
    assert key != cache.key("another question")


async def test_a_new_model_version_does_not_see_old_vectors() -> None:
    redis = FakeRedis()
    old = _cache(redis, model="bge-m3@1")
    await old.set("q", [0.1] * DIMS)
    new = _cache(redis, model="bge-m3@2")
    assert await old.get("q") is not None
    assert await new.get("q") is None


# --- Redis problems never fail a request ----------------------------------------------------


@pytest.mark.parametrize("error", [ConnectionError("down"), TimeoutError(), RuntimeError("boom")])
async def test_a_redis_error_only_means_no_cache(error: Exception) -> None:
    embedder, inner, redis = _setup()
    redis.fail_with = error
    vector = await embedder.embed_query("q")
    assert vector == (await inner.embed_query("q"))
    assert inner.query_calls == ["q", "q"]


async def test_a_slow_redis_is_cut_off_by_the_timeout() -> None:
    embedder, inner, redis = _setup(timeout_s=0.02)
    redis.delay_s = 0.5
    await embedder.embed_query("q")
    assert inner.query_calls == ["q"]


async def test_a_damaged_cache_entry_is_a_miss() -> None:
    redis = FakeRedis()
    cache = _cache(redis)
    redis.data[cache.key("q")] = b"short"
    assert await cache.get("q") is None
    redis.data[cache.key("q")] = struct.pack("<4f", 1, 2, 3, 4)  # an older vector size
    assert await cache.get("q") is None


async def test_a_vector_of_the_wrong_size_is_not_stored() -> None:
    redis = FakeRedis()
    await _cache(redis).set("q", [0.1, 0.2])
    assert redis.set_calls == 0


# --- circuit breaker ------------------------------------------------------------------------


async def test_after_repeated_failures_redis_is_skipped_for_a_while() -> None:
    clock = _Clock()
    embedder, _, redis = _setup(clock=clock, failures=3)
    redis.fail_with = ConnectionError("down")
    for _ in range(3):
        await embedder.embed_query("q")
    calls_when_open = redis.get_calls + redis.set_calls
    for _ in range(5):
        await embedder.embed_query("q")  # Redis is not touched
    assert redis.get_calls + redis.set_calls == calls_when_open


async def test_redis_is_used_again_after_the_cooldown() -> None:
    clock = _Clock()
    embedder, _, redis = _setup(clock=clock, failures=2)
    redis.fail_with = ConnectionError("down")
    for _ in range(2):
        await embedder.embed_query("q")
    calls = redis.get_calls
    clock.now += 11  # cooldown is 10 seconds
    redis.fail_with = None
    await embedder.embed_query("q")
    assert redis.get_calls > calls


async def test_a_success_resets_the_failure_count() -> None:
    clock = _Clock()
    embedder, _, redis = _setup(clock=clock, failures=3)
    redis.fail_with = ConnectionError("down")
    await embedder.embed_query("a")
    await embedder.embed_query("b")  # 4 failures so far (get and set each time) -> already open?
    redis.fail_with = None
    clock.now += 11
    await embedder.embed_query("c")
    await embedder.embed_query("d")
    redis.fail_with = ConnectionError("down")
    await embedder.embed_query("e")  # one failure after successes: not yet open
    redis.fail_with = None
    before = redis.get_calls
    await embedder.embed_query("f")
    assert redis.get_calls > before


async def test_queries_are_never_logged() -> None:
    embedder, _, redis = _setup()
    redis.fail_with = ConnectionError("down")
    with structlog.testing.capture_logs() as logs:
        for _ in range(4):
            await embedder.embed_query("synthetic secret question")
    assert "synthetic secret question" not in str(logs)
