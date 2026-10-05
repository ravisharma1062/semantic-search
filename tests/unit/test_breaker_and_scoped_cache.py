import json

import pytest
import structlog

from app.core.breaker import BreakerOpenError, CircuitBreaker
from app.core.errors import UpstreamUnavailableError
from app.core.security import Identity
from app.retrieval.acl import AclFilter
from app.retrieval.cache import ScopedCache
from tests.fakes.redis import FakeRedis


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


async def _fail() -> None:
    raise UpstreamUnavailableError()


async def _ok() -> str:
    return "ok"


# --- circuit breaker ------------------------------------------------------------------------


async def test_it_opens_after_the_failures_and_refuses_calls() -> None:
    clock = _Clock()
    breaker = CircuitBreaker(3, 10, clock=clock)
    for _ in range(3):
        with pytest.raises(UpstreamUnavailableError):
            await breaker.call(_fail)
    assert breaker.is_open
    with pytest.raises(BreakerOpenError):
        await breaker.call(_ok)


async def test_a_success_in_between_resets_the_count() -> None:
    breaker = CircuitBreaker(3, 10, clock=_Clock())
    for _ in range(2):
        with pytest.raises(UpstreamUnavailableError):
            await breaker.call(_fail)
    await breaker.call(_ok)
    for _ in range(2):
        with pytest.raises(UpstreamUnavailableError):
            await breaker.call(_fail)
    assert not breaker.is_open


async def test_after_the_cooldown_one_trial_decides() -> None:
    clock = _Clock()
    breaker = CircuitBreaker(2, 10, clock=clock)
    for _ in range(2):
        with pytest.raises(UpstreamUnavailableError):
            await breaker.call(_fail)
    clock.now = 11
    with pytest.raises(UpstreamUnavailableError):  # the trial fails: open again at once
        await breaker.call(_fail)
    assert breaker.is_open
    clock.now = 22
    assert await breaker.call(_ok) == "ok"  # the trial works: closed
    assert not breaker.is_open
    await breaker.call(_ok)


async def test_timeouts_count_and_other_errors_pass_through() -> None:
    breaker = CircuitBreaker(1, 10, clock=_Clock())

    async def slow() -> None:
        raise TimeoutError

    with pytest.raises(TimeoutError):
        await breaker.call(slow)
    assert breaker.is_open

    other = CircuitBreaker(1, 10, clock=_Clock())

    async def bug() -> None:
        raise ValueError

    with pytest.raises(ValueError):
        await other.call(bug)
    assert not other.is_open  # a programming error is not a dependency failure


# --- scoped cache ---------------------------------------------------------------------------


def _scope(user: str, *groups: str) -> AclFilter:
    return AclFilter.from_identity(Identity(user, groups, "svc"))


def _cache(redis: FakeRedis, clock: _Clock | None = None, failures: int = 3) -> ScopedCache:
    return ScopedCache(
        redis,
        namespace="answer",
        ttl_s=600,
        timeout_s=0.05,
        breaker=CircuitBreaker(failures, 10, clock=clock or _Clock()),
    )


async def test_a_value_is_served_to_the_same_scope() -> None:
    cache = _cache(FakeRedis())
    await cache.set(_scope("alice", "g1"), "the answer", "what is the penalty", "v1")
    assert await cache.get(_scope("alice", "g1"), "what is the penalty", "v1") == "the answer"


async def test_a_value_is_never_served_to_another_scope() -> None:
    cache = _cache(FakeRedis())
    await cache.set(_scope("alice", "g1"), "alice's answer", "same question")
    for other in (
        _scope("bob", "g1"),  # another user, same group
        _scope("alice"),  # same user, fewer groups
        _scope("alice", "g1", "g2"),  # same user, more groups
        _scope("alice", "g2"),
    ):
        assert await cache.get(other, "same question") is None


async def test_the_order_of_groups_does_not_matter() -> None:
    cache = _cache(FakeRedis())
    await cache.set(_scope("alice", "a", "b"), "x", "q")
    assert await cache.get(_scope("alice", "b", "a"), "q") == "x"


async def test_other_parts_other_entries() -> None:
    cache = _cache(FakeRedis())
    await cache.set(_scope("alice"), "x", "q", "prompt-v1", "model-1")
    assert await cache.get(_scope("alice"), "q", "prompt-v2", "model-1") is None
    assert await cache.get(_scope("alice"), "q", "prompt-v1", "model-2") is None


def test_the_key_holds_hashes_and_never_the_text_or_the_user() -> None:
    cache = _cache(FakeRedis())
    key = cache.key(_scope("alice", "g-legal"), "a very private question")
    assert key.startswith("answer:")
    for secret in ("private", "alice", "g-legal"):
        assert secret not in key


async def test_the_entry_expires() -> None:
    redis = FakeRedis()
    await _cache(redis).set(_scope("alice"), "x", "q")
    assert list(redis.ttls.values()) == [600]


async def test_values_stored_as_bytes_by_the_real_redis_are_decoded() -> None:
    redis = FakeRedis()
    cache = _cache(redis)
    redis.data[cache.key(_scope("alice"), "q")] = json.dumps({"a": 1}).encode()
    assert await cache.get(_scope("alice"), "q") == '{"a": 1}'


@pytest.mark.parametrize("error", [ConnectionError("down"), TimeoutError(), RuntimeError("boom")])
async def test_a_redis_error_is_a_miss_and_never_an_error(error: Exception) -> None:
    redis = FakeRedis()
    redis.fail_with = error
    cache = _cache(redis)
    assert await cache.get(_scope("alice"), "q") is None
    await cache.set(_scope("alice"), "x", "q")  # does not raise


async def test_a_slow_redis_is_cut_off() -> None:
    redis = FakeRedis()
    redis.delay_s = 0.5
    assert await _cache(redis).get(_scope("alice"), "q") is None


async def test_after_repeated_failures_redis_is_skipped_for_a_while() -> None:
    clock = _Clock()
    redis = FakeRedis()
    redis.fail_with = ConnectionError("down")
    cache = _cache(redis, clock, failures=2)
    for _ in range(2):
        await cache.get(_scope("alice"), "q")
    calls = redis.get_calls
    for _ in range(5):
        await cache.get(_scope("alice"), "q")
    assert redis.get_calls == calls
    clock.now = 11
    redis.fail_with = None
    await cache.get(_scope("alice"), "q")
    assert redis.get_calls == calls + 1


async def test_the_question_is_never_logged() -> None:
    redis = FakeRedis()
    redis.fail_with = ConnectionError("down")
    with structlog.testing.capture_logs() as logs:
        await _cache(redis).get(_scope("alice"), "synthetic secret question")
    assert "synthetic secret question" not in str(logs)
