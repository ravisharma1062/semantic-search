import asyncio

import pytest

from app.core.retry import RetryPolicy, with_retries


class _Flaky:
    def __init__(self, failures: int, error: Exception) -> None:
        self.failures = failures
        self.error = error
        self.calls = 0

    async def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return "ok"


async def _no_sleep(_seconds: float) -> None:
    return None


async def test_succeeds_after_retries() -> None:
    operation = _Flaky(2, TimeoutError())
    result = await with_retries(
        operation,
        policy=RetryPolicy(attempts=3),
        retry_if=lambda e: isinstance(e, TimeoutError),
        sleep=_no_sleep,
    )
    assert result == "ok"
    assert operation.calls == 3


async def test_operation_may_be_a_lambda_returning_an_awaitable() -> None:
    operation = _Flaky(1, TimeoutError())
    result = await with_retries(
        lambda: operation(),
        policy=RetryPolicy(attempts=3),
        retry_if=lambda e: True,
        sleep=_no_sleep,
    )
    assert result == "ok"
    assert operation.calls == 2


async def test_gives_up_and_raises_the_last_error() -> None:
    operation = _Flaky(10, TimeoutError("last"))
    with pytest.raises(TimeoutError, match="last"):
        await with_retries(
            operation,
            policy=RetryPolicy(attempts=3),
            retry_if=lambda e: True,
            sleep=_no_sleep,
        )
    assert operation.calls == 3


async def test_error_that_is_not_safe_to_retry_is_raised_at_once() -> None:
    operation = _Flaky(10, ValueError())
    with pytest.raises(ValueError):
        await with_retries(
            operation,
            policy=RetryPolicy(attempts=5),
            retry_if=lambda e: isinstance(e, TimeoutError),
            sleep=_no_sleep,
        )
    assert operation.calls == 1


async def test_cancellation_is_never_retried() -> None:
    calls = 0

    async def cancelled() -> None:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await with_retries(
            cancelled, policy=RetryPolicy(attempts=5), retry_if=lambda e: True, sleep=_no_sleep
        )
    assert calls == 1


async def test_waits_grow_and_stay_within_the_maximum() -> None:
    waits: list[float] = []

    async def record(seconds: float) -> None:
        waits.append(seconds)

    with pytest.raises(TimeoutError):
        await with_retries(
            _Flaky(10, TimeoutError()),
            policy=RetryPolicy(attempts=5, initial_delay_s=1, max_delay_s=3, jitter_s=0),
            retry_if=lambda e: True,
            sleep=record,
        )
    assert waits == [1, 2, 3, 3]


async def test_one_attempt_means_no_retry() -> None:
    operation = _Flaky(1, TimeoutError())
    with pytest.raises(TimeoutError):
        await with_retries(
            operation, policy=RetryPolicy(attempts=1), retry_if=lambda e: True, sleep=_no_sleep
        )
    assert operation.calls == 1
