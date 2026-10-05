"""The shared retry helper (rule 8): exponential backoff with jitter, safe calls only.

Callers say which errors are safe to retry. Everything else, and cancellation, is raised at once.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

T = TypeVar("T")


class RetryPolicy(BaseModel):
    """How often and how long to wait between attempts."""

    attempts: int = Field(3, ge=1)
    initial_delay_s: float = Field(0.2, ge=0)
    max_delay_s: float = Field(5.0, ge=0)
    jitter_s: float = Field(0.2, ge=0)


async def with_retries(
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    retry_if: Callable[[Exception], bool],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run ``operation`` up to ``policy.attempts`` times. The last error is re-raised as it is."""
    retrying = AsyncRetrying(
        stop=stop_after_attempt(policy.attempts),
        wait=wait_exponential_jitter(
            initial=policy.initial_delay_s, max=policy.max_delay_s, jitter=policy.jitter_s
        ),
        retry=retry_if_exception(lambda error: isinstance(error, Exception) and retry_if(error)),
        reraise=True,
        sleep=sleep,
    )

    async def attempt() -> T:
        # tenacity awaits only coroutine functions, not lambdas that return an awaitable.
        return await operation()

    return await retrying(attempt)
