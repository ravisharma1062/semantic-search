"""A rate limit for the backfill, so it never competes with live traffic (HLD section 4)."""

import asyncio
import time
from collections.abc import Awaitable, Callable


class RateLimiter:
    """Token bucket. ``acquire(n)`` waits until ``n`` more items may be sent.

    The bucket may go into debt for a request larger than its capacity, so a page of any size
    is allowed and the average rate still holds.
    """

    def __init__(
        self,
        rate_per_s: float,
        *,
        burst: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate_per_s <= 0:
            raise ValueError("rate must be positive")
        self._rate = rate_per_s
        self._capacity = max(burst if burst is not None else rate_per_s, 1.0)
        self._tokens = self._capacity
        self._clock = clock
        self._sleep = sleep
        self._last = clock()

    async def acquire(self, count: int) -> None:
        """Take ``count`` tokens and wait for the ones that are not there yet."""
        now = self._clock()
        self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
        self._last = now
        self._tokens -= count
        if self._tokens < 0:
            await self._sleep(-self._tokens / self._rate)
