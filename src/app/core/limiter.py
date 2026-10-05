"""A concurrency limit that lowers itself when the upstream is overloaded.

HLD section 4: on overload, retry with backoff and *lower parallelism*. The limit is halved on
every overload answer and grows back by one after a number of successes.
"""

import asyncio
from types import TracebackType


class AdaptiveLimiter:
    """Use as ``async with limiter:``. Call ``on_overload`` and ``on_success`` from outcomes."""

    def __init__(self, maximum: int, recover_after: int = 8) -> None:
        self._maximum = maximum
        self._limit = maximum
        self._recover_after = recover_after
        self._active = 0
        self._successes = 0
        self._changed = asyncio.Condition()

    @property
    def limit(self) -> int:
        """How many calls may run at the same time right now."""
        return self._limit

    @property
    def active(self) -> int:
        """How many calls run right now."""
        return self._active

    async def __aenter__(self) -> None:
        async with self._changed:
            await self._changed.wait_for(lambda: self._active < self._limit)
            self._active += 1

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        async with self._changed:
            self._active -= 1
            self._changed.notify_all()

    def on_overload(self) -> None:
        """Halve the limit, but keep at least one call."""
        self._limit = max(1, self._limit // 2)
        self._successes = 0

    def on_success(self) -> None:
        """After enough successes, allow one more call again, up to the maximum.

        Waiting callers see the new limit when the next running call ends.
        """
        self._successes += 1
        if self._successes >= self._recover_after and self._limit < self._maximum:
            self._limit += 1
            self._successes = 0
