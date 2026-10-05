"""A circuit breaker for calls to model servers and caches (HLD section 11, "Reliability patterns").

After ``failures`` errors in a row the breaker opens and calls are refused at once for
``cooldown_s`` seconds, so a dead server does not eat the time budget of every request. After the
cooldown one trial call goes through: if it works the breaker closes, if it fails the breaker opens
again.
"""

import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from app.core.errors import AppError, UpstreamOverloadedError
from app.observability.metrics import get_metrics

T = TypeVar("T")


class BreakerOpenError(UpstreamOverloadedError):
    """The call was not made because the breaker is open."""

    default_message = "Circuit breaker is open"


class CircuitBreaker:
    """Counts failures of one dependency."""

    def __init__(
        self,
        failures: int,
        cooldown_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        name: str = "",
    ) -> None:
        self._name = name
        self._threshold = failures
        self._cooldown_s = cooldown_s
        self._clock = clock
        self._failures = 0
        self._open_until = 0.0

    @property
    def is_open(self) -> bool:
        """True while calls are refused."""
        if self._open_until and self._clock() >= self._open_until:
            self._open_until = 0.0
            self._failures = self._threshold - 1  # half open: the next failure opens it again
            self._publish()
        return self._open_until > 0.0

    def record_success(self) -> None:
        """The dependency works. A breaker that was open is closed."""
        self._failures = 0
        self._open_until = 0.0
        self._publish()

    def record_failure(self) -> None:
        """The dependency failed."""
        self._failures += 1
        if self._failures >= self._threshold:
            self._open_until = self._clock() + self._cooldown_s
        self._publish()

    def _publish(self) -> None:
        """Show the state in the ``circuit_breaker_open`` metric (breakers with a name only)."""
        if self._name:
            get_metrics().breaker_open.labels(self._name).set(1 if self._open_until > 0 else 0)

    async def call(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run ``operation`` unless the breaker is open. Our own errors and timeouts count as
        failures."""
        if self.is_open:
            raise BreakerOpenError
        try:
            result = await operation()
        except (AppError, TimeoutError):
            self.record_failure()
            raise
        self.record_success()
        return result
