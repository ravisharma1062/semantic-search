"""Rate limits per user and per service (HLD section 9, "Rate limits").

A fixed window per key, kept in the memory of the pod. It limits abuse and cost. It is not exact
across pods (the real limit is the setting times the number of pods), and it is lost on restart,
which is fine: it holds no business data. A limit of 0 switches it off.
"""

import time
from collections.abc import Callable

from app.core.errors import RateLimitedError
from app.observability.metrics import get_metrics


class RateLimiter:
    """Counts requests per key in one-minute windows."""

    def __init__(
        self,
        user_per_min: int,
        service_per_min: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_keys: int = 100_000,
    ) -> None:
        self._limits = {"user": user_per_min, "service": service_per_min}
        self._clock = clock
        self._max_keys = max_keys
        self._windows: dict[tuple[str, str], tuple[float, int]] = {}

    def check(self, kind: str, key: str) -> None:
        """Count one request. Raises ``RateLimitedError`` (with Retry-After) over the limit."""
        limit = self._limits[kind]
        if limit <= 0:
            return
        now = self._clock()
        start, count = self._windows.get((kind, key), (now, 0))
        if now - start >= 60:
            start, count = now, 0
        if count >= limit:
            get_metrics().rate_limited.labels(kind).inc()
            raise RateLimitedError(retry_after_s=max(1, int(60 - (now - start)) + 1))
        if len(self._windows) >= self._max_keys:
            self._drop_old(now)
        self._windows[(kind, key)] = (start, count + 1)

    def _drop_old(self, now: float) -> None:
        self._windows = {k: v for k, v in self._windows.items() if now - v[0] < 60}
