"""Small helpers that record a metric and open a span around a call to a dependency."""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from app.core.breaker import BreakerOpenError
from app.core.errors import AppError, UpstreamTimeoutError
from app.observability.metrics import get_metrics
from app.observability.tracing import span


def outcome_of(error: BaseException) -> str:
    """A short label for how a call ended."""
    if isinstance(error, BreakerOpenError):
        return "breaker_open"
    if isinstance(error, TimeoutError | UpstreamTimeoutError):
        return "timeout"
    if isinstance(error, asyncio.CancelledError | GeneratorExit):
        return "cancelled"
    if isinstance(error, AppError):
        return "error"
    return "failure"


@asynccontextmanager
async def observed(dependency: str, *, stage: str | None = None) -> AsyncIterator[None]:
    """Count and time a call to ``dependency`` (embedding, reranker, llm, elasticsearch...). With
    ``stage`` the time also goes to the search stage histogram."""
    metrics = get_metrics()
    started = time.perf_counter()
    outcome = "ok"
    with span(dependency, dependency=dependency):
        try:
            yield
        except BaseException as exc:
            outcome = outcome_of(exc)
            raise
        finally:
            elapsed = time.perf_counter() - started
            metrics.upstream_calls.labels(dependency, outcome).inc()
            metrics.upstream_duration.labels(dependency).observe(elapsed)
            if stage is not None:
                metrics.search_stage.labels(stage).observe(elapsed)
