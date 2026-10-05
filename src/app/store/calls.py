"""Runs an Elasticsearch call with retries and our typed errors (rules 5 and 8)."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from elasticsearch import ApiError, ConflictError
from elasticsearch import TransportError as ClientTransportError

from app.core.retry import RetryPolicy, with_retries
from app.store.client import is_retryable, map_es_error

T = TypeVar("T")


async def guarded(
    operation: Callable[[], Awaitable[T]],
    retry: RetryPolicy,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run ``operation``. Safe to repeat calls only: timeouts, connection loss, 429 and 5xx retry.

    A version conflict (409) is raised as it is, for callers that handle it.
    """

    async def attempt() -> T:
        try:
            return await operation()
        except ConflictError:
            raise
        except (ApiError, ClientTransportError) as exc:
            raise map_es_error(exc) from exc

    return await with_retries(attempt, policy=retry, retry_if=is_retryable, sleep=sleep)
