"""JSON over HTTP to the model servers, with timeouts, retries and typed errors (rules 5 and 8).

Used by the embedding, reranker and LLM providers. Error messages are generic: the response
body is never copied into an error or a log, because it may echo the request text.
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

from app.core.errors import (
    NonRetryableError,
    UpstreamOverloadedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.retry import RetryPolicy, with_retries

_OVERLOADED = frozenset({429, 503})


def is_retryable(error: Exception) -> bool:
    """Timeouts, connection problems, overload and 5xx are worth another try."""
    return isinstance(error, UpstreamUnavailableError | UpstreamTimeoutError)


class JsonHttpClient:
    """Posts JSON and returns the parsed answer."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._retry = retry
        self._sleep = sleep

    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        timeout_s: float,
        headers: Mapping[str, str] | None = None,
        on_overload: Callable[[], None] | None = None,
        on_success: Callable[[], None] | None = None,
    ) -> Any:
        """POST with retries. ``on_overload`` runs on every 429 or 503 answer."""

        async def attempt() -> Any:
            try:
                response = await self._client.post(
                    url, json=payload, headers=headers, timeout=timeout_s
                )
            except httpx.TimeoutException as exc:
                raise UpstreamTimeoutError("Model server timeout") from exc
            except httpx.HTTPError as exc:
                raise UpstreamUnavailableError("Model server unavailable") from exc
            status = response.status_code
            if status in _OVERLOADED:
                if on_overload is not None:
                    on_overload()
                raise UpstreamOverloadedError
            if status >= 500:
                raise UpstreamUnavailableError("Model server error")
            if status >= 400:
                raise NonRetryableError(f"Model server rejected the request ({status})")
            try:
                data = response.json()
            except ValueError as exc:
                raise NonRetryableError("Model server sent an invalid answer") from exc
            if on_success is not None:
                on_success()
            return data

        return await with_retries(
            attempt, policy=self._retry, retry_if=is_retryable, sleep=self._sleep
        )
