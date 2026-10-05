"""An LLM client with a circuit breaker and time limits (HLD section 11).

When the model server is down the breaker opens after a few failures, and answers fail at once with
``UPSTREAM_UNAVAILABLE`` so the Java app can fall back to keyword search.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence

from app.core.breaker import BreakerOpenError, CircuitBreaker
from app.core.errors import AppError
from app.llm.base import ChatMessage, Completion, LLMClient, LLMOptions


class GuardedLLM:
    """Wraps an ``LLMClient`` with a breaker and a hard time limit."""

    def __init__(
        self, inner: LLMClient, breaker: CircuitBreaker, timeout_s: float, first_token_s: float
    ) -> None:
        self._inner = inner
        self._breaker = breaker
        self._timeout_s = timeout_s
        self._first_token_s = first_token_s
        self.model_name = inner.model_name

    async def complete(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> Completion:
        """The answer with usage, or a quick error if the server is slow or the breaker is open."""
        return await self._breaker.call(
            lambda: asyncio.wait_for(self._inner.complete(messages, options), self._timeout_s)
        )

    async def generate(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> str:
        """The answer text."""
        return (await self.complete(messages, options)).text

    async def stream(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> AsyncIterator[str]:
        """Pieces of the answer. The first piece must come within ``first_token_s`` and the whole
        answer within ``timeout_s``. A broken stream counts against the breaker."""
        if self._breaker.is_open:
            raise BreakerOpenError
        loop = asyncio.get_running_loop()
        iterator = self._inner.stream(messages, options).__aiter__()
        deadline = loop.time() + self._timeout_s
        first = True
        try:
            while True:
                limit = self._first_token_s if first else deadline - loop.time()
                try:
                    piece = await asyncio.wait_for(iterator.__anext__(), max(limit, 0.001))
                except StopAsyncIteration:
                    break
                first = False
                yield piece
        except (AppError, TimeoutError):
            self._breaker.record_failure()
            raise
        else:
            self._breaker.record_success()
        finally:
            aclose = getattr(iterator, "aclose", None)
            if aclose is not None:
                await aclose()
