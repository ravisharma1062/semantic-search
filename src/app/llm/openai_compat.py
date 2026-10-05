"""LLM client for servers that speak the OpenAI chat completions API.

The in-house LLM server (for example vLLM) and OpenAI use the same request and answer format, so one
class serves both. Only the URL and the headers differ. Plain HTTP, no SDK (rule 5).

Neither the request nor the answer text is ever logged or copied into an error: error messages are
fixed text (rule 2).
"""

import json
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any

import httpx

from app.core.errors import (
    NonRetryableError,
    UpstreamOverloadedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.http import JsonHttpClient
from app.core.limiter import AdaptiveLimiter
from app.core.settings import LlmSettings
from app.llm.base import ChatMessage, Completion, LLMOptions, Usage, estimate_tokens

_OVERLOADED = frozenset({429, 503})
_BAD_ANSWER = "LLM sent an unexpected answer"


class OpenAICompatibleLLM:
    """``LLMClient`` for an OpenAI-style chat endpoint."""

    def __init__(
        self,
        settings: LlmSettings,
        http: JsonHttpClient,
        client: httpx.AsyncClient,
        *,
        url: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._cfg = settings
        self._http = http
        self._client = client
        self._url = url
        self._headers = headers or {}
        self.model_name = f"{settings.model}@{settings.model_version}"
        self._limiter = AdaptiveLimiter(settings.max_concurrency)

    def _payload(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None, *, stream: bool
    ) -> dict[str, Any]:
        opts = options or LLMOptions()
        payload: dict[str, Any] = {
            "model": self._cfg.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": opts.max_output_tokens or self._cfg.max_output_tokens,
            "temperature": self._cfg.temperature if opts.temperature is None else opts.temperature,
            "stream": stream,
        }
        if stream:
            payload["stream_options"] = {"include_usage": True}
        return payload

    async def complete(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> Completion:
        """The full answer with token usage."""
        async with self._limiter:
            raw = await self._http.post_json(
                self._url,
                self._payload(messages, options, stream=False),
                timeout_s=self._cfg.timeout_s,
                headers=self._headers,
                on_overload=self._limiter.on_overload,
                on_success=self._limiter.on_success,
            )
        try:
            text = raw["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise NonRetryableError(_BAD_ANSWER) from exc
        if not isinstance(text, str):
            raise NonRetryableError(_BAD_ANSWER)
        return Completion(text=text, usage=_usage(raw.get("usage"), messages, text))

    async def generate(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> str:
        """The full answer."""
        return (await self.complete(messages, options)).text

    async def stream(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> AsyncIterator[str]:
        """The answer in pieces. A stream is not retried: the caller may already have text."""
        payload = self._payload(messages, options, stream=True)
        timeout = httpx.Timeout(self._cfg.timeout_s, connect=self._cfg.first_token_timeout_s)
        async with self._limiter:
            try:
                async with self._client.stream(
                    "POST", self._url, json=payload, headers=self._headers, timeout=timeout
                ) as response:
                    _check_status(response.status_code, self._limiter.on_overload)
                    async for line in response.aiter_lines():
                        piece = _piece(line)
                        if piece:
                            yield piece
            except httpx.TimeoutException as exc:
                raise UpstreamTimeoutError("LLM timeout") from exc
            except httpx.HTTPError as exc:
                raise UpstreamUnavailableError("LLM unavailable") from exc
            self._limiter.on_success()


def _check_status(status: int, on_overload: Callable[[], None]) -> None:
    if status in _OVERLOADED:
        on_overload()
        raise UpstreamOverloadedError
    if status >= 500:
        raise UpstreamUnavailableError("LLM server error")
    if status >= 400:
        raise NonRetryableError(f"LLM rejected the request ({status})")


def _piece(line: str) -> str:
    """The text of one server-sent event line, or an empty string."""
    if not line.startswith("data:"):
        return ""
    data = line[5:].strip()
    if not data or data == "[DONE]":
        return ""
    try:
        choices = json.loads(data).get("choices") or []
        if not choices:
            return ""
        content = choices[0].get("delta", {}).get("content")
    except (ValueError, AttributeError, TypeError) as exc:
        raise NonRetryableError(_BAD_ANSWER) from exc
    return content if isinstance(content, str) else ""


def _usage(raw: Any, messages: Sequence[ChatMessage], text: str) -> Usage:
    try:
        used_in, used_out = int(raw["prompt_tokens"]), int(raw["completion_tokens"])
        if used_in >= 0 and used_out >= 0:
            return Usage(input_tokens=used_in, output_tokens=used_out)
    except (KeyError, TypeError, ValueError):
        pass
    return Usage(
        input_tokens=sum(estimate_tokens(m.content) for m in messages),
        output_tokens=estimate_tokens(text),
    )
