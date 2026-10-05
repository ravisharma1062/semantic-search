import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from app.core.breaker import BreakerOpenError, CircuitBreaker
from app.core.errors import (
    NonRetryableError,
    UpstreamOverloadedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import LlmSettings
from app.llm.base import ChatMessage, LLMClient, LLMOptions
from app.llm.factory import create_llm
from app.llm.guarded import GuardedLLM
from app.llm.openai_compat import OpenAICompatibleLLM
from tests.fakes import FakeLLMClient

URL = "http://llm.test/v1/chat/completions"
MESSAGES = [ChatMessage(role="system", content="rules"), ChatMessage(role="user", content="q")]


def _cfg(**changes: object) -> LlmSettings:
    values: dict[str, object] = {"model": "inhouse-llm", "endpoint": "http://llm.test"}
    return LlmSettings(**{**values, **changes})


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as c:
        yield c


def _llm(client: httpx.AsyncClient, cfg: LlmSettings | None = None) -> OpenAICompatibleLLM:
    http = JsonHttpClient(
        client, RetryPolicy(attempts=2, initial_delay_s=0, max_delay_s=0, jitter_s=0)
    )
    return OpenAICompatibleLLM(cfg or _cfg(), http, client, url=URL)


def _answer(text: str, usage: object = None) -> dict[str, object]:
    body: dict[str, object] = {"choices": [{"message": {"role": "assistant", "content": text}}]}
    if usage is not None:
        body["usage"] = usage
    return body


# --- complete -------------------------------------------------------------------------------


async def test_request_shape_and_usage(client: httpx.AsyncClient) -> None:
    llm = _llm(client, _cfg(temperature=0.0, max_output_tokens=300))
    assert isinstance(llm, LLMClient)
    with respx.mock() as router:
        route = router.post(URL).respond(
            json=_answer("Yes [1]", {"prompt_tokens": 120, "completion_tokens": 4})
        )
        result = await llm.complete(MESSAGES)
    sent = json.loads(route.calls[0].request.content)
    assert sent["model"] == "inhouse-llm"
    assert sent["messages"] == [
        {"role": "system", "content": "rules"},
        {"role": "user", "content": "q"},
    ]
    assert sent["max_tokens"] == 300 and sent["temperature"] == 0.0 and sent["stream"] is False
    assert result.text == "Yes [1]"
    assert (result.usage.input_tokens, result.usage.output_tokens) == (120, 4)
    assert llm.model_name == "inhouse-llm@1"


async def test_options_override_the_settings(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        route = router.post(URL).respond(json=_answer("x"))
        await _llm(client).generate(MESSAGES, LLMOptions(max_output_tokens=10, temperature=0.5))
    sent = json.loads(route.calls[0].request.content)
    assert sent["max_tokens"] == 10 and sent["temperature"] == 0.5


async def test_usage_is_estimated_when_the_server_sends_none(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        router.post(URL).respond(json=_answer("a" * 40))
        result = await _llm(client).complete(MESSAGES)
    assert result.usage.output_tokens == 10 and result.usage.input_tokens >= 1


@pytest.mark.parametrize("body", [{}, {"choices": []}, {"choices": [{"message": {}}]}, [1]])
async def test_odd_answers_are_rejected(client: httpx.AsyncClient, body: object) -> None:
    with respx.mock() as router:
        router.post(URL).respond(json=body)
        with pytest.raises(NonRetryableError):
            await _llm(client).complete(MESSAGES)


async def test_failures_are_typed_and_never_copy_the_body(client: httpx.AsyncClient) -> None:
    llm = _llm(client)
    with respx.mock() as router:
        router.post(URL).respond(500, json={"echo": "synthetic secret question"})
        with pytest.raises(UpstreamUnavailableError) as error:
            await llm.complete(MESSAGES)
    assert "synthetic secret question" not in str(error.value)
    with respx.mock() as router:
        router.post(URL).respond(429)
        with pytest.raises(UpstreamOverloadedError):
            await llm.complete(MESSAGES)
    with respx.mock() as router:
        router.post(URL).mock(side_effect=httpx.ReadTimeout("slow"))
        with pytest.raises(UpstreamTimeoutError):
            await llm.complete(MESSAGES)
    with respx.mock() as router:
        router.post(URL).respond(400, json={"error": "synthetic secret question"})
        with pytest.raises(NonRetryableError) as bad:
            await llm.complete(MESSAGES)
    assert "synthetic secret question" not in str(bad.value)


# --- stream ---------------------------------------------------------------------------------


def _sse(*pieces: str) -> str:
    lines = [
        "data: " + json.dumps({"choices": [{"delta": {"content": piece}}]}) for piece in pieces
    ]
    lines.append("data: " + json.dumps({"choices": [], "usage": {"prompt_tokens": 1}}))
    lines.append("data: [DONE]")
    return "\n\n".join(lines) + "\n\n"


async def test_the_stream_yields_the_pieces(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        route = router.post(URL).respond(
            content=_sse("The ", "answer ", "[1]."),
            headers={"content-type": "text/event-stream"},
        )
        pieces = [p async for p in _llm(client).stream(MESSAGES)]
    assert pieces == ["The ", "answer ", "[1]."]
    assert json.loads(route.calls[0].request.content)["stream"] is True


async def test_the_stream_ignores_role_only_and_empty_events(client: httpx.AsyncClient) -> None:
    body = (
        'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
        'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
        ": keep-alive\n\ndata: [DONE]\n\n"
    )
    with respx.mock() as router:
        router.post(URL).respond(content=body)
        assert [p async for p in _llm(client).stream(MESSAGES)] == ["hi"]


@pytest.mark.parametrize(
    "status,error",
    [(500, UpstreamUnavailableError), (429, UpstreamOverloadedError), (400, NonRetryableError)],
)
async def test_stream_status_errors_are_typed(
    client: httpx.AsyncClient, status: int, error: type[Exception]
) -> None:
    with respx.mock() as router:
        router.post(URL).respond(status)
        with pytest.raises(error):
            [p async for p in _llm(client).stream(MESSAGES)]


async def test_stream_timeouts_and_broken_events(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        router.post(URL).mock(side_effect=httpx.ConnectTimeout("slow"))
        with pytest.raises(UpstreamTimeoutError):
            [p async for p in _llm(client).stream(MESSAGES)]
    with respx.mock() as router:
        router.post(URL).mock(side_effect=httpx.ConnectError("down"))
        with pytest.raises(UpstreamUnavailableError):
            [p async for p in _llm(client).stream(MESSAGES)]
    with respx.mock() as router:
        router.post(URL).respond(content="data: {not json\n\n")
        with pytest.raises(NonRetryableError):
            [p async for p in _llm(client).stream(MESSAGES)]


# --- guarded --------------------------------------------------------------------------------


async def test_a_slow_answer_is_cut_off_and_opens_the_breaker() -> None:
    fake = FakeLLMClient(["x"])
    original = fake.complete

    async def slow(*args: object, **kwargs: object) -> object:
        await asyncio.sleep(1)
        return await original(*args, **kwargs)  # type: ignore[arg-type]

    fake.complete = slow  # type: ignore[method-assign,assignment]
    guarded = GuardedLLM(fake, CircuitBreaker(2, 10), timeout_s=0.02, first_token_s=0.02)
    for _ in range(2):
        with pytest.raises(TimeoutError):
            await guarded.complete(MESSAGES)
    with pytest.raises(BreakerOpenError):
        await guarded.complete(MESSAGES)
    with pytest.raises(BreakerOpenError):
        [p async for p in guarded.stream(MESSAGES)]


async def test_the_guarded_stream_passes_pieces_and_counts_a_broken_stream() -> None:
    fake = FakeLLMClient(["one two three four"])
    breaker = CircuitBreaker(2, 10)
    guarded = GuardedLLM(fake, breaker, timeout_s=1, first_token_s=1)
    assert "".join([p async for p in guarded.stream(MESSAGES)]) == "one two three four"
    fake.fail_stream_after = 1
    for _ in range(2):
        with pytest.raises(UpstreamUnavailableError):
            [p async for p in guarded.stream(MESSAGES)]
    assert breaker.is_open


async def test_a_stream_that_never_starts_times_out() -> None:
    class Hanging:
        model_name = "hang"

        async def stream(self, messages: object, options: object = None) -> AsyncIterator[str]:
            await asyncio.sleep(5)
            yield "never"

    guarded = GuardedLLM(Hanging(), CircuitBreaker(3, 10), timeout_s=1, first_token_s=0.02)  # type: ignore[arg-type]
    with pytest.raises(TimeoutError):
        [p async for p in guarded.stream(MESSAGES)]


# --- factory --------------------------------------------------------------------------------


async def test_the_factory_builds_a_guarded_inhouse_client(client: httpx.AsyncClient) -> None:
    llm = create_llm(_cfg(), client, RetryPolicy(attempts=1))
    assert isinstance(llm, GuardedLLM)
    assert llm.model_name == "inhouse-llm@1"
    with respx.mock() as router:
        route = router.post(URL).respond(json=_answer("ok"))
        assert await llm.generate(MESSAGES) == "ok"
    assert "authorization" not in route.calls[0].request.headers


async def test_openai_is_off_unless_allowed_and_has_a_key(client: httpx.AsyncClient) -> None:
    retry = RetryPolicy(attempts=1)
    with pytest.raises(NonRetryableError):
        create_llm(_cfg(provider="openai"), client, retry)
    with pytest.raises(NonRetryableError):
        create_llm(_cfg(provider="openai", allow_external=True), client, retry)
    cfg = _cfg(
        provider="openai",
        allow_external=True,
        openai_api_key="sk-synthetic",
        openai_base_url="https://api.openai.test/v1",
    )
    llm = create_llm(cfg, client, retry)
    with respx.mock() as router:
        route = router.post("https://api.openai.test/v1/chat/completions").respond(
            json=_answer("ok")
        )
        await llm.generate(MESSAGES)
    assert route.calls[0].request.headers["authorization"] == "Bearer sk-synthetic"
