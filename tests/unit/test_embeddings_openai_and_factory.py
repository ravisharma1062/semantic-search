import json
import math
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import respx
from pydantic import SecretStr

from app.core.errors import NonRetryableError, UpstreamOverloadedError
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import EmbeddingSettings, RedisSettings
from app.embeddings.cache import CachedEmbedder
from app.embeddings.factory import create_embedder, create_http_client
from app.embeddings.inhouse import InHouseEmbedder
from app.embeddings.openai import OpenAIEmbedder
from tests.fakes.redis import FakeRedis

URL = "https://api.openai.test/v1/embeddings"
DIMS = 4
_FAST = RetryPolicy(attempts=2, initial_delay_s=0, max_delay_s=0, jitter_s=0)


def _settings(**changes: Any) -> EmbeddingSettings:
    values: dict[str, Any] = {
        "provider": "openai",
        "model": "text-embedding-3-small",
        "endpoint": "unused",
        "dims": DIMS,
        "batch_size": 2,
        "allow_external": True,
        "openai_base_url": "https://api.openai.test/v1",
        "openai_api_key": SecretStr("test-key-not-real"),
    }
    return EmbeddingSettings(**{**values, **changes})


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as http:
        yield http


def _unit(value: float) -> list[float]:
    rest = math.sqrt(1 - value * value)
    return [value, rest, 0.0, 0.0]


def _answer_out_of_order(request: httpx.Request) -> httpx.Response:
    inputs = json.loads(request.content)["input"]
    data = [{"index": i, "embedding": _unit(0.1 * (i + 1))} for i in range(len(inputs))]
    return httpx.Response(200, json={"data": list(reversed(data))})


# --- the OpenAI provider --------------------------------------------------------------------


async def test_request_shape_and_authorization(client: httpx.AsyncClient) -> None:
    embedder = OpenAIEmbedder(_settings(), JsonHttpClient(client, _FAST))
    with respx.mock() as router:
        route = router.post(URL).mock(side_effect=_answer_out_of_order)
        await embedder.embed_documents(["a", "b"])
    request = route.calls[0].request
    assert json.loads(request.content) == {
        "model": "text-embedding-3-small",
        "input": ["a", "b"],
        "dimensions": DIMS,
    }
    assert request.headers["authorization"] == "Bearer test-key-not-real"


async def test_results_are_put_back_in_input_order(client: httpx.AsyncClient) -> None:
    embedder = OpenAIEmbedder(_settings(), JsonHttpClient(client, _FAST))
    with respx.mock() as router:
        router.post(URL).mock(side_effect=_answer_out_of_order)
        vectors = await embedder.embed_documents(["a", "b", "c"])
        query = await embedder.embed_query("q")
    assert [round(v[0], 2) for v in vectors] == [0.1, 0.2, 0.1]  # two batches: (a, b), (c)
    assert query == pytest.approx(_unit(0.1))


async def test_api_key_is_required() -> None:
    with pytest.raises(NonRetryableError):
        OpenAIEmbedder(_settings(openai_api_key=None), JsonHttpClient(httpx.AsyncClient(), _FAST))


async def test_bad_answers_are_rejected(client: httpx.AsyncClient) -> None:
    embedder = OpenAIEmbedder(_settings(), JsonHttpClient(client, _FAST))
    with respx.mock() as router:
        router.post(URL).respond(json={"unexpected": True})
        with pytest.raises(NonRetryableError):
            await embedder.embed_query("q")


async def test_rate_limit_is_retried_then_reported(client: httpx.AsyncClient) -> None:
    embedder = OpenAIEmbedder(_settings(), JsonHttpClient(client, _FAST))
    with respx.mock() as router:
        route = router.post(URL).respond(429)
        with pytest.raises(UpstreamOverloadedError):
            await embedder.embed_query("q")
    assert route.call_count == 2


async def test_an_invalid_key_is_not_retried(client: httpx.AsyncClient) -> None:
    embedder = OpenAIEmbedder(_settings(), JsonHttpClient(client, _FAST))
    with respx.mock() as router:
        route = router.post(URL).respond(401)
        with pytest.raises(NonRetryableError):
            await embedder.embed_query("q")
    assert route.call_count == 1


# --- the factory ----------------------------------------------------------------------------


def _inhouse(**changes: Any) -> EmbeddingSettings:
    return _settings(provider="inhouse", openai_api_key=None, allow_external=False, **changes)


async def test_inhouse_is_the_default(client: httpx.AsyncClient) -> None:
    assert EmbeddingSettings(model="m", endpoint="e").provider == "inhouse"
    assert isinstance(create_embedder(_inhouse(), client, _FAST), InHouseEmbedder)


async def test_openai_needs_the_external_switch(client: httpx.AsyncClient) -> None:
    with pytest.raises(NonRetryableError, match="allow_external"):
        create_embedder(_settings(allow_external=False), client, _FAST)
    assert isinstance(create_embedder(_settings(), client, _FAST), OpenAIEmbedder)


async def test_the_cache_wraps_the_embedder_when_redis_is_given(client: httpx.AsyncClient) -> None:
    redis_settings = RedisSettings(url="redis://redis.test:6379/0")
    embedder = create_embedder(
        _inhouse(), client, _FAST, redis=FakeRedis(), redis_settings=redis_settings
    )
    assert isinstance(embedder, CachedEmbedder)
    assert embedder.model_name == "text-embedding-3-small@1"


async def test_no_redis_means_no_cache(client: httpx.AsyncClient) -> None:
    embedder = create_embedder(_inhouse(), client, _FAST)
    assert not isinstance(embedder, CachedEmbedder)


async def test_the_proxy_is_only_used_for_openai() -> None:
    inhouse = create_http_client(_inhouse(proxy="http://proxy.test:3128"))
    external = create_http_client(_settings(proxy="http://proxy.test:3128"))
    try:
        assert not inhouse._mounts  # no proxy for in-house traffic
        assert external._mounts  # the egress proxy is mounted
    finally:
        await inhouse.aclose()
        await external.aclose()
