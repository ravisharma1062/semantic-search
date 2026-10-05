import asyncio
import hashlib
import json
import math
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx
import pytest
import respx
import structlog

from app.core.errors import (
    NonRetryableError,
    UpstreamOverloadedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import EmbeddingSettings
from app.embeddings.base import Embedder
from app.embeddings.inhouse import InHouseEmbedder

URL = "http://model.test/embed"
DIMS = 4


def _vector(text: str) -> list[float]:
    digest = hashlib.sha256(text.encode()).digest()
    raw = [digest[i] - 127.5 for i in range(DIMS)]
    norm = math.sqrt(sum(x * x for x in raw))
    return [x / norm for x in raw]


def _settings(**changes: Any) -> EmbeddingSettings:
    values: dict[str, Any] = {
        "model": "bge-m3",
        "model_version": "7",
        "endpoint": "http://model.test",
        "dims": DIMS,
        "batch_size": 2,
        "max_concurrency": 2,
        "timeout_s": 0.5,
        "document_timeout_s": 3.0,
    }
    return EmbeddingSettings(**{**values, **changes})


_FAST = RetryPolicy(attempts=3, initial_delay_s=0, max_delay_s=0, jitter_s=0)


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as http:
        yield http


def _embedder(client: httpx.AsyncClient, **changes: Any) -> InHouseEmbedder:
    return InHouseEmbedder(_settings(**changes), JsonHttpClient(client, _FAST))


def _answer(request: httpx.Request) -> httpx.Response:
    texts = json.loads(request.content)["inputs"]
    return httpx.Response(200, json=[_vector(t) for t in texts])


@pytest.fixture
def server() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as router:
        router.post(URL).mock(side_effect=_answer)
        yield router


def _bodies(router: respx.MockRouter) -> list[dict[str, Any]]:
    return [json.loads(call.request.content) for call in router.calls]


# --- the basics -----------------------------------------------------------------------------


async def test_satisfies_the_embedder_interface(client: httpx.AsyncClient) -> None:
    embedder = _embedder(client)
    assert isinstance(embedder, Embedder)
    assert embedder.model_name == "bge-m3@7"  # name and version
    assert embedder.dims == DIMS


async def test_request_shape(client: httpx.AsyncClient, server: respx.MockRouter) -> None:
    await _embedder(client).embed_documents(["one", "two"])
    assert _bodies(server) == [{"inputs": ["one", "two"], "normalize": True, "truncate": False}]


async def test_truncate_comes_from_settings(
    client: httpx.AsyncClient, server: respx.MockRouter
) -> None:
    await _embedder(client, truncate=True).embed_documents(["one"])
    assert _bodies(server)[0]["truncate"] is True


async def test_documents_are_batched_and_order_is_kept(
    client: httpx.AsyncClient, server: respx.MockRouter
) -> None:
    texts = [f"text {i}" for i in range(7)]
    vectors = await _embedder(client).embed_documents(texts)
    assert sorted(len(b["inputs"]) for b in _bodies(server)) == [1, 2, 2, 2]
    assert vectors == [_vector(t) for t in texts]


async def test_no_texts_means_no_call(client: httpx.AsyncClient, server: respx.MockRouter) -> None:
    assert await _embedder(client).embed_documents([]) == []
    assert server.calls.call_count == 0


async def test_query_embedding(client: httpx.AsyncClient, server: respx.MockRouter) -> None:
    assert await _embedder(client).embed_query("a question") == _vector("a question")


async def test_timeouts_come_from_settings(
    client: httpx.AsyncClient, server: respx.MockRouter
) -> None:
    embedder = _embedder(client, timeout_s=0.4, document_timeout_s=2.5)
    await embedder.embed_query("q")
    await embedder.embed_documents(["d"])
    reads = [call.request.extensions["timeout"]["read"] for call in server.calls]
    assert reads == [0.4, 2.5]


async def test_endpoint_trailing_slash_is_fine(
    client: httpx.AsyncClient, server: respx.MockRouter
) -> None:
    await _embedder(client, endpoint="http://model.test/").embed_query("q")
    assert server.calls.call_count == 1


# --- vectors --------------------------------------------------------------------------------


async def test_vectors_that_are_not_unit_length_are_normalized(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        router.post(URL).respond(json=[[3.0, 4.0, 0.0, 0.0]])
        [vector] = await _embedder(client).embed_documents(["x"])
    assert vector == pytest.approx([0.6, 0.8, 0.0, 0.0])


@pytest.mark.parametrize(
    "answer",
    [
        [[0.5, 0.5, 0.5]],  # wrong size
        [[0.5, 0.5, 0.5, 0.5], [0.5, 0.5, 0.5, 0.5]],  # wrong count
        [[0.0, 0.0, 0.0, 0.0]],  # zero vector
        [["a", 0.1, 0.1, 0.1]],  # not numbers
        {"error": "x"},  # not a list
        [[1.0, 0.0, 0.0, float("nan")]],
    ],
)
async def test_bad_answers_are_rejected_without_retry(
    client: httpx.AsyncClient, answer: object
) -> None:
    with respx.mock() as router:
        route = router.post(URL).respond(content=json.dumps(answer, allow_nan=True))
        with pytest.raises(NonRetryableError):
            await _embedder(client).embed_documents(["x"])
    assert route.call_count == 1


async def test_a_non_json_answer_is_rejected(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        router.post(URL).respond(content=b"<html>proxy page</html>")
        with pytest.raises(NonRetryableError):
            await _embedder(client).embed_query("x")


# --- failures: retries, overload, timeouts --------------------------------------------------


async def test_a_server_error_is_retried_and_then_succeeds(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        route = router.post(URL).mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(500),
                httpx.Response(200, json=[_vector("x")]),
            ]
        )
        assert await _embedder(client).embed_query("x") == _vector("x")
    assert route.call_count == 3


async def test_gives_up_after_the_retry_limit(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        route = router.post(URL).respond(500)
        with pytest.raises(UpstreamUnavailableError):
            await _embedder(client).embed_query("x")
    assert route.call_count == 3


async def test_overload_is_reported_as_overloaded(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        router.post(URL).respond(429)
        with pytest.raises(UpstreamOverloadedError):
            await _embedder(client).embed_query("x")


@pytest.mark.parametrize("status", [400, 401, 413, 422])
async def test_a_rejected_request_is_not_retried(client: httpx.AsyncClient, status: int) -> None:
    with respx.mock() as router:
        route = router.post(URL).respond(status, json={"error": "synthetic input text"})
        with pytest.raises(NonRetryableError) as error:
            await _embedder(client).embed_documents(["x"])
    assert route.call_count == 1
    assert "synthetic input text" not in str(error.value)  # the body is never copied


async def test_timeout_is_retried_and_then_raised(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        route = router.post(URL).mock(side_effect=httpx.ReadTimeout("slow"))
        with pytest.raises(UpstreamTimeoutError):
            await _embedder(client).embed_query("x")
    assert route.call_count == 3


async def test_connection_failure_is_upstream_unavailable(client: httpx.AsyncClient) -> None:
    with respx.mock() as router:
        router.post(URL).mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(UpstreamUnavailableError):
            await _embedder(client).embed_documents(["x"])


async def test_one_failed_batch_fails_the_call_with_our_error_type(
    client: httpx.AsyncClient,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        texts = json.loads(request.content)["inputs"]
        if "bad" in texts:
            return httpx.Response(400)
        return httpx.Response(200, json=[_vector(t) for t in texts])

    with respx.mock() as router:
        router.post(URL).mock(side_effect=handler)
        with pytest.raises(NonRetryableError):
            await _embedder(client).embed_documents(["a", "b", "bad", "c", "d"])


async def test_parallel_calls_stay_within_the_limit(client: httpx.AsyncClient) -> None:
    running = 0
    peak = 0

    async def slow(request: httpx.Request) -> httpx.Response:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1
        return _answer(request)

    with respx.mock() as router:
        router.post(URL).mock(side_effect=slow)
        embedder = _embedder(client, batch_size=1, max_concurrency=3)
        await embedder.embed_documents([f"t{i}" for i in range(12)])
    assert peak == 3


async def test_parallelism_drops_when_the_server_is_overloaded(client: httpx.AsyncClient) -> None:
    calls = 0

    def overloaded_once(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429) if calls == 1 else _answer(request)

    with respx.mock() as router:
        router.post(URL).mock(side_effect=overloaded_once)
        embedder = _embedder(client, max_concurrency=4)
        await embedder.embed_query("x")
    assert embedder._limiter.limit == 2  # halved once


async def test_texts_are_never_logged(client: httpx.AsyncClient) -> None:
    with respx.mock() as router, structlog.testing.capture_logs() as logs:
        router.post(URL).respond(500)
        with pytest.raises(UpstreamUnavailableError):
            await _embedder(client).embed_query("synthetic secret question")
    assert "synthetic secret question" not in str(logs)
