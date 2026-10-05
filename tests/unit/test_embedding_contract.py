"""The client and the local fake model server (deploy/local/fake-model-server) agree on the API."""

import importlib.util
import math
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import EmbeddingSettings
from app.embeddings.inhouse import InHouseEmbedder

_SERVER = Path(__file__).resolve().parents[2] / "deploy/local/fake-model-server/server.py"
DIMS = 16


@pytest.fixture
async def embedder(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[InHouseEmbedder]:
    monkeypatch.setenv("FAKE_DIMS", str(DIMS))
    spec = importlib.util.spec_from_file_location("fake_model_server_contract", _SERVER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    app: FastAPI = module.app
    transport = httpx.ASGITransport(app=app)  # in process: no network
    async with httpx.AsyncClient(transport=transport) as http:
        settings = EmbeddingSettings(
            model="bge-m3", endpoint="http://fake-model", dims=DIMS, batch_size=3
        )
        yield InHouseEmbedder(settings, JsonHttpClient(http, RetryPolicy(attempts=1)))


async def test_documents_and_queries_work_against_the_fake_server(
    embedder: InHouseEmbedder,
) -> None:
    texts = [f"synthetic chunk {i}" for i in range(8)]
    vectors = await embedder.embed_documents(texts)
    assert len(vectors) == 8
    assert all(len(v) == DIMS for v in vectors)
    assert all(math.isclose(math.sqrt(sum(x * x for x in v)), 1.0) for v in vectors)
    assert await embedder.embed_query("synthetic chunk 3") == pytest.approx(vectors[3])
