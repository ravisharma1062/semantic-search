"""The local fake model server (deploy/local/fake-model-server) keeps working."""

import importlib.util
import math
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_SERVER = Path(__file__).resolve().parents[2] / "deploy/local/fake-model-server/server.py"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("FAKE_DIMS", "16")
    spec = importlib.util.spec_from_file_location("fake_model_server", _SERVER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TestClient(module.app)


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_embed_is_deterministic_normalized_and_sized(client: TestClient) -> None:
    first = client.post("/embed", json={"inputs": ["alpha", "beta"]}).json()
    again = client.post("/embed", json={"inputs": ["alpha"]}).json()
    assert first[0] == again[0]
    assert first[0] != first[1]
    assert len(first[0]) == 16
    assert math.isclose(math.sqrt(sum(x * x for x in first[0])), 1.0)


def test_rerank_orders_by_overlap(client: TestClient) -> None:
    body = {"query": "late delivery", "texts": ["unrelated", "late delivery penalty"]}
    result = client.post("/rerank", json=body).json()
    assert [item["index"] for item in result] == [1, 0]


def test_chat_returns_not_found(client: TestClient) -> None:
    body = {"messages": [{"role": "user", "content": "synthetic question"}]}
    reply = client.post("/v1/chat/completions", json=body).json()
    assert reply["choices"][0]["message"]["content"] == "NOT_FOUND"
