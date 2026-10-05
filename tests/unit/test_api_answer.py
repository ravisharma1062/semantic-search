"""POST /v1/answer and /v1/answer/stream through the real app, with fake backends."""

import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core.errors import UpstreamTimeoutError, UpstreamUnavailableError
from app.core.settings import ApiSettings, Settings
from app.main import create_app
from app.services import Services
from tests.fakes import FakeLLMClient, FakeReranker
from tests.unit.test_answer_service import make_service

HEADERS = {"Authorization": "Bearer token-java", "X-User-Id": "alice", "X-User-Groups": "g1"}
QUESTION = "penalty for late delivery"

Build = Callable[..., tuple[TestClient, FakeLLMClient]]


def _api(settings: Settings) -> Settings:
    return settings.model_copy(
        update={
            "api": ApiSettings(
                service_tokens={"java-search": SecretStr("token-java")},
                identity_services=["java-search"],
            )
        }
    )


@pytest.fixture
def build(settings: Settings) -> Iterator[Build]:
    clients: list[TestClient] = []

    def make(*replies: str, **kwargs: Any) -> tuple[TestClient, FakeLLMClient]:
        llm = FakeLLMClient(list(replies) or ["The vendor pays one percent [1]."])
        service, _ = make_service(_api(settings), llm, **kwargs)
        app = create_app(_api(settings), Services(search=service._search, answer=service))
        client = TestClient(app, raise_server_exceptions=False)
        client.__enter__()
        clients.append(client)
        return client, llm

    try:
        yield make
    finally:
        for c in clients:
            c.__exit__(None, None, None)


def _events(response: Any) -> list[tuple[str, dict[str, Any]]]:
    out = []
    for block in response.text.strip().split("\n\n"):
        name, data = block.split("\n", 1)
        out.append((name.removeprefix("event: "), json.loads(data.removeprefix("data: "))))
    return out


# --- /v1/answer -----------------------------------------------------------------------------


def test_the_answer_has_the_documented_shape(build: Build) -> None:
    client, _ = build()
    response = client.post(
        "/v1/answer",
        json={"question": QUESTION, "top_k": 5},
        headers={**HEADERS, "X-Request-Id": "r1"},
    )
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "request_id",
        "answer",
        "found",
        "reason",
        "citations",
        "warnings",
        "mode_used",
        "model",
        "prompt_version",
        "usage",
    }
    assert body["request_id"] == "r1" and body["found"] is True
    assert body["answer"] == "The vendor pays one percent [1]."
    assert body["citations"] == [
        {"ref": 1, "doc_id": "D1", "pages": [4, 5], "snippet": body["citations"][0]["snippet"]}
    ]
    assert set(body["usage"]) == {"input_tokens", "output_tokens"}


def test_not_found_is_a_normal_answer(build: Build) -> None:
    client, _ = build("NOT_FOUND")
    body = client.post("/v1/answer", json={"question": QUESTION}, headers=HEADERS).json()
    assert (body["found"], body["reason"], body["answer"], body["citations"]) == (
        False,
        "not_found",
        "",
        [],
    )


def test_the_gate_answer_costs_no_model_call(build: Build) -> None:
    client, llm = build(reranker=FakeReranker(), rag={"min_score": 0.9})
    body = client.post(
        "/v1/answer", json={"question": "unrelated words only"}, headers=HEADERS
    ).json()
    assert body["reason"] == "low_relevance"
    assert llm.calls == []


@pytest.mark.parametrize(
    "error,status,code",
    [
        (UpstreamUnavailableError(), 503, "UPSTREAM_UNAVAILABLE"),
        (UpstreamTimeoutError(), 504, "TIMEOUT"),
    ],
)
def test_model_errors_tell_java_to_fall_back(
    build: Build, error: Exception, status: int, code: str
) -> None:
    client, llm = build()
    llm.fail_with = error
    response = client.post("/v1/answer", json={"question": QUESTION}, headers=HEADERS)
    assert response.status_code == status
    assert response.json()["error"]["code"] == code


def test_rag_switched_off_is_a_fallback_error(build: Build) -> None:
    client, _ = build(rag_on=False)
    response = client.post("/v1/answer", json={"question": QUESTION}, headers=HEADERS)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "UPSTREAM_UNAVAILABLE"


def test_no_answer_service_is_a_fallback_error(settings: Settings) -> None:
    from tests.unit.test_api_search import Rig

    rig = Rig(settings)
    with TestClient(rig.app, raise_server_exceptions=False) as client:
        response = client.post(
            "/v1/answer",
            json={"question": QUESTION},
            headers={**HEADERS, "Authorization": "Bearer token-java"},
        )
    assert response.status_code == 503


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"question": ""},
        {"question": "x", "top_k": 0},
        {"question": "x", "top_k": 99},
        {"question": "x", "extra": 1},
    ],
)
def test_bad_input_is_400_without_echoing_the_question(build: Build, body: dict[str, Any]) -> None:
    client, _ = build()
    response = client.post("/v1/answer", json=body, headers=HEADERS)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_REQUEST"


def test_a_too_long_question_is_refused(build: Build) -> None:
    client, _ = build()
    response = client.post("/v1/answer", json={"question": "x" * 1001}, headers=HEADERS)
    assert response.status_code == 400


def test_authentication_is_required(build: Build) -> None:
    client, _ = build()
    assert client.post("/v1/answer", json={"question": QUESTION}).status_code == 401
    assert client.post("/v1/answer/stream", json={"question": QUESTION}).status_code == 401
    no_user = {"Authorization": "Bearer token-java"}
    assert client.post("/v1/answer", json={"question": QUESTION}, headers=no_user).status_code in (
        400,
        401,
    )


@pytest.mark.access
def test_the_answer_of_one_user_never_cites_a_document_of_another(build: Build) -> None:
    client, llm = build("x [1][2][3]")
    body = client.post(
        "/v1/answer", json={"question": "late delivery penalty merger secret"}, headers=HEADERS
    ).json()
    assert {c["doc_id"] for c in body["citations"]} <= {"D1", "D2"}
    assert "merger plan" not in json.dumps(llm.calls[0], default=lambda o: o.model_dump())


def test_the_log_has_ids_but_no_text(build: Build) -> None:
    import structlog

    client, _ = build("Synthetic secret answer [1].")
    with structlog.testing.capture_logs() as logs:
        client.post(
            "/v1/answer",
            json={"question": "synthetic secret question late delivery"},
            headers=HEADERS,
        )
    text = str(logs).lower()
    assert "synthetic secret" not in text
    done = next(entry for entry in logs if entry["event"] == "answer_done")
    assert done["citations"] == ["D1:0"] and done["user_id"] == "alice"


# --- /v1/answer/stream ----------------------------------------------------------------------


def test_the_stream_sends_token_citations_done(build: Build) -> None:
    client, _ = build("The vendor pays one percent [1].")
    response = client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-store"
    events = _events(response)
    names = [n for n, _ in events]
    assert names[-2:] == ["citations", "done"] and set(names[:-2]) == {"token"}
    text = "".join(d["text"] for n, d in events if n == "token")
    assert text == "The vendor pays one percent [1]."
    assert events[-2][1]["citations"][0]["doc_id"] == "D1"
    done = events[-1][1]
    assert done["found"] is True and done["reason"] == "answered"
    assert done["prompt_version"] == "v1" and done["usage"]["output_tokens"] > 0


def test_not_found_is_not_streamed_as_text(build: Build) -> None:
    client, _ = build("NOT_FOUND")
    events = _events(client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS))
    assert [n for n, _ in events] == ["citations", "done"]
    assert events[-1][1]["found"] is False and events[-1][1]["reason"] == "not_found"


def test_the_stream_stops_at_the_gate_with_a_done_event(build: Build) -> None:
    client, llm = build(reranker=FakeReranker(), rag={"min_score": 0.9})
    response = client.post(
        "/v1/answer/stream", json={"question": "unrelated words only"}, headers=HEADERS
    )
    events = _events(response)
    assert [n for n, _ in events] == ["done"]
    assert events[0][1]["reason"] == "low_relevance" and llm.calls == []


def test_errors_before_the_first_token_are_normal_errors(build: Build) -> None:
    client, _ = build(rag_on=False)
    response = client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "UPSTREAM_UNAVAILABLE"


def test_a_broken_stream_ends_with_an_error_event(build: Build) -> None:
    client, llm = build("one two three four five [1]")
    llm.fail_stream_after = 2
    response = client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS)
    events = _events(response)
    assert events[-1][0] == "error"
    assert events[-1][1]["code"] == "UPSTREAM_UNAVAILABLE"
    assert "done" not in [n for n, _ in events]


def test_a_streamed_answer_with_bad_citations_is_declared_unverified_in_done(build: Build) -> None:
    client, _ = build("No source given here.")
    events = _events(client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS))
    done = events[-1][1]
    assert done["found"] is False and done["reason"] == "unverified"
    assert done["warnings"] == ["no_citations"]


def test_pii_is_masked_in_the_stream(build: Build) -> None:
    client, _ = build("Write to anna.k@example.org [1]. Thanks.")
    events = _events(client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS))
    text = "".join(d["text"] for n, d in events if n == "token")
    assert "example.org" not in text and "[EMAIL]" in text


def test_the_stream_is_in_the_contract(build: Build) -> None:
    client, _ = build()
    paths = client.get("/openapi.json").json()["paths"]
    assert "/v1/answer" in paths and "/v1/answer/stream" in paths
    assert "text/event-stream" in paths["/v1/answer/stream"]["post"]["responses"]["200"]["content"]
