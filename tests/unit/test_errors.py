import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from app.api.middleware import REQUEST_ID_HEADER
from app.core.errors import (
    AppError,
    ErrorCode,
    ForbiddenError,
    InvalidRequestError,
    RateLimitedError,
    UnauthorizedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.settings import Settings
from app.main import create_app


class _Body(BaseModel):
    question: str


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    app = create_app(settings)

    @app.get("/boom/{kind}")
    async def boom(kind: str) -> None:
        errors: dict[str, Exception] = {
            "invalid": InvalidRequestError(),
            "unauthorized": UnauthorizedError(),
            "forbidden": ForbiddenError(),
            "rate": RateLimitedError(),
            "upstream": UpstreamUnavailableError(),
            "timeout": UpstreamTimeoutError(),
            "unexpected": RuntimeError("document text that must not leak"),
        }
        raise errors[kind]

    @app.post("/echo")
    async def echo(body: _Body) -> _Body:
        return body

    return app


@pytest.mark.parametrize(
    ("kind", "status", "code"),
    [
        ("invalid", 400, ErrorCode.INVALID_REQUEST),
        ("unauthorized", 401, ErrorCode.UNAUTHORIZED),
        ("forbidden", 403, ErrorCode.UNAUTHORIZED),
        ("rate", 429, ErrorCode.RATE_LIMITED),
        ("upstream", 503, ErrorCode.UPSTREAM_UNAVAILABLE),
        ("timeout", 504, ErrorCode.TIMEOUT),
        ("unexpected", 500, ErrorCode.INTERNAL_ERROR),
    ],
)
def test_error_maps_to_status_and_code(
    app: FastAPI, kind: str, status: int, code: ErrorCode
) -> None:
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/boom/{kind}", headers={REQUEST_ID_HEADER: "req-1"})
    assert response.status_code == status
    error = response.json()["error"]
    assert error["code"] == code.value
    assert error["request_id"] == "req-1"
    assert response.headers[REQUEST_ID_HEADER] == "req-1"


def test_unexpected_error_does_not_leak_its_message(
    app: FastAPI, capsys: pytest.CaptureFixture[str]
) -> None:
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/boom/unexpected")
    assert "must not leak" not in response.text
    assert "must not leak" not in capsys.readouterr().out


def test_validation_error_is_400_and_does_not_echo_input(app: FastAPI) -> None:
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/echo", json={"question": 123, "extra": "secret-question"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
    assert "secret-question" not in response.text
    assert json.loads(response.text)["error"]["message"].endswith("body.question")


def test_app_error_default_message() -> None:
    assert AppError().message == "Internal error"
    assert AppError("custom").message == "custom"
