import json

import pytest
from fastapi.testclient import TestClient

from app.api.middleware import REQUEST_ID_HEADER


def _log_lines(text: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in text.splitlines() if line.startswith("{")]


def test_incoming_request_id_is_echoed(client: TestClient) -> None:
    response = client.get("/health/live", headers={REQUEST_ID_HEADER: "req-123"})
    assert response.headers[REQUEST_ID_HEADER] == "req-123"


def test_missing_request_id_is_generated(client: TestClient) -> None:
    response = client.get("/health/live")
    assert len(response.headers[REQUEST_ID_HEADER]) == 32


@pytest.mark.parametrize("bad_id", ["has space", "x" * 129, "semi;colon", "new\tline"])
def test_invalid_request_id_is_replaced(client: TestClient, bad_id: str) -> None:
    response = client.get("/health/live", headers={REQUEST_ID_HEADER: bad_id})
    assert response.headers[REQUEST_ID_HEADER] != bad_id


def test_request_id_is_in_the_access_log(
    client: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    client.get("/health/live", headers={REQUEST_ID_HEADER: "req-abc"})
    lines = [r for r in _log_lines(capsys.readouterr().out) if r["event"] == "http_request"]
    assert len(lines) == 1
    assert lines[0]["request_id"] == "req-abc"
    assert lines[0]["path"] == "/health/live"
    assert lines[0]["status"] == 200


def test_access_log_has_no_query_string(
    client: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    client.get("/health/live?q=secret-question")
    assert "secret-question" not in capsys.readouterr().out
