import json
import logging

import pytest
import structlog

from app.core.logging import REDACTED, configure_logging


def _last_json_line(text: str) -> dict[str, object]:
    lines = [line for line in text.splitlines() if line.startswith("{")]
    parsed: dict[str, object] = json.loads(lines[-1])
    return parsed


def test_log_line_is_json_with_request_id(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    structlog.contextvars.bind_contextvars(request_id="req-9")
    try:
        structlog.get_logger("test").info("something_happened", item_id="ITEM-1")
    finally:
        structlog.contextvars.clear_contextvars()
    line = _last_json_line(capsys.readouterr().out)
    assert line["event"] == "something_happened"
    assert line["request_id"] == "req-9"
    assert line["item_id"] == "ITEM-1"
    assert line["level"] == "info"


@pytest.mark.parametrize("key", ["text", "content", "question", "answer", "query", "prompt"])
def test_sensitive_fields_are_redacted(key: str, capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    structlog.get_logger("test").info("event", **{key: "synthetic secret value"})
    out = capsys.readouterr().out
    assert "synthetic secret value" not in out
    assert _last_json_line(out)[key] == REDACTED


def test_standard_library_logs_use_the_same_format(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    logging.getLogger("uvicorn.error").info("library message")
    assert _last_json_line(capsys.readouterr().out)["event"] == "library message"


def test_httpx_request_urls_are_not_logged(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    logging.getLogger("httpx").info("HTTP Request: GET http://host/path?q=synthetic-question")
    assert "synthetic-question" not in capsys.readouterr().out


def test_level_filters_lower_levels(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("WARNING", json_output=True)
    structlog.get_logger("test").info("hidden")
    assert "hidden" not in capsys.readouterr().out
