"""The OpenAPI contract shared with the Java team."""

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from fastapi.testclient import TestClient

from app.api.openapi_export import CONTRACT_PATH, contract_text, main
from app.api.schemas import ErrorResponse, SearchResponse
from app.core.settings import Settings
from app.main import create_app
from app.services import Services


def _contract() -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    return loaded


def test_the_committed_file_is_the_current_contract() -> None:
    committed = CONTRACT_PATH.read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == contract_text(), "run: python -m app.api.openapi_export"
    assert main(["--check"]) == 0


def test_the_running_app_serves_the_same_contract(settings: Settings) -> None:
    app = create_app(settings, Services(search=None))  # type: ignore[arg-type]
    with TestClient(app) as client:
        served = client.get("/openapi.json").json()
    assert served == _contract()


def test_the_search_operation_is_described() -> None:
    contract = _contract()
    operation = contract["paths"]["/v1/search"]["post"]
    assert operation["security"] == [{"serviceToken": []}]
    headers = {p["name"] for p in operation["parameters"]}
    assert {"X-User-Id", "X-User-Groups", "X-Request-Id"} <= headers
    codes = set(operation["responses"])
    assert {"200", "400", "401", "403", "429", "503", "504"} <= codes
    assert "422" not in codes  # bad input is 400 in our own error format


def test_health_is_open_and_the_rest_is_protected() -> None:
    paths = _contract()["paths"]
    assert paths["/health/live"]["get"]["security"] == []
    assert paths["/v1/search"]["post"]["security"] != []


def test_the_response_has_mode_used_and_no_chunk_text() -> None:
    schemas = _contract()["components"]["schemas"]
    assert "mode_used" in schemas["SearchResponse"]["required"]
    assert set(schemas["SearchResultItem"]["properties"]) == {
        "doc_id",
        "chunk_id",
        "score",
        "pages",
        "snippet",
        "highlights",
    }


def test_the_error_format_is_in_the_contract() -> None:
    schemas = _contract()["components"]["schemas"]
    assert set(schemas["ErrorBody"]["properties"]) == {"code", "message", "request_id"}
    assert "HTTPValidationError" not in schemas


def test_a_real_answer_fits_the_documented_models(settings: Settings) -> None:
    body = {
        "request_id": "r",
        "mode_used": "bm25",
        "results": [
            {
                "doc_id": "D",
                "chunk_id": "D:0",
                "score": 1.0,
                "pages": [1],
                "snippet": "s",
                "highlights": [],
            }
        ],
        "took_ms": 3,
    }
    assert SearchResponse.model_validate(body).mode_used == "bm25"
    error = {"error": {"code": "TIMEOUT", "message": "Timeout", "request_id": None}}
    assert ErrorResponse.model_validate(error).error.code == "TIMEOUT"


def test_the_file_is_in_the_repository() -> None:
    assert Path(CONTRACT_PATH).is_file()
