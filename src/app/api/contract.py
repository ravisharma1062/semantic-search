"""The OpenAPI contract shared with the Java team (``openapi/openapi.yaml``).

FastAPI writes most of it from the routes and models. Here we add what it cannot see: the service
token, the identity headers and the request ID header. ``python -m app.api.openapi_export`` writes
the file, and a test checks that the committed file is the current contract. A change to the file
needs approval and must be backward compatible (CLAUDE.md).
"""

from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

TITLE = "Semantic Search Service"

_HEADERS: list[dict[str, Any]] = [
    {
        "name": "X-User-Id",
        "in": "header",
        "required": True,
        "description": "The end user. Accepted only from a service that is allowed to send it.",
        "schema": {"type": "string", "maxLength": 256},
    },
    {
        "name": "X-User-Groups",
        "in": "header",
        "required": False,
        "description": "Comma separated groups of the end user.",
        "schema": {"type": "string"},
    },
    {
        "name": "X-Request-Id",
        "in": "header",
        "required": False,
        "description": "Trace ID. Returned in the response and passed to all downstream calls.",
        "schema": {"type": "string", "maxLength": 128},
    },
]

_USER_PATHS = {"/v1/search", "/v1/answer", "/v1/answer/stream"}


def build_contract(app: FastAPI) -> dict[str, Any]:
    """The OpenAPI document of the app, with security and headers added."""
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=TITLE, version=app.version, routes=app.routes)
    schema.setdefault("components", {})["securitySchemes"] = {
        "serviceToken": {
            "type": "http",
            "scheme": "bearer",
            "description": "Service-to-service token. mTLS is enforced at the ingress.",
        },
        "adminToken": {"type": "http", "scheme": "bearer", "description": "Admin token."},
    }
    for path, methods in schema["paths"].items():
        for operation in methods.values():
            if path in _USER_PATHS:
                operation["security"] = [{"serviceToken": []}]
                operation.setdefault("parameters", []).extend(_HEADERS)
            elif path.startswith("/health"):
                operation["security"] = []
            else:
                operation["security"] = [{"adminToken": []}]
                operation.setdefault("parameters", []).append(_HEADERS[2])
    # Bad input is answered with 400 INVALID_REQUEST in our error format, never with FastAPI's 422.
    for methods in schema["paths"].values():
        for operation in methods.values():
            operation.get("responses", {}).pop("422", None)
    for name in ("HTTPValidationError", "ValidationError"):
        schema["components"]["schemas"].pop(name, None)
    app.openapi_schema = schema
    return schema
