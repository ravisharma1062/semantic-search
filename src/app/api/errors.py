"""Maps typed errors to API error responses. This is the only place that does it."""

from typing import cast

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.errors import AppError, ErrorCode, InvalidRequestError, RateLimitedError

_log = structlog.get_logger(__name__)


def current_request_id() -> str | None:
    """Request ID bound by the request context middleware, if any."""
    value = structlog.contextvars.get_contextvars().get("request_id")
    return value if isinstance(value, str) else None


def error_response(
    status_code: int,
    code: ErrorCode,
    message: str,
    request_id: str | None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Build the common error body."""
    body = {"error": {"code": code.value, "message": message, "request_id": request_id}}
    return JSONResponse(status_code=status_code, content=body, headers=headers)


async def _handle_app_error(_request: Request, exc: Exception) -> JSONResponse:
    error = cast(AppError, exc)  # registered for AppError only
    _log.warning("app_error", code=error.code.value, status=error.http_status)
    headers = None
    if isinstance(error, RateLimitedError):
        headers = {"Retry-After": str(error.retry_after_s)}
    return error_response(
        error.http_status, error.code, error.message, current_request_id(), headers
    )


async def _handle_validation_error(_request: Request, exc: Exception) -> JSONResponse:
    # Only field locations are returned. The default FastAPI body echoes the input, which
    # may hold a question or document text.
    error = cast(RequestValidationError, exc)  # registered for RequestValidationError only
    fields = [".".join(str(part) for part in err["loc"]) for err in error.errors()]
    _log.warning("invalid_request", fields=fields)
    message = f"{InvalidRequestError.default_message}: {', '.join(fields)}"
    return error_response(
        InvalidRequestError.http_status, InvalidRequestError.code, message, current_request_id()
    )


def register_error_handlers(app: FastAPI) -> None:
    """Install the handlers on the app."""
    app.add_exception_handler(AppError, _handle_app_error)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
