"""Typed errors and the API error codes (HLD section 8).

Messages are fixed, generic text. Never put document text, questions or answers in them.
"""

from enum import StrEnum


class ErrorCode(StrEnum):
    """Error codes returned to the Java app."""

    INVALID_REQUEST = "INVALID_REQUEST"
    UNAUTHORIZED = "UNAUTHORIZED"
    RATE_LIMITED = "RATE_LIMITED"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class AppError(Exception):
    """Base class. Subclasses fix the code and the HTTP status."""

    code: ErrorCode = ErrorCode.INTERNAL_ERROR
    http_status: int = 500
    default_message: str = "Internal error"

    def __init__(self, message: str | None = None) -> None:
        self.message = message or self.default_message
        super().__init__(self.message)


class InvalidRequestError(AppError):
    """Bad input or unknown filter."""

    code = ErrorCode.INVALID_REQUEST
    http_status = 400
    default_message = "Invalid request"


class UnauthorizedError(AppError):
    """Missing or invalid service token."""

    code = ErrorCode.UNAUTHORIZED
    http_status = 401
    default_message = "Unauthorized"


class ForbiddenError(AppError):
    """Valid caller without the needed rights."""

    code = ErrorCode.UNAUTHORIZED
    http_status = 403
    default_message = "Forbidden"


class RateLimitedError(AppError):
    """Too many requests for the user or service."""

    code = ErrorCode.RATE_LIMITED
    http_status = 429
    default_message = "Too many requests"


class UpstreamUnavailableError(AppError):
    """Model server, OpenAI or Elasticsearch is down."""

    code = ErrorCode.UPSTREAM_UNAVAILABLE
    http_status = 503
    default_message = "Upstream service unavailable"


class UpstreamTimeoutError(AppError):
    """A step took too long."""

    code = ErrorCode.TIMEOUT
    http_status = 504
    default_message = "Timeout"
