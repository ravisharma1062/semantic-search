"""Typed errors and the API error codes (HLD section 8).

Messages are fixed, generic text. Never put document text, questions or answers in them.
"""

from enum import StrEnum


class ErrorCode(StrEnum):
    """Error codes returned to the Java app."""

    INVALID_REQUEST = "INVALID_REQUEST"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_FOUND = "NOT_FOUND"
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


class NonRetryableError(AppError):
    """A processing error that retrying cannot fix. The message goes straight to the DLQ."""

    default_message = "Non-retryable processing error"


class SourceNotReadyError(AppError):
    """The source document is not visible (yet). The event is tried again later."""

    default_message = "Source document not available yet"


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


class NotFoundError(AppError):
    """An admin resource (a job) does not exist."""

    code = ErrorCode.NOT_FOUND
    http_status = 404
    default_message = "Not found"


class RateLimitedError(AppError):
    """Too many requests for the user or service."""

    code = ErrorCode.RATE_LIMITED
    http_status = 429
    default_message = "Too many requests"

    def __init__(self, message: str | None = None, retry_after_s: int = 1) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class UpstreamUnavailableError(AppError):
    """Model server, OpenAI or Elasticsearch is down."""

    code = ErrorCode.UPSTREAM_UNAVAILABLE
    http_status = 503
    default_message = "Upstream service unavailable"


class UpstreamOverloadedError(UpstreamUnavailableError):
    """The upstream asked us to slow down (HTTP 429 or 503). Callers lower their parallelism."""

    default_message = "Upstream service overloaded"


class UpstreamTimeoutError(AppError):
    """A step took too long."""

    code = ErrorCode.TIMEOUT
    http_status = 504
    default_message = "Timeout"
