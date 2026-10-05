"""Elasticsearch client factory and error mapping. Timeouts and retries are set by the callers."""

from typing import Any

from elastic_transport import ConnectionTimeout, TransportError
from elasticsearch import ApiError, AsyncElasticsearch

from app.core.errors import (
    AppError,
    NonRetryableError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from app.core.settings import ElasticsearchSettings

# 429 and 5xx mean "try again later". Other 4xx mean the request itself is wrong.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def create_es_client(settings: ElasticsearchSettings) -> AsyncElasticsearch:
    """Create the async client. Retries are done by ``core.retry``, not by the client."""
    options: dict[str, Any] = {
        "hosts": settings.hosts,
        "verify_certs": settings.verify_certs,
        "max_retries": 0,
        "retry_on_timeout": False,
        "request_timeout": settings.source_read_timeout_s,
    }
    if settings.api_key is not None:
        options["api_key"] = settings.api_key.get_secret_value()
    if settings.ca_certs:
        options["ca_certs"] = settings.ca_certs
    return AsyncElasticsearch(**options)


def map_es_error(error: Exception) -> AppError:
    """Turn a client error into one of our typed errors. The message is generic (rule 2)."""
    if isinstance(error, ConnectionTimeout):
        return UpstreamTimeoutError("Elasticsearch timeout")
    if isinstance(error, TransportError):
        return UpstreamUnavailableError("Elasticsearch unavailable")
    if isinstance(error, ApiError):
        if error.meta.status in _RETRYABLE_STATUS:
            return UpstreamUnavailableError("Elasticsearch unavailable")
        return NonRetryableError(f"Elasticsearch rejected the request ({error.meta.status})")
    return UpstreamUnavailableError("Elasticsearch call failed")


def is_retryable(error: Exception) -> bool:
    """Safe to retry: the service was busy or unreachable."""
    return isinstance(error, UpstreamUnavailableError | UpstreamTimeoutError)
