"""Headers for the retry topic and the dead-letter queue.

The original message value is passed on untouched. Error text is limited to the error type and,
for our own typed errors, their generic message: other exception messages may hold document text.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from app.core.errors import AppError
from app.ingestion.kafka_io import KafkaMessage

RETRY_COUNT = "x-retry-count"
NOT_BEFORE = "x-not-before"
ORIGIN = "x-origin"
ERROR = "x-error"
FAILED_AT = "x-failed-at"
REASON = "x-reason"


def safe_error_text(error: BaseException) -> str:
    """Error type, plus the message only when it is one of our own generic messages."""
    if isinstance(error, AppError):
        return f"{type(error).__name__}: {error.message}"
    return type(error).__name__


def retry_count_of(headers: Mapping[str, bytes]) -> int:
    """Retry count from the headers. Missing or broken means 0."""
    try:
        return max(0, int(headers.get(RETRY_COUNT, b"0")))
    except ValueError:
        return 0


def not_before_of(headers: Mapping[str, bytes]) -> float:
    """Earliest processing time (epoch seconds) from the headers. Missing or broken means 0."""
    try:
        return float(headers.get(NOT_BEFORE, b"0"))
    except ValueError:
        return 0.0


def origin_of(message: KafkaMessage) -> str:
    """Where the message first came from, kept across retries."""
    existing = message.headers.get(ORIGIN)
    if existing:
        return existing.decode(errors="replace")
    return f"{message.topic}/{message.partition}/{message.offset}"


@dataclass(frozen=True)
class Outgoing:
    """A message to publish to the retry topic or the DLQ."""

    topic: str
    key: bytes | None
    value: bytes | None
    headers: dict[str, bytes]


def build_retry(
    message: KafkaMessage,
    topic: str,
    retry_count: int,
    delay_s: float,
    error: BaseException,
    now: float,
) -> Outgoing:
    """The retry-topic copy: same key and value, retry count and not-before in the headers."""
    headers = {
        RETRY_COUNT: str(retry_count).encode(),
        NOT_BEFORE: str(now + delay_s).encode(),
        ORIGIN: origin_of(message).encode(),
        ERROR: safe_error_text(error).encode(),
    }
    return Outgoing(topic, message.key, message.value, headers)


def build_dlq(
    message: KafkaMessage,
    topic: str,
    retry_count: int,
    reason: str,
    error: BaseException | None,
    now: float,
) -> Outgoing:
    """The DLQ copy: same key and value, the reason and the error in the headers."""
    headers = {
        RETRY_COUNT: str(retry_count).encode(),
        ORIGIN: origin_of(message).encode(),
        REASON: reason.encode(),
        FAILED_AT: str(now).encode(),
    }
    if error is not None:
        headers[ERROR] = safe_error_text(error).encode()
    return Outgoing(topic, message.key, message.value, headers)
