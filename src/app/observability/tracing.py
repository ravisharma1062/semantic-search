"""OpenTelemetry spans with IDs and numbers only (HLD section 18, rule 2).

Spans are created through ``span()``. Only attribute names from ``ALLOWED_ATTRIBUTES`` are kept
and values must be short strings, numbers or booleans: anything else is dropped, so a question, a
passage or an answer cannot reach a trace by accident. Exceptions are recorded by type only,
because exception messages can echo text.

Without ``observability.otlp_endpoint`` nothing is exported and ``span()`` costs almost nothing.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import structlog
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.trace import Span, Status, StatusCode, Tracer

from app.core.settings import ObservabilitySettings

ALLOWED_ATTRIBUTES = frozenset(
    {
        "request_id",
        "item_id",
        "doc_id",
        "chunk_ids",
        "mode",
        "mode_used",
        "model",
        "prompt_version",
        "found",
        "reason",
        "results",
        "candidates",
        "context_chunks",
        "outcome",
        "dependency",
        "input_tokens",
        "output_tokens",
        "status",
        "event_type",
        "retry_count",
        "error.type",
    }
)
_MAX_STRING = 128

_tracer: Tracer | None = None


def _clean(value: Any) -> str | int | float | bool | None:
    if isinstance(value, bool | int | float):
        return value
    if isinstance(value, str) and len(value) <= _MAX_STRING:
        return value
    return None


def build_provider(
    settings: ObservabilitySettings, exporter: SpanExporter | None = None
) -> TracerProvider | None:
    """A provider that exports to the OTLP collector (or to ``exporter``, for tests). ``None`` if
    tracing is not configured."""
    test_exporter = exporter
    if exporter is None:
        if not settings.otlp_endpoint:
            return None
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter(endpoint=settings.otlp_endpoint)
    provider = TracerProvider(sampler=TraceIdRatioBased(settings.trace_sample_ratio))
    # Spans of a test exporter are exported at once, so a test can read them without a flush.
    simple = test_exporter is not None
    provider.add_span_processor(
        SimpleSpanProcessor(exporter) if simple else BatchSpanProcessor(exporter)
    )
    return provider


def configure_tracing(provider: TracerProvider | None) -> None:
    """Use this provider for all spans. ``None`` switches tracing off."""
    global _tracer
    _tracer = provider.get_tracer("semantic-search") if provider is not None else None


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[Span | None]:
    """A span around a block of work. Attributes: IDs, modes, counts (``ALLOWED_ATTRIBUTES``)."""
    if _tracer is None:
        yield None
        return
    request_id = structlog.contextvars.get_contextvars().get("request_id")
    if request_id is not None:
        attributes.setdefault("request_id", request_id)
    with _tracer.start_as_current_span(
        name, record_exception=False, set_status_on_exception=False
    ) as s:
        set_attributes(s, **attributes)
        try:
            yield s
        except BaseException as exc:
            s.set_attribute("error.type", type(exc).__name__)
            s.set_status(Status(StatusCode.ERROR))
            raise


def set_attributes(current: Span | None, **attributes: Any) -> None:
    """Add allowed attributes to a span. Unknown names and unsafe values are dropped."""
    if current is None:
        return
    for key, value in attributes.items():
        if key not in ALLOWED_ATTRIBUTES:
            continue
        if isinstance(value, list | tuple):
            value = ",".join(str(v) for v in value[:20])
        cleaned = _clean(value)
        if cleaned is not None:
            current.set_attribute(key, cleaned)
