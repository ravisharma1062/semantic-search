"""Prometheus metrics (HLD section 18).

All metrics live in one private registry, so creating the app twice (tests) does not register a
metric twice. Labels are short fixed values (a route template, a mode, an outcome). A label never
holds a user, a document, a question or any text: that would break rule 2 and explode the number of
series.

``alert_metric_names`` is used by a test: every metric that an alert rule or a dashboard panel uses
must exist here.
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_LATENCY = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 5.0, 10.0, 30.0)
_NS = "semsearch"


class Metrics:
    """The collectors of the service."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        r = self.registry
        self._declared: set[str] = set()

        def counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Counter:
            self._declared.add(f"{_NS}_{name}")
            return Counter(name, doc, labels, namespace=_NS, registry=r)

        def histogram(name: str, doc: str, labels: tuple[str, ...] = ()) -> Histogram:
            base = f"{_NS}_{name}"
            self._declared.update({f"{base}_bucket", f"{base}_sum", f"{base}_count"})
            return Histogram(name, doc, labels, namespace=_NS, buckets=_LATENCY, registry=r)

        def gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Gauge:
            self._declared.add(f"{_NS}_{name}")
            return Gauge(name, doc, labels, namespace=_NS, registry=r)

        # API
        self.http_requests = counter(
            "http_requests_total", "HTTP requests", ("route", "method", "status")
        )
        self.http_duration = histogram(
            "http_request_duration_seconds", "HTTP request time", ("route",)
        )
        self.rate_limited = counter("rate_limited_total", "Requests over a limit", ("scope",))
        # Search
        self.search_requests = counter(
            "search_requests_total", "Searches by the mode that really ran", ("mode_used",)
        )
        self.search_fallbacks = counter(
            "search_fallbacks_total", "Searches that ran a simpler mode than requested", ("reason",)
        )
        self.search_stage = histogram(
            "search_stage_duration_seconds", "Time per search stage", ("stage",)
        )
        self.search_errors = counter("search_errors_total", "Searches that failed", ("code",))
        self.search_results = histogram("search_results_returned", "Results per search")
        # Dependencies
        self.upstream_calls = counter(
            "upstream_calls_total", "Calls to model servers and stores", ("dependency", "outcome")
        )
        self.upstream_duration = histogram(
            "upstream_call_duration_seconds", "Time of calls to dependencies", ("dependency",)
        )
        self.breaker_open = gauge("circuit_breaker_open", "1 while a breaker is open", ("name",))
        self.cache_lookups = counter("cache_lookups_total", "Cache lookups", ("cache", "result"))
        # RAG
        self.answers = counter("answers_total", "Answers by outcome", ("reason",))
        self.llm_tokens = counter("llm_tokens_total", "Model tokens", ("direction",))
        self.first_token = histogram(
            "answer_first_token_seconds", "Time to the first streamed token"
        )
        # Indexing worker
        self.events = counter("ingestion_events_total", "Events by result", ("result",))
        self.event_duration = histogram(
            "ingestion_event_duration_seconds", "Time to handle an event"
        )
        self.dlq = counter(
            "ingestion_dlq_total", "Messages sent to the dead letter topic", ("reason",)
        )
        self.chunks_written = counter("ingestion_chunks_written_total", "Chunks written")
        self.bulk_failures = counter(
            "ingestion_bulk_item_failures_total", "Chunks that failed in a bulk"
        )
        self.admin_actions = counter(
            "admin_actions_total", "Admin API actions", ("action", "outcome")
        )
        # Telemetry sinks
        self.telemetry_dropped = counter("telemetry_dropped_total", "Telemetry not sent", ("sink",))

    def exposition(self) -> bytes:
        """The text format that Prometheus scrapes."""
        return generate_latest(self.registry)

    def declared_names(self) -> set[str]:
        """Every metric name as Prometheus sees it (counters with ``_total``, histograms with
        ``_bucket``, ``_sum`` and ``_count``), also those that have no sample yet."""
        return set(self._declared)


_metrics: Metrics | None = None


def get_metrics() -> Metrics:
    """The metrics of this process."""
    global _metrics
    if _metrics is None:
        _metrics = Metrics()
    return _metrics
