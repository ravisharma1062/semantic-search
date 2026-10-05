"""Metrics, traces, LLM telemetry, alert rules and the "no sensitive text anywhere" test (T5.1)."""

import asyncio
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
import yaml  # type: ignore[import-untyped]
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from pydantic import SecretStr

from app.core.breaker import CircuitBreaker
from app.core.errors import UpstreamUnavailableError
from app.core.logging import configure_logging
from app.core.settings import (
    ApiSettings,
    LangfuseSettings,
    ObservabilitySettings,
    Settings,
)
from app.ingestion.source import SourceDocument, SourcePage
from app.main import create_app
from app.observability.langfuse import LangfuseSink, pseudonym
from app.observability.metrics import Metrics, get_metrics
from app.observability.tracing import (
    ALLOWED_ATTRIBUTES,
    build_provider,
    configure_tracing,
    set_attributes,
    span,
)
from app.services import Services
from tests.fakes import FakeEmbedder, FakeLLMClient, FakeReranker
from tests.fakes.search_es import FakeSearchEs, chunk_doc
from tests.unit.test_answer_service import make_service
from tests.unit.test_worker import FIRST_TRY, Rig, _event

ROOT = Path(__file__).resolve().parents[2]
HEADERS = {"Authorization": "Bearer token-java", "X-User-Id": "alice", "X-User-Groups": "g1"}
CANARY = "CANARY-7f3a91"


def sample(metric: str, **labels: str) -> float:
    """Current value of one series (0 if it does not exist yet)."""
    return get_metrics().registry.get_sample_value(metric, labels) or 0.0


class Collect(SpanExporter):
    """Keeps finished spans in memory."""

    def __init__(self) -> None:
        self.spans: list[Any] = []

    def export(self, spans: Any) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:  # pragma: no cover - nothing to release
        pass


@pytest.fixture
def spans() -> Iterator[Collect]:
    exporter = Collect()
    provider = build_provider(ObservabilitySettings(trace_sample_ratio=1.0), exporter)
    configure_tracing(provider)
    yield exporter
    assert provider is not None
    provider.force_flush()
    configure_tracing(None)


# --- metrics --------------------------------------------------------------------------------


def _client(settings: Settings, llm: FakeLLMClient | None = None, **make: Any) -> TestClient:
    s = settings.model_copy(
        update={
            "api": ApiSettings(
                service_tokens={"java-search": SecretStr("token-java")},
                identity_services=["java-search"],
            )
        }
    )
    service, _ = make_service(s, llm or FakeLLMClient(["Yes [1]."]), **make)
    return TestClient(
        create_app(s, Services(search=service._search, answer=service)),
        raise_server_exceptions=False,
    )


def test_the_metrics_endpoint_is_text_and_not_in_the_contract(settings: Settings) -> None:
    with _client(settings) as client:
        body = client.get("/metrics")
        assert body.status_code == 200
        assert body.headers["content-type"].startswith("text/plain")
        assert "semsearch_http_requests_total" in body.text
        assert "/metrics" not in client.get("/openapi.json").json()["paths"]


def test_search_and_answer_are_counted(settings: Settings) -> None:
    before = sample("semsearch_search_requests_total", mode_used="hybrid")
    answers = sample("semsearch_answers_total", reason="answered")
    tokens = sample("semsearch_llm_tokens_total", direction="input")
    http = sample("semsearch_http_requests_total", route="/v1/search", method="POST", status="200")
    with _client(settings) as client:
        assert (
            client.post("/v1/search", json={"query": "late delivery"}, headers=HEADERS).status_code
            == 200
        )
        assert (
            client.post(
                "/v1/answer", json={"question": "late delivery"}, headers=HEADERS
            ).status_code
            == 200
        )
    assert sample("semsearch_search_requests_total", mode_used="hybrid") >= before + 2
    assert sample("semsearch_answers_total", reason="answered") == answers + 1
    assert sample("semsearch_llm_tokens_total", direction="input") > tokens
    assert (
        sample("semsearch_http_requests_total", route="/v1/search", method="POST", status="200")
        == http + 1
    )
    assert sample("semsearch_search_stage_duration_seconds_count", stage="total") > 0
    assert sample("semsearch_upstream_calls_total", dependency="elasticsearch", outcome="ok") > 0


def test_a_degraded_search_counts_as_a_fallback(settings: Settings) -> None:
    from typing import cast

    from elasticsearch import AsyncElasticsearch

    from app.core.retry import RetryPolicy
    from app.core.security import Identity
    from app.retrieval.query import QueryBuilder
    from app.retrieval.searcher import HybridSearcher
    from app.retrieval.service import SearchService

    embedder = FakeEmbedder(dims=4)
    embedder.fail_with = UpstreamUnavailableError()
    es = FakeSearchEs([chunk_doc("D1:0", "D1", "late delivery", users=["alice"])])
    cfg = settings.search
    searcher = HybridSearcher(
        cast(AsyncElasticsearch, es),
        cfg.index_alias,
        QueryBuilder(cfg),
        cfg,
        settings.elasticsearch,
        RetryPolicy(attempts=1),
    )
    reranker = FakeReranker()
    reranker.fail_with = UpstreamUnavailableError()
    service = SearchService(
        embedder=embedder, searcher=searcher, settings=settings, reranker=reranker
    )
    before = sample("semsearch_search_fallbacks_total", reason="hybrid_to_bm25")
    skipped = sample("semsearch_search_fallbacks_total", reason="rerank_skipped")
    errors = sample("semsearch_upstream_calls_total", dependency="embedding", outcome="error")
    result = asyncio.run(
        service.search(query="late", identity=Identity("alice", (), "s"), rerank=True)
    )
    assert result.mode_used == "bm25"
    assert sample("semsearch_search_fallbacks_total", reason="hybrid_to_bm25") == before + 1
    assert sample("semsearch_search_fallbacks_total", reason="rerank_skipped") == skipped + 1
    assert (
        sample("semsearch_upstream_calls_total", dependency="embedding", outcome="error")
        == errors + 1
    )


def test_a_failing_search_counts_the_error_code(settings: Settings) -> None:
    before = sample("semsearch_search_errors_total", code="UPSTREAM_UNAVAILABLE")
    s = settings.model_copy(
        update={
            "feature_flags": settings.feature_flags.model_copy(update={"semantic_search": False})
        }
    )
    with _client(s) as client:
        response = client.post("/v1/search", json={"query": "late"}, headers=HEADERS)
    assert response.status_code == 503
    assert (
        sample("semsearch_http_requests_total", route="/v1/search", method="POST", status="503")
        >= 1
    )
    assert sample("semsearch_search_errors_total", code="UPSTREAM_UNAVAILABLE") >= before


def test_the_http_route_label_is_the_template_not_the_raw_path(settings: Settings) -> None:
    with _client(settings) as client:
        client.get("/no/such/path/with-ids-12345")
    text = get_metrics().exposition().decode()
    assert "12345" not in text
    assert 'route="unmatched"' in text


def test_a_named_breaker_shows_its_state() -> None:
    breaker = CircuitBreaker(2, 100, name="unit-test")
    breaker.record_failure()
    assert sample("semsearch_circuit_breaker_open", name="unit-test") == 0
    breaker.record_failure()
    assert sample("semsearch_circuit_breaker_open", name="unit-test") == 1
    breaker.record_success()
    assert sample("semsearch_circuit_breaker_open", name="unit-test") == 0


def test_the_worker_counts_events_and_chunks() -> None:
    before = sample("semsearch_ingestion_events_total", result="indexed")
    rig = Rig([_doc_with("ITEM-1", "plain words")])
    asyncio.run(rig.worker.handle(_event(), FIRST_TRY))
    assert sample("semsearch_ingestion_events_total", result="indexed") == before + 1
    assert sample("semsearch_ingestion_event_duration_seconds_count") > 0


def _doc_with(item_id: str, text: str, pages: int = 3) -> SourceDocument:
    sentence = f"{text} number {{}} is written here for the test."
    return SourceDocument(
        item_id=item_id,
        pages=[
            SourcePage(page_no=p, text=" ".join(sentence.format(p * 10 + i) for i in range(6)))
            for p in range(1, pages + 1)
        ],
        doc_type="contract",
        tags=[],
        acl_users=["u1"],
        acl_groups=[],
        version=1,
    )


def test_a_failed_event_is_counted_and_raised() -> None:
    before = sample("semsearch_ingestion_events_total", result="failed")
    rig = Rig([_doc_with("ITEM-1", "plain words")])
    rig.indexer.fail_bulk_with = UpstreamUnavailableError()
    with pytest.raises(UpstreamUnavailableError):
        asyncio.run(rig.worker.handle(_event(), FIRST_TRY))
    assert sample("semsearch_ingestion_events_total", result="failed") == before + 1


# --- alert rules and the dashboard ----------------------------------------------------------

_METRIC = re.compile(r"\b(semsearch_[a-z0-9_]+)\b")


def _rules() -> list[dict[str, Any]]:
    data = yaml.safe_load((ROOT / "deploy/observability/prometheus-rules.yaml").read_text("utf-8"))
    return [rule for group in data["groups"] for rule in group["rules"]]


def test_every_service_metric_in_the_alerts_exists() -> None:
    declared = Metrics().declared_names()
    for rule in _rules():
        for name in _METRIC.findall(rule["expr"]):
            assert name in declared, (rule["alert"], name)


def test_every_service_metric_in_the_dashboard_exists() -> None:
    declared = Metrics().declared_names()
    dashboard = json.loads(
        (ROOT / "deploy/observability/grafana-dashboard.json").read_text("utf-8")
    )
    used = {
        name
        for panel in dashboard["panels"]
        for target in panel["targets"]
        for name in _METRIC.findall(target["expr"])
    }
    assert used and used <= declared, used - declared


def test_the_alerts_of_the_design_exist_and_say_what_to_do() -> None:
    names = {rule["alert"] for rule in _rules()}
    assert {
        "SearchLatencyHigh",
        "ErrorRateHigh",
        "ConsumerLagGrowing",
        "DlqGrowth",
        "GpuMemoryHigh",
        "ElasticsearchRejections",
        "FallbackRateHigh",
        "CostAnomaly",
    } <= names
    for rule in _rules():
        assert rule["labels"]["severity"] in {"info", "warning", "critical"}
        assert rule["annotations"]["summary"] and rule["annotations"]["action"]


def test_the_metric_labels_never_hold_free_text() -> None:
    """Label names are fixed and short. None of them is a place for a user, document or text."""
    forbidden = {"user", "user_id", "item_id", "doc_id", "chunk_id", "query", "question", "text"}
    for family in Metrics().registry.collect():
        for s in family.samples:
            assert not forbidden & set(s.labels), (s.name, s.labels)


# --- traces ---------------------------------------------------------------------------------


def test_spans_carry_ids_and_counts_only(settings: Settings, spans: Collect) -> None:
    with _client(settings) as client:
        client.post(
            "/v1/answer",
            json={"question": f"{CANARY} late delivery"},
            headers={**HEADERS, "X-Request-Id": "req-42"},
        )
    names = {s.name for s in spans.spans}
    assert {"search", "elasticsearch", "answer.llm", "llm"} <= names
    for s in spans.spans:
        assert set(s.attributes) <= ALLOWED_ATTRIBUTES, s.attributes
        assert CANARY not in json.dumps(dict(s.attributes))
    assert any(s.attributes.get("request_id") == "req-42" for s in spans.spans)
    search = next(s for s in spans.spans if s.name == "search")
    assert search.attributes["mode_used"].startswith("hybrid")


def test_unknown_attributes_and_long_or_complex_values_are_dropped(spans: Collect) -> None:
    with span("unit") as current:
        set_attributes(
            current,
            question="what is the secret",  # not an allowed name
            reason="x" * 500,  # allowed name, but too long to be an ID
            results=3,
            found=True,
            chunk_ids=["a:1", "b:2"],
            mode={"nested": "dict"},  # not a primitive
        )
    [finished] = spans.spans
    assert dict(finished.attributes) == {"results": 3, "found": True, "chunk_ids": "a:1,b:2"}


def test_an_exception_is_recorded_by_type_never_by_message(spans: Collect) -> None:
    with pytest.raises(ValueError), span("unit"):
        raise ValueError(f"{CANARY} leaked in a message")
    [finished] = spans.spans
    assert finished.attributes["error.type"] == "ValueError"
    assert CANARY not in str(finished.attributes) and not finished.events
    assert finished.status.status_code.name == "ERROR"


def test_without_a_collector_spans_do_nothing() -> None:
    configure_tracing(None)
    assert build_provider(ObservabilitySettings()) is None
    with span("unit", item_id="x") as current:
        assert current is None


# --- Langfuse -------------------------------------------------------------------------------

LF = LangfuseSettings(
    enabled=True,
    host="http://langfuse.test",
    public_key=SecretStr("pk-synthetic"),
    secret_key=SecretStr("sk-synthetic"),
    flush_interval_s=0.05,
    batch_size=10,
)


def _submit(sink: LangfuseSink, **changes: Any) -> None:
    values: dict[str, Any] = {
        "request_id": "req-1",
        "user_id": "alice",
        "model": "m@1",
        "prompt_version": "v1",
        "input_tokens": 100,
        "output_tokens": 10,
        "latency_ms": 900,
        "found": True,
        "reason": "answered",
        "mode_used": "hybrid",
    }
    sink.submit(**{**values, **changes})


async def test_langfuse_gets_metadata_only_with_a_hashed_user() -> None:
    async with httpx.AsyncClient() as client:
        sink = LangfuseSink(LF, client)
        with respx.mock() as router:
            route = router.post("http://langfuse.test/api/public/ingestion").respond(207, json={})
            await sink.start()
            _submit(sink)
            await sink.close()
    request = route.calls[0].request
    assert request.headers["authorization"].startswith("Basic ")
    body = json.loads(request.content)
    kinds = [event["type"] for event in body["batch"]]
    assert kinds == ["trace-create", "generation-create"]
    text = json.dumps(body)
    assert "alice" not in text and pseudonym("alice") in text
    for forbidden in ("question", 'input":', 'output":', 'answer":'):
        assert forbidden not in text.replace('"input": 100', "").replace('"output": 10', "")
    generation = body["batch"][1]["body"]
    assert generation["model"] == "m@1" and generation["usage"] == {
        "input": 100,
        "output": 10,
        "unit": "TOKENS",
    }
    assert generation["traceId"] == "req-1"


async def test_a_failing_langfuse_never_raises_and_is_counted() -> None:
    before = sample("semsearch_telemetry_dropped_total", sink="langfuse")
    async with httpx.AsyncClient() as client:
        sink = LangfuseSink(LF, client)
        with respx.mock() as router:
            router.post("http://langfuse.test/api/public/ingestion").respond(500)
            await sink.start()
            _submit(sink)
            await sink.close()
    assert sample("semsearch_telemetry_dropped_total", sink="langfuse") == before + 2


async def test_a_full_queue_drops_instead_of_blocking() -> None:
    before = sample("semsearch_telemetry_dropped_total", sink="langfuse")
    small = LF.model_copy(update={"queue_size": 2})
    async with httpx.AsyncClient() as client:
        sink = LangfuseSink(small, client)  # not started: nothing drains the queue
        for _ in range(3):
            _submit(sink)
    assert sample("semsearch_telemetry_dropped_total", sink="langfuse") == before + 4


async def test_missing_credentials_drop_the_batch_without_a_call() -> None:
    open_ = LF.model_copy(update={"public_key": None})
    async with httpx.AsyncClient() as client:
        sink = LangfuseSink(open_, client)
        with respx.mock(assert_all_called=False) as router:
            route = router.post("http://langfuse.test/api/public/ingestion").respond(207)
            await sink.start()
            _submit(sink)
            await sink.close()
    assert route.call_count == 0


def test_the_answer_endpoint_sends_telemetry_when_a_sink_is_set(settings: Settings) -> None:
    sent: list[dict[str, Any]] = []

    class Spy:
        def submit(self, **kwargs: Any) -> None:
            sent.append(kwargs)

        async def start(self) -> None:
            pass

        async def close(self) -> None:
            pass

    s = settings.model_copy(
        update={
            "api": ApiSettings(
                service_tokens={"java-search": SecretStr("token-java")},
                identity_services=["java-search"],
            )
        }
    )
    service, _ = make_service(s, FakeLLMClient(["Yes [1]."]))
    services = Services(search=service._search, answer=service, langfuse=Spy())  # type: ignore[arg-type]
    with TestClient(create_app(s, services)) as client:
        client.post("/v1/answer", json={"question": "late delivery"}, headers=HEADERS)
        client.post("/v1/answer/stream", json={"question": "late delivery"}, headers=HEADERS)
    assert len(sent) == 2
    for call in sent:
        assert set(call) == {
            "request_id",
            "user_id",
            "model",
            "prompt_version",
            "input_tokens",
            "output_tokens",
            "latency_ms",
            "found",
            "reason",
            "mode_used",
        }


# --- no sensitive text anywhere (rule 2) ----------------------------------------------------

QUESTION = f"{CANARY}-question late delivery penalty"
DOC_TEXT = f"{CANARY}-document The late delivery penalty is one percent."
ANSWER = f"{CANARY}-answer one percent [1]."


def _leak_services(settings: Settings, llm: FakeLLMClient, langfuse: Any = None) -> TestClient:
    s = settings.model_copy(
        update={
            "api": ApiSettings(
                service_tokens={"java-search": SecretStr("token-java")},
                identity_services=["java-search"],
            )
        }
    )
    es = FakeSearchEs([chunk_doc("D1:0", "D1", DOC_TEXT, users=["alice"], pages=(4, 4))])
    service, _ = make_service(s, llm, es=es, reranker=FakeReranker())
    return TestClient(
        create_app(s, Services(search=service._search, answer=service, langfuse=langfuse)),
        raise_server_exceptions=False,
    )


def test_a_canary_text_reaches_no_log_metric_trace_or_telemetry(
    settings: Settings, spans: Collect, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("DEBUG", True)
    sent: list[dict[str, Any]] = []

    class Spy:
        def submit(self, **kwargs: Any) -> None:
            sent.append(kwargs)

        async def start(self) -> None:
            pass

        async def close(self) -> None:
            pass

    llm = FakeLLMClient([ANSWER])
    client = _leak_services(settings, llm, Spy())
    error_bodies: list[str] = []
    with client:
        client.post("/v1/search", json={"query": QUESTION}, headers=HEADERS)
        client.post("/v1/answer", json={"question": QUESTION}, headers=HEADERS)
        client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS)
        # Error paths: a model failure, a bad request, a missing token, an over-long question.
        llm.fail_with = UpstreamUnavailableError()
        error_bodies.append(
            client.post("/v1/answer", json={"question": QUESTION}, headers=HEADERS).text
        )
        error_bodies.append(
            client.post("/v1/answer/stream", json={"question": QUESTION}, headers=HEADERS).text
        )
        error_bodies.append(
            client.post("/v1/answer", json={"question": QUESTION, "x": 1}, headers=HEADERS).text
        )
        error_bodies.append(client.post("/v1/search", json={"query": QUESTION}).text)
        error_bodies.append(
            client.post("/v1/answer", json={"question": QUESTION * 100}, headers=HEADERS).text
        )
        metrics_text = client.get("/metrics").text
    configure_tracing(None)
    out = capsys.readouterr().out
    exported = json.dumps(
        [
            {"name": s.name, "attributes": dict(s.attributes), "events": [str(e) for e in s.events]}
            for s in spans.spans
        ]
    )
    assert out, "the test must have produced log output"
    for where, text in {
        "logs": out,
        "metrics": metrics_text,
        "spans": exported,
        "telemetry": json.dumps(sent),
        "error responses": " ".join(error_bodies),
    }.items():
        assert CANARY not in text, where


async def test_a_canary_in_a_document_reaches_no_log_or_metric_in_the_worker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("DEBUG", True)
    rig = Rig([_doc_with("ITEM-1", f"{CANARY}-text")])
    await rig.worker.handle(_event(), FIRST_TRY)
    rig.indexer.fail_bulk_with = UpstreamUnavailableError(f"{CANARY}-error")
    rig.source.documents["ITEM-1"] = _doc_with("ITEM-1", f"{CANARY}-text changed")
    rig.source.documents["ITEM-1"].version = 2
    with pytest.raises(UpstreamUnavailableError):
        await rig.worker.handle(_event(), FIRST_TRY)
    out = capsys.readouterr().out
    assert "event_processed" in out
    assert CANARY not in out, "the worker wrote document text to the logs"
    assert CANARY not in get_metrics().exposition().decode()
