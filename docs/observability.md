# Observability (T5.1)

Rule 2 holds everywhere on this page: no document text, chunk text, question or answer in a log, a
metric, a trace, an LLM trace or an error. IDs and numbers only. A test proves it with a canary
string (`tests/unit/test_observability.py`).

## Metrics (Prometheus)

API: `GET /metrics` on the API port. Worker: port `observability.worker_metrics_port` (9100). Both are
scraped from inside the cluster (pod annotations in the chart, the NetworkPolicy lets only the
monitoring namespace in). `/metrics` is not part of the OpenAPI contract and has no token.

All names start with `semsearch_`. Labels are fixed short values: no user, no document ID, no text,
and the HTTP route is the route template, never the raw path.

| Metric | Labels | What it shows |
| --- | --- | --- |
| `http_requests_total`, `http_request_duration_seconds` | route, method, status | Traffic, errors, latency per route |
| `search_requests_total` | mode_used | Which search really ran (`hybrid+rerank`, `hybrid`, `bm25`, `knn`) |
| `search_fallbacks_total` | reason | A simpler search than requested (`hybrid_to_bm25`, `rerank_skipped`, ...) |
| `search_stage_duration_seconds` | stage | `embed`, `retrieve`, `rerank`, `total` |
| `search_errors_total` | code | Failed searches by API error code |
| `search_results_returned` | | Results per search (a drop means empty answers) |
| `upstream_calls_total`, `upstream_call_duration_seconds` | dependency, outcome | Calls to `embedding`, `reranker`, `llm`, `elasticsearch`: `ok`, `error`, `timeout`, `breaker_open` |
| `circuit_breaker_open` | name | 1 while a breaker is open (`reranker`, `llm`) |
| `cache_lookups_total` | cache, result | Query embedding cache hits and misses |
| `answers_total` | reason | `answered`, `not_found`, `low_relevance`, `unverified`, `blocked`, `refused`, `error` |
| `llm_tokens_total` | direction | Input and output tokens (the cost) |
| `answer_first_token_seconds` | | Time to the first streamed token (SLO: p95 under 3 s) |
| `ingestion_events_total` | result | `indexed`, `unchanged`, `skipped`, `deleted`, `stale_event`, `failed`, `retried`, `dead_lettered`, `invalid` |
| `ingestion_event_duration_seconds` | | Time to handle one event |
| `ingestion_dlq_total` | reason | Messages sent to the dead-letter topic |
| `ingestion_chunks_written_total`, `ingestion_bulk_item_failures_total` | | Elasticsearch bulk results |
| `rate_limited_total` | scope | Requests over a limit (`user`, `service`) |
| `admin_actions_total` | action, outcome | Admin API calls (also the refused ones) |
| `telemetry_dropped_total` | sink | LLM telemetry that was not sent |

Kafka lag, GPU and Elasticsearch pressure come from the platform exporters (`kafka_*`, `DCGM_*`,
`elasticsearch_*`). The alert rules use their usual names: check them against the exporters in use.

## Alerts and dashboard

- `deploy/observability/prometheus-rules.yaml`: the alerts of HLD section 18 (latency, error rate,
  fallback rate, lag, DLQ growth, GPU, Elasticsearch pressure, cost) and a few more (breaker open, RAG
  first token, rate limiting, telemetry dropping). Every alert has a severity and an action that
  points at a runbook. Checked with `promtool check rules` in CI.
- `deploy/observability/grafana-dashboard.json`: 20 panels in the order a person reads them.
- A unit test fails if an alert or a panel uses a `semsearch_*` metric that the service does not
  export, so a rename cannot silently break monitoring.
- Thresholds are the HLD starting values. The cost threshold is a placeholder: set it from the budget.

## Traces (OpenTelemetry)

Set `observability.otlp_endpoint` (OTLP over HTTP) to export. Without it spans are no-ops.
`observability.trace_sample_ratio` (default 0.1) samples. Spans: `search`, `elasticsearch`,
`embedding`, `reranker`, `answer.llm`, `llm`, and `ingest.event` in the worker. Attributes are limited
to an allow-list in `observability/tracing.py` (request ID, item ID, mode, counts, token counts,
error type) and values must be short. An exception is recorded by type only, never by message.

## LLM telemetry (Langfuse)

Enable with `observability.langfuse.enabled`, `host`, `public_key`, `secret_key`. The service sends
events over Langfuse's HTTP ingestion API in the background (no SDK, no new dependency): one trace and
one generation per answer, with model, prompt version, token counts, latency, outcome and a hashed
user ID. No question, no passages, no answer. A full queue or a failing Langfuse drops events and
counts `telemetry_dropped_total`: telemetry never slows down or breaks an answer.

## Logs

One JSON line per event (`log_json`), request ID on every line. `http_request` (method, route path,
status, duration), `search_done` and `answer_done` (user ID, `mode_used`, chunk IDs, counts, tokens),
worker lines with `item_id`, `admin_action` lines (`audit=true`) for the audit store. A processor
redacts the field names `text`, `content`, `question`, `query`, `answer`, `prompt`, `snippet`, `body`
as a safety net, but the rule is not to log them in the first place.
