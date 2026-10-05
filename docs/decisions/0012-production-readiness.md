# 0012: Production readiness (T5.1 to T5.5)

Status: accepted (defaults). Nothing in this phase has run against a real cluster, real models or
real documents. What is proven and what is not is stated per item.

## Observability (T5.1)

- **Metrics:** one private Prometheus registry (`observability/metrics.py`), fixed labels, no user,
  document or text in any label. API on `/metrics`, worker on a port. New dependency:
  `prometheus-client` (named in the stack of CLAUDE.md).
- **Traces:** OpenTelemetry through `observability/tracing.span()`. Attribute names come from an
  allow-list and values must be short, so text cannot reach a trace by accident. Exceptions are
  recorded by type only. Off without an OTLP endpoint. New dependencies: `opentelemetry-api`, `-sdk`,
  `-exporter-otlp-proto-http`.
- **LLM telemetry:** Langfuse through its HTTP ingestion API, no SDK. Metadata only (model, prompt
  version, tokens, latency, outcome, hashed user). A bounded queue and a background sender: telemetry
  can never block or break an answer.
- **Alerts and dashboard:** the alerts of HLD section 18 plus a few more, validated by `promtool` in
  CI. A unit test fails if an alert or a panel uses a metric the service does not export. Metrics from
  the platform (`kafka_*`, `DCGM_*`, `elasticsearch_*`) are assumed by their usual exporter names: check
  them against the exporters in use.
- **Leak test:** a canary string goes through search, answers, streaming and the error paths and the
  worker. It must not appear in logs, metrics, spans, Langfuse payloads or error responses.
- A fix found on the way: a circuit breaker that was open now closes on `record_success()`.

## Load and performance (T5.2)

`loadtest/` has an open-loop generator (no coordinated omission), nearest-rank percentiles and SLO
checks, tested in process against the real app. `docs/performance.md` has the method, the test plan
and the tuning levers. **It has no numbers, on purpose.** They need the real cluster and models.

## Security (T5.3)

- CI: `pip-audit` on the locked runtime dependencies, Trivy on the image and on the Dockerfile and
  charts, `.trivyignore` (empty) for accepted findings. Both are CI tools, not dependencies.
- `docs/threat-model-check.md` maps each threat of HLD section 9 to its control, its test and what is
  still open (egress allow-list, approved base images, role-based admin access, penetration test,
  prompts against a real model).
- **Admin API** (`/v1/index/documents`, `/v1/documents/{docId}`, `/v1/admin/reindex`, status): a
  separate admin token, an audit line for every call (also refused ones, without the token), IDs
  validated. Manual index and delete publish an `ITEM_ID` event to the live topic: one code path and one
  set of idempotency rules for all changes. The API never runs a long scan: a re-index is *requested*
  (job status `REQUESTED`) and `backfill run-requested` in a job process runs it.
- `NOT_FOUND` was added as an API error code (admin job status). It is additive.

## Operations (T5.4)

- Runbooks 1 to 9, plus release and rollback.
- **DLQ replay tool** (`app.jobs.dlq_replay`): the runbooks needed one and it did not exist. It
  republishes valid events to the live topic with the original key and value, commits after the
  publish, skips invalid messages, and has a dry run on a group of its own.
- **Snapshots** (`app.jobs.snapshot`): snapshot the indices behind the alias and the state index;
  restore always under a new name, never over a live index. Found by the real cluster: snapshot names
  must be lowercase. The repository itself is the platform team's.
- `ingestion.backfill_consumer_enabled` switches the backfill consumer off without touching live
  indexing.
- API failure tests: every dependency failure gives either a clear fallback error (503, 504) or a
  successful answer whose `mode_used` says what ran. An unusable LLM answer is a 503, not a 500.

## Deployment (T5.5)

- Helm: HPA on CPU for the API, KEDA on Kafka lag for the workers, PodDisruptionBudgets, anti-affinity
  and zone spread, default-deny NetworkPolicies with explicit flows (the chart refuses to render
  without `apiIngressFrom` when policies are on), scrape annotations. Rendered and linted with Helm 3.16
  in a container.
- **Canary design:** a second API Deployment (`api-canary`) with the new image shares the Service with
  the stable pods, so traffic splits by replica count. No service mesh or weighted ingress is assumed.
  `loadtest/canary_check.py` compares the canary with the stable pods in Prometheus and fails a canary
  without traffic. Rollback of a canary is removing it. Workers use the normal rolling update.
- **Pipeline** (`.github/workflows/deploy.yml`): build, scan, push, deploy to Test with the evaluation
  smoke gates and a load check, manual approval through the `production` environment, canary, full
  rollout, rollback. Checked with `actionlint`. **Not run:** it needs the registry, the clusters, the
  secrets and the environment values that the platform team provides. The first run in Test is its
  verification.

## Open items

- Everything that needs real systems (see above), and the business evaluation set that sets
  `reranker` use, `rag.min_score` and the quality-gate thresholds in the pipeline (the values in
  `deploy.yml` are placeholders).
- The architect's decision on live updates and the state index during a re-index (decision 0007).
- Who is an admin (role-based access) and shipping audit lines to the central audit store.
- The cost alert threshold (a placeholder), and the SLO targets (proposed in the HLD, to be agreed).
