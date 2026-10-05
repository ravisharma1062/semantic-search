# Threat model check (T5.3)

This page checks the threat model of HLD section 9 against what exists in the repository: the control,
the test that proves it, and what is **not** covered by code and still needs a person. It is a
checklist for the security review, not a certificate. The security team owns the final list.

Legend: **Code** = done and tested in this repository. **Config** = done in the chart or the
pipeline, to be verified in a real environment. **Open** = needs the platform, security or business
team.

## Threats

| Threat | Controls in this repository | Proof | Status |
| --- | --- | --- | --- |
| **Access-control bypass** | Every Elasticsearch search is built by `retrieval.QueryBuilder` and carries the filter from `retrieval.AclFilter` (rule 1). The builder verifies the filter is in the text query, inside kNN and in both retriever legs, and refuses to build without it. The same service feeds `/v1/search`, the reranker and the RAG prompt, so restricted chunks never reach the reranker or the model | `tests/unit/test_query_builder.py`, `tests/unit/test_query_inventory.py` (an AST test: no search call outside the builder), `tests/unit/test_access_isolation.py`, `tests/integration/test_search_es.py` (many users, all modes, real Elasticsearch, also for the RAG context). CI job "Access isolation" (`pytest -m access`) | Code |
| **Data leaves the network** | In-house models by default. OpenAI only with `provider: openai` **and** `allow_external: true` **and** a key, through the `proxy` setting. The network policy denies egress except an allow-list | `tests/unit/test_llm_client.py::test_openai_is_off_unless_allowed_and_has_a_key`, the same for embeddings. Chart: `networkPolicy.egress` | Code, Config. **Open:** the egress allow-list, the proxy, and the data classes approved for OpenAI |
| **Prompt injection from documents** | The prompt calls the context data and tells the model to ignore instructions in it. Values are put in with one pass, so `{{...}}` in a document is not expanded. The model has no tools. The model writes only source numbers, and the service maps them to documents from its own list. An answer without a valid citation is rejected. Guardrails mask PII and can block topics | `tests/unit/test_answer_service.py` (instructions in documents stay data, an answer that obeys them is rejected, invented citations never become sources), `tests/unit/test_rag_pieces.py` | Code. **Open:** the HLD asks for prompts that try to reveal other documents or the system prompt: run them against the real model with the evaluation owner. A fake model cannot show that a real one resists |
| **Sensitive data in logs and traces** | Rule 2. A log processor redacts known text fields. Spans keep only allow-listed attribute names and short values. Metrics have fixed labels. LLM telemetry is metadata only, with a hashed user. Errors are fixed texts (the body of an upstream answer is never copied) | `tests/unit/test_observability.py::test_a_canary_text_reaches_no_log_metric_trace_or_telemetry` (a canary string in the question, the document and the answer, through search, answers, streaming and error paths, checked in logs, metrics, spans, Langfuse payloads and error responses), the worker version of it, and the error tests in `test_llm_client.py` and `test_rerank.py` | Code |
| **Model supply chain** | The service loads no model: models run on separate servers. The tokenizer is a local file. Nothing is downloaded at runtime. The chart mounts model files that come from the internal artifact repository | Chart values (`APP_CHUNKING__TOKENIZER_FILE`) | Config. **Open:** checksums and scans of the model files in the artifact repository (platform and ML) |
| **Vulnerable dependencies** | Versions are pinned in `uv.lock`. CI runs `pip-audit` on the locked runtime dependencies and Trivy on the image and on the Dockerfile and Helm files. The image runs as non-root with a read-only file system and no capabilities | `.github/workflows/ci.yml` (jobs `dependencies`, `docker`), `deploy/helm/semantic-search/templates/_helpers.tpl` (security context) | Config. **Open:** approved base images from the internal registry (the Dockerfile uses a placeholder), SCA policy and the exceptions process (`.trivyignore` needs a reason and an owner per line) |
| **Denial of service and cost abuse** | Rate limits per user and per service (`api.rate_limit_*`), a time limit on every call, a maximum question length, `top_k` limits, a context budget, a circuit breaker per model server, an alert on token use | `tests/unit/test_security_and_ratelimit.py`, `tests/unit/test_api_failures.py`, the alerts `RateLimitingHigh` and `CostAnomaly` | Code. **Open:** quotas per business unit, and the budget for the cost alert |
| **Information in vectors** | Vectors live only in the chunk index, with the same access filter as the text. The API never returns a vector or a chunk text beyond a snippet | `tests/unit/test_api_search.py` (no `content` in a result), the Elasticsearch source filter in `retrieval/query.py` | Code. **Open:** encryption at rest and disk access (platform) |
| **Stale permissions** | A permission event updates only the `acl_*` fields. Reconciliation compares the permission hash of the source with the index and republishes the difference. Deletes and permission changes use the live topic | `tests/unit/test_worker.py` (ACL change without embedding again), `tests/unit/test_reconcile.py`, `tests/integration/test_backfill_es_kafka.py` | Code. **Open:** the nightly schedule and the target freshness (HLD: 95% within 5 minutes, to be agreed) |
| **Misuse of admin endpoints** | A separate admin token that a service token cannot replace, and an admin token cannot call the user API. Every admin call, also a refused one, writes an audit line with the actor, the action, the outcome and IDs, and counts a metric. Admin calls are rate limited. Document IDs are validated | `tests/unit/test_admin_api.py` | Code. **Open:** role-based admin access beyond one token per admin service (who is an admin is an identity-provider decision), and shipping the audit lines to the central audit store |

## Other controls from HLD section 9

| Area | Status |
| --- | --- |
| Identity: only the trusted Java service may send the user ID and groups | Code (`ApiSettings.identity_services`, `tests/unit/test_security_and_ratelimit.py`). mTLS at the ingress is **Open** (platform) |
| Pre-filter, not post-filter | Code: the filter is inside the kNN query (`test_search_es.py::test_knn_filters_before_it_picks_neighbours`) |
| TLS and KMS | **Open** (platform) |
| Secrets in a secret store | Config: the chart reads one Kubernetes Secret (`secretName`). Rotation: runbook 9. The store itself is **Open** |
| PII masking before text goes to models | Code (`rag/pii.py`, in-house, regular expressions, e-mail, phone, card, IBAN, PAN, Aadhaar). The context sent to the LLM and the answer are masked. **Open:** the embedding path does not mask (the HLD says "where policy needs it"): decide per document class. Test the masker on approved real documents |
| Audit: who asked, which documents were used | Code: `search_done` and `answer_done` log lines have the user and the chunk IDs, no text. **Open:** shipping to the audit store and the retention |

## Security tests before go-live (HLD)

| Test | Status |
| --- | --- |
| Users with different access levels run the same queries | Done in CI, on real Elasticsearch |
| Prompts that try to reveal other documents or instructions | Partly: injection tests with a fake model. Needs a run against the real model |
| Penetration test of the service and its network paths | **Open:** a person, after the Test environment exists. Give them the OpenAPI file, this page and the network policy |

## What this page does not claim

Nothing here was run against a real cluster, real models or real documents. The checks of the
controls are tests with fakes and, for Elasticsearch, Kafka and Redis, real containers with synthetic
data. The accepted-risk list (`.trivyignore`) is empty.
