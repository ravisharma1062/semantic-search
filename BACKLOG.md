# BACKLOG.md: Development tasks for Claude Code

Task IDs match the work packages in section 21 of the HLD.
Work package 0.x (approvals, data profiling, GPU benchmark, evaluation questions) is human work and is not listed here, except where Claude Code can help.

## How to run a task with Claude Code

1. Start a fresh session for each task. Give it the task ID.
2. Starter prompt:

   > Read CLAUDE.md and docs/HLD.md. We are doing task T<id> from BACKLOG.md. First write a plan: files to change, approach, tests, risks. Do not write code until I approve the plan.

3. Review the plan. Fix it, then say "go ahead".
4. Ask Claude to write the tests first, then the code, then to run ruff, mypy and pytest.
5. Review the diff yourself. Ask for changes. Then open the pull request.
6. Mark the task done only when every "Done when" line is true.

Tip: use plan mode for the plan step, and keep each session to one task, so the context stays small and focused.

## Order of work

```
T1.1 -> T1.2 -> T1.3 -> T1.4 -> T1.5 -> T1.6 -> T1.7 -> T1.8   (indexing pipeline)
T1.9 (Java repo, in parallel)
T2.1 -> T2.2 -> T2.4 -> T2.5 -> T2.6 -> T2.3                    (hybrid search)
T3.1 -> T3.2                                                    (reranking)
T4.1 -> T4.2 -> T4.3 -> T4.4                                    (RAG)
T5.1 -> T5.2 -> T5.3 -> T5.4 -> T5.5                            (production readiness)
```

T3 and T4 can run in parallel after T2 if there are enough engineers.

---

## Phase 1: Foundation and indexing

### T1.1 Project skeleton, CI/CD, Docker, Helm (6-8 days)
- **Goal:** A working empty service with the folder layout from CLAUDE.md, quality checks and a local environment.
- **Depends on:** none.
- **Done when:**
  - `uv sync`, `pytest`, `ruff`, `mypy` all run in CI.
  - `GET /health/live` and `/health/ready` work.
  - Settings load from environment and yaml. Structured logging with request ID is on.
  - Dockerfile builds a non-root image. A basic Helm chart exists with `api`, `worker` and `batch` modes.
  - `docker compose` starts Elasticsearch, Kafka, Redis and a fake model server locally.
  - A fakes package exists in `tests/fakes` with fake `Embedder`, `Reranker`, `LLMClient`, `SourceReader`.
- **Scope decisions** (see `docs/decisions/0001-skeleton.md`): CI is GitHub Actions. The Helm chart has no HPA, KEDA, PodDisruptionBudget, anti-affinity or NetworkPolicy (moved to T5.5). There are no integration tests yet. The retry helper in `core` was deferred to T1.2 and is done there.

### T1.2 Kafka consumer framework (8-10 days)
- **Goal:** A reusable consumer with safe commit, retry, retry topic and DLQ.
- **Depends on:** T1.1.
- **Done in T1.2 (moved from T1.1):** the shared retry helper in `core/retry.py`, and the CI integration step no longer tolerates "no tests collected". The worker mode stays an idle stub until T1.6, because a consumer with no real handler would commit events without indexing them. T1.6 wires it and adds the worker probe to the Helm chart.
- **Done when:**
  - Event schema v1 (`UPSERT`, `DELETE`, `ACL_CHANGE`) is validated with Pydantic. Bad messages go to the DLQ with the error.
  - Offsets are committed only after the handler succeeds.
  - 3 quick retries in process, then the retry topic with delay, then the DLQ after the limit.
  - Events for the same `item_id` waiting in a batch are coalesced.
  - Graceful shutdown finishes the current message.
  - Integration tests with Testcontainers cover duplicates, poison messages and rebalance.

### T1.3 Source reader and OCR text normalizer (5-8 days)
- **Goal:** Read a document by `ITEM_ID` from the existing index and produce clean text with page information.
- **Depends on:** T1.1.
- **Done when:**
  - `SourceReader.get` and `get_many` work through `_mget`, with timeouts and retries.
  - A `SourceDocument` model carries text per page (or document level when pages are missing), metadata and ACL fields. Field names come from settings.
  - The normalizer fixes common OCR problems (hyphenation, broken lines, repeated headers and footers) without losing page mapping.
  - Missing, empty and huge documents are handled and tested.
- **Note:** Field names must be confirmed with the Java team first.

### T1.4 Chunker (8-12 days)
- **Goal:** Split a document into chunks of about 400 tokens with overlap.
- **Depends on:** T1.3.
- **Done when:**
  - Splitting follows headings and paragraphs first, then size. Target 400 tokens, maximum 512, overlap 60. All values come from settings.
  - Tables are kept as separate chunks and never cut in the middle of a row.
  - Each chunk has `chunk_no`, `page_start`, `page_end`, `section_title`, `content_hash` and the deterministic `chunk_id`.
  - Tokens are counted with the embedding model's tokenizer.
  - A 3,000 page document is processed without high memory use (stream by page ranges).
  - Property-based tests: no text is lost, order is kept, no chunk exceeds the maximum.

### T1.5 Embedding client and embedding server deployment (8-12 days)
- **Goal:** Create vectors through the `Embedder` interface using a model server.
- **Depends on:** T1.1.
- **Done when:**
  - The in-house provider calls the embedding server over HTTP with batching, a concurrency limit, timeouts and retries.
  - The OpenAI provider exists behind configuration and is off by default.
  - Query embeddings are cached in Redis (the key includes the model version).
  - Helm values and a deployment recipe for the embedding server exist (Hugging Face TEI or an approved equivalent).
  - Tests use the fake server and `respx`. Failure and overload cases are covered.

### T1.6 Indexer, index templates, alias scripts, state store (8-10 days)
- **Goal:** Write chunks and state safely, and manage index versions.
- **Depends on:** T1.4, T1.5.
- **Done when:**
  - Index templates for the chunk index and the state index match section 5 and section 17 of the HLD.
  - Bulk writes retry only failed items. Writing the same chunk twice overwrites it.
  - Stale chunks of a document are deleted after a successful write.
  - `StateStore` tracks `PENDING`, `INDEXED`, `SKIPPED`, `FAILED`, `DELETED`, and skips unchanged documents (same hash and model).
  - Scripts: create index, switch alias, delete an old version.
  - The worker loop from the HLD is complete end to end with fakes, and with real Elasticsearch in an integration test.

### T1.7 Backfill producer and wave control (6-8 days)
- **Goal:** Index existing documents in waves without hurting live traffic.
- **Depends on:** T1.6.
- **Done when:**
  - A job scans the existing index in a stable order and publishes `ITEM_ID` events to the backfill topic with a wave number.
  - Speed is limited by configuration, and the job can pause and resume.
  - A progress report shows counts per status and wave from the state index.
  - A reconciliation job compares the state index with the source and republishes the differences.

### T1.8 Tests for the indexing pipeline (8-10 days)
- **Goal:** A strong test suite for the whole indexing path.
- **Depends on:** T1.2 to T1.7.
- **Done when:**
  - End-to-end test: event, read, chunk, embed, index, state, commit.
  - Fault tests: Elasticsearch bulk errors, embedding timeouts, worker crash and restart, duplicate and out-of-order events, delete after update.
  - Coverage report for `ingestion` and `store` is above the team's threshold.

### T1.9 Java app: publish ITEM_ID events (6-8 days) — Java repository
- **Goal:** The Java app sends events for new, changed, deleted documents and permission changes.
- **Depends on:** event schema from T1.2 (agree early).
- **Done when:**
  - Events follow schema v1 and use `ITEM_ID` as the Kafka key.
  - Producer retries are configured. A failed publish does not break the existing flow.
  - Tests cover each event type. A feature flag can turn publishing off.
- **Note:** This task is for the Java team. Claude Code can help there too, with its own `CLAUDE.md`.

---

## Phase 2: Hybrid search

### T2.1 Query builder, ACL filter, hybrid search (8-10 days)
- **Goal:** One safe place that builds all search queries.
- **Depends on:** T1.6.
- **Done when:**
  - `AclFilter.from_identity` builds the filter from user and groups. It is always added, also inside the kNN part.
  - `HybridSearcher` runs BM25 + kNN with RRF (the retriever API, or a Python merge if the version does not support it, selected by settings).
  - Filters for document type, date and tags work.
  - Results include `doc_id`, `chunk_id`, pages, score, snippet and highlights.
  - Unit tests check the generated queries. Integration tests prove the access rules with at least 3 users of different rights.

### T2.2 Search API (6-8 days)
- **Goal:** `POST /v1/search` as defined in section 8 of the HLD.
- **Depends on:** T2.1.
- **Done when:**
  - Request and response models match the OpenAPI file. Errors use the agreed codes.
  - Service authentication works. User identity headers are accepted only from the trusted caller.
  - Rate limits, time budgets and `mode_used` are in place.
  - If embedding or kNN fails, the API runs BM25 only and reports it in `mode_used`.
  - Contract tests pass against the OpenAPI file.

### T2.3 Java integration, feature flag, fallback (6-8 days) — Java repository
- **Goal:** The Java Search API calls the new service and falls back to keyword search.
- **Depends on:** T2.2.
- **Done when:**
  - The Java client waits at most 3 seconds, then falls back to the existing keyword search.
  - A feature flag controls off, internal users, a pilot group and all users.
  - The fallback rate is measured and logged.

### T2.4 Redis cache for query embeddings (3-4 days)
- **Goal:** Faster repeated queries without risk.
- **Depends on:** T1.5, T2.2.
- **Done when:**
  - Cache keys include the model version. TTL comes from settings.
  - A Redis outage only slows requests down. It never fails them.
  - Answers are never cached across different access scopes.

### T2.5 Evaluation runner and retrieval metrics (6-8 days)
- **Goal:** Measure search quality on the evaluation set.
- **Depends on:** T2.2.
- **Done when:**
  - A runner reads the evaluation file (questions with the correct documents and pages), calls the search, and reports recall@k, MRR and nDCG@10.
  - Results are saved with the config used (models, chunker version, search settings).
  - A small smoke set runs in CI. The full set runs on demand.

### T2.6 Access isolation tests (4-5 days)
- **Goal:** Prove that users see only their documents.
- **Depends on:** T2.1.
- **Done when:**
  - An automated suite with several test users runs the same queries and checks that results differ as expected.
  - It covers search, highlights, snippets and later RAG context.
  - The suite runs in CI and blocks the merge on failure.

---

## Phase 3: Reranking

### T3.1 Reranker server and adapter (5-7 days)
- **Goal:** Rerank the top candidates through the `Reranker` interface.
- **Depends on:** T2.2.
- **Done when:**
  - The in-house provider calls the reranker server with batching, timeouts and retries.
  - Reranking can be switched on or off per request and by configuration.
  - If the reranker fails or is slow, the API returns the RRF order and reports it in `mode_used`.

### T3.2 Experiments and tuning (6-8 days)
- **Goal:** Choose settings with evidence.
- **Depends on:** T2.5, T3.1.
- **Done when:**
  - Reports compare BM25 only, kNN only and hybrid, with and without reranking, and at least two chunk sizes.
  - A short decision note records the chosen values and the metric gain.

---

## Phase 4: RAG

### T4.1 RAG pipeline (8-10 days)
- **Goal:** Build the answer pipeline with the score gate.
- **Depends on:** T3.1.
- **Done when:**
  - The context builder takes the top 5 to 8 chunks, removes near-duplicates, respects the token budget and numbers the chunks.
  - The score gate returns "not found" without calling the LLM when retrieval is weak.
  - The prompt comes from `prompts/` with a version number.

### T4.2 LLM adapters and streaming (6-8 days)
- **Goal:** Call the LLM through the `LLMClient` interface, with streaming.
- **Depends on:** T4.1.
- **Done when:**
  - In-house and OpenAI providers exist. The OpenAI provider is off by default.
  - `POST /v1/answer` and `POST /v1/answer/stream` (Server-Sent Events) work.
  - Time limits and clear errors are in place.

### T4.3 Citation mapping, guardrails, PII masking (6-8 days)
- **Goal:** Safe and verifiable answers.
- **Depends on:** T4.1.
- **Done when:**
  - The model returns only source numbers. The service maps them to document IDs and pages.
  - Answers with missing or invalid citations are rejected or flagged.
  - A guardrail hook and PII masking run where the policy requires.
  - Prompt injection tests: documents that contain instructions do not change the behaviour.

### T4.4 RAG evaluation and prompt tuning (5-6 days)
- **Goal:** Measure answer quality.
- **Depends on:** T4.2, T4.3.
- **Done when:**
  - The runner reports faithfulness, answer relevance, citation accuracy and "not found" accuracy.
  - Prompt versions are compared and the choice is documented.

---

## Phase 5: Production readiness

### T5.1 Observability (6-8 days)
- **Done when:** Metrics, traces, LLM telemetry and dashboards cover every interface in the HLD. Alerts from section 18 exist. No sensitive text appears anywhere (tested).

### T5.2 Load and performance tests, Elasticsearch tuning (8-12 days)
- **Done when:** Load tests show p95 under 3 seconds at the target traffic and at 10%, 50% and 100% of the target index size. Tuning notes are written.

### T5.3 Security review support and fixes (6-8 days)
- **Done when:** Dependency and image scans are clean or approved. Findings from the security review and penetration test are fixed. The threat model in section 9 is checked against the code.

### T5.4 Runbooks, alerts, failure and recovery tests (5-6 days)
- **Done when:** All runbooks listed in section 18 are written and tried. Failure tests (pod, model server, Elasticsearch, Kafka) and a snapshot restore test pass.

### T5.5 Production deployment and canary release (4-5 days)
- **Done when:** The service is deployed through the pipeline with a canary step. Rollback is tested. The Helm chart has HPA, KEDA (worker, on consumer lag), PodDisruptionBudget, anti-affinity and NetworkPolicy (moved from T1.1).

---

## Useful prompts

**Review a pull request**

> Review the diff of this branch against CLAUDE.md and the HLD. List rule violations first, then bugs, then test gaps. Do not change any code.

**Find access-control risks**

> Search the code base for every place that builds or sends an Elasticsearch query. Confirm each one goes through `QueryBuilder` and includes `AclFilter`. List any exception.

**Check for sensitive logging**

> Search the code for logging, tracing and exception messages that may contain document text, questions or answers. List each place and propose a fix.

**Write tests for a module**

> Write unit tests for `<module>` using the fakes in `tests/fakes`. Cover failure paths and edge cases. Do not change production code.
