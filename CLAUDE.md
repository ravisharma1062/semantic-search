# CLAUDE.md: Semantic Search and RAG Service

This file tells Claude Code how to work in this repository. Read it fully before every task.
The full design is in `docs/HLD.md` (exported from the High-Level Design document). When this file and the HLD disagree, stop and ask.

## What this project is

A Python service that adds semantic search, reranking and RAG (answers with citations) to an existing document search platform.

- The existing Java app, keyword index and OCR flow stay unchanged.
- The Java app sends Kafka events that contain only an `ITEM_ID`.
- Our worker reads the OCR text from the existing Elasticsearch index by `ITEM_ID`, splits it into chunks, creates vectors with in-house models, and writes them to a new chunk index.
- The API does hybrid search (BM25 + kNN, merged with RRF), optional reranking, and optional RAG.
- Scale: about 100 million documents, 1 to 3,000 pages each. Search must answer within 3 seconds.

## Tech stack

- Python 3.11+, FastAPI, Uvicorn, Pydantic and Pydantic Settings
- Kafka client: confluent-kafka
- Elasticsearch 8.x: elasticsearch-py (async client, bulk helpers)
- HTTP to model servers: httpx. Retries: tenacity
- Models: bge-m3 (embeddings), bge-reranker, in-house LLM. OpenAI only as an optional provider
- Cache: Redis. Telemetry: OpenTelemetry, prometheus-client, structlog, Langfuse
- Tests: pytest, pytest-asyncio, pytest-cov, hypothesis, Testcontainers, respx. Quality: ruff, mypy (strict)
- Packaging: uv, Docker, Helm

Approved beyond the list above (decided in the project): PyYAML (through `pydantic-settings[yaml]`), `redis` (redis-py), `tokenizers` (Hugging Face, for token counts), `hypothesis`, `pytest-cov`. The Elasticsearch client is pinned to `>=8.15,<9`: a 9.x client cannot talk to an 8.x server. No OpenAI SDK: OpenAI is called over plain HTTP.

Do not add any new dependency without asking. All libraries need security approval.

## Commands

```bash
uv sync                                   # install dependencies
uv run pytest -q                          # unit tests (no Docker)
uv run pytest -q -m integration           # integration tests (Docker: real Kafka, Elasticsearch, Redis)
uv run ruff check . && uv run ruff format --check .
uv run mypy src tests
uv run pytest -q --cov --cov-report= && uv run pytest -q -m integration --cov --cov-append --cov-report= && uv run coverage report   # ingestion and store, threshold in pyproject.toml
uv run uvicorn app.main:app --reload      # run the API locally
docker compose -f deploy/local/docker-compose.yml up -d   # local Elasticsearch, Kafka, Redis, fake model server
uv run python -m app.jobs.index_admin --help   # index versions, alias switch (runbook: docs/local-dev.md)
uv run python -m app.jobs.cli --help           # backfill and reconcile (runbook: docs/runbooks/backfill.md)
```

Before you say a task is done, all of these must pass: ruff, mypy, pytest (unit and integration). If a check cannot run (no Docker, no network, a blocked tool), say so plainly. Never report it as passed.

`APP_MODE` selects the process: `api`, `worker` (the indexing worker) or `batch` (idle unless a Helm `command` runs a job).

## Repository layout

```
src/app/
  main.py            FastAPI app factory
  api/               routers: search, answer, admin, health
  core/              settings, logging, errors, retry helper, JSON HTTP client, adaptive limiter
  ingestion/         events, Kafka consumer loop and adapters, offsets, DLQ, source reader,
                     normalizer, tokens, chunker, indexer, state store, worker loop, runtime
  retrieval/         query builder, ACL filter, hybrid searcher (task T2.1)
  rerank/            Reranker interface and providers
  embeddings/        Embedder interface, in-house and OpenAI providers, Redis query cache, factory
  llm/               LLMClient interface and providers
  rag/               context builder, answer service, citations, gates
  store/             Elasticsearch client factory and error mapping, templates, aliases
  observability/     metrics, tracing, log filters
  jobs/              backfill producer, reconciliation, job store, rate limit, scan, CLIs
prompts/             versioned prompt files
config/              yaml per environment (base, dev, test, prod)
openapi/             API contract shared with the Java team
eval/                evaluation set and runner
tests/unit  tests/integration  tests/fakes
deploy/              Dockerfile, Helm charts (semantic-search, embedding-server), local docker-compose
docs/                HLD.md, decisions/ (one record per task), runbooks/, local-dev.md
```

Done so far: phase 1 (T1.1 to T1.8, the indexing pipeline) is committed, except T1.9 (Java team). Next: phase 2, hybrid search (T2.1). See `BACKLOG.md` and `docs/decisions/`.

## Rules that must never be broken

1. **Access control.** Every Elasticsearch search query must be built by `retrieval.QueryBuilder` and must contain the access filter from `retrieval.AclFilter`. Never write a search query anywhere else. Every change near search needs a test that proves a user cannot see a document they have no access to. Internal batch and maintenance requests that have no user (backfill scan, state counts, delete or update by query on our own index) are listed in `docs/decisions/0006` and `0007`. **That exception still needs human sign-off.** Do not add any other query outside `retrieval`, and no API route may call them.
2. **No sensitive text in logs, traces, metrics or error messages.** Never log document text, chunk text, user questions or answers. Log IDs (`item_id`, `chunk_id`, request ID) only. Error text in headers, state and logs is the error type, plus the message only for our own typed errors. Never copy a response body or a library message. URLs can carry text, so httpx logging is off.
3. **Kafka offsets.** Commit an offset only after the chunks are written to Elasticsearch and the state is saved (or the event is safely in the retry topic or the DLQ). Offsets move up only past finished messages. Processing must be idempotent, because messages can arrive twice or out of order. Deletes and permission fixes beat updates.
4. **Chunk IDs are deterministic:** `f"{item_id}:{chunk_no}:{content_hash}"`. This is also the Elasticsearch `_id`.
5. **Provider interfaces.** Pipeline code only talks to `Embedder`, `Reranker`, `LLMClient` and `SourceReader`. Never call a model SDK or URL directly from pipeline code.
6. **Models are in-house by default.** OpenAI is optional, behind configuration, and only for approved data. Bedrock is not used.
7. **Everything configurable stays in settings:** model names, endpoints, topic names, index names, alias names, limits, timeouts. No hard-coded values.
8. **Timeouts and retries.** Every external call has a timeout. Retries go through the shared helper `core.retry` (or `backoff_delays` when only part of a call is repeated), with backoff and jitter, and only for safe calls. `tenacity` awaits only coroutine functions: always go through `with_retries`.
9. **Index changes.** Never change the mapping of the existing document index. Chunk index changes create a new versioned index behind an alias. The index tools refuse any index that is not `{prefix}_v*`.
10. **Fallback.** If anything in semantic search fails, the API returns a clear error code that makes the Java app fall back to keyword search. It must never return partial results without saying which mode was used (`mode_used`).
11. **Prompts** live in `prompts/` as versioned files. Never build prompts from string pieces inside the code.
12. **No real data.** Use only synthetic or sample test data. Never put secrets, real documents, real user names or real questions in code, tests, fixtures or comments.

## Coding conventions

- Type hints everywhere. `mypy --strict` must pass.
- Async I/O in the API and the worker. Never block the event loop. Run CPU-heavy work in a pool.
- Pydantic models for all request, response, event and config data.
- Small functions, clear names, docstrings for public functions. No dead code and no TODOs without a task ID.
- Errors: raise typed errors from `core.errors`. Map them to API error codes in one place.
- Keep modules independent: `api` may call services, services may call interfaces, interfaces never import `api`.
- Follow the API contract in `openapi/`. Changing it needs approval, and the change must be backward compatible.
- Errors that retrying cannot fix are `NonRetryableError` (the consumer sends them straight to the DLQ). Retryable ones are the `Upstream*` errors and `SourceNotReadyError`.
- Elasticsearch delete and update by query see only refreshed documents. When a document was written recently, refresh first (`refresh_first`), and treat version conflicts as retryable, never skip them.
- Never use literal invisible or special characters in source files. Use `\N{NAME}` escapes (for example `\N{SOFT HYPHEN}`).
- `ruff format` must not touch Markdown (it would rewrite code blocks in `docs/HLD.md`). It is excluded in `pyproject.toml`.
- Windows development: heredocs with backslashes can be changed by tools. For code with escapes, use the Edit or Write tool, or build strings with `chr(92)`.

## Testing rules

- Write the test first, or together with the code. Every task includes tests.
- Unit tests: no network, no Docker. Use the fakes in `tests/fakes` (embedder, reranker, LLM, source reader, in-memory Kafka, fake Elasticsearch clients, indexer and state store, Redis, jobs). Mock HTTP with respx.
- Integration tests (`@pytest.mark.integration`): real Elasticsearch, Kafka and Redis through Testcontainers, and the fake model server on a real port. Run them per task when Docker works, not only at the end of a phase.
- Property-based tests (hypothesis) for the chunker, the normalizer and the worker (model based: the index must equal a fresh index of the current document).
- Test the failure paths: timeouts, partial bulk failures, duplicate and out-of-order messages, deleted documents, empty documents, crash and restart.
- A test that never fails proves nothing: when a test passes at once, check that it can fail (log capture, mock scope, blocking calls in async tests).
- Coverage of `ingestion` and `store` is measured with unit and integration tests together (threshold in `pyproject.toml`).
- Quality tests (retrieval and RAG metrics) live in `eval/` and run separately.

## How to work (important)

1. First write a short plan (files to change, approach, dependencies, tests, risks, open questions) and wait for approval. Do not start coding before that. One approved plan may cover a whole phase (for example T1.2 to T1.8). Then the tasks run in order without asking again, but stop and ask when: this file and the HLD disagree, a new dependency is needed, a decision needs the Java team, or a failing test can only be fixed by weakening it.
2. Keep each task small: one task is one branch, one commit and one pull request (stacked on the previous task when they depend on each other).
3. Write a decision record `docs/decisions/NNNN-name.md` for each task: what was decided, deviations from the HLD, open items.
4. Do not change unrelated files and do not reformat the whole repository.
5. If something in the design is unclear or looks wrong, ask. Do not guess. Say plainly what is an assumption (for example source field names that the Java team must confirm).
6. After coding: run the full check list, then summarise what changed, what was tested, what could not be run, and what is not done.
7. Update `docs/` when behaviour, configuration or interfaces change.
8. Do not work around a blocked tool or a failed safety check by using another tool. Report it and continue with other work.

## Git and review

- Branch name: `feature/T<id>-short-name`. Commit messages: `T<id>: what and why`.
- Open a pull request with the task ID, the acceptance criteria and test results. A human reviews every pull request.
- Never commit secrets, `.env` files or model files.

## Definition of done

- Acceptance criteria in `BACKLOG.md` are met.
- Tests added and passing. ruff and mypy clean.
- No rule above is broken.
- Docs and OpenAPI updated if needed.
- Metrics and logs added for new paths, without sensitive text.

## Do not

- Do not call real external services (OpenAI, production Elasticsearch, production Kafka) from tests or local runs.
- Do not add frameworks (LangChain, LlamaIndex) as the main pipeline. Small helpers are fine after approval.
- Do not store state in memory that must survive a restart. API pods are stateless.
- Do not weaken a test to make it pass. Fix the cause or ask.
