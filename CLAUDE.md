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
- Tests: pytest, pytest-asyncio, Testcontainers, respx. Quality: ruff, mypy (strict)
- Packaging: uv, Docker, Helm

Do not add any new dependency without asking. All libraries need security approval.

## Commands

Some of these exist only after task T1.1 is done.

```bash
uv sync                                   # install dependencies
uv run pytest -q                          # unit tests
uv run pytest -q -m integration           # integration tests (need Docker)
uv run ruff check . && uv run ruff format --check .
uv run mypy src
uv run uvicorn app.main:app --reload      # run the API locally
docker compose -f deploy/local/docker-compose.yml up -d   # local Elasticsearch, Kafka, Redis, fake model server
```

Before you say a task is done, all of these must pass: ruff, mypy, pytest (unit and integration).

## Repository layout

```
src/app/
  main.py            FastAPI app factory
  api/               routers: search, answer, admin, health
  core/              settings, logging, security, errors, retry helpers
  ingestion/         Kafka consumer, source reader, chunker, indexer, state store, DLQ
  retrieval/         query builder, ACL filter, hybrid searcher
  rerank/            Reranker interface and providers
  embeddings/        Embedder interface and providers
  llm/               LLMClient interface and providers
  rag/               context builder, answer service, citations, gates
  store/             Elasticsearch client, index templates, alias helpers
  observability/     metrics, tracing, log filters
  jobs/              backfill producer, re-index, reconciliation, ACL refresh
prompts/             versioned prompt files
config/              yaml per environment
openapi/             API contract shared with the Java team
eval/                evaluation set and runner
tests/unit  tests/integration
deploy/              Dockerfile, Helm chart, local docker-compose
docs/                HLD.md, decisions, runbooks
```

## Rules that must never be broken

1. **Access control.** Every Elasticsearch search query must be built by `retrieval.QueryBuilder` and must contain the access filter from `retrieval.AclFilter`. Never write a search query anywhere else. Every change near search needs a test that proves a user cannot see a document they have no access to.
2. **No sensitive text in logs, traces, metrics or error messages.** Never log document text, chunk text, user questions or answers. Log IDs (`item_id`, `chunk_id`, request ID) only.
3. **Kafka offsets.** Commit an offset only after the chunks are written to Elasticsearch and the state is saved. Processing must be idempotent, because messages can arrive twice.
4. **Chunk IDs are deterministic:** `f"{item_id}:{chunk_no}:{content_hash}"`. This is also the Elasticsearch `_id`.
5. **Provider interfaces.** Pipeline code only talks to `Embedder`, `Reranker`, `LLMClient` and `SourceReader`. Never call a model SDK or URL directly from pipeline code.
6. **Models are in-house by default.** OpenAI is optional, behind configuration, and only for approved data. Bedrock is not used.
7. **Everything configurable stays in settings:** model names, endpoints, topic names, index names, alias names, limits, timeouts. No hard-coded values.
8. **Timeouts and retries.** Every external call has a timeout. Retries go through the shared helper in `core`, with backoff and jitter, and only for safe calls.
9. **Index changes.** Never change the mapping of the existing document index. Chunk index changes create a new versioned index behind an alias.
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

## Testing rules

- Write the test first, or together with the code. Every task includes tests.
- Unit tests: no network, no Docker. Use the fakes in `tests/fakes` (fake embedder, fake reranker, fake LLM, fake Elasticsearch where simple).
- Integration tests (`@pytest.mark.integration`): real Elasticsearch and Kafka through Testcontainers.
- Test the failure paths: timeouts, partial bulk failures, duplicate messages, deleted documents, empty documents.
- Quality tests (retrieval and RAG metrics) live in `eval/` and run separately.

## How to work (important)

1. For every task, first write a short plan (files to change, approach, risks) and wait for approval. Do not start coding before that.
2. Do one task at a time. Keep the change small: one task is one pull request.
3. Do not change unrelated files and do not reformat the whole repository.
4. If something in the design is unclear or looks wrong, ask. Do not guess.
5. After coding: run the full check list, then summarise what changed, what was tested, and what is not done.
6. Update `docs/` when behaviour, configuration or interfaces change.

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
