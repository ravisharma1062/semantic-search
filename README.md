# Semantic Search and RAG Service

Python service that adds semantic search, reranking and RAG (answers with citations) to the
existing document search platform. Rules: `CLAUDE.md`. Design: `docs/HLD.md`. Tasks: `BACKLOG.md`.

## Run locally

The code is in `src/` and is not installed as a package. Set `PYTHONPATH=src` for every
`python -m app...` command, or you get `No module named 'app'`.

1. Install dependencies (needs [uv](https://docs.astral.sh/uv/)):

   ```bash
   uv sync
   ```

2. Start the local stack (needs Docker): Elasticsearch, Kafka, Redis and a fake model server.

   ```bash
   docker compose -f deploy/local/docker-compose.yml up -d
   ```

3. Create the indexes (first time only). PowerShell:

   ```powershell
   $env:PYTHONPATH = "src"; $env:APP_ENV = "dev"
   uv run python -m app.jobs.index_admin install-templates
   uv run python -m app.jobs.index_admin create-state-index
   uv run python -m app.jobs.index_admin create-index --version v1
   uv run python -m app.jobs.index_admin switch-alias --index doc_chunks_v1_bgem3 --allow-empty
   ```

   bash: `export PYTHONPATH=src APP_ENV=dev`, then the same four commands.

4. Start the API:

   ```powershell
   $env:PYTHONPATH = "src"; $env:APP_ENV = "dev"
   uv run python -m app
   ```

5. Start the indexing worker in a second terminal:

   ```powershell
   $env:PYTHONPATH = "src"; $env:APP_ENV = "dev"; $env:APP_MODE = "worker"
   uv run python -m app
   ```

Run modes of the single image: `APP_MODE=api` (default), `worker`, `batch`.

### Local URLs

| What | URL |
| --- | --- |
| API docs (Swagger) | http://localhost:8080/docs |
| OpenAPI contract | http://localhost:8080/openapi.json |
| Health | http://localhost:8080/health/live and `/health/ready` |
| Search, answer | `POST` http://localhost:8080/v1/search and `/v1/answer` (need `Authorization` and `X-User-Id` headers; RAG needs `APP_FEATURE_FLAGS__RAG=true`) |
| Elasticsearch | http://localhost:9200 |
| Fake model server | http://localhost:8081 |
| Worker metrics | http://localhost:9100/metrics |
| Redis, Kafka | `localhost:6379`, `localhost:9092` (not HTTP) |

`python -m app` uses port 8080. Running `uvicorn app.main:app` directly uses port 8000 unless
you pass `--port`.

## Checks

```bash
uv run pytest -q                          # unit tests
uv run ruff check . && uv run ruff format --check .
uv run mypy
```

Local stack and settings: `docs/local-dev.md`.
