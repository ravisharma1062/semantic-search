# Semantic Search and RAG Service

Python service that adds semantic search, reranking and RAG (answers with citations) to the
existing document search platform. Rules: `CLAUDE.md`. Design: `docs/HLD.md`. Tasks: `BACKLOG.md`.

```bash
uv sync                                   # install dependencies
uv run pytest -q                          # unit tests
uv run ruff check . && uv run ruff format --check .
uv run mypy src tests
APP_ENV=dev uv run uvicorn app.main:app --reload
```

Run modes of the single image: `APP_MODE=api` (default), `worker`, `batch`.
Local stack and settings: `docs/local-dev.md`.
