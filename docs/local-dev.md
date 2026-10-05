# Local development

## Setup

```bash
uv sync
uv run pytest -q
```

`uv sync` installs Python 3.11 (see `.python-version`) and the locked dependencies.
The service runs from `src/` and is not installed as a package, so use `uv run`
(pytest, mypy and ruff are configured in `pyproject.toml`).

## Local stack

```bash
docker compose -f deploy/local/docker-compose.yml up -d
```

| Service | Port | Notes |
| --- | --- | --- |
| Elasticsearch 8.15 | 9200 | Single node, security off, no persistence |
| Kafka (KRaft) | 9092 | From the host use `localhost:9092`, from other containers `kafka:19092`. The `kafka-init` service creates the four topics from `config/base.yaml` |
| Redis 7 | 6379 | |
| Fake model server | 8081 | `/embed`, `/rerank`, `/v1/chat/completions`. Deterministic fake output, never real models |

All ports are bound to `127.0.0.1`. Then start the API against it:

```bash
APP_ENV=dev uv run uvicorn app.main:app --reload
```

## Settings

Order of priority: environment variables, `config/<APP_ENV>.yaml`, `config/base.yaml`.

- `APP_ENV` picks the yaml file (`dev`, `test`, `prod`). `APP_CONFIG_DIR` changes the config folder.
- `APP_MODE` is `api`, `worker` or `batch`.
- Nested values use `__`: `APP_KAFKA__BOOTSTRAP_SERVERS`. Lists are JSON:
  `APP_ELASTICSEARCH__HOSTS='["http://es:9200"]'`.
- Endpoints, the source index and the LLM model have no default. The service refuses to
  start without them. `prod.yaml` leaves them out on purpose: the Helm chart sets them.

## Logging

JSON to stdout (readable console format in `dev`). Every line of a request carries
`request_id` (from `X-Request-Id`, or generated). Fields named `text`, `content`, `question`,
`answer`, `query`, `prompt`, `snippet` and `body` are replaced by `[REDACTED]`. Log IDs only.

## Image and Helm

```bash
docker build -f deploy/Dockerfile -t semantic-search:dev .
helm lint deploy/helm/semantic-search
helm template rel deploy/helm/semantic-search --set batch.enabled=true
```

The base images in `deploy/Dockerfile` are placeholders until the approved internal images
are named (HLD section 9). The chart deploys `api` and `worker` Deployments and an on-demand
`batch` Job (`--set batch.enabled=true`). It sets `APP_*` variables from `values.yaml: config`.
HPA, KEDA, PodDisruptionBudget, anti-affinity and NetworkPolicy come with task T5.5.

## Tests

- `uv run pytest -q`: unit tests. No network and no Docker. Fakes are in `tests/fakes`.
- `uv run pytest -q -m integration`: integration tests (Testcontainers). None exist before T1.2.
