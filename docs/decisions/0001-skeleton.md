# 0001: Project skeleton (task T1.1)

Date: 2026-10-05. Approved by the project owner before coding.

## Decisions

| Topic | Decision |
| --- | --- |
| CI | GitHub Actions (`.github/workflows/ci.yml`), only official `actions/*` actions. uv is installed with pip, pinned |
| Packaging | uv. The service runs from `src/` (`package = false`), no build backend |
| Python | 3.11 |
| Base images | `python:3.11-slim` and `ghcr.io/astral-sh/uv:0.8.22` are placeholders. Replace them with the approved internal images (HLD section 9) |
| Dependencies added | fastapi, uvicorn, pydantic, pydantic-settings[yaml] (brings PyYAML, approved for yaml settings), structlog. Dev: httpx (TestClient), pytest, pytest-asyncio, ruff, mypy |
| Deferred dependencies | confluent-kafka (T1.2), testcontainers (T1.2), elasticsearch (T1.3), httpx at runtime, tenacity, redis (T1.5), respx (T1.5), prometheus-client, OpenTelemetry, Langfuse (T5.1), pytest-cov (T1.8) |
| Helm scope | `api`, `worker`, `batch` only. HPA, KEDA, PodDisruptionBudget, anti-affinity and NetworkPolicy move to T5.5. The worker and batch pods have no probes until T1.2 |
| Integration tests | None in T1.1. The marker exists, CI tolerates "no tests collected" until T1.2 and that tolerance is removed then |
| Retry helper (`core`) | Deferred. It needs `tenacity`, so it comes with the first caller (T1.2 or T1.5). Rule 8 applies from then on |
| Fake model server | Local only. Shapes (`/embed`, `/rerank`, OpenAI-style chat) are a best guess until T1.5, T3.1 and T4.2 fix the contracts |
| `SourceDocument` | Minimal version in `ingestion/source.py`. T1.3 owns the real one. Field names still need the Java team |
| `LLMClient` signature | Deviates from the HLD sketch (`list[dict]`, `**options`): it takes `Sequence[ChatMessage]` and `LLMOptions`, because `mypy --strict` rejects untyped dicts |
| Error codes | Adds `INTERNAL_ERROR` (HTTP 500) to the HLD section 8 table, for unhandled errors. Additive. Add it to `openapi/` when the contract is written (work package 0.6, or T2.2) |
| Validation errors | HTTP 400 `INVALID_REQUEST` (as in the HLD), not FastAPI's default 422. The body lists field names only and never echoes input |
| Logging | httpx and httpcore loggers are set to WARNING, because they log request URLs |

## Verified on a developer machine (2026-10-05)

- `docker build -f deploy/Dockerfile .` works. The container runs as UID 10001 on a read-only
  root filesystem, answers `/health/*`, and the worker mode exits 0 on `docker stop`.
- `helm lint` passes (only the "icon is recommended" note). `helm template` renders the
  ConfigMap, three ServiceAccounts, the api and worker Deployments, the Service and the batch Job.
- `docker compose up -d` gives healthy Elasticsearch 8.15.3, Kafka, Redis and the fake model
  server. `kafka-init` creates the four topics.
- Kafka has two listeners: `localhost:9092` for the host and `kafka:19092` for containers.
  A single advertised `localhost` listener makes other containers connect to themselves.
- Not run: the GitHub Actions workflow itself (no remote yet) and the Helm chart on a cluster.
