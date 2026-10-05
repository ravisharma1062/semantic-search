# One image, three run modes (APP_MODE=api|worker|batch).
# Base images are placeholders: replace them with the approved internal registry images
# (HLD section 9). Build from the repository root: docker build -f deploy/Dockerfile .

FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

FROM python:3.11-slim
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY config ./config
COPY prompts ./prompts
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_CONFIG_DIR=/app/config \
    APP_ENV=prod \
    APP_MODE=api
USER 10001:10001
EXPOSE 8080
CMD ["python", "-m", "app"]
