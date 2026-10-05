"""FastAPI app factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api import health
from app.api.errors import register_error_handlers
from app.api.middleware import RequestContextMiddleware
from app.core.logging import configure_logging
from app.core.settings import Settings, get_settings

_log = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the app. Settings are loaded from env and yaml unless given."""
    settings = settings or get_settings()
    configure_logging(settings.service.log_level, settings.service.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ready = True
        _log.info("service_started", mode=settings.mode.value, env=settings.env)
        yield
        app.state.ready = False
        _log.info("service_stopped")

    app = FastAPI(title="Semantic Search Service", lifespan=lifespan)
    app.state.settings = settings
    app.state.ready = False
    app.add_middleware(RequestContextMiddleware)
    register_error_handlers(app)
    app.include_router(health.router)
    return app


def __getattr__(name: str) -> FastAPI:
    """Create ``app`` on first access, so ``uvicorn app.main:app`` works without import-time I/O."""
    if name == "app":
        return create_app()
    raise AttributeError(name)
