"""FastAPI app factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api import health, search
from app.api.contract import build_contract
from app.api.errors import register_error_handlers
from app.api.middleware import RequestContextMiddleware
from app.api.ratelimit import RateLimiter
from app.core.logging import configure_logging
from app.core.settings import Settings, get_settings
from app.services import Services, build_services

API_VERSION = "0.2.0"

_log = structlog.get_logger(__name__)


def include_routes(app: FastAPI) -> None:
    """All routers. The OpenAPI contract is built from these."""
    app.include_router(health.router)
    app.include_router(search.router)


def create_app(settings: Settings | None = None, services: Services | None = None) -> FastAPI:
    """Build the app. Settings are loaded from env and yaml unless given.

    Tests pass ``services`` built from fakes. Otherwise the real services are built at startup.
    """
    settings = settings or get_settings()
    configure_logging(settings.service.log_level, settings.service.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = services is None
        app.state.services = services if services is not None else await build_services(settings)
        app.state.ready = True
        _log.info("service_started", mode=settings.mode.value, env=settings.env)
        yield
        app.state.ready = False
        if owned:
            await app.state.services.close()
        _log.info("service_stopped")

    app = FastAPI(title="Semantic Search Service", version=API_VERSION, lifespan=lifespan)
    app.state.settings = settings
    app.state.ready = False
    app.state.limiter = RateLimiter(
        settings.api.rate_limit_user_per_min, settings.api.rate_limit_service_per_min
    )
    app.add_middleware(RequestContextMiddleware)
    register_error_handlers(app)
    include_routes(app)
    app.openapi = lambda: build_contract(app)  # type: ignore[method-assign]
    return app


def __getattr__(name: str) -> FastAPI:
    """Create ``app`` on first access, so ``uvicorn app.main:app`` works without import-time I/O."""
    if name == "app":
        return create_app()
    raise AttributeError(name)
