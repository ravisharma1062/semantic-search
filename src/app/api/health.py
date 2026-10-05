"""Kubernetes probes.

``live`` says the process runs. ``ready`` says the app finished startup. Dependency checks
(Elasticsearch, Redis, model servers) are added together with those clients.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def live() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    """Readiness probe. 503 before startup has finished and during shutdown."""
    if getattr(request.app.state, "ready", False):
        return JSONResponse({"status": "ready"})
    return JSONResponse({"status": "not_ready"}, status_code=503)
