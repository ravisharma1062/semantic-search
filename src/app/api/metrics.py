"""``GET /metrics`` for Prometheus.

Not part of the OpenAPI contract and not behind the service token: Prometheus scrapes it from inside
the cluster, and the NetworkPolicy lets only the monitoring namespace reach it. The metrics hold no
text, no user and no document ID (see ``observability/metrics.py``).
"""

from fastapi import APIRouter, Response

from app.observability.metrics import get_metrics

router = APIRouter()

_TEXT_FORMAT = "text/plain; version=0.0.4; charset=utf-8"


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """The metrics in the Prometheus text format."""
    return Response(get_metrics().exposition(), media_type=_TEXT_FORMAT)
