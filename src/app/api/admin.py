"""Admin router (HLD section 8): manual indexing, deleting a document, re-index jobs.

Needs an admin token (separate from the service tokens, see ``api.admin_tokens``). Every call writes
an audit line with the admin's name, the action and the IDs involved, also when it is refused.
Nothing here returns document text.
"""

from typing import Any

import structlog
from fastapi import APIRouter, Depends, Path
from pydantic import BaseModel, ConfigDict, Field

from app.api.deps import get_admin, get_services
from app.api.errors import current_request_id
from app.api.schemas import ErrorResponse
from app.core.errors import AppError, NotFoundError, UpstreamUnavailableError
from app.core.security import Caller
from app.jobs.admin import AdminService, job_view
from app.observability import audit
from app.services import Services

router = APIRouter(tags=["admin"])
_log = structlog.get_logger(__name__)

_ERRORS: dict[int | str, dict[str, Any]] = {
    code: {"model": ErrorResponse} for code in (400, 401, 403, 404, 429, 503, 504)
}


class IndexRequest(BaseModel):
    """``POST /v1/index/documents``."""

    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1, max_length=128)


class ReindexRequest(BaseModel):
    """``POST /v1/admin/reindex``."""

    model_config = ConfigDict(extra="forbid")

    wave: int = Field(ge=1)
    restart: bool = False


class QueuedResponse(BaseModel):
    """A request that was queued. The work is done by the workers, not by this call."""

    request_id: str | None
    item_id: str
    event_id: str
    status: str = "queued"


class JobResponse(BaseModel):
    """A re-index job."""

    request_id: str | None
    job: dict[str, Any]


def _service(services: Services) -> AdminService:
    if services.admin is None:
        raise UpstreamUnavailableError("The admin API is not available")
    return services.admin


@router.post(
    "/v1/index/documents", status_code=202, response_model=QueuedResponse, responses=_ERRORS
)
async def index_document(
    body: IndexRequest,
    admin: Caller = Depends(get_admin),
    services: Services = Depends(get_services),
) -> QueuedResponse:
    """Queue one document for indexing by its ITEM_ID."""
    try:
        event_id = await _service(services).index_document(body.item_id)
    except AppError:
        audit.record(admin.service, "index_document", "failed", item_id=body.item_id[:128])
        raise
    audit.record(
        admin.service, "index_document", "accepted", item_id=body.item_id, event_id=event_id
    )
    return QueuedResponse(request_id=current_request_id(), item_id=body.item_id, event_id=event_id)


@router.delete(
    "/v1/documents/{doc_id}", status_code=202, response_model=QueuedResponse, responses=_ERRORS
)
async def delete_document(
    doc_id: str = Path(min_length=1, max_length=128),
    admin: Caller = Depends(get_admin),
    services: Services = Depends(get_services),
) -> QueuedResponse:
    """Queue the removal of all chunks of a document. The source document is not touched."""
    try:
        event_id = await _service(services).delete_document(doc_id)
    except AppError:
        audit.record(admin.service, "delete_document", "failed", item_id=doc_id[:128])
        raise
    audit.record(admin.service, "delete_document", "accepted", item_id=doc_id, event_id=event_id)
    return QueuedResponse(request_id=current_request_id(), item_id=doc_id, event_id=event_id)


@router.post("/v1/admin/reindex", status_code=202, response_model=JobResponse, responses=_ERRORS)
async def request_reindex(
    body: ReindexRequest,
    admin: Caller = Depends(get_admin),
    services: Services = Depends(get_services),
) -> JobResponse:
    """Queue a batch re-index job for a wave. A job process starts it (see the backfill runbook)."""
    try:
        record = await _service(services).request_reindex(body.wave, restart=body.restart)
    except AppError:
        audit.record(admin.service, "request_reindex", "failed", wave=body.wave)
        raise
    audit.record(
        admin.service,
        "request_reindex",
        "accepted",
        job_id=record.job_id,
        wave=body.wave,
        restart=body.restart,
    )
    return JobResponse(request_id=current_request_id(), job=job_view(record))


@router.get("/v1/admin/reindex/{job_id}", response_model=JobResponse, responses=_ERRORS)
async def reindex_status(
    job_id: str = Path(min_length=1, max_length=128),
    admin: Caller = Depends(get_admin),
    services: Services = Depends(get_services),
) -> JobResponse:
    """The state of a re-index job."""
    try:
        record = await _service(services).job(job_id)
    except AppError:
        audit.record(admin.service, "reindex_status", "failed", job_id=job_id[:128])
        raise
    if record is None:
        audit.record(admin.service, "reindex_status", "rejected", job_id=job_id)
        raise NotFoundError("No such job")
    audit.record(admin.service, "reindex_status", "accepted", job_id=job_id)
    return JobResponse(request_id=current_request_id(), job=job_view(record))
