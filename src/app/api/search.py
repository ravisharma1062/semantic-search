"""``POST /v1/search`` (HLD section 8)."""

from typing import Any

import structlog
from fastapi import APIRouter, Depends

from app.api.deps import get_identity, get_services, get_settings_dep
from app.api.errors import current_request_id
from app.api.schemas import ErrorResponse, SearchRequest, SearchResponse, SearchResultItem
from app.core.errors import InvalidRequestError
from app.core.security import Identity
from app.core.settings import Settings
from app.services import Services

router = APIRouter(tags=["search"])
_log = structlog.get_logger(__name__)

_ERRORS: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorResponse},
    401: {"model": ErrorResponse},
    403: {"model": ErrorResponse},
    429: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
    504: {"model": ErrorResponse},
}


@router.post("/v1/search", response_model=SearchResponse, responses=_ERRORS)
async def search(
    body: SearchRequest,
    identity: Identity = Depends(get_identity),
    services: Services = Depends(get_services),
    settings: Settings = Depends(get_settings_dep),
) -> SearchResponse:
    """Hybrid search within the rights of the user. Never returns what the user may not open."""
    if len(body.query) > settings.api.max_query_chars:
        raise InvalidRequestError("Query is too long")
    result = await services.search.search(
        query=body.query,
        identity=identity,
        top_k=body.top_k,
        mode=body.mode,
        filters=body.filters,
        group_by_document=body.group_by_document,
        rerank=body.rerank,
    )
    _log.info(
        "search_done",
        service=identity.service,
        user_id=identity.user_id,
        mode_used=result.mode_used,
        results=len(result.hits),
        chunk_ids=[h.chunk_id for h in result.hits],
        took_ms=result.took_ms,
    )
    return SearchResponse(
        request_id=current_request_id(),
        mode_used=result.mode_used,
        results=[
            SearchResultItem(
                doc_id=h.doc_id,
                chunk_id=h.chunk_id,
                score=h.score,
                pages=h.pages,
                snippet=h.snippet,
                highlights=h.highlights,
            )
            for h in result.hits
        ],
        took_ms=result.took_ms,
    )
