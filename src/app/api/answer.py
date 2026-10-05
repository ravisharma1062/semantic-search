"""``POST /v1/answer`` and ``POST /v1/answer/stream`` (HLD sections 7 and 8)."""

import json
import time
from collections.abc import AsyncIterator
from typing import Any

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.api.deps import get_identity, get_services, get_settings_dep
from app.api.errors import current_request_id
from app.api.schemas import (
    AnswerRequest,
    AnswerResponse,
    CitationItem,
    ErrorResponse,
    UsageItem,
)
from app.core.errors import AppError, InvalidRequestError, UpstreamUnavailableError
from app.core.security import Identity
from app.core.settings import Settings
from app.observability.langfuse import LangfuseSink
from app.observability.metrics import get_metrics
from app.rag.citations import is_not_found
from app.rag.service import Answer, AnswerService, Prepared
from app.rag.stream import StreamAssembler
from app.services import Services

router = APIRouter(tags=["answer"])
_log = structlog.get_logger(__name__)

_ERRORS: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorResponse},
    401: {"model": ErrorResponse},
    403: {"model": ErrorResponse},
    429: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
    504: {"model": ErrorResponse},
}


def _service(services: Services) -> AnswerService:
    if services.answer is None:
        raise UpstreamUnavailableError("Answers are not available")
    return services.answer


def _check_question(body: AnswerRequest, settings: Settings) -> None:
    if len(body.question) > settings.api.max_query_chars:
        raise InvalidRequestError("Question is too long")


def _response(result: Answer) -> AnswerResponse:
    return AnswerResponse(
        request_id=current_request_id(),
        answer=result.answer,
        found=result.found,
        reason=result.reason,
        citations=[
            CitationItem(ref=c.ref, doc_id=c.doc_id, pages=c.pages, snippet=c.snippet)
            for c in result.citations
        ],
        warnings=result.warnings,
        mode_used=result.mode_used,
        model=result.model,
        prompt_version=result.prompt_version,
        usage=UsageItem(
            input_tokens=result.usage.input_tokens, output_tokens=result.usage.output_tokens
        ),
    )


def _record(identity: Identity, result: Answer, started: float, sink: LangfuseSink | None) -> None:
    """Metrics, the log line and LLM telemetry of one finished answer (IDs and counts only)."""
    metrics = get_metrics()
    metrics.answers.labels(result.reason).inc()
    if result.usage.input_tokens or result.usage.output_tokens:
        metrics.llm_tokens.labels("input").inc(result.usage.input_tokens)
        metrics.llm_tokens.labels("output").inc(result.usage.output_tokens)
    if sink is not None and result.model:
        sink.submit(
            request_id=current_request_id(),
            user_id=identity.user_id,
            model=result.model,
            prompt_version=result.prompt_version,
            input_tokens=result.usage.input_tokens,
            output_tokens=result.usage.output_tokens,
            latency_ms=round((time.perf_counter() - started) * 1000),
            found=result.found,
            reason=result.reason,
            mode_used=result.mode_used,
        )
    _log_done(identity, result)


def _log_done(identity: Identity, result: Answer) -> None:
    _log.info(
        "answer_done",
        service=identity.service,
        user_id=identity.user_id,
        found=result.found,
        reason=result.reason,
        mode_used=result.mode_used,
        citations=[c.chunk_id for c in result.citations],
        warnings=result.warnings,
        prompt_version=result.prompt_version,
        input_tokens=result.usage.input_tokens,
        output_tokens=result.usage.output_tokens,
    )


@router.post("/v1/answer", response_model=AnswerResponse, responses=_ERRORS)
async def answer(
    body: AnswerRequest,
    identity: Identity = Depends(get_identity),
    services: Services = Depends(get_services),
    settings: Settings = Depends(get_settings_dep),
) -> AnswerResponse:
    """An answer from the documents the user may read, with citations."""
    _check_question(body, settings)
    started = time.perf_counter()
    result = await _service(services).answer(
        question=body.question, identity=identity, top_k=body.top_k, filters=body.filters
    )
    _record(identity, result, started, services.langfuse)
    return _response(result)


def _event(name: str, data: dict[str, Any]) -> bytes:
    return f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode()


def _done_data(result: Answer, request_id: str | None) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "found": result.found,
        "reason": result.reason,
        "warnings": result.warnings,
        "mode_used": result.mode_used,
        "model": result.model,
        "prompt_version": result.prompt_version,
        "usage": {
            "input_tokens": result.usage.input_tokens,
            "output_tokens": result.usage.output_tokens,
        },
    }


async def _stream(
    svc: AnswerService,
    prepared: Prepared,
    identity: Identity,
    request_id: str | None,
    sink: LangfuseSink | None,
    started: float,
) -> AsyncIterator[bytes]:
    if prepared.early is not None:
        _record(identity, prepared.early, started, sink)
        yield _event("done", _done_data(prepared.early, request_id))
        return
    assembler = StreamAssembler(svc.guardrails, {c.ref for c in prepared.context})
    first_sent = False
    try:
        async for piece in svc.stream_pieces(prepared):
            for text in assembler.feed(piece):
                if not first_sent:
                    first_sent = True
                    get_metrics().first_token.observe(time.perf_counter() - started)
                yield _event("token", {"text": text})
        result = svc.finish(prepared, assembler.raw, svc.stream_usage(prepared, assembler.raw))
        for text in assembler.finish(not_found=is_not_found(assembler.raw)):
            yield _event("token", {"text": text})
    except AppError as exc:
        _log.warning("answer_stream_failed", code=exc.code.value)
        get_metrics().answers.labels("error").inc()
        yield _event(
            "error", {"code": exc.code.value, "message": exc.message, "request_id": request_id}
        )
        return
    except TimeoutError:
        _log.warning("answer_stream_failed", code="TIMEOUT")
        get_metrics().answers.labels("error").inc()
        yield _event(
            "error", {"code": "TIMEOUT", "message": "LLM timeout", "request_id": request_id}
        )
        return
    yield _event(
        "citations",
        {
            "citations": [
                {"ref": c.ref, "doc_id": c.doc_id, "pages": c.pages, "snippet": c.snippet}
                for c in result.citations
            ]
        },
    )
    _record(identity, result, started, sink)
    yield _event("done", _done_data(result, request_id))


@router.post(
    "/v1/answer/stream",
    responses={**_ERRORS, 200: {"content": {"text/event-stream": {}}}},
    response_class=StreamingResponse,
)
async def answer_stream(
    body: AnswerRequest,
    identity: Identity = Depends(get_identity),
    services: Services = Depends(get_services),
    settings: Settings = Depends(get_settings_dep),
) -> StreamingResponse:
    """The same answer as server-sent events: ``token``, ``citations``, ``done`` (or ``error``).

    Everything that can fail before the first token (search, rights, gate) answers with a normal
    error status, so the Java app can fall back. If ``done`` says ``found: false`` after tokens
    were sent (the answer failed the citation check or a guardrail), the client discards them.
    """
    _check_question(body, settings)
    started = time.perf_counter()
    svc = _service(services)
    prepared = await svc.prepare(
        question=body.question, identity=identity, top_k=body.top_k, filters=body.filters
    )
    return StreamingResponse(
        _stream(svc, prepared, identity, current_request_id(), services.langfuse, started),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )
