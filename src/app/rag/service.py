"""The answer use case (HLD section 7): search, gate, context, prompt, model, checks.

Steps:
    1. refuse questions that the guardrails deny
    2. search within the user's rights (the same service as ``/v1/search``, so rule 1 holds)
    3. gate: weak retrieval gives NOT_FOUND and the model is never called
    4. build the numbered context from the allowed chunks, with PII masked
    5. call the model (versioned prompt, time limit, breaker)
    6. NOT_FOUND becomes ``found: false``; every ``[n]`` is checked and mapped to a document
    7. mask PII in the answer

Errors from search or from the model are typed errors (``UPSTREAM_UNAVAILABLE``, ``TIMEOUT``), so
the Java app falls back (rule 10). The question, the passages and the answer are never logged
(rule 2).
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Literal

import structlog

from app.core.errors import InvalidRequestError, NonRetryableError, UpstreamUnavailableError
from app.core.security import Identity
from app.core.settings import Settings
from app.ingestion.tokens import TokenCounter
from app.llm.base import ChatMessage, LLMClient, LLMOptions, Usage, estimate_tokens
from app.observability.instrument import observed
from app.observability.tracing import set_attributes, span
from app.rag.citations import Citation, check_citations, is_not_found
from app.rag.context import ContextChunk, build_context
from app.rag.guardrails import Guardrails
from app.rag.prompts import PromptTemplate
from app.retrieval.filters import SearchFilters
from app.retrieval.models import SearchHit
from app.retrieval.service import SearchService

_log = structlog.get_logger(__name__)

Reason = Literal[
    "answered", "not_found", "low_relevance", "no_context", "blocked", "unverified", "refused"
]


@dataclass
class Answer:
    """The result of the answer use case."""

    answer: str
    found: bool
    reason: Reason
    citations: list[Citation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    model: str = ""
    prompt_version: str = ""
    mode_used: str = ""
    usage: Usage = field(default_factory=Usage)


@dataclass
class Prepared:
    """Everything before the model call. ``early`` is set if no model call is needed."""

    mode_used: str
    context: list[ContextChunk]
    messages: list[ChatMessage]
    early: Answer | None = None


class AnswerService:
    """Answers a question from the documents the user may read."""

    def __init__(
        self,
        *,
        search: SearchService,
        llm: LLMClient,
        prompt: PromptTemplate,
        settings: Settings,
        counter: TokenCounter,
    ) -> None:
        self._search = search
        self._llm = llm
        self._prompt = prompt
        self._settings = settings
        self._counter = counter
        self._guard = Guardrails(settings.rag.guardrails)

    @property
    def guardrails(self) -> Guardrails:
        """The guardrails, for the streaming layer."""
        return self._guard

    def _answer(
        self,
        reason: Reason,
        mode_used: str = "",
        *,
        usage: Usage | None = None,
        warnings: list[str] | None = None,
    ) -> Answer:
        return Answer(
            answer="",
            found=reason == "answered",
            reason=reason,
            warnings=warnings or [],
            model=self._llm.model_name,
            prompt_version=self._prompt.version,
            mode_used=mode_used,
            usage=usage or Usage(),
        )

    async def prepare(
        self,
        *,
        question: str,
        identity: Identity,
        top_k: int | None,
        filters: SearchFilters | None,
    ) -> Prepared:
        """Steps 1 to 4."""
        cfg = self._settings
        if not cfg.feature_flags.rag:
            raise UpstreamUnavailableError("Answers are switched off")
        if not question.strip():
            raise InvalidRequestError("Empty question")
        if not self._guard.check_question(question):
            return Prepared("", [], [], self._answer("refused"))
        wanted = min(top_k or cfg.rag.max_chunks, cfg.rag.max_chunks)
        # Ask for more than we need: near-duplicates and the per-document limit drop some.
        result = await self._search.search(
            query=question,
            identity=identity,
            top_k=min(wanted * 3, cfg.search.max_top_k),
            mode="hybrid",
            filters=filters,
        )
        if not result.hits:
            return Prepared(result.mode_used, [], [], self._answer("no_context", result.mode_used))
        if (
            cfg.rag.min_score > 0
            and result.mode_used.endswith("+rerank")
            and result.hits[0].score < cfg.rag.min_score
        ):
            return Prepared(
                result.mode_used, [], [], self._answer("low_relevance", result.mode_used)
            )
        rag_cfg = cfg.rag.model_copy(update={"max_chunks": wanted})
        hits = [self._masked(h) for h in result.hits]
        context, text = build_context(hits, rag_cfg, self._counter)
        if not context:
            return Prepared(result.mode_used, [], [], self._answer("no_context", result.mode_used))
        messages = self._prompt.render(context=text, question=question)
        return Prepared(result.mode_used, context, messages)

    def _masked(self, hit: SearchHit) -> SearchHit:
        if not self._guard.masks_pii:
            return hit
        return hit.model_copy(update={"content": self._guard.mask(hit.content).text})

    def _options(self) -> LLMOptions:
        return LLMOptions(
            max_output_tokens=self._settings.llm.max_output_tokens,
            temperature=self._settings.llm.temperature,
        )

    async def answer(
        self,
        *,
        question: str,
        identity: Identity,
        top_k: int | None = None,
        filters: SearchFilters | None = None,
    ) -> Answer:
        """The full answer with citations."""
        answer, _ = await self.answer_with_context(
            question=question, identity=identity, top_k=top_k, filters=filters
        )
        return answer

    async def answer_with_context(
        self,
        *,
        question: str,
        identity: Identity,
        top_k: int | None = None,
        filters: SearchFilters | None = None,
    ) -> tuple[Answer, list[ContextChunk]]:
        """The answer and the passages the model saw. For the evaluation runner: the passages
        are text of documents and must not be logged or returned by the API."""
        prepared = await self.prepare(
            question=question, identity=identity, top_k=top_k, filters=filters
        )
        if prepared.early is not None:
            return prepared.early, prepared.context
        with span("answer.llm", model=self._llm.model_name) as current:
            try:
                async with observed("llm"):
                    completion = await self._llm.complete(prepared.messages, self._options())
            except NonRetryableError as exc:
                raise UpstreamUnavailableError("The language model could not answer") from exc
            set_attributes(
                current,
                input_tokens=completion.usage.input_tokens,
                output_tokens=completion.usage.output_tokens,
            )
        return self.finish(prepared, completion.text, completion.usage), prepared.context

    def finish(self, prepared: Prepared, raw: str, usage: Usage) -> Answer:
        """Steps 6 and 7 on the model's text."""
        cfg = self._settings.rag
        if is_not_found(raw):
            return self._answer("not_found", prepared.mode_used, usage=usage)
        check = check_citations(raw, prepared.context)
        warnings: list[str] = []
        if check.invalid_refs:
            warnings.append("invalid_citations")
        if check.uncited:
            warnings.append("no_citations")
        if warnings and cfg.on_bad_citations == "reject":
            _log.warning("answer_rejected", reasons=warnings)
            return self._answer("unverified", prepared.mode_used, usage=usage, warnings=warnings)
        cleaned = self._guard.clean_answer(check.text)
        if cleaned.blocked:
            return self._answer("blocked", prepared.mode_used, usage=usage)
        if cleaned.masked:
            warnings.append("pii_masked")
        citations = [
            Citation(c.ref, c.doc_id, c.chunk_id, c.pages, self._guard.mask(c.snippet).text)
            for c in check.citations
        ]
        answer = self._answer("answered", prepared.mode_used, usage=usage, warnings=warnings)
        answer.answer = cleaned.text
        answer.citations = citations
        return answer

    async def stream_pieces(self, prepared: Prepared) -> AsyncIterator[str]:
        """Raw pieces from the model for the streaming endpoint."""
        try:
            async with observed("llm"):
                async for piece in self._llm.stream(prepared.messages, self._options()):
                    yield piece
        except NonRetryableError as exc:
            raise UpstreamUnavailableError("The language model could not answer") from exc

    def stream_usage(self, prepared: Prepared, raw: str) -> Usage:
        """Estimated usage for a streamed answer (the stream does not report it)."""
        return Usage(
            input_tokens=sum(estimate_tokens(m.content) for m in prepared.messages),
            output_tokens=estimate_tokens(raw),
        )
