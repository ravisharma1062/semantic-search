"""Request and response models of the API: the OpenAPI contract (``openapi/openapi.yaml``).

Inside one API version fields are added, never removed or renamed (HLD section 8).
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.retrieval.filters import SearchFilters


class ErrorBody(BaseModel):
    """Details of an error."""

    code: str
    message: str
    request_id: str | None = None


class ErrorResponse(BaseModel):
    """The common error format. ``UPSTREAM_UNAVAILABLE`` and ``TIMEOUT`` make the caller fall
    back."""

    error: ErrorBody


class SearchRequest(BaseModel):
    """``POST /v1/search``."""

    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1, max_length=4000)
    top_k: int | None = Field(default=None, ge=1, le=200)
    mode: Literal["hybrid", "bm25", "vector"] = "hybrid"
    rerank: bool | None = None
    filters: SearchFilters = Field(default_factory=SearchFilters)
    group_by_document: bool = False


class SearchResultItem(BaseModel):
    """One found chunk. No chunk text beyond the snippet."""

    doc_id: str
    chunk_id: str
    score: float
    pages: list[int]
    snippet: str
    highlights: list[str]


class SearchResponse(BaseModel):
    """The answer of ``POST /v1/search``. ``mode_used`` says which search really ran."""

    request_id: str | None
    mode_used: str
    results: list[SearchResultItem]
    took_ms: int


class AnswerRequest(BaseModel):
    """``POST /v1/answer`` and ``POST /v1/answer/stream``."""

    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=4000)
    top_k: int | None = Field(default=None, ge=1, le=20)
    filters: SearchFilters = Field(default_factory=SearchFilters)


class CitationItem(BaseModel):
    """A source of the answer. ``ref`` is the number used in the answer text."""

    ref: int
    doc_id: str
    pages: list[int]
    snippet: str


class UsageItem(BaseModel):
    """Token counts of the model call."""

    input_tokens: int
    output_tokens: int


class AnswerResponse(BaseModel):
    """The answer of ``POST /v1/answer``.

    ``found`` is false when there is no answer. ``reason`` tells why: ``not_found`` (the model
    found nothing), ``low_relevance`` (the gate stopped weak retrieval before the model),
    ``no_context``, ``unverified`` (the answer had no valid citation), ``blocked`` or ``refused``
    (guardrails).
    """

    request_id: str | None
    answer: str
    found: bool
    reason: str
    citations: list[CitationItem]
    warnings: list[str]
    mode_used: str
    model: str
    prompt_version: str
    usage: UsageItem
