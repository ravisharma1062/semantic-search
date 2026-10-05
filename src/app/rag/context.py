"""Builds the numbered context for the prompt (HLD section 7, "Context building").

From the ranked chunks it takes the best ones, drops near-duplicates, prefers different documents,
keeps within the token budget and numbers what is left ``[1]``, ``[2]``, ... The list of
``ContextChunk`` is also the only source for the citations in the answer: the model writes numbers,
the service maps them to documents and pages from this list.
"""

import re
from dataclasses import dataclass

from app.core.settings import RagSettings
from app.ingestion.tokens import TokenCounter, count_tokens
from app.retrieval.models import SearchHit

_WORD = re.compile(r"\w+")
_SHINGLE = 3
_MIN_USEFUL_TOKENS = 20


@dataclass(frozen=True)
class ContextChunk:
    """One numbered passage."""

    ref: int
    chunk_id: str
    doc_id: str
    pages: list[int]
    section_title: str | None
    text: str
    score: float


def _shingles(text: str) -> set[tuple[str, ...]]:
    words = _WORD.findall(text.lower())
    if len(words) < _SHINGLE:
        return {tuple(words)} if words else set()
    return {tuple(words[i : i + _SHINGLE]) for i in range(len(words) - _SHINGLE + 1)}


def similarity(a: str, b: str) -> float:
    """Share of shared word triples (Jaccard), 1.0 for the same text."""
    left, right = _shingles(a), _shingles(b)
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _label(hit: SearchHit) -> str:
    pages = hit.pages
    where = (
        f"page {pages[0]}" if len(pages) == 1 else f"pages {pages[0]}-{pages[-1]}" if pages else ""
    )
    parts = [hit.doc_id, *([hit.section_title] if hit.section_title else [])]
    if where:
        parts.append(where)
    return ", ".join(parts)


def _truncate(text: str, counter: TokenCounter, tokens: int) -> str:
    spans = counter.spans(text)
    if len(spans) <= tokens:
        return text
    return text[: spans[tokens - 1][1]]


def select_hits(hits: list[SearchHit], cfg: RagSettings) -> list[SearchHit]:
    """The best distinct chunks: no near-duplicates, at most N per document, at most
    ``max_chunks`` in total. Chunks from new documents come first, so one long document does not
    crowd out the others."""
    picked: list[SearchHit] = []
    per_doc: dict[str, int] = {}
    spill: list[SearchHit] = []
    for hit in hits:
        if any(
            similarity(hit.content, other.content) >= cfg.duplicate_similarity for other in picked
        ):
            continue
        if per_doc.get(hit.doc_id, 0) >= 1:
            spill.append(hit)
            continue
        per_doc[hit.doc_id] = 1
        picked.append(hit)
    for hit in spill:
        if len(picked) >= cfg.max_chunks:
            break
        if per_doc[hit.doc_id] >= cfg.max_chunks_per_document:
            continue
        if any(
            similarity(hit.content, other.content) >= cfg.duplicate_similarity for other in picked
        ):
            continue
        per_doc[hit.doc_id] += 1
        picked.append(hit)
    chosen = {h.chunk_id for h in picked[: cfg.max_chunks]}
    return [h for h in hits if h.chunk_id in chosen]


def build_context(
    hits: list[SearchHit], cfg: RagSettings, counter: TokenCounter
) -> tuple[list[ContextChunk], str]:
    """The numbered chunks and the context text for the prompt. Empty if nothing fits."""
    chunks: list[ContextChunk] = []
    lines: list[str] = []
    remaining = cfg.context_token_budget
    for hit in select_hits(hits, cfg):
        text = hit.content.strip()
        header_cost = count_tokens(counter, _label(hit)) + 3
        room = remaining - header_cost
        if room < min(_MIN_USEFUL_TOKENS, count_tokens(counter, text)):
            break
        text = _truncate(text, counter, room)
        ref = len(chunks) + 1
        chunks.append(
            ContextChunk(
                ref, hit.chunk_id, hit.doc_id, hit.pages, hit.section_title, text, hit.score
            )
        )
        lines.append(f"[{ref}] ({_label(hit)}) {text}")
        remaining -= header_cost + count_tokens(counter, text)
    return chunks, "\n\n".join(lines)
