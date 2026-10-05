"""Search results as the service uses them."""

import html
import re
from typing import Any, Literal

from pydantic import BaseModel

_EM = re.compile(r"<em>(.*?)</em>", re.DOTALL)
_TAGS = re.compile(r"</?em>")
_WORD = re.compile(r"[a-z0-9]+")

SearchMode = Literal["hybrid", "bm25", "knn"]


class SearchHit(BaseModel):
    """One chunk found by a search. ``content`` is for the reranker and the RAG context, and is
    never part of a search response."""

    chunk_id: str
    doc_id: str
    score: float
    page_start: int | None = None
    page_end: int | None = None
    section_title: str | None = None
    content: str
    snippet: str
    highlights: list[str] = []

    @property
    def pages(self) -> list[int]:
        """Page numbers of the chunk, first to last. Empty for document-level text."""
        if self.page_start is None:
            return []
        end = self.page_end if self.page_end is not None else self.page_start
        return list(range(self.page_start, max(self.page_start, end) + 1))[:50]


class SearchOutcome(BaseModel):
    """What the searcher found, and which search ran."""

    hits: list[SearchHit]
    mode: SearchMode


def _query_terms(query: str) -> list[str]:
    return [w for w in dict.fromkeys(_WORD.findall(query.lower())) if len(w) >= 3]


def _local_snippet(content: str, query: str, limit: int) -> tuple[str, list[str]]:
    """A snippet and highlight terms made here, for hits that Elasticsearch did not highlight
    (vector-only hits, and the rrf retriever, which cannot be combined with highlighting)."""
    text = content.strip()
    lowered = text.lower()
    found = sorted((lowered.find(term), term) for term in _query_terms(query) if term in lowered)
    terms = [term for _, term in found]
    start = max(0, found[0][0] - limit // 4) if found else 0
    window = text[start : start + limit].strip()
    prefix = "..." if start > 0 else ""
    suffix = "..." if start + limit < len(text) else ""
    return f"{prefix}{window}{suffix}", terms


def _snippet(content: str, fragment: str | None, limit: int, query: str) -> tuple[str, list[str]]:
    if fragment:
        terms = list(dict.fromkeys(html.unescape(m).lower() for m in _EM.findall(fragment)))
        return html.unescape(_TAGS.sub("", fragment)).strip(), terms
    return _local_snippet(content, query, limit)


def parse_hit(hit: dict[str, Any], snippet_chars: int, query: str = "") -> SearchHit:
    """A hit of the Elasticsearch answer."""
    source = hit["_source"]
    fragments = (hit.get("highlight") or {}).get("content") or []
    snippet, terms = _snippet(
        source.get("content", ""), fragments[0] if fragments else None, snippet_chars, query
    )
    return SearchHit(
        chunk_id=source["chunk_id"],
        doc_id=source["doc_id"],
        score=float(hit.get("_score") or 0.0),
        page_start=source.get("page_start"),
        page_end=source.get("page_end"),
        section_title=source.get("section_title"),
        content=source.get("content", ""),
        snippet=snippet,
        highlights=terms,
    )


def rrf_merge(lists: list[list[SearchHit]], rank_constant: int, size: int) -> list[SearchHit]:
    """Reciprocal Rank Fusion: ``score(d) = sum of 1 / (k + rank)`` over the lists (HLD section
    6)."""
    scores: dict[str, float] = {}
    best: dict[str, SearchHit] = {}
    for hits in lists:
        for rank, hit in enumerate(hits, start=1):
            scores[hit.chunk_id] = scores.get(hit.chunk_id, 0.0) + 1.0 / (rank_constant + rank)
            kept = best.get(hit.chunk_id)
            if kept is None or (not kept.highlights and hit.highlights):
                best[hit.chunk_id] = hit  # keep the one with highlights
    ordered = sorted(scores, key=lambda cid: (-scores[cid], cid))[:size]
    return [best[cid].model_copy(update={"score": scores[cid]}) for cid in ordered]
