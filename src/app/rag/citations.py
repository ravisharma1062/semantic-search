"""Reads the source numbers from the answer and maps them to documents (HLD section 7).

The model writes only numbers like ``[1]`` or ``[2][3]``. The service maps each number to a
``doc_id`` and pages from its own context list, so the model can never invent a document reference.
"""

import re
from dataclasses import dataclass

from app.rag.context import ContextChunk

NOT_FOUND = "NOT_FOUND"
_REF = re.compile(r"\[(\d{1,3})\]")


@dataclass(frozen=True)
class Citation:
    """A source that the answer cites."""

    ref: int
    doc_id: str
    chunk_id: str
    pages: list[int]
    snippet: str


@dataclass(frozen=True)
class CitationCheck:
    """The result of checking an answer against the context."""

    text: str  # the answer; with ``flag`` it has the invalid markers removed
    citations: list[Citation]
    invalid_refs: list[int]
    uncited: bool  # no valid citation at all


def is_not_found(answer: str) -> bool:
    """True if the model said it cannot answer."""
    return answer.strip().strip(".").strip() == NOT_FOUND


def snippet_of(text: str, limit: int = 240) -> str:
    """The start of a passage, on a word boundary."""
    flat = " ".join(text.split())
    if len(flat) <= limit:
        return flat
    return flat[:limit].rsplit(" ", 1)[0] + "..."


def check_citations(answer: str, context: list[ContextChunk]) -> CitationCheck:
    """Map the numbers in the answer to the context. Numbers that are not in the context are
    reported and removed from the text."""
    by_ref = {c.ref: c for c in context}
    seen: list[int] = []
    invalid: list[int] = []
    for match in _REF.finditer(answer):
        ref = int(match.group(1))
        target = seen if ref in by_ref else invalid
        if ref not in target:
            target.append(ref)
    text = _REF.sub(lambda m: m.group(0) if int(m.group(1)) in by_ref else "", answer)
    text = re.sub(r"[ \t]{2,}", " ", text).strip()
    citations = [
        Citation(
            r, by_ref[r].doc_id, by_ref[r].chunk_id, by_ref[r].pages, snippet_of(by_ref[r].text)
        )
        for r in seen
    ]
    return CitationCheck(text, citations, invalid, uncited=not citations)
