"""Splits a document into chunks for embedding (HLD section 4).

- Headings and paragraphs first, then size: target 400 tokens, maximum 512, overlap 60.
- A heading starts a new chunk once the current one holds at least ``min_tokens``.
- Tables are separate chunks. They are cut between rows only, and the header row is repeated.
- Overlap is whole sentences taken from the end of the previous chunk, never across a heading
  or a table.
- Very short pieces are merged into the previous text chunk when they fit, and never dropped, so
  no text is lost. Only chunks without any letter or digit are skipped.
- Tokens are counted with the embedding model's tokenizer. A final check recounts each chunk and
  splits it further if it is over the maximum.
- Chunks are produced one by one (a generator), so a 3,000 page document is never held as a
  list of chunks.
"""

import asyncio
import hashlib
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Literal

import structlog
from pydantic import BaseModel, ConfigDict

from app.core.settings import ChunkingSettings
from app.ingestion.normalizer import is_table_line
from app.ingestion.source import SourceDocument
from app.ingestion.tokens import TokenCounter, count_tokens

_log = structlog.get_logger(__name__)

_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[\"'(\[]?[A-Z0-9])")
_NUMBERED_TITLE = re.compile(r"^\d+(?:\.\d+)*\.?\s+\S")
_TERMINAL = (".", "!", "?", ",", ";")
_MAX_TITLE_CHARS = 200
_MAX_HEADING_CHARS = 80


class Chunk(BaseModel):
    """One piece of a document, ready for embedding."""

    model_config = ConfigDict(frozen=True)

    item_id: str
    chunk_no: int
    chunk_id: str
    content: str
    page_start: int | None
    page_end: int | None
    section_title: str | None
    content_hash: str
    token_count: int
    chunker_version: str
    is_table: bool = False

    @property
    def embedding_text(self) -> str:
        """The text sent to the embedding model: the section title in front of the content."""
        title = self.section_title
        if title and not self.content.startswith(title):
            return f"{title}\n{self.content}"
        return self.content


def make_chunk_id(item_id: str, chunk_no: int, content_hash: str) -> str:
    """The deterministic ID. It is also the Elasticsearch ``_id`` (rule 4)."""
    return f"{item_id}:{chunk_no}:{content_hash}"


def content_hash_of(content: str) -> str:
    """Short hash of the chunk text."""
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def is_title(line: str) -> bool:
    """A heading line: short, no sentence punctuation, and ALL CAPS or a numbered section."""
    if len(line) > _MAX_HEADING_CHARS or line.endswith(_TERMINAL):
        return False
    letters = [c for c in line if c.isalpha()]
    shouting = len(letters) >= 3 and all(c.isupper() for c in letters)
    return shouting or bool(_NUMBERED_TITLE.match(line))


@dataclass
class _Unit:
    """A sentence, a heading line or a table row with its page and token count."""

    text: str
    tokens: int
    page: int | None
    joiner: str = "\n"  # what goes between this unit and the previous one


@dataclass
class _Draft:
    units: list[_Unit] = field(default_factory=list)
    title: str | None = None
    kind: Literal["text", "table"] = "text"
    carried: int = 0  # leading units that are overlap from the previous chunk

    def tokens(self) -> int:
        return sum(u.tokens for u in self.units)

    def new_tokens(self) -> int:
        return sum(u.tokens for u in self.units[self.carried :])


class Chunker:
    """Splits documents. One instance can be used for many documents."""

    def __init__(self, settings: ChunkingSettings, counter: TokenCounter) -> None:
        self._cfg = settings
        self._counter = counter

    def split(self, document: SourceDocument) -> list[Chunk]:
        """All chunks of the document."""
        return list(self.iter_chunks(document))

    async def split_async(self, document: SourceDocument) -> list[Chunk]:
        """Same as ``split``, in a worker thread so the event loop is not blocked."""
        return await asyncio.to_thread(self.split, document)

    def iter_chunks(self, document: SourceDocument) -> Iterator[Chunk]:
        """Chunks in document order, one at a time. Numbers are consecutive from 0."""
        number = 0
        for draft in self._drafts(document):
            for piece in self._pieces(draft):
                if not any(c.isalnum() for c in piece):
                    continue
                if number >= self._cfg.max_chunks_per_document:
                    _log.warning("chunks_capped", item_id=document.item_id, limit=number)
                    return
                yield self._build(document, draft, piece, number)
                number += 1

    # --- tokens -----------------------------------------------------------------------------

    def _count(self, text: str) -> int:
        return count_tokens(self._counter, text)

    def _cut(self, text: str, limit: int) -> list[str]:
        """Cut text into pieces of at most ``limit`` tokens, at token boundaries."""
        spans = self._counter.spans(text)
        if len(spans) <= limit:
            return [text]
        pieces: list[str] = []
        start = 0
        for end_index in range(limit, len(spans) + limit, limit):
            end_index = min(end_index, len(spans))
            piece = text[start : spans[end_index - 1][1]]
            pieces.append(piece.strip())
            start = spans[end_index - 1][1]
        return [p for p in pieces if p]

    # --- documents to units ---------------------------------------------------------------

    def _lines(self, document: SourceDocument) -> Iterator[tuple[str, int | None]]:
        """Non-empty lines with their page. A blank line is ``("", page)``."""
        if document.pages:
            for page in document.pages:
                for line in page.text.split("\n"):
                    yield line.rstrip(), page.page_no
        else:
            for line in document.text.split("\n"):
                yield line.rstrip(), None

    def _sentences(self, line: str, page: int | None) -> Iterator[_Unit]:
        """A text line as sentence units. Anything over the maximum is cut by tokens."""
        for index, sentence in enumerate(_SENTENCE_END.split(line.strip())):
            for piece_no, piece in enumerate(self._cut(sentence, self._cfg.max_tokens)):
                joiner = " " if index or piece_no else "\n"
                yield _Unit(piece, self._count(piece), page, joiner)

    # --- the main loop --------------------------------------------------------------------

    def _drafts(self, document: SourceDocument) -> Iterator[_Draft]:
        cfg = self._cfg
        draft = _Draft()
        pending: _Draft | None = None
        title: str | None = None
        table_rows: list[tuple[str, int | None]] = []

        def commit(new: _Draft) -> Iterator[_Draft]:
            """Hand over drafts in order. A small text draft joins the previous text draft."""
            nonlocal pending
            if (
                pending is not None
                and new.kind == "text"
                and pending.kind == "text"
                and new.new_tokens() < cfg.min_tokens
                and pending.tokens() + new.new_tokens() <= cfg.max_tokens
            ):
                pending.units.extend(new.units[new.carried :])
                return
            if pending is not None:
                yield pending
            pending = new

        def flush(*, overlap: bool) -> Iterator[_Draft]:
            nonlocal draft
            if not draft.units:
                return
            done = draft
            yield from commit(done)
            carry = self._overlap(done) if overlap else []
            draft = _Draft(units=list(carry), title=title, carried=len(carry))

        def flush_table() -> Iterator[_Draft]:
            nonlocal table_rows
            if table_rows:
                yield from flush(overlap=False)
                for table_draft in self._table_drafts(table_rows, title):
                    yield from commit(table_draft)
                table_rows = []

        for line, page in self._lines(document):
            if is_table_line(line):
                table_rows.append((line, page))
                continue
            yield from flush_table()
            if not line.strip():
                continue
            if is_title(line.strip()):
                if draft.new_tokens() >= cfg.min_tokens:
                    yield from flush(overlap=False)
                title = line.strip()[:_MAX_TITLE_CHARS]
                if not draft.units:
                    draft.title = title
                draft.units.append(_Unit(line.strip(), self._count(line), page))
                continue
            for unit in self._sentences(line, page):
                if draft.units and draft.tokens() + unit.tokens > cfg.target_tokens:
                    small = draft.new_tokens() < cfg.min_tokens
                    if not (small and draft.tokens() + unit.tokens <= cfg.max_tokens):
                        yield from flush(overlap=True)
                        if draft.tokens() + unit.tokens > cfg.max_tokens:
                            draft = _Draft(title=title)  # the overlap does not fit: drop it
                if not draft.units:
                    draft.title = title
                draft.units.append(unit)
        yield from flush_table()
        yield from flush(overlap=False)
        if pending is not None:
            yield pending

    def _overlap(self, draft: _Draft) -> list[_Unit]:
        """The last sentences of a chunk, up to ``overlap_tokens``."""
        carry: list[_Unit] = []
        total = 0
        for unit in reversed(draft.units):
            if total + unit.tokens > self._cfg.overlap_tokens:
                break
            carry.append(unit)
            total += unit.tokens
        carry.reverse()
        return carry

    def _table_drafts(
        self, rows: list[tuple[str, int | None]], title: str | None
    ) -> Iterator[_Draft]:
        """Table rows packed into chunks. Cut between rows only, header row repeated."""
        cfg = self._cfg
        header: _Unit | None = None
        current = _Draft(title=title, kind="table")
        for index, (text, page) in enumerate(rows):
            for piece_no, piece in enumerate(self._cut(text.strip(), cfg.max_tokens)):
                unit = _Unit(piece, self._count(piece), page)
                if index == 0 and piece_no == 0:
                    header = unit
                if current.units and current.tokens() + unit.tokens > cfg.target_tokens:
                    yield current
                    current = _Draft(title=title, kind="table")
                    if header is not None and header.tokens + unit.tokens <= cfg.max_tokens:
                        current.units.append(header)
                current.units.append(unit)
        if current.units:
            yield current

    # --- drafts to chunks -----------------------------------------------------------------

    def _pieces(self, draft: _Draft) -> list[str]:
        """The text of a draft. If it is over the maximum it is cut (a safety net)."""
        if draft.kind == "table":
            text = "\n".join(u.text for u in draft.units)
        else:
            text = ""
            for position, unit in enumerate(draft.units):
                text += unit.text if position == 0 else unit.joiner + unit.text
        return self._cut(text, self._cfg.max_tokens)

    def _build(self, document: SourceDocument, draft: _Draft, piece: str, number: int) -> Chunk:
        pages = [u.page for u in draft.units if u.page is not None]
        digest = content_hash_of(piece)
        return Chunk(
            item_id=document.item_id,
            chunk_no=number,
            chunk_id=make_chunk_id(document.item_id, number, digest),
            content=piece,
            page_start=min(pages) if pages else None,
            page_end=max(pages) if pages else None,
            section_title=draft.title,
            content_hash=digest,
            token_count=self._count(piece),
            chunker_version=self._cfg.version,
            is_table=draft.kind == "table",
        )
