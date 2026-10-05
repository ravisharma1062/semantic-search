"""Source document model, the SourceReader interface and the mapping from the raw index document.

Field names come from settings (``SourceSettings``) and still need to be confirmed with the Java
team. Everything read from the index is treated as untrusted: wrong types are skipped, not raised.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

import structlog
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from app.core.settings import SourceSettings

_log = structlog.get_logger(__name__)
_DATETIME = TypeAdapter(datetime)


class SourcePage(BaseModel):
    """OCR text of one page."""

    page_no: int = Field(ge=1)
    text: str


class SourceDocument(BaseModel):
    """One document read from the existing index by ``ITEM_ID``.

    Text is per page when the index keeps pages, otherwise at document level in ``text``.
    """

    item_id: str
    pages: list[SourcePage] = []
    text: str = ""
    doc_type: str | None = None
    tags: list[str] = []
    created_at: datetime | None = None
    owner: str | None = None
    language: str | None = None
    version: int | None = None
    acl_users: list[str] = []
    acl_groups: list[str] = []
    truncated: bool = False

    @property
    def has_text(self) -> bool:
        """False for a document with no usable OCR text. Such a document is skipped."""
        return any(p.text.strip() for p in self.pages) or bool(self.text.strip())


@runtime_checkable
class SourceReader(Protocol):
    """Reads documents from the existing document index."""

    async def get(self, item_id: str) -> SourceDocument | None:
        """Return the document, or ``None`` if it does not exist."""
        ...

    async def get_many(self, item_ids: list[str]) -> dict[str, SourceDocument]:
        """Return the documents found. Missing IDs are left out."""
        ...


def source_includes(cfg: SourceSettings) -> list[str]:
    """The fields to fetch. Nothing else is read from the (large) source documents."""
    return [
        cfg.pages_field,
        cfg.text_field,
        cfg.doc_type_field,
        cfg.tags_field,
        cfg.created_at_field,
        cfg.owner_field,
        cfg.acl_users_field,
        cfg.acl_groups_field,
        cfg.version_field,
        cfg.language_field,
    ]


def _as_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    return [str(v).strip() for v in items if v is not None and str(v).strip()]


def _as_str(value: Any) -> str | None:
    return str(value).strip() or None if isinstance(value, str | int) else None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_datetime(value: Any) -> datetime | None:
    try:
        return _DATETIME.validate_python(value)
    except ValidationError:
        return None


def _read_pages(raw: Any, cfg: SourceSettings) -> tuple[list[SourcePage], bool]:
    """Pages in order. Entries without text are skipped. Stops at the page and size limits."""
    pages: list[SourcePage] = []
    if not isinstance(raw, list):
        return pages, False
    total = 0
    for position, entry in enumerate(raw, start=1):
        if not isinstance(entry, Mapping):
            continue
        text = entry.get(cfg.page_text_field)
        if not isinstance(text, str):
            continue
        if len(pages) >= cfg.max_pages or total + len(text) > cfg.max_chars:
            return pages, True
        number = _as_int(entry.get(cfg.page_no_field))
        pages.append(SourcePage(page_no=number if number and number >= 1 else position, text=text))
        total += len(text)
    return pages, False


def map_source(item_id: str, source: Mapping[str, Any], cfg: SourceSettings) -> SourceDocument:
    """Build a ``SourceDocument`` from the raw ``_source`` of the index."""
    pages, truncated = _read_pages(source.get(cfg.pages_field), cfg)
    text = ""
    if not pages:
        raw_text = source.get(cfg.text_field)
        if isinstance(raw_text, str):
            truncated = len(raw_text) > cfg.max_chars
            text = raw_text[: cfg.max_chars]
    if truncated:
        _log.warning("source_truncated", item_id=item_id)
    return SourceDocument(
        item_id=item_id,
        pages=pages,
        text=text,
        doc_type=_as_str(source.get(cfg.doc_type_field)),
        tags=_as_str_list(source.get(cfg.tags_field)),
        created_at=_as_datetime(source.get(cfg.created_at_field)),
        owner=_as_str(source.get(cfg.owner_field)),
        language=_as_str(source.get(cfg.language_field)),
        version=_as_int(source.get(cfg.version_field)),
        acl_users=_as_str_list(source.get(cfg.acl_users_field)),
        acl_groups=_as_str_list(source.get(cfg.acl_groups_field)),
        truncated=truncated,
    )
