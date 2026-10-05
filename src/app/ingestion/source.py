"""Source document model and the SourceReader interface.

The ``SourceDocument`` here is a minimal version. Task T1.3 owns the real one, with
field names confirmed by the Java team.
"""

from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field


class SourcePage(BaseModel):
    """OCR text of one page."""

    page_no: int = Field(ge=1)
    text: str


class SourceDocument(BaseModel):
    """One document read from the existing index by ``ITEM_ID``."""

    item_id: str
    pages: list[SourcePage] = []
    text: str = ""
    acl_users: list[str] = []
    acl_groups: list[str] = []
    metadata: dict[str, str] = {}


@runtime_checkable
class SourceReader(Protocol):
    """Reads documents from the existing document index."""

    async def get(self, item_id: str) -> SourceDocument | None:
        """Return the document, or ``None`` if it does not exist."""
        ...

    async def get_many(self, item_ids: list[str]) -> dict[str, SourceDocument]:
        """Return the documents found. Missing IDs are left out."""
        ...
