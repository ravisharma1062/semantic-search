"""The filters a user may ask for: document type, date and tags (FR-8)."""

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SearchFilters(BaseModel):
    """Optional filters. All of them are added next to the access filter, never instead of it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    doc_type: list[str] = Field(default_factory=list, max_length=50)
    tags: list[str] = Field(default_factory=list, max_length=50)
    created_from: date | None = None
    created_to: date | None = None

    @model_validator(mode="after")
    def _check(self) -> "SearchFilters":
        for value in (*self.doc_type, *self.tags):
            if not value or len(value) > 128:
                raise ValueError("filter values must be 1 to 128 characters")
        if self.created_from and self.created_to and self.created_from > self.created_to:
            raise ValueError("created_from is after created_to")
        return self

    def clauses(self) -> list[dict[str, Any]]:
        """Elasticsearch filter clauses."""
        out: list[dict[str, Any]] = []
        if self.doc_type:
            out.append({"terms": {"doc_type": self.doc_type}})
        for tag in self.tags:
            out.append({"term": {"tags": tag}})  # every tag must be present
        if self.created_from or self.created_to:
            bounds: dict[str, str] = {}
            if self.created_from:
                bounds["gte"] = self.created_from.isoformat()
            if self.created_to:
                bounds["lte"] = self.created_to.isoformat()
            out.append({"range": {"created_at": bounds}})
        return out

    def cache_key(self) -> str:
        """A stable text for cache keys."""
        return self.model_dump_json(exclude_none=True)
