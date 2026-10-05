"""Kafka event schema, version 1 (HLD section 15).

The message carries only the ``item_id`` and metadata. Never document text or permissions.
Unknown fields are ignored, so the Java side can add fields inside one schema version.
"""

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class EventType(StrEnum):
    """What happened to the document."""

    UPSERT = "UPSERT"
    DELETE = "DELETE"
    ACL_CHANGE = "ACL_CHANGE"


class InvalidEventError(Exception):
    """The message is not a valid schema v1 event. The reason never contains message content."""


class IndexEvent(BaseModel):
    """One document event."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    schema_version: Literal[1]
    event_id: str = Field(min_length=1, max_length=128)
    event_type: EventType
    item_id: str = Field(min_length=1, max_length=256)
    doc_version: int | None = Field(None, ge=0)
    occurred_at: datetime
    source: str = Field(min_length=1, max_length=64)
    priority: Literal["live", "backfill"] = "live"
    wave: int | None = Field(None, ge=0)

    @field_validator("occurred_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        """A time without a zone is read as UTC, so events can always be ordered."""
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def parse_event(raw: bytes | None) -> IndexEvent:
    """Validate a message value. Raises ``InvalidEventError`` with a content-free reason."""
    if raw is None:
        raise InvalidEventError("empty message value")
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEventError("value is not valid JSON") from exc
    if not isinstance(data, dict):
        raise InvalidEventError("value is not a JSON object")
    try:
        return IndexEvent.model_validate(data)
    except ValidationError as exc:
        # Field names and error types only: the default text echoes the input value.
        problems = sorted({f"{'.'.join(map(str, e['loc']))}:{e['type']}" for e in exc.errors()})
        raise InvalidEventError("schema v1 violation: " + ", ".join(problems)) from exc
