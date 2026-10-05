"""Builds the Kafka events that the backfill and reconciliation jobs publish (schema v1)."""

import uuid
from datetime import UTC, datetime
from typing import Literal

from app.ingestion.events import EventType, IndexEvent


def build_event(
    item_id: str,
    event_type: EventType,
    *,
    source: str,
    priority: Literal["live", "backfill"],
    wave: int | None = None,
    doc_version: int | None = None,
    now: datetime | None = None,
) -> tuple[bytes, bytes]:
    """The Kafka key (``ITEM_ID``) and the JSON value. The event carries no text or permissions."""
    event = IndexEvent(
        schema_version=1,
        event_id=uuid.uuid4().hex,
        event_type=event_type,
        item_id=item_id,
        doc_version=doc_version,
        occurred_at=now or datetime.now(UTC),
        source=source,
        priority=priority,
        wave=wave,
    )
    return item_id.encode(), event.model_dump_json(exclude_none=True).encode()
