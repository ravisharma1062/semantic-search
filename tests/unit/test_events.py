import json
from datetime import UTC, datetime

import pytest

from app.ingestion.events import EventType, InvalidEventError, parse_event

_VALID = {
    "schema_version": 1,
    "event_id": "b6f1c1de-5f7d-4b43-9a53-0b7f4c2d1a11",
    "event_type": "UPSERT",
    "item_id": "ITEM-1",
    "doc_version": 17,
    "occurred_at": "2026-10-05T10:15:30Z",
    "source": "java-ingestion",
    "priority": "live",
}


def _raw(**changes: object) -> bytes:
    data = {**_VALID, **changes}
    return json.dumps({k: v for k, v in data.items() if v is not ...}).encode()


def test_valid_event_is_parsed() -> None:
    event = parse_event(_raw())
    assert event.event_type is EventType.UPSERT
    assert event.item_id == "ITEM-1"
    assert event.doc_version == 17
    assert event.occurred_at == datetime(2026, 10, 5, 10, 15, 30, tzinfo=UTC)


@pytest.mark.parametrize("event_type", ["UPSERT", "DELETE", "ACL_CHANGE"])
def test_all_event_types_are_accepted(event_type: str) -> None:
    assert parse_event(_raw(event_type=event_type)).event_type.value == event_type


def test_unknown_fields_are_ignored() -> None:
    assert parse_event(_raw(added_later="x")).item_id == "ITEM-1"


def test_optional_fields_have_defaults() -> None:
    event = parse_event(_raw(doc_version=..., priority=...))
    assert event.doc_version is None
    assert event.priority == "live"
    assert event.wave is None


def test_time_without_zone_is_read_as_utc() -> None:
    event = parse_event(_raw(occurred_at="2026-10-05T10:15:30"))
    assert event.occurred_at.tzinfo is UTC


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"",
        b"not json",
        b"\xff\xfe",
        b"[1, 2]",
        b'"text"',
        _raw(schema_version=2),
        _raw(event_type="REINDEX"),
        _raw(item_id=""),
        _raw(item_id=...),
        _raw(occurred_at="yesterday"),
        _raw(doc_version=-1),
        _raw(priority="urgent"),
    ],
)
def test_invalid_messages_are_rejected(raw: bytes | None) -> None:
    with pytest.raises(InvalidEventError):
        parse_event(raw)


def test_reason_never_contains_message_content() -> None:
    with pytest.raises(InvalidEventError) as error:
        parse_event(_raw(event_type="synthetic-secret-value"))
    assert "synthetic-secret-value" not in str(error.value)
    assert "event_type" in str(error.value)
