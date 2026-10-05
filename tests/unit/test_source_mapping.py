from datetime import UTC, datetime
from typing import Any

import pytest

from app.core.settings import SourceSettings
from app.ingestion.source import SourceDocument, SourcePage, map_source, source_includes

CFG = SourceSettings()


def _source(**changes: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "pages": [{"page_no": 1, "text": "page one"}, {"page_no": 2, "text": "page two"}],
        "doc_type": "contract",
        "tags": ["vendor", "2024"],
        "created_at": "2024-03-01T10:00:00Z",
        "owner": "user-1",
        "acl_users": ["user-1"],
        "acl_groups": ["group-a", "group-b"],
        "version": 17,
        "language": "en",
    }
    return {**base, **changes}


def test_full_document_is_mapped() -> None:
    doc = map_source("ITEM-1", _source(), CFG)
    assert doc.item_id == "ITEM-1"
    assert [(p.page_no, p.text) for p in doc.pages] == [(1, "page one"), (2, "page two")]
    assert doc.doc_type == "contract"
    assert doc.tags == ["vendor", "2024"]
    assert doc.created_at == datetime(2024, 3, 1, 10, 0, tzinfo=UTC)
    assert doc.owner == "user-1"
    assert doc.acl_users == ["user-1"]
    assert doc.acl_groups == ["group-a", "group-b"]
    assert doc.version == 17
    assert doc.language == "en"
    assert not doc.truncated
    assert doc.has_text


def test_field_names_come_from_settings() -> None:
    cfg = SourceSettings(pages_field="ocr_pages", page_text_field="body", page_no_field="n")
    doc = map_source("ITEM-1", {"ocr_pages": [{"n": 3, "body": "hello"}]}, cfg)
    assert [(p.page_no, p.text) for p in doc.pages] == [(3, "hello")]


def test_document_level_text_is_used_when_there_are_no_pages() -> None:
    doc = map_source("ITEM-1", {"ocr_text": "whole document"}, CFG)
    assert doc.pages == []
    assert doc.text == "whole document"
    assert doc.has_text


def test_pages_win_over_document_text() -> None:
    doc = map_source("ITEM-1", _source(ocr_text="whole document"), CFG)
    assert doc.text == ""
    assert len(doc.pages) == 2


@pytest.mark.parametrize("source", [{}, {"pages": []}, {"pages": None}, {"ocr_text": "  \n "}])
def test_missing_or_blank_text_gives_a_document_without_text(source: dict[str, Any]) -> None:
    doc = map_source("ITEM-1", source, CFG)
    assert not doc.has_text


def test_wrong_types_are_skipped_not_raised() -> None:
    source = {
        "pages": ["junk", {"page_no": "x", "text": "kept"}, {"text": 5}, {"page_no": 4}, None],
        "tags": 7,
        "owner": ["list"],
        "version": "abc",
        "created_at": "not a date",
        "acl_users": None,
        "acl_groups": "group-a",
        "doc_type": {"nested": 1},
    }
    doc = map_source("ITEM-1", source, CFG)
    assert [(p.page_no, p.text) for p in doc.pages] == [(2, "kept")]  # page number: position
    assert doc.tags == ["7"]
    assert doc.owner is None
    assert doc.version is None
    assert doc.created_at is None
    assert doc.acl_users == []
    assert doc.acl_groups == ["group-a"]
    assert doc.doc_type is None


def test_page_numbers_fall_back_to_the_position() -> None:
    doc = map_source("ITEM-1", {"pages": [{"text": "a"}, {"text": "b"}]}, CFG)
    assert [p.page_no for p in doc.pages] == [1, 2]


@pytest.mark.parametrize("bad_number", [0, -3, True, "zero"])
def test_invalid_page_numbers_fall_back_to_the_position(bad_number: object) -> None:
    doc = map_source("ITEM-1", {"pages": [{"page_no": bad_number, "text": "a"}]}, CFG)
    assert doc.pages[0].page_no == 1


def test_blank_acl_entries_are_dropped() -> None:
    doc = map_source("ITEM-1", {"acl_users": ["a", "", "  ", "b"]}, CFG)
    assert doc.acl_users == ["a", "b"]


def test_huge_document_is_cut_at_the_page_limit_and_flagged() -> None:
    cfg = SourceSettings(max_pages=3)
    pages = [{"page_no": i, "text": f"page {i}"} for i in range(1, 11)]
    doc = map_source("ITEM-1", {"pages": pages}, cfg)
    assert [p.page_no for p in doc.pages] == [1, 2, 3]
    assert doc.truncated


def test_huge_document_is_cut_at_the_size_limit_and_flagged() -> None:
    cfg = SourceSettings(max_chars=20)
    pages = [{"page_no": i, "text": "x" * 8} for i in range(1, 6)]
    doc = map_source("ITEM-1", {"pages": pages}, cfg)
    assert len(doc.pages) == 2
    assert doc.truncated


def test_huge_document_level_text_is_cut_and_flagged() -> None:
    doc = map_source("ITEM-1", {"ocr_text": "y" * 100}, SourceSettings(max_chars=30))
    assert len(doc.text) == 30
    assert doc.truncated


def test_a_document_at_the_limit_is_not_flagged() -> None:
    doc = map_source(
        "ITEM-1", {"pages": [{"page_no": 1, "text": "x" * 20}]}, SourceSettings(max_chars=20)
    )
    assert not doc.truncated


def test_only_the_needed_fields_are_fetched() -> None:
    assert set(source_includes(CFG)) == {
        "pages", "ocr_text", "doc_type", "tags", "created_at", "owner",
        "acl_users", "acl_groups", "version", "language",
    }  # fmt: skip


def test_model_has_text_property() -> None:
    assert SourceDocument(item_id="x", pages=[SourcePage(page_no=1, text="a")]).has_text
    assert not SourceDocument(item_id="x", pages=[SourcePage(page_no=1, text="  ")]).has_text
