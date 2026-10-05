import re

from hypothesis import given, settings
from hypothesis import strategies as st

from app.core.settings import NormalizerSettings
from app.ingestion.normalizer import normalize_document, normalize_pages, normalize_text
from app.ingestion.source import SourceDocument, SourcePage


def _pages(*texts: str) -> list[SourcePage]:
    return [SourcePage(page_no=i, text=t) for i, t in enumerate(texts, start=1)]


# --- characters -----------------------------------------------------------------------------


def test_ligatures_soft_hyphens_and_control_characters_are_cleaned() -> None:
    raw = (
        "o\N{LATIN SMALL LIGATURE FFI}ce\N{SOFT HYPHEN} work"
        "\N{LATIN SMALL LIGATURE FL}ow\x00 \N{ZERO WIDTH SPACE}done\x0c"
    )
    assert normalize_text(raw) == "office workflow done"


def test_line_endings_and_odd_spaces_are_unified() -> None:
    assert normalize_text("a\r\nb\N{NO-BREAK SPACE}\N{NO-BREAK SPACE}c") == "a b c"


def test_empty_and_blank_text() -> None:
    assert normalize_text("") == ""
    assert normalize_text(" \n \n\t") == ""


# --- hyphenation and broken lines -----------------------------------------------------------


def test_hyphenated_word_at_a_line_end_is_joined() -> None:
    assert normalize_text("The agree-\nment is valid.") == "The agreement is valid."


def test_hyphen_before_a_capital_or_digit_is_kept() -> None:
    assert normalize_text("Coca-\nCola") == "Coca- Cola"
    assert normalize_text("section 4-\n5 applies") == "section 4- 5 applies"


def test_a_dash_with_spaces_is_not_a_hyphen() -> None:
    assert normalize_text("range 5 -\nten") == "range 5 - ten"
    assert normalize_text("wait--\nnow") == "wait-- now"


def test_lines_broken_inside_a_paragraph_are_joined() -> None:
    raw = "The supplier shall deliver\nwithin fourteen days\nunless agreed otherwise"
    assert (
        normalize_text(raw)
        == "The supplier shall deliver within fourteen days unless agreed otherwise"
    )


def test_blank_lines_keep_paragraphs_apart() -> None:
    assert (
        normalize_text("first part\nof one\n\n\n\nsecond one") == "first part of one\n\nsecond one"
    )


def test_a_short_sentence_ends_a_paragraph() -> None:
    assert normalize_text("Signed.\nThe next part") == "Signed.\nThe next part"


def test_headings_and_list_items_keep_their_own_line() -> None:
    raw = "1. PAYMENT TERMS\nInvoices are due\n- first item\n- second item\nNotes:\nsee annex"
    assert normalize_text(raw).split("\n") == [
        "1. PAYMENT TERMS",
        "Invoices are due",
        "- first item",
        "- second item",
        "Notes:",
        "see annex",
    ]


# --- tables ---------------------------------------------------------------------------------


def test_table_rows_are_kept_line_by_line_with_their_column_spacing() -> None:
    raw = "Item    Qty    Price\nBolts    10    2.50\nNuts    20    1.10"
    assert normalize_text(raw) == raw


def test_pipe_and_tab_tables_are_kept() -> None:
    raw = "a | b | c\nd | e | f\n\nx\ty\nz\tw"
    assert normalize_text(raw) == raw


def test_text_after_a_table_is_not_glued_to_the_last_row() -> None:
    out = normalize_text("Qty    Price\n10    2.50\nTotal paid in full\nby the buyer")
    assert out.split("\n") == ["Qty    Price", "10    2.50", "Total paid in full by the buyer"]


# --- headers, footers, page numbers ---------------------------------------------------------


def _report(n: int) -> list[SourcePage]:
    return [
        SourcePage(
            page_no=i,
            text=f"ACME Bank Confidential\nBody text of page {i}\nmore body text\nPage {i} of {n}",
        )
        for i in range(1, n + 1)
    ]


def test_repeated_headers_footers_and_page_numbers_are_removed() -> None:
    result = normalize_pages(_report(6))
    assert len(result) == 6
    for page in result:
        assert "ACME" not in page.text
        assert "Page " not in page.text
        assert "Body text of page" in page.text


def test_page_numbers_and_count_never_change() -> None:
    result = normalize_pages(_report(6))
    assert [p.page_no for p in result] == [1, 2, 3, 4, 5, 6]


def test_pages_without_text_stay_in_the_list() -> None:
    result = normalize_pages(_pages("", "some text", "  \n "))
    assert [(p.page_no, p.text) for p in result] == [(1, ""), (2, "some text"), (3, "")]


def test_a_short_page_is_never_treated_as_all_header() -> None:
    pages = [SourcePage(page_no=i, text=f"Header line {i}") for i in range(1, 8)]
    assert [p.text for p in normalize_pages(pages)] == [f"Header line {i}" for i in range(1, 8)]


def test_a_line_that_repeats_only_in_the_middle_of_pages_is_kept() -> None:
    pages = [
        SourcePage(page_no=i, text=f"a\nb\nc\nd\nTOTAL DUE\ne\nf\ng\nh\nunique {i}")
        for i in range(1, 7)
    ]
    assert all("TOTAL DUE" in p.text for p in normalize_pages(pages))


def test_no_header_detection_for_short_documents() -> None:
    result = normalize_pages(_report(3))  # below repeat_min_pages
    assert "ACME" in result[0].text


def test_a_line_on_few_pages_is_not_a_header() -> None:
    pages = [SourcePage(page_no=i, text=f"Intro\nbody {i}\nend {i}") for i in range(1, 11)]
    pages[0] = SourcePage(page_no=1, text="Title page\nbody 1\nend 1")
    assert "Title page" in normalize_pages(pages)[0].text


def test_thresholds_come_from_settings() -> None:
    cfg = NormalizerSettings(repeat_min_pages=2, repeat_ratio=0.5)
    pages = _pages(
        "Top\nalpha\nbeta\ngamma\ndelta\nepsilon",
        "Top\nzeta\neta\ntheta\niota\nkappa",
    )
    assert normalize_pages(pages, cfg)[0].text == "alpha beta gamma delta epsilon"
    default = normalize_pages(pages)  # two pages are below the default minimum
    assert default[0].text.startswith("Top alpha")


def test_no_pages() -> None:
    assert normalize_pages([]) == []


# --- documents ------------------------------------------------------------------------------


def test_document_metadata_and_permissions_are_untouched() -> None:
    doc = SourceDocument(
        item_id="ITEM-1",
        pages=_pages("agree-\nment"),
        text="doc-\nument",
        acl_users=["u1"],
        acl_groups=["g1"],
        doc_type="contract",
        version=4,
    )
    result = normalize_document(doc)
    assert result.pages[0].text == "agreement"
    assert result.text == "document"
    assert result.model_dump(exclude={"pages", "text"}) == doc.model_dump(exclude={"pages", "text"})


# --- properties -----------------------------------------------------------------------------

_LINES = st.text(alphabet=st.characters(min_codepoint=32, max_codepoint=126), max_size=60)
_TEXT = st.lists(_LINES, max_size=12).map("\n".join)


def _alnum(text: str) -> str:
    return "".join(c for c in text if c.isalnum())


@given(_TEXT)
@settings(max_examples=200)
def test_text_normalization_loses_no_letters_or_digits(text: str) -> None:
    assert _alnum(normalize_text(text)) == _alnum(text)


@given(_TEXT)
@settings(max_examples=200)
def test_text_normalization_is_stable(text: str) -> None:
    once = normalize_text(text)
    assert normalize_text(once) == once


@given(_TEXT)
@settings(max_examples=200)
def test_text_has_no_blank_runs_or_edge_whitespace(text: str) -> None:
    out = normalize_text(text)
    assert out == out.strip()
    assert not re.search(r"\n\n\n", out)


@given(st.lists(_TEXT, max_size=8))
@settings(max_examples=100)
def test_pages_keep_numbers_order_and_count(texts: list[str]) -> None:
    pages = _pages(*texts)
    result = normalize_pages(pages)
    assert [p.page_no for p in result] == [p.page_no for p in pages]


@given(st.lists(_TEXT, max_size=3))
@settings(max_examples=100)
def test_short_documents_lose_no_letters_or_digits(texts: list[str]) -> None:
    result = normalize_pages(_pages(*texts))  # fewer pages than repeat_min_pages: nothing dropped
    assert [_alnum(p.text) for p in result] == [_alnum(t) for t in texts]
