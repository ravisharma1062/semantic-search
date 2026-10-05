"""The chunker. Small token limits make every rule easy to see."""

import itertools
import tracemalloc
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pydantic import ValidationError
from tokenizers import Tokenizer, models, pre_tokenizers

from app.core.errors import NonRetryableError
from app.core.settings import ChunkingSettings
from app.ingestion.chunker import Chunk, Chunker, content_hash_of, is_title, make_chunk_id
from app.ingestion.source import SourceDocument, SourcePage
from app.ingestion.tokens import (
    HfTokenCounter,
    TokenCounter,
    WhitespaceTokenCounter,
    count_tokens,
    create_token_counter,
)

TARGET, MAX, OVERLAP, MIN = 20, 30, 6, 5


def _settings(**changes: object) -> ChunkingSettings:
    values: dict[str, object] = {
        "version": "v-test",
        "target_tokens": TARGET,
        "max_tokens": MAX,
        "overlap_tokens": OVERLAP,
        "min_tokens": MIN,
        "tokenizer": "whitespace",
    }
    return ChunkingSettings(**{**values, **changes})


def _chunker(**changes: object) -> Chunker:
    return Chunker(_settings(**changes), WhitespaceTokenCounter())


def _doc(*pages: str, item_id: str = "ITEM-1") -> SourceDocument:
    return SourceDocument(
        item_id=item_id, pages=[SourcePage(page_no=i, text=t) for i, t in enumerate(pages, 1)]
    )


def _words(count: int, start: int = 0) -> str:
    return " ".join(f"w{i}" for i in range(start, start + count))


def _sentences(count: int, words_each: int = 5) -> str:
    """Sentences of unique words: 'W0 w1 w2 w3 w4. W5 ...'"""
    out = []
    for s in range(count):
        ws = [f"w{s * words_each + i}" for i in range(words_each)]
        ws[0] = ws[0].capitalize()
        out.append(" ".join(ws) + ".")
    return " ".join(out)


# --- basics ---------------------------------------------------------------------------------


@pytest.mark.parametrize("text", ["", "  ", "\n\n \n"])
def test_empty_document_gives_no_chunks(text: str) -> None:
    assert _chunker().split(_doc(text)) == []
    assert _chunker().split(SourceDocument(item_id="X", text=text)) == []
    assert _chunker().split(SourceDocument(item_id="X")) == []


def test_short_document_is_one_chunk_with_ids_and_pages() -> None:
    [chunk] = _chunker().split(_doc("A short paragraph of text."))
    assert chunk.content == "A short paragraph of text."
    assert chunk.chunk_no == 0
    assert chunk.chunk_id == f"ITEM-1:0:{chunk.content_hash}"
    assert chunk.content_hash == content_hash_of(chunk.content)
    assert (chunk.page_start, chunk.page_end) == (1, 1)
    assert chunk.token_count == 5
    assert chunk.chunker_version == "v-test"
    assert not chunk.is_table


def test_chunk_ids_are_deterministic_and_follow_the_rule() -> None:
    doc = _doc(_sentences(12))
    first = _chunker().split(doc)
    again = _chunker().split(doc)
    assert [c.chunk_id for c in first] == [c.chunk_id for c in again]
    for chunk in first:
        assert chunk.chunk_id == make_chunk_id("ITEM-1", chunk.chunk_no, chunk.content_hash)


def test_different_text_gives_a_different_id_and_same_text_a_different_number() -> None:
    a = _chunker().split(_doc("One sentence here."))
    b = _chunker().split(_doc("Another sentence here."))
    assert a[0].chunk_id != b[0].chunk_id
    twice = _chunker().split(_doc("Same words here.\n\n" + _sentences(8) + "\n\nSame words here."))
    assert len({c.chunk_id for c in twice}) == len(twice)


def test_chunk_numbers_are_consecutive_from_zero() -> None:
    chunks = _chunker().split(_doc(_sentences(30)))
    assert [c.chunk_no for c in chunks] == list(range(len(chunks)))


# --- size and overlap -----------------------------------------------------------------------


def test_long_text_is_split_into_chunks_within_the_limits() -> None:
    chunks = _chunker().split(_doc(_sentences(40)))  # 200 words
    assert len(chunks) > 5
    assert all(c.token_count <= MAX for c in chunks)
    assert all(c.token_count <= TARGET for c in chunks[:-1])


def test_consecutive_chunks_overlap_by_whole_sentences() -> None:
    chunks = _chunker().split(_doc(_sentences(40)))
    for previous, following in itertools.pairwise(chunks):
        shared = set(previous.content.lower().split()) & set(following.content.lower().split())
        assert 0 < len(shared) <= OVERLAP
        assert following.content.lower().split()[0] in previous.content.lower().split()


def test_zero_overlap_means_no_shared_words() -> None:
    chunks = _chunker(overlap_tokens=0).split(_doc(_sentences(40)))
    for previous, following in itertools.pairwise(chunks):
        assert not set(previous.content.lower().split()) & set(following.content.lower().split())


def test_a_sentence_longer_than_the_maximum_is_cut_without_losing_words() -> None:
    chunks = _chunker().split(_doc(_words(100)))  # no punctuation at all
    assert all(c.token_count <= MAX for c in chunks)
    joined = " ".join(c.content for c in chunks).split()
    assert joined == _words(100).split()


def test_target_and_limits_come_from_settings() -> None:
    small = _chunker(target_tokens=10, max_tokens=12, overlap_tokens=2, min_tokens=2)
    chunks = small.split(_doc(_sentences(20)))
    assert all(c.token_count <= 12 for c in chunks)
    assert len(chunks) > len(_chunker().split(_doc(_sentences(20))))


# --- headings -------------------------------------------------------------------------------


def test_a_heading_starts_a_new_chunk_and_gives_the_section_title() -> None:
    text = f"{_sentences(3)}\n1. PAYMENT TERMS\n{_sentences(3)}"
    chunks = _chunker().split(_doc(text))
    assert len(chunks) == 2
    assert chunks[0].section_title is None
    assert chunks[1].section_title == "1. PAYMENT TERMS"
    assert chunks[1].content.startswith("1. PAYMENT TERMS")


def test_a_small_section_does_not_become_a_tiny_chunk() -> None:
    text = "INTRO\nShort intro.\nSECOND PART\n" + _sentences(2)
    chunks = _chunker().split(_doc(text))
    assert len(chunks) == 1  # the small first section is merged forward
    assert "INTRO" in chunks[0].content
    assert chunks[0].section_title == "INTRO"


def test_the_title_stays_for_all_following_chunks_of_the_section() -> None:
    chunks = _chunker().split(_doc("DELIVERY TERMS\n" + _sentences(40)))
    assert len(chunks) > 3
    assert {c.section_title for c in chunks} == {"DELIVERY TERMS"}


def test_embedding_text_puts_the_title_in_front() -> None:
    chunks = _chunker().split(_doc("DELIVERY TERMS\n" + _sentences(40)))
    assert chunks[0].embedding_text == chunks[0].content  # it starts with the title already
    assert chunks[1].embedding_text == "DELIVERY TERMS\n" + chunks[1].content


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("PAYMENT TERMS", True),
        ("1. Payment terms", True),
        ("2.1 Late delivery", True),
        ("The supplier shall pay.", False),
        ("Notes:", False),
        ("OK", False),
        ("A very long ALL CAPS line " * 5, False),
        ("INVOICE DUE.", False),
    ],
)
def test_title_detection(line: str, expected: bool) -> None:
    assert is_title(line) is expected


# --- tables ---------------------------------------------------------------------------------

_ROWS = [f"Item{i}    {i * 3}    {i}.50" for i in range(1, 7)]


def test_a_table_is_its_own_chunk_between_text_chunks() -> None:
    text = "Before the table.\n" + "\n".join(_ROWS[:3]) + "\nAfter the table."
    chunks = _chunker().split(_doc(text))
    assert [c.is_table for c in chunks] == [False, True, False]
    assert chunks[1].content == "\n".join(_ROWS[:3])  # column spacing is kept
    assert chunks[0].content == "Before the table."
    assert chunks[2].content == "After the table."


def test_a_big_table_is_cut_between_rows_with_the_header_repeated() -> None:
    rows = ["Name    Qty    Price", *[f"Row{i}    {i}    {i}.00" for i in range(1, 25)]]
    chunks = [c for c in _chunker().split(_doc("\n".join(rows))) if c.is_table]
    assert len(chunks) > 1
    for chunk in chunks:
        lines = chunk.content.split("\n")
        assert lines[0] == rows[0]  # header on every chunk
        assert all(line in rows for line in lines)  # only whole rows
        assert chunk.token_count <= MAX
    body = [line for c in chunks for line in c.content.split("\n") if line != rows[0]]
    assert body == rows[1:]  # every row exactly once, in order


def test_a_row_longer_than_the_maximum_is_cut_as_a_last_resort() -> None:
    long_row = "Col    " + _words(80)
    chunks = [c for c in _chunker().split(_doc(long_row)) if c.is_table]
    assert chunks
    assert all(c.token_count <= MAX for c in chunks)


def test_a_table_that_continues_on_the_next_page_is_one_table_with_both_pages() -> None:
    chunks = _chunker().split(_doc("\n".join(_ROWS[:2]), "\n".join(_ROWS[2:4])))
    [table] = [c for c in chunks if c.is_table]
    assert (table.page_start, table.page_end) == (1, 2)
    assert table.content == "\n".join(_ROWS[:4])


def test_tables_get_no_overlap() -> None:
    text = _sentences(6) + "\n" + "\n".join(_ROWS[:2]) + "\n" + _sentences(6, 100)
    chunks = _chunker().split(_doc(text))
    table_index = next(i for i, c in enumerate(chunks) if c.is_table)
    assert not set(chunks[table_index - 1].content.split()) & set(
        chunks[table_index].content.split()
    )
    assert not set(chunks[table_index + 1].content.split()) & set(
        chunks[table_index].content.split()
    )


# --- pages ----------------------------------------------------------------------------------


def test_chunk_pages_span_the_pages_of_their_text() -> None:
    page_one = _sentences(3)  # 15 words
    page_two = _sentences(3, 5).replace("w", "x")  # 15 more words, so one chunk crosses the break
    chunks = _chunker().split(_doc(page_one, page_two))
    assert chunks[0].page_start == 1
    assert chunks[-1].page_end == 2
    assert any(c.page_start == 1 and c.page_end == 2 for c in chunks)
    assert all(c.page_start is not None and c.page_start <= (c.page_end or 0) for c in chunks)


def test_document_level_text_has_no_pages() -> None:
    chunks = _chunker().split(SourceDocument(item_id="X", text=_sentences(10)))
    assert chunks
    assert all(c.page_start is None and c.page_end is None for c in chunks)


def test_empty_pages_are_skipped_but_later_pages_keep_their_numbers() -> None:
    [chunk] = _chunker().split(_doc("", "", "Only on page three."))
    assert (chunk.page_start, chunk.page_end) == (3, 3)


# --- small pieces ---------------------------------------------------------------------------


def test_a_short_last_piece_is_merged_into_the_previous_chunk_and_not_lost() -> None:
    text = _sentences(4) + " " + "Tail one."  # 20 words + a 2-word remainder
    chunks = _chunker().split(_doc(text))
    assert "Tail one." in chunks[-1].content
    assert chunks[-1].token_count <= MAX
    assert " ".join(c.content for c in chunks).count("Tail") == 1


def test_a_short_piece_that_does_not_fit_stays_a_chunk_of_its_own() -> None:
    chunks = _chunker(target_tokens=10, max_tokens=12, overlap_tokens=0, min_tokens=5).split(
        _doc(_sentences(2) + " Tail.")
    )
    assert chunks[-1].content.endswith("Tail.")


def test_chunks_without_letters_or_digits_are_skipped() -> None:
    assert _chunker().split(_doc("-----\n=====")) == []
    chunks = _chunker().split(_doc("Real text here.\n-----"))
    assert all(any(c.isalnum() for c in chunk.content) for chunk in chunks)


def test_the_chunk_cap_stops_a_huge_document() -> None:
    chunks = _chunker(max_chunks_per_document=3).split(_doc(_sentences(100)))
    assert len(chunks) == 3


@pytest.mark.parametrize(
    "text", ["Naïve café — 日本語のテキスト。 Ünïcödé text.", "emoji 😀 text."]
)
def test_non_ascii_text_is_kept(text: str) -> None:
    [chunk] = _chunker().split(_doc(text))
    assert chunk.content == text


async def test_split_async_gives_the_same_chunks() -> None:
    doc = _doc(_sentences(20))
    assert await _chunker().split_async(doc) == _chunker().split(doc)


def test_chunks_are_produced_lazily() -> None:
    iterator = _chunker().iter_chunks(_doc(*[_sentences(10)] * 500))
    first = next(iterator)
    assert first.chunk_no == 0


# --- settings and tokenizers ----------------------------------------------------------------


@pytest.mark.parametrize(
    "changes",
    [
        {"overlap_tokens": 20},
        {"target_tokens": 40},
        {"min_tokens": 25},
        {"tokenizer": "hf"},
    ],
)
def test_inconsistent_settings_are_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _settings(**changes)


def _write_tokenizer(path: Path) -> Path:
    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]"))  # noqa: S106
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()  # splits punctuation off, like most
    tokenizer.save(str(path))
    return path


def test_hf_counter_counts_with_the_tokenizer_file(tmp_path: Path) -> None:
    counter = HfTokenCounter(str(_write_tokenizer(tmp_path / "tokenizer.json")))
    assert isinstance(counter, TokenCounter)
    assert count_tokens(counter, "Hello, world.") == 4  # Hello , world .
    assert counter.spans("ab cd") == [(0, 2), (3, 5)]
    assert count_tokens(WhitespaceTokenCounter(), "Hello, world.") == 2


def test_chunks_fit_the_limit_when_counted_with_the_hf_tokenizer(tmp_path: Path) -> None:
    counter = HfTokenCounter(str(_write_tokenizer(tmp_path / "tokenizer.json")))
    chunker = Chunker(_settings(), counter)
    chunks = chunker.split(_doc(_sentences(60)))
    assert chunks
    assert all(count_tokens(counter, c.content) <= MAX for c in chunks)
    assert all(c.token_count == count_tokens(counter, c.content) for c in chunks)


def test_factory_picks_the_counter_from_settings(tmp_path: Path) -> None:
    assert isinstance(create_token_counter(_settings()), WhitespaceTokenCounter)
    file = _write_tokenizer(tmp_path / "tokenizer.json")
    hf = _settings(tokenizer="hf", tokenizer_file=str(file))
    assert isinstance(create_token_counter(hf), HfTokenCounter)


def test_missing_tokenizer_file_is_a_clear_error(tmp_path: Path) -> None:
    with pytest.raises(NonRetryableError):
        HfTokenCounter(str(tmp_path / "missing.json"))


# --- a 3,000 page document ------------------------------------------------------------------


def test_a_3000_page_document_is_chunked_with_low_memory() -> None:
    page = " ".join(_sentences(60, 6) for _ in range(1))  # 360 words per page
    doc = _doc(*[page] * 3000)
    chunker = Chunker(
        ChunkingSettings(version="v1", tokenizer="whitespace"), WhitespaceTokenCounter()
    )
    tracemalloc.start()
    count = 0
    last: Chunk | None = None
    for chunk in chunker.iter_chunks(doc):  # consumed one by one, not collected
        count += 1
        last = chunk
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert count > 3000
    assert last is not None
    assert last.page_end == 3000
    assert peak < 20 * 1024 * 1024  # a few MB, independent of the number of pages


# --- properties -----------------------------------------------------------------------------


def _word(n: int) -> str:
    letters = ""
    n += 26 * 26 * 26  # at least three letters, so headings are recognised
    while n:
        n, r = divmod(n, 26)
        letters = chr(97 + r) + letters
    return letters


@st.composite
def _documents(draw: st.DrawFn) -> list[str]:
    counter = itertools.count()
    pages: list[str] = []
    for _ in range(draw(st.integers(1, 6))):
        lines: list[str] = []
        for _ in range(draw(st.integers(0, 8))):
            kind = draw(st.sampled_from(["para", "para", "para", "heading", "blank"]))
            if kind == "blank":
                lines.append("")
            elif kind == "heading":
                lines.append(
                    " ".join(_word(next(counter)).upper() for _ in range(draw(st.integers(1, 4))))
                )
            else:
                sentences = []
                for _ in range(draw(st.integers(1, 6))):
                    ws = [_word(next(counter)) for _ in range(draw(st.integers(1, 14)))]
                    ws[0] = ws[0].capitalize()
                    sentences.append(" ".join(ws) + ".")
                lines.append(" ".join(sentences))
        pages.append("\n".join(lines))
    return pages


_PROPERTY = settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow], deadline=None)


@given(_documents())
@_PROPERTY
def test_property_no_text_lost_order_kept_and_limits_respected(pages: list[str]) -> None:
    chunks = _chunker().split(_doc(*pages))
    doc_words = [w.lower() for page in pages for w in page.split()]
    position = {w: i for i, w in enumerate(doc_words)}
    assert len(position) == len(doc_words)  # the words are unique, so places are exact

    covered_to = -1
    last_start = -1
    for chunk in chunks:
        words = [w.lower() for w in chunk.content.split()]
        start = position[words[0]]
        assert words == doc_words[start : start + len(words)]  # a real slice of the document
        assert start > last_start  # order is kept
        assert start <= covered_to + 1  # no gap between chunks
        covered_to = max(covered_to, start + len(words) - 1)
        last_start = start
        assert chunk.token_count <= MAX  # no chunk is over the maximum
    assert covered_to == len(doc_words) - 1  # nothing is lost
    if not doc_words:
        assert chunks == []


@given(_documents())
@_PROPERTY
def test_property_numbers_ids_and_pages_are_consistent(pages: list[str]) -> None:
    chunks = _chunker().split(_doc(*pages))
    page_of = {w.lower(): n for n, page in enumerate(pages, 1) for w in page.split()}
    assert [c.chunk_no for c in chunks] == list(range(len(chunks)))
    assert len({c.chunk_id for c in chunks}) == len(chunks)
    for chunk in chunks:
        assert chunk.chunk_id == f"ITEM-1:{chunk.chunk_no}:{chunk.content_hash}"
        assert chunk.page_start is not None
        assert chunk.page_end is not None
        assert 1 <= chunk.page_start <= chunk.page_end <= len(pages)
        words_pages = [page_of[w.lower()] for w in chunk.content.split()]
        assert chunk.page_start <= min(words_pages)
        assert chunk.page_end >= max(words_pages)


@given(_documents())
@_PROPERTY
def test_property_chunking_is_deterministic(pages: list[str]) -> None:
    doc = _doc(*pages)
    assert _chunker().split(doc) == _chunker().split(doc)
