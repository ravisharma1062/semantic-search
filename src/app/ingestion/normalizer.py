"""OCR text clean-up that keeps the page mapping.

Fixes hyphenation at line ends, lines broken inside a paragraph, repeated headers and footers
(including page numbers), ligatures and stray control characters. Table-like lines are kept
line by line, so the chunker can still find table rows. Page numbers and page count never change:
a page that loses all its text stays in the list with an empty text.
"""

import re
import unicodedata
from collections import Counter

from app.core.settings import NormalizerSettings
from app.ingestion.source import SourceDocument, SourcePage

_LIGATURES = {
    "\N{LATIN SMALL LIGATURE FF}": "ff",
    "\N{LATIN SMALL LIGATURE FI}": "fi",
    "\N{LATIN SMALL LIGATURE FL}": "fl",
    "\N{LATIN SMALL LIGATURE FFI}": "ffi",
    "\N{LATIN SMALL LIGATURE FFL}": "ffl",
    "\N{LATIN SMALL LIGATURE LONG S T}": "st",
    "\N{LATIN SMALL LIGATURE ST}": "st",
}
_REMOVED = dict.fromkeys(
    map(
        ord,
        "\N{SOFT HYPHEN}\N{ZERO WIDTH SPACE}\N{ZERO WIDTH NON-JOINER}"
        "\N{ZERO WIDTH JOINER}\N{WORD JOINER}\N{ZERO WIDTH NO-BREAK SPACE}",
    )
)
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SPACES = re.compile(
    r"[ \t\N{NO-BREAK SPACE}\N{EN QUAD}-\N{HAIR SPACE}\N{NARROW NO-BREAK SPACE}"
    r"\N{MEDIUM MATHEMATICAL SPACE}\N{IDEOGRAPHIC SPACE}]+"
)
_TABLE_LINE = re.compile(r"\t|\S {2,}\S|\S\s*\|\s*\S.*\|")
_LIST_MARKER = re.compile(r"^\s*(?:\(?\d{1,3}[.)]|\(?[a-zA-Z][.)]|[-\N{BULLET}*\N{EN DASH}])\s+")
_DIGITS = re.compile(r"\d+")
_TERMINAL = (".", "!", "?")


def _clean_chars(text: str) -> str:
    text = unicodedata.normalize("NFC", text).translate(_REMOVED)
    for ligature, letters in _LIGATURES.items():
        text = text.replace(ligature, letters)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\N{LINE SEPARATOR}", "\n")
    return _CONTROL.sub("", text)


def _is_table_line(line: str) -> bool:
    return bool(_TABLE_LINE.search(line))


def _is_heading(line: str) -> bool:
    letters = [c for c in line if c.isalpha()]
    shouting = len(letters) >= 3 and all(c.isupper() for c in letters) and len(line) <= 80
    return shouting or line.endswith(":")


def _starts_structure(line: str) -> bool:
    return bool(_LIST_MARKER.match(line)) or _is_heading(line)


def _join(previous: str, line: str) -> str:
    """Join two lines of one paragraph. A hyphen before a lower-case letter is removed."""
    hyphenated = previous.endswith("-") and not previous.endswith(("--", " -"))
    if hyphenated and len(previous) > 1 and previous[-2].isalpha() and line[:1].islower():
        return previous[:-1] + line
    return f"{previous} {line}"


def _paragraph_end(previous: str, cfg: NormalizerSettings) -> bool:
    return _is_heading(previous) or (
        previous.endswith(_TERMINAL) and len(previous) < cfg.short_line_chars
    )


def _reflow(text: str, cfg: NormalizerSettings) -> str:
    """Join broken lines and fix hyphenation, per block of lines between blank lines."""
    output: list[str] = []
    blank_run = 0
    for raw in text.split("\n"):
        # Table rows keep their column spacing, so they can still be recognised later.
        table_row = _is_table_line(raw)
        line = raw.strip() if table_row else _SPACES.sub(" ", raw).strip()
        if not line:
            blank_run += 1
            continue
        if blank_run and output:
            output.append("")  # one blank line between paragraphs
        blank_run = 0
        if not output or output[-1] == "":
            output.append(line)
            continue
        previous = output[-1]
        keep_break = (
            table_row
            or _is_table_line(previous)
            or _starts_structure(line)
            or _paragraph_end(previous, cfg)
        )
        if keep_break:
            output.append(line)
        else:
            output[-1] = _join(previous, line)
    return "\n".join(output)


def normalize_text(text: str, cfg: NormalizerSettings | None = None) -> str:
    """Clean one piece of text (a page, or a whole document without pages)."""
    return _reflow(_clean_chars(text), cfg or NormalizerSettings())


def _edge_key(line: str) -> str:
    """Lines that differ only in digits count as the same line (page numbers, dates)."""
    return _DIGITS.sub("#", _SPACES.sub(" ", line.strip().lower()))


def _edge_positions(lines: list[str], edge: int) -> set[int]:
    """Indexes of the first and last ``edge`` non-empty lines.

    The edge is at most a third of the page, so a short page never counts as all header.
    """
    filled = [i for i, line in enumerate(lines) if line.strip()]
    size = min(edge, len(filled) // 3)
    if size == 0:
        return set()
    return set(filled[:size]) | set(filled[-size:])


def _repeated_edge_keys(pages: list[list[str]], cfg: NormalizerSettings) -> set[str]:
    if len(pages) < cfg.repeat_min_pages:
        return set()
    counts: Counter[str] = Counter()
    for lines in pages:
        keys = {_edge_key(lines[i]) for i in _edge_positions(lines, cfg.edge_lines)}
        counts.update(keys)
    threshold = max(2.0, cfg.repeat_ratio * len(pages))  # a repeat needs at least two pages
    return {key for key, count in counts.items() if key and count >= threshold}


def normalize_pages(
    pages: list[SourcePage], cfg: NormalizerSettings | None = None
) -> list[SourcePage]:
    """Clean the pages of one document. Same pages, same numbers, same order."""
    cfg = cfg or NormalizerSettings()
    cleaned = [_clean_chars(page.text).split("\n") for page in pages]
    repeated = _repeated_edge_keys(cleaned, cfg)
    result: list[SourcePage] = []
    for page, lines in zip(pages, cleaned, strict=True):
        drop = {
            i for i in _edge_positions(lines, cfg.edge_lines) if _edge_key(lines[i]) in repeated
        }
        kept = [line for i, line in enumerate(lines) if i not in drop]
        result.append(SourcePage(page_no=page.page_no, text=_reflow("\n".join(kept), cfg)))
    return result


def normalize_document(
    document: SourceDocument, cfg: NormalizerSettings | None = None
) -> SourceDocument:
    """A copy of the document with clean text. Metadata and permissions are untouched."""
    cfg = cfg or NormalizerSettings()
    return document.model_copy(
        update={
            "pages": normalize_pages(document.pages, cfg),
            "text": normalize_text(document.text, cfg),
        }
    )
