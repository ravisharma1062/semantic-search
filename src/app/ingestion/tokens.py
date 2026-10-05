"""Token counting for the chunker.

The chunker must count tokens with the embedding model's own tokenizer, otherwise chunks can
exceed the model's input size. The tokenizer comes from a local file (the internal artifact
repository). Nothing is downloaded at runtime.
"""

import re
from pathlib import Path
from typing import Protocol, runtime_checkable

from tokenizers import Tokenizer

from app.core.errors import NonRetryableError
from app.core.settings import ChunkingSettings

Span = tuple[int, int]
_WORD = re.compile(r"\S+")


@runtime_checkable
class TokenCounter(Protocol):
    """Counts tokens and says where each token sits in the text."""

    def spans(self, text: str) -> list[Span]:
        """Start and end character index of every token."""
        ...


def count_tokens(counter: TokenCounter, text: str) -> int:
    """Number of tokens in the text."""
    return len(counter.spans(text))


class WhitespaceTokenCounter:
    """One token per word. For local development and tests only."""

    def spans(self, text: str) -> list[Span]:
        """Start and end of every word."""
        return [match.span() for match in _WORD.finditer(text)]


class HfTokenCounter:
    """Counts with a Hugging Face ``tokenizer.json`` file, without special tokens."""

    def __init__(self, tokenizer_file: str) -> None:
        path = Path(tokenizer_file)
        if not path.is_file():
            raise NonRetryableError("Tokenizer file not found")
        self._tokenizer = Tokenizer.from_file(str(path))

    def spans(self, text: str) -> list[Span]:
        """Start and end of every token. Tokens without a position (specials) are left out."""
        encoding = self._tokenizer.encode(text, add_special_tokens=False)
        return [(start, end) for start, end in encoding.offsets if end > start]


def create_token_counter(settings: ChunkingSettings) -> TokenCounter:
    """The counter chosen by settings."""
    if settings.tokenizer == "whitespace":
        return WhitespaceTokenCounter()
    # The settings check that a file is given when the tokenizer is "hf".
    return HfTokenCounter(settings.tokenizer_file or "")
