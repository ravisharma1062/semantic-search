"""Turns the model's pieces into the text events of the streaming endpoint.

What it does while the text streams:

* ``NOT_FOUND`` is held back, so the user never sees it as a token (``found: false`` says it).
* Source numbers that are not in the context are dropped. A marker split over two pieces
  (``[`` then ``1]``) is held until it is complete.
* With PII masking on, text is released sentence by sentence and each part is masked, so a value
  cannot be cut in half between two events.

The final verdict (found, citations, rejected answers) comes from the answer service on the full
text. If it differs from what was streamed, the ``done`` event says so and the client discards the
streamed text.
"""

import re

from app.rag.citations import NOT_FOUND
from app.rag.guardrails import Guardrails

_PARTIAL_MARKER = re.compile(r"\[\d{0,3}$")
_REF = re.compile(r"\[(\d{1,3})\]")
_SENTENCE_END = re.compile(r"[.!?\n]\s*$")


class StreamAssembler:
    """Collects pieces and says what text to send."""

    def __init__(self, guard: Guardrails, valid_refs: set[int]) -> None:
        self._guard = guard
        self._valid = valid_refs
        self._pending = ""
        self._deciding = True
        self._holding = False
        self.raw = ""

    def feed(self, piece: str) -> list[str]:
        """Text to send now (zero or one parts)."""
        self.raw += piece
        self._pending += piece
        if self._deciding:
            probe = self._pending.strip()
            if probe.startswith(NOT_FOUND):
                self._holding = True
                self._deciding = False
                return []
            if NOT_FOUND.startswith(probe):
                return []  # could still become NOT_FOUND
            self._deciding = False
        if self._holding:
            return []
        return self._release(final=False)

    def finish(self, *, not_found: bool) -> list[str]:
        """The rest of the text. Nothing if the answer turned out to be NOT_FOUND."""
        if not_found:
            self._pending = ""
            return []
        return self._release(final=True)

    def _release(self, *, final: bool) -> list[str]:
        text = self._pending
        if not final:
            if self._guard.masks_pii and not _SENTENCE_END.search(text):
                return []
            partial = _PARTIAL_MARKER.search(text)
            if partial:
                text, self._pending = text[: partial.start()], text[partial.start() :]
            else:
                self._pending = ""
        else:
            self._pending = ""
        text = _REF.sub(lambda m: m.group(0) if int(m.group(1)) in self._valid else "", text)
        text = self._guard.mask(text).text
        return [text] if text else []
