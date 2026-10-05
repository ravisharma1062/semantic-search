"""Guardrail hook around the answer pipeline (HLD section 7).

Three checks, all in-house and switched by settings:

* ``check_question``: a question that matches a denied pattern is refused before any search.
* ``clean_context``: PII is masked in the passages before they go to the model.
* ``clean_answer``: PII is masked in the answer, and an answer that matches a denied pattern is
  replaced by a refusal.

The checks never log the text they look at.
"""

import re
from dataclasses import dataclass

from app.core.settings import GuardrailSettings
from app.rag.pii import mask_pii


@dataclass(frozen=True)
class Cleaned:
    """Text after masking."""

    text: str
    masked: int = 0
    blocked: bool = False


class Guardrails:
    """Policy checks for questions, context and answers."""

    def __init__(self, settings: GuardrailSettings) -> None:
        self._mask = settings.mask_pii
        self._deny_question = [
            re.compile(p, re.IGNORECASE) for p in settings.denied_question_patterns
        ]
        self._deny_answer = [re.compile(p, re.IGNORECASE) for p in settings.denied_answer_patterns]

    @property
    def masks_pii(self) -> bool:
        """True if text is masked."""
        return self._mask

    def check_question(self, question: str) -> bool:
        """False if the question must be refused."""
        return not any(p.search(question) for p in self._deny_question)

    def mask(self, text: str) -> Cleaned:
        """Mask PII if switched on."""
        if not self._mask:
            return Cleaned(text)
        masked, count = mask_pii(text)
        return Cleaned(masked, count)

    def clean_answer(self, answer: str) -> Cleaned:
        """Mask PII and apply the denied patterns."""
        if any(p.search(answer) for p in self._deny_answer):
            return Cleaned("", blocked=True)
        return self.mask(answer)
