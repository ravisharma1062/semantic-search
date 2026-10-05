"""In-house PII masking (HLD section 7: guardrails and PII).

Masks e-mail addresses, phone numbers, payment card numbers (with a Luhn check), IBANs, Indian PAN
and Aadhaar numbers. Each match is replaced by ``[TYPE]``. The masker works on text in memory and
never logs it.

This is a safety net with known limits (regular expressions find formats, not meaning). Test it on
your own documents before relying on it.
"""

import re
from collections.abc import Callable

_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b")
_IBAN = re.compile(r"\b[A-Z]{2}\d{2}(?: ?[A-Z0-9]{4}){2,7}(?: ?[A-Z0-9]{1,4})?\b")
_PAN = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_AADHAAR = re.compile(r"(?<![\d-])(?<!\d )[2-9]\d{3}[ -]?\d{4}[ -]?\d{4}(?![\d-])(?! \d)")
_CARD = re.compile(r"(?<![\d-])(?:\d[ -]?){13,19}(?![\d-])")
_PHONE = re.compile(
    r"(?<![\w-])(?<!\d )(?:\+\d{1,3}[ -]?)?(?:\(\d{2,4}\)[ -]?)?"
    r"\d{2,5}[ -]?\d{3,5}(?:[ -]?\d{3,5})?(?!\w)(?! \d)"
)


def _luhn(number: str) -> bool:
    digits = [int(c) for c in number if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total = 0
    for index, digit in enumerate(reversed(digits)):
        if index % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def _iban_ok(text: str) -> bool:
    compact = text.replace(" ", "")
    if not 15 <= len(compact) <= 34:
        return False
    rearranged = compact[4:] + compact[:4]
    number = "".join(str(int(c, 36)) for c in rearranged)
    return int(number) % 97 == 1


def _phone_ok(text: str) -> bool:
    digits = [c for c in text if c.isdigit()]
    return 10 <= len(digits) <= 15


_RULES: list[tuple[str, re.Pattern[str], Callable[[str], bool]]] = [
    ("EMAIL", _EMAIL, lambda _t: True),
    ("IBAN", _IBAN, _iban_ok),
    ("CARD", _CARD, _luhn),
    ("PAN", _PAN, lambda _t: True),
    ("AADHAAR", _AADHAAR, lambda t: len([c for c in t if c.isdigit()]) == 12),
    ("PHONE", _PHONE, _phone_ok),
]


def mask_pii(text: str) -> tuple[str, int]:
    """The text with personal data replaced, and how many values were replaced."""
    count = 0
    for label, pattern, accept in _RULES:

        def replace(
            match: re.Match[str], label: str = label, accept: Callable[[str], bool] = accept
        ) -> str:
            nonlocal count
            if not accept(match.group(0)):
                return match.group(0)
            count += 1
            return f"[{label}]"

        text = pattern.sub(replace, text)
    return text, count
