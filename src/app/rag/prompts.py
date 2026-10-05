"""Versioned prompt files (rule 11).

A prompt lives in ``prompts/<name>.<version>.txt`` with a ``### SYSTEM`` and a ``### USER`` section.
The user section holds ``{{placeholders}}``. Values are put in with one pass over the template, so
a value that itself contains ``{{...}}`` (for example a chunk written by an attacker) is never
expanded.
"""

import re
from pathlib import Path

from app.core.errors import NonRetryableError
from app.llm.base import ChatMessage

_SYSTEM = "### SYSTEM"
_USER = "### USER"
_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")
_SAFE_PART = re.compile(r"^[A-Za-z0-9_-]+$")


class PromptTemplate:
    """One prompt version, loaded and checked."""

    def __init__(self, name: str, version: str, system: str, user: str) -> None:
        self.name = name
        self.version = version
        self._system = system
        self._user = user
        self.placeholders = frozenset(_PLACEHOLDER.findall(user))

    def render(self, **values: str) -> list[ChatMessage]:
        """The chat messages with the values filled in."""
        if set(values) != self.placeholders:
            raise NonRetryableError("Prompt values do not match the template")

        def put(match: re.Match[str]) -> str:
            return values[match.group(1)]

        return [
            ChatMessage(role="system", content=self._system),
            ChatMessage(role="user", content=_PLACEHOLDER.sub(put, self._user)),
        ]


def load_prompt(directory: str | Path, name: str, version: str) -> PromptTemplate:
    """Read ``<directory>/<name>.<version>.txt``."""
    if not (_SAFE_PART.match(name) and _SAFE_PART.match(version)):
        raise NonRetryableError("Invalid prompt name or version")
    path = Path(directory) / f"{name}.{version}.txt"
    if not path.is_file():
        raise NonRetryableError(f"Prompt file not found: {path.name}")
    text = path.read_text(encoding="utf-8")
    head, sep, rest = text.partition(_SYSTEM)
    system, sep2, user = rest.partition(_USER)
    if head.strip() or not sep or not sep2 or not system.strip() or not user.strip():
        raise NonRetryableError(f"Prompt file is malformed: {path.name}")
    return PromptTemplate(name, version, system.strip(), user.strip())
