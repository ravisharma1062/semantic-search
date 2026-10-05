"""LLM client interface."""

from collections.abc import AsyncIterator, Sequence
from typing import Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict


class ChatMessage(BaseModel):
    """One message of a chat prompt."""

    model_config = ConfigDict(frozen=True)

    role: Literal["system", "user", "assistant"]
    content: str


class LLMOptions(BaseModel):
    """Per-call options. ``None`` means the provider default from settings."""

    model_config = ConfigDict(frozen=True)

    max_output_tokens: int | None = None
    temperature: float | None = None


class Usage(BaseModel):
    """Token counts of one call."""

    input_tokens: int = 0
    output_tokens: int = 0


class Completion(BaseModel):
    """A full answer and what it cost."""

    text: str
    usage: Usage = Usage()


def estimate_tokens(text: str) -> int:
    """A rough token count (4 characters per token) for providers that do not report usage."""
    return (len(text) + 3) // 4


@runtime_checkable
class LLMClient(Protocol):
    """Generates text from chat messages."""

    model_name: str

    async def generate(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> str:
        """Return the full answer."""
        ...

    async def complete(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> Completion:
        """Return the full answer with the token usage."""
        ...

    def stream(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> AsyncIterator[str]:
        """Yield the answer in pieces as the model writes it."""
        ...
