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


@runtime_checkable
class LLMClient(Protocol):
    """Generates text from chat messages."""

    model_name: str

    async def generate(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> str:
        """Return the full answer."""
        ...

    def stream(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> AsyncIterator[str]:
        """Yield the answer in pieces as the model writes it."""
        ...
