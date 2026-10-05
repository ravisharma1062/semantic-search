"""Fake LLMClient: scripted replies."""

from collections.abc import AsyncIterator, Sequence

from app.llm.base import ChatMessage, LLMOptions


class FakeLLMClient:
    """Returns the scripted replies in order, then repeats the last one."""

    def __init__(
        self, replies: Sequence[str] = ("NOT_FOUND",), model_name: str = "fake-llm"
    ) -> None:
        if not replies:
            raise ValueError("replies must not be empty")
        self.model_name = model_name
        self.replies = list(replies)
        self.fail_with: Exception | None = None
        self.calls: list[list[ChatMessage]] = []

    def _next_reply(self, messages: Sequence[ChatMessage]) -> str:
        if self.fail_with:
            raise self.fail_with
        self.calls.append(list(messages))
        index = min(len(self.calls) - 1, len(self.replies) - 1)
        return self.replies[index]

    async def generate(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> str:
        """Return the next scripted reply."""
        return self._next_reply(messages)

    async def stream(
        self, messages: Sequence[ChatMessage], options: LLMOptions | None = None
    ) -> AsyncIterator[str]:
        """Yield the next scripted reply word by word."""
        reply = self._next_reply(messages)
        for position, word in enumerate(reply.split(" ")):
            yield word if position == 0 else f" {word}"
