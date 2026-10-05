"""Fake Redis for the query cache: a dict with failure and slowness switches."""

import asyncio
from typing import Any


class FakeRedis:
    """``get`` and ``set`` over a dict. Can fail, or answer slowly."""

    def __init__(self) -> None:
        self.data: dict[str, bytes | str] = {}
        self.ttls: dict[str, int | None] = {}
        self.fail_with: Exception | None = None
        self.delay_s = 0.0
        self.get_calls = 0
        self.set_calls = 0

    async def _pause(self) -> None:
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.fail_with:
            raise self.fail_with

    async def get(self, name: str) -> bytes | str | None:
        """The stored value, or ``None``."""
        self.get_calls += 1
        await self._pause()
        return self.data.get(name)

    async def set(self, name: str, value: bytes | str, ex: int | None = None) -> Any:
        """Store a value."""
        self.set_calls += 1
        await self._pause()
        self.data[name] = value
        self.ttls[name] = ex
        return True
