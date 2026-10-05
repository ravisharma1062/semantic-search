"""Embeds many texts as batches that run side by side."""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from app.core.errors import AppError

BatchEmbedder = Callable[[list[str]], Coroutine[Any, Any, list[list[float]]]]


async def embed_in_batches(
    embed_batch: BatchEmbedder, texts: list[str], batch_size: int
) -> list[list[float]]:
    """One vector per text, in order. If a batch fails, the others are cancelled.

    The concurrency limit lives in ``embed_batch``, so all batches can be started at once.
    The first of our own typed errors is raised as it is, not wrapped in an exception group.
    """
    batches = [texts[start : start + batch_size] for start in range(0, len(texts), batch_size)]
    if not batches:
        return []
    try:
        async with asyncio.TaskGroup() as group:
            tasks: list[asyncio.Task[list[list[float]]]] = [
                group.create_task(embed_batch(batch)) for batch in batches
            ]
    except ExceptionGroup as group_error:
        first = next((e for e in group_error.exceptions if isinstance(e, AppError)), None)
        raise first or group_error.exceptions[0] from None
    return [vector for task in tasks for vector in task.result()]
