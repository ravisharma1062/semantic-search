import asyncio

from app.core.settings import AppMode
from app.modes import run_until_stopped


async def test_idle_loop_waits_for_stop() -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(run_until_stopped(AppMode.WORKER, stop))
    await asyncio.sleep(0)
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, timeout=1)


async def test_idle_loop_returns_at_once_if_already_stopped() -> None:
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(run_until_stopped(AppMode.BATCH, stop), timeout=1)
