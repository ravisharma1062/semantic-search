import asyncio
import os
from pathlib import Path

from app.ingestion.runtime import heartbeat


async def test_heartbeat_touches_the_file_and_stops_with_the_signal(tmp_path: Path) -> None:
    target = tmp_path / "sub" / "alive"
    stop = asyncio.Event()
    task = asyncio.create_task(heartbeat(str(target), 0.01, stop))
    deadline = asyncio.get_running_loop().time() + 2
    while not target.exists():
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.005)
    first = target.stat().st_mtime
    old = first - 100
    os.utime(target, (old, old))  # make it look old
    deadline = asyncio.get_running_loop().time() + 2
    while target.stat().st_mtime < first - 50:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.005)  # the heartbeat made it fresh again
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert task.done()
