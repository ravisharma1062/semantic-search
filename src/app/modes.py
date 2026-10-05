"""Run loop for the ``worker`` and ``batch`` modes.

The real consumer (task T1.2) and batch jobs (task T1.7) replace the idle loop. Until then
the process starts, logs and waits for a stop signal, so the Helm chart can be tested.
"""

import asyncio
import signal

import structlog

from app.core.settings import AppMode

_log = structlog.get_logger(__name__)


def install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    """Set ``stop`` on SIGINT or SIGTERM. Works on Windows too, where loop handlers do not."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: loop.call_soon_threadsafe(stop.set))


async def run_until_stopped(mode: AppMode, stop: asyncio.Event) -> None:
    """Idle until ``stop`` is set."""
    _log.info("service_started", mode=mode.value, note="idle stub")
    await stop.wait()
    _log.info("service_stopped", mode=mode.value)
