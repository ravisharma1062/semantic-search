"""Entry point of the image: ``python -m app``. The run mode comes from ``APP_MODE``."""

import asyncio

import uvicorn

from app.core.logging import configure_logging
from app.core.settings import AppMode, get_settings
from app.modes import install_signal_handlers, run_until_stopped


async def _run_idle(mode: AppMode) -> None:
    stop = asyncio.Event()
    install_signal_handlers(asyncio.get_running_loop(), stop)
    await run_until_stopped(mode, stop)


def main() -> None:
    """Start the service in the configured mode."""
    settings = get_settings()
    configure_logging(settings.service.log_level, settings.service.log_json)
    if settings.mode is AppMode.API:
        # The request middleware writes the access log, so uvicorn's own is off.
        uvicorn.run(
            "app.main:app",
            host=settings.service.host,
            port=settings.service.port,
            log_config=None,
            access_log=False,
        )
    else:
        asyncio.run(_run_idle(settings.mode))


if __name__ == "__main__":
    main()
