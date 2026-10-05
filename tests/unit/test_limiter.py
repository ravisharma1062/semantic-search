import asyncio

from app.core.limiter import AdaptiveLimiter


async def test_runs_up_to_the_limit_and_makes_others_wait() -> None:
    limiter = AdaptiveLimiter(2)
    running = 0
    peak = 0

    async def work() -> None:
        nonlocal running, peak
        async with limiter:
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.01)
            running -= 1

    await asyncio.gather(*(work() for _ in range(8)))
    assert peak == 2
    assert limiter.active == 0


async def test_overload_halves_the_limit_down_to_one() -> None:
    limiter = AdaptiveLimiter(8)
    for expected in (4, 2, 1, 1):
        limiter.on_overload()
        assert limiter.limit == expected


async def test_success_brings_the_limit_back_one_step_at_a_time() -> None:
    limiter = AdaptiveLimiter(4, recover_after=3)
    limiter.on_overload()
    assert limiter.limit == 2
    for _ in range(3):
        limiter.on_success()
    assert limiter.limit == 3
    for _ in range(30):
        limiter.on_success()
    assert limiter.limit == 4  # never above the maximum


async def test_overload_resets_the_success_count() -> None:
    limiter = AdaptiveLimiter(4, recover_after=3)
    limiter.on_overload()
    limiter.on_success()
    limiter.on_success()
    limiter.on_overload()
    limiter.on_success()
    assert limiter.limit == 1


async def test_a_lowered_limit_applies_to_calls_that_start_later() -> None:
    limiter = AdaptiveLimiter(4)
    limiter.on_overload()
    limiter.on_overload()  # limit is 1
    running = 0
    peak = 0

    async def work() -> None:
        nonlocal running, peak
        async with limiter:
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.005)
            running -= 1

    await asyncio.gather(*(work() for _ in range(5)))
    assert peak == 1


async def test_a_cancelled_waiter_does_not_leak_a_slot() -> None:
    limiter = AdaptiveLimiter(1)
    gate = asyncio.Event()

    async def hold() -> None:
        async with limiter:
            await gate.wait()

    first = asyncio.create_task(hold())
    await asyncio.sleep(0)
    second = asyncio.create_task(hold())
    await asyncio.sleep(0)
    second.cancel()
    gate.set()
    await first
    await asyncio.gather(second, return_exceptions=True)
    assert limiter.active == 0
    async with limiter:
        assert limiter.active == 1
