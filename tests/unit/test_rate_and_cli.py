import argparse

import pytest

from app.core.errors import NonRetryableError
from app.core.settings import BackfillSettings, Settings, WaveSpec
from app.jobs.cli import build_parser, find_wave
from app.jobs.rate import RateLimiter


class _Time:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def _limiter(rate: float, time: _Time, burst: float | None = None) -> RateLimiter:
    return RateLimiter(rate, burst=burst, clock=time.clock, sleep=time.sleep)


async def test_a_burst_up_to_the_rate_goes_through_at_once() -> None:
    time = _Time()
    await _limiter(10, time).acquire(10)
    assert time.slept == []


async def test_more_than_the_burst_waits_for_the_difference() -> None:
    time = _Time()
    limiter = _limiter(10, time)
    await limiter.acquire(10)
    await limiter.acquire(5)
    assert time.slept == [0.5]


async def test_the_average_rate_holds_for_many_pages() -> None:
    time = _Time()
    limiter = _limiter(100, time)
    for _ in range(50):
        await limiter.acquire(20)
    assert time.now == pytest.approx(
        (1000 - 100) / 100
    )  # 1000 items at 100 per second, minus the burst


async def test_a_page_larger_than_the_bucket_is_allowed() -> None:
    time = _Time()
    await _limiter(10, time).acquire(100)
    assert time.slept == [9.0]


async def test_idle_time_refills_the_bucket_up_to_its_size() -> None:
    time = _Time()
    limiter = _limiter(10, time)
    await limiter.acquire(10)
    time.now += 100  # a long pause
    await limiter.acquire(10)
    assert time.slept == []
    await limiter.acquire(10)  # but it does not save up more than one bucket
    assert time.slept == [1.0]


def test_a_rate_must_be_positive() -> None:
    with pytest.raises(ValueError):
        RateLimiter(0)


def test_waves_are_found_by_number(settings: Settings) -> None:
    configured = settings.model_copy(
        update={
            "backfill": BackfillSettings(
                waves=[WaveSpec(number=1, name="pilot"), WaveSpec(number=2)]
            )
        }
    )
    assert find_wave(configured, 1).name == "pilot"
    with pytest.raises(NonRetryableError, match="not defined"):
        find_wave(configured, 9)


@pytest.mark.parametrize(
    "argv",
    [
        ["backfill", "start", "--wave", "1"],
        ["backfill", "start", "--wave", "2", "--job-id", "j", "--restart"],
        ["backfill", "pause", "--job-id", "j"],
        ["backfill", "resume", "--job-id", "j"],
        ["backfill", "status"],
        ["reconcile"],
        ["reconcile", "--wave", "1", "--max-items", "100", "--no-orphans"],
    ],
)
def test_command_line_accepts_the_documented_commands(argv: list[str]) -> None:
    assert isinstance(build_parser().parse_args(argv), argparse.Namespace)


@pytest.mark.parametrize("argv", [[], ["backfill"], ["backfill", "start"], ["backfill", "pause"]])
def test_command_line_rejects_incomplete_commands(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(argv)
