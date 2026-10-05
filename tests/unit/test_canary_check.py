import httpx
import pytest
import respx
from loadtest.canary_check import Limits, Prometheus, Reading, judge, queries, watch

URL = "http://prom.test/api/v1/query"


def _reading(
    rps: float | None = 5, err: float | None = 0.0, p95: float | None = 0.5, fb: float | None = 0.0
) -> Reading:
    return Reading(rps, err, p95, fb)


# --- the verdict ----------------------------------------------------------------------------


def test_a_canary_as_good_as_the_stable_pods_passes() -> None:
    assert judge(_reading(), _reading(), Limits()) == []


def test_a_canary_without_traffic_proves_nothing() -> None:
    assert judge(_reading(rps=None), _reading(), Limits())
    assert judge(_reading(rps=0.001), _reading(), Limits())


def test_more_errors_than_the_stable_pods_fail_even_under_the_absolute_limit() -> None:
    problems = judge(_reading(err=0.015), _reading(err=0.001), Limits())
    assert any("error rate" in p for p in problems)


def test_a_tiny_error_rate_is_tolerated_when_the_stable_pods_have_none() -> None:
    assert judge(_reading(err=0.004), _reading(err=0.0), Limits()) == []
    assert judge(_reading(err=0.03), _reading(err=0.03), Limits())  # over the absolute limit


def test_slower_than_the_stable_pods_fails() -> None:
    assert any("p95" in p for p in judge(_reading(p95=1.2), _reading(p95=0.5), Limits()))
    assert judge(_reading(p95=0.6), _reading(p95=0.5), Limits()) == []
    assert any("p95" in p for p in judge(_reading(p95=3.0), _reading(p95=None), Limits()))


def test_fallbacks_over_the_limit_fail() -> None:
    assert any("fallback" in p for p in judge(_reading(fb=0.2), _reading(), Limits()))


def test_missing_stable_data_uses_the_absolute_limits_only() -> None:
    stable = _reading(rps=None, err=None, p95=None, fb=None)
    assert judge(_reading(err=0.01, p95=2.0), stable, Limits()) == []
    assert judge(_reading(err=0.5), stable, Limits())


# --- the queries ----------------------------------------------------------------------------


def test_the_queries_select_the_pods_and_only_the_user_routes() -> None:
    q = queries(".*-api-canary-.*", "5m")
    assert set(q) == {"requests_per_s", "error_rate", "p95_s", "fallback_rate"}
    for expr in q.values():
        assert 'pod=~".*-api-canary-.*"' in expr
        assert "[5m]" in expr
    assert "/v1/(search|answer.*)" in q["error_rate"]
    assert 'status=~"5.."' in q["error_rate"]


# --- reading Prometheus ---------------------------------------------------------------------


def _answer(value: str | None) -> dict[str, object]:
    result = [] if value is None else [{"metric": {}, "value": [0, value]}]
    return {"status": "success", "data": {"resultType": "vector", "result": result}}


async def test_values_no_data_and_nan() -> None:
    async with httpx.AsyncClient() as client:
        prom = Prometheus(client, "http://prom.test", token="tok")  # noqa: S106
        with respx.mock() as router:
            route = router.get(URL).mock(
                side_effect=[
                    httpx.Response(200, json=_answer("0.25")),
                    httpx.Response(200, json=_answer(None)),
                    httpx.Response(200, json=_answer("NaN")),
                ]
            )
            assert await prom.instant("q") == 0.25
            assert await prom.instant("q") is None
            assert await prom.instant("q") is None
    assert route.calls[0].request.headers["authorization"] == "Bearer tok"
    assert route.calls[0].request.url.params["query"] == "q"


async def test_a_prometheus_error_is_raised() -> None:
    async with httpx.AsyncClient() as client:
        with respx.mock() as router:
            router.get(URL).respond(500)
            with pytest.raises(httpx.HTTPStatusError):
                await Prometheus(client, "http://prom.test").instant("q")


class Fake:
    """A Prometheus with scripted readings: (canary, stable) per check."""

    def __init__(self, checks: list[tuple[Reading, Reading]]) -> None:
        self.checks = checks
        self.reads = 0

    async def read(self, pod: str, window: str) -> Reading:
        index = self.reads // 2
        canary, stable = self.checks[min(index, len(self.checks) - 1)]
        self.reads += 1
        return canary if pod == "canary" else stable


class Time:
    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


async def _watch(
    checks: list[tuple[Reading, Reading]], duration: float = 300
) -> tuple[list[str], int]:
    fake, time = Fake(checks), Time()
    problems = await watch(
        fake,  # type: ignore[arg-type]
        canary_pod="canary",
        stable_pod="stable",
        window="5m",
        duration_s=duration,
        interval_s=60,
        limits=Limits(),
        clock=time.clock,
        sleep=time.sleep,
    )
    return problems, fake.reads // 2


async def test_a_good_canary_is_checked_for_the_whole_duration() -> None:
    problems, checks = await _watch([(_reading(), _reading())])
    assert problems == [] and checks == 6  # at 0, 60, ..., 300 seconds


async def test_a_bad_canary_stops_the_check_at_once() -> None:
    problems, checks = await _watch(
        [(_reading(), _reading()), (_reading(), _reading()), (_reading(err=0.2), _reading())]
    )
    assert problems and checks == 3


async def test_no_traffic_fails_the_check() -> None:
    problems, checks = await _watch([(_reading(rps=None), _reading())])
    assert problems == ["not enough traffic on the canary to judge it"] and checks == 1
