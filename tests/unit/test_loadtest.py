import asyncio
import json
from pathlib import Path

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st
from loadtest.run import HttpSender, Query, load_queries, run_load
from loadtest.stats import Sample, check_slo, percentile, summarise
from pydantic import SecretStr

from app.core.settings import ApiSettings, Settings
from app.main import create_app
from app.services import Services
from tests.fakes import FakeLLMClient
from tests.unit.test_answer_service import make_service

# --- percentiles ----------------------------------------------------------------------------


def test_percentiles_use_the_nearest_rank() -> None:
    values = [float(n) for n in range(1, 101)]
    assert percentile(values, 50) == 50
    assert percentile(values, 95) == 95
    assert percentile(values, 99) == 99
    assert percentile(values, 100) == 100
    assert percentile(values, 0) == 1
    assert percentile([7.0], 99) == 7.0
    assert percentile([], 95) == 0.0


def test_a_percentile_outside_0_to_100_is_refused() -> None:
    with pytest.raises(ValueError):
        percentile([1.0], 101)
    with pytest.raises(ValueError):
        percentile([1.0], -1)


@given(
    st.lists(st.floats(min_value=0, max_value=1e6, allow_nan=False), min_size=1, max_size=200),
    st.floats(min_value=0, max_value=100),
    st.floats(min_value=0, max_value=100),
)
def test_a_percentile_is_a_measured_value_and_never_goes_down(
    values: list[float], p: float, q: float
) -> None:
    low, high = sorted((p, q))
    assert percentile(values, low) in values
    assert percentile(values, low) <= percentile(values, high)
    assert min(values) <= percentile(values, high) <= max(values)


# --- the summary ----------------------------------------------------------------------------


def _ok(latency: float, mode: str = "hybrid+rerank", first: float | None = None) -> Sample:
    return Sample(latency_s=latency, status=200, mode_used=mode, first_byte_s=first)


def test_the_summary_counts_errors_fallbacks_and_modes() -> None:
    samples = [
        _ok(0.1),
        _ok(0.2),
        _ok(0.3, "bm25"),
        _ok(0.4, "hybrid"),
        Sample(2.0, 503),
        Sample(30.0, 0),
    ]
    s = summarise(samples, duration_s=3.0)
    assert s.requests == 6 and s.achieved_rps == 2.0
    assert s.error_rate == round(2 / 6, 4)
    assert s.fallback_rate == 0.25  # one of four successful answers ran bm25 only
    assert s.statuses == {"0": 1, "200": 4, "503": 1}
    assert s.modes_used == {"hybrid+rerank": 2, "bm25": 1, "hybrid": 1}
    assert s.latency_s["max"] == 30.0  # slow errors count in the latency


def test_an_empty_run_is_all_zeros() -> None:
    s = summarise([], duration_s=0)
    assert (s.requests, s.achieved_rps, s.error_rate, s.fallback_rate) == (0, 0.0, 0.0, 0.0)
    assert s.latency_s["p95"] == 0.0


def test_first_token_times_are_summarised_when_present() -> None:
    s = summarise([_ok(1.0, first=0.2), _ok(1.0, first=0.4), _ok(1.0)], duration_s=1)
    assert s.first_byte_s == {"p50": 0.2, "p95": 0.4}


def test_the_limits_report_what_was_broken() -> None:
    s = summarise([_ok(0.5)] * 17 + [_ok(4.0, "bm25")] * 2 + [Sample(1.0, 500)], duration_s=10)
    assert check_slo(s) == []
    broken = check_slo(s, p95_max_s=3.0, error_rate_max=0.01, fallback_rate_max=0.01)
    assert len(broken) == 3
    assert check_slo(s, p95_max_s=10, error_rate_max=0.5, fallback_rate_max=0.5) == []


def test_a_run_limited_by_the_client_is_never_a_pass() -> None:
    s = summarise([_ok(0.1)], duration_s=1, dropped=5)
    assert any("not sent" in line for line in check_slo(s))


def test_a_first_token_limit_needs_a_measurement() -> None:
    s = summarise([_ok(0.1)], duration_s=1)
    assert check_slo(s, first_byte_p95_max_s=3.0)  # a search run cannot prove a first token time


# --- the generator --------------------------------------------------------------------------


class FakeTime:
    """A clock that only moves when the generator sleeps, so runs are exact and fast."""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


async def test_requests_are_sent_at_the_target_rate_in_open_loop() -> None:
    time = FakeTime()
    sent_at: list[float] = []

    async def send(query: Query) -> Sample:
        sent_at.append(time.now)
        return Sample(0.01, 200, "hybrid")

    summary = await run_load(
        send, [Query("a"), Query("b")], rate=10, duration_s=2, clock=time.clock, sleep=time.sleep
    )
    assert summary.requests == 20 and summary.dropped == 0
    assert sent_at == pytest.approx([n * 0.1 for n in range(20)])


async def test_a_slow_server_does_not_slow_the_sending_down() -> None:
    time = FakeTime()
    sent_at: list[float] = []

    async def send(query: Query) -> Sample:
        sent_at.append(time.now)
        await asyncio.sleep(0.05)  # the answer is slow, the schedule must not wait for it
        return Sample(5.0, 200, "hybrid")

    summary = await run_load(
        send, [Query("a")], rate=10, duration_s=1, clock=time.clock, sleep=time.sleep
    )
    assert summary.requests == 10
    assert sent_at == pytest.approx([n * 0.1 for n in range(10)])
    assert summary.latency_s["p95"] == 5.0


async def test_too_many_in_flight_requests_are_counted_as_dropped() -> None:
    time = FakeTime()
    gate = asyncio.Event()

    async def send(query: Query) -> Sample:
        await gate.wait()
        return Sample(0.1, 200, "hybrid")

    async def release() -> None:
        while time.now < 0.9:  # noqa: ASYNC110 (waits for the fake clock, not for an event)
            await asyncio.sleep(0)
        gate.set()

    releaser = asyncio.create_task(release())
    summary = await run_load(
        send,
        [Query("a")],
        rate=10,
        duration_s=1,
        max_in_flight=3,
        clock=time.clock,
        sleep=time.sleep,
    )
    await releaser
    assert summary.requests == 3 and summary.dropped == 7


async def test_bad_arguments_are_refused() -> None:
    async def send(query: Query) -> Sample:  # pragma: no cover - never called
        return Sample(0, 200)

    with pytest.raises(ValueError):
        await run_load(send, [Query("a")], rate=0, duration_s=1)
    with pytest.raises(ValueError):
        await run_load(send, [Query("a")], rate=1, duration_s=0)


def test_questions_load_from_the_evaluation_set_or_a_text_file(tmp_path: Path) -> None:
    jsonl = tmp_path / "set.jsonl"
    jsonl.write_text(
        json.dumps({"question": "q1", "as_user": "ann", "groups": ["g1"]})
        + "\n\n"
        + '{"question": "q2"}\n',
        encoding="utf-8",
    )
    assert load_queries(jsonl) == [Query("q1", "ann", ("g1",)), Query("q2")]
    text = tmp_path / "q.txt"
    text.write_text("first\n\nsecond\n", encoding="utf-8")
    assert [q.text for q in load_queries(text)] == ["first", "second"]
    (tmp_path / "empty.txt").write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_queries(tmp_path / "empty.txt")


# --- against the real app, in process -------------------------------------------------------


def _app(settings: Settings, replies: list[str]):  # type: ignore[no-untyped-def]
    s = settings.model_copy(
        update={
            "api": ApiSettings(
                service_tokens={"java-search": SecretStr("tok")}, identity_services=["java-search"]
            )
        }
    )
    service, _ = make_service(s, FakeLLMClient(replies))
    app = create_app(s, Services(search=service._search, answer=service))
    app.state.services = Services(search=service._search, answer=service)
    return app


@pytest.mark.parametrize("endpoint", ["search", "answer", "stream"])
async def test_the_sender_talks_to_every_endpoint(settings: Settings, endpoint: str) -> None:
    app = _app(settings, ["One percent [1]."])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        sender = HttpSender(client, "http://test", "tok", endpoint)
        sample = await sender(Query("late delivery penalty", "alice", ("g1",)))
    assert sample.status == 200
    assert sample.mode_used.startswith(("hybrid", "bm25")) or endpoint == "stream"
    if endpoint == "stream":
        assert sample.first_byte_s is not None and sample.mode_used.startswith("hybrid")


async def test_a_wrong_token_and_a_dead_server_are_recorded_as_errors(settings: Settings) -> None:
    app = _app(settings, ["x"])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (
            await HttpSender(client, "http://test", "wrong", "search")(Query("q"))
        ).status == 401
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ConnectError("down")))
    ) as client:
        for endpoint in ("search", "stream"):
            sample = await HttpSender(client, "http://dead.test", "t", endpoint)(Query("q"))
            assert sample.status == 0


async def test_a_short_run_against_the_app_passes_its_limits(settings: Settings) -> None:
    app = _app(settings, ["One percent [1]."])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        sender = HttpSender(client, "http://test", "tok", "search")
        summary = await run_load(
            sender,
            [Query("late delivery", "alice", ("g1",))],
            rate=20,
            duration_s=0.5,
        )
    assert summary.requests == 10 and summary.error_rate == 0.0
    assert check_slo(summary, p95_max_s=3.0, error_rate_max=0.0) == []
