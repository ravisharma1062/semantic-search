"""Numbers for a load test: percentiles and a summary of one run.

Plain Python, no dependencies, so the arithmetic can be tested without a server.
"""

import math
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field


def percentile(values: Sequence[float], p: float) -> float:
    """The ``p``-th percentile (0 to 100) with the nearest-rank method. 0.0 for no values.

    Nearest rank is simple and never invents a value that was not measured.
    """
    if not 0 <= p <= 100:
        raise ValueError("p must be between 0 and 100")
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


@dataclass(frozen=True)
class Sample:
    """The result of one request."""

    latency_s: float
    status: int  # HTTP status, or 0 if no answer came (timeout, connection error)
    mode_used: str = ""
    first_byte_s: float | None = None


@dataclass
class Summary:
    """What a run measured."""

    requests: int
    duration_s: float
    achieved_rps: float
    error_rate: float
    fallback_rate: float
    latency_s: dict[str, float]
    statuses: dict[str, int]
    modes_used: dict[str, int]
    dropped: int = 0  # requests that could not start because too many were in flight
    first_byte_s: dict[str, float] = field(default_factory=dict)


def summarise(
    samples: Sequence[Sample],
    duration_s: float,
    *,
    dropped: int = 0,
    fallback_modes: frozenset[str] = frozenset({"bm25", "knn"}),
) -> Summary:
    """The summary of a run. An error is any answer that is not 2xx, and no answer at all.
    A fallback is a successful answer whose mode is one of ``fallback_modes``."""
    ok = [s for s in samples if 200 <= s.status < 300]
    errors = len(samples) - len(ok)
    modes = Counter(s.mode_used for s in ok if s.mode_used)
    fallbacks = sum(count for mode, count in modes.items() if mode.split("+")[0] in fallback_modes)
    # Latency of failed requests counts too: a slow error is still a slow answer.
    latencies = [s.latency_s for s in samples]
    first_bytes = [s.first_byte_s for s in ok if s.first_byte_s is not None]
    return Summary(
        requests=len(samples),
        duration_s=round(duration_s, 3),
        achieved_rps=round(len(samples) / duration_s, 2) if duration_s > 0 else 0.0,
        error_rate=round(errors / len(samples), 4) if samples else 0.0,
        fallback_rate=round(fallbacks / len(ok), 4) if ok else 0.0,
        latency_s={
            "p50": percentile(latencies, 50),
            "p90": percentile(latencies, 90),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": max(latencies, default=0.0),
        },
        statuses={str(k): v for k, v in sorted(Counter(s.status for s in samples).items())},
        modes_used=dict(modes),
        dropped=dropped,
        first_byte_s=(
            {"p50": percentile(first_bytes, 50), "p95": percentile(first_bytes, 95)}
            if first_bytes
            else {}
        ),
    )


def check_slo(
    summary: Summary,
    *,
    p95_max_s: float | None = None,
    error_rate_max: float | None = None,
    fallback_rate_max: float | None = None,
    first_byte_p95_max_s: float | None = None,
) -> list[str]:
    """The limits that the run broke, as short sentences. Empty means the run passed."""
    broken: list[str] = []
    if p95_max_s is not None and summary.latency_s["p95"] > p95_max_s:
        broken.append(f"p95 {summary.latency_s['p95']:.3f}s > {p95_max_s}s")
    if error_rate_max is not None and summary.error_rate > error_rate_max:
        broken.append(f"error rate {summary.error_rate} > {error_rate_max}")
    if fallback_rate_max is not None and summary.fallback_rate > fallback_rate_max:
        broken.append(f"fallback rate {summary.fallback_rate} > {fallback_rate_max}")
    if first_byte_p95_max_s is not None:
        measured = summary.first_byte_s.get("p95")
        if measured is None or measured > first_byte_p95_max_s:
            broken.append(f"first token p95 {measured} > {first_byte_p95_max_s}s")
    if summary.dropped:
        broken.append(f"{summary.dropped} requests were not sent: the client was the limit")
    return broken
