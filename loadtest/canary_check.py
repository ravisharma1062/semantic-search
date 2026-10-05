"""Judges a canary of the API from Prometheus (task T5.5).

    python -m loadtest.canary_check --prometheus https://prometheus.example \
        --canary-pod '.*-api-canary-.*' --stable-pod '.*-api-[0-9a-f]+-.*' \
        --window 5m --duration 600 --interval 60

The canary pods get a share of the real traffic (see ``api-canary.yaml``). For ``--duration``
seconds the check reads, for the canary and for the stable pods, the error rate, the search p95 and
the fallback rate, and compares them. It stops with exit code 1 as soon as the canary is clearly
worse, and also if there is not enough traffic to judge (a canary that nobody called proves
nothing). Exit code 0 means the canary was at least as good as the limits for the whole time.

The metrics are the ones the service exports (``deploy/observability``). The ``pod`` label comes
from the Prometheus service discovery.
"""

import argparse
import asyncio
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

_ROUTES = 'route=~"/v1/(search|answer.*)"'


@dataclass(frozen=True)
class Reading:
    """What one group of pods measured over the window. ``None`` means no data."""

    requests_per_s: float | None
    error_rate: float | None
    p95_s: float | None
    fallback_rate: float | None


@dataclass(frozen=True)
class Limits:
    """How much worse than the stable pods the canary may be."""

    min_requests_per_s: float = 0.05
    max_error_rate: float = 0.02  # absolute
    max_error_rate_ratio: float = 2.0  # of the stable error rate
    max_p95_s: float = 2.5  # absolute
    max_p95_ratio: float = 1.5  # of the stable p95
    max_fallback_rate: float = 0.05  # absolute


def queries(pod: str, window: str) -> dict[str, str]:
    """The PromQL queries for the pods that match ``pod`` (a regular expression)."""
    sel = f'pod=~"{pod}",{_ROUTES}'
    total = f"sum(rate(semsearch_http_requests_total{{{sel}}}[{window}]))"
    errors = f'sum(rate(semsearch_http_requests_total{{{sel},status=~"5.."}}[{window}]))'
    stage = f'pod=~"{pod}",stage="total"'
    return {
        "requests_per_s": total,
        "error_rate": f"{errors} / clamp_min({total}, 1e-9)",
        "p95_s": (
            "histogram_quantile(0.95, sum by (le) "
            f"(rate(semsearch_search_stage_duration_seconds_bucket{{{stage}}}[{window}])))"
        ),
        "fallback_rate": (
            f'sum(rate(semsearch_search_fallbacks_total{{pod=~"{pod}"}}[{window}])) / clamp_min('
            f'sum(rate(semsearch_search_requests_total{{pod=~"{pod}"}}[{window}])), 1e-9)'
        ),
    }


class Prometheus:
    """The instant query API of Prometheus."""

    def __init__(self, client: httpx.AsyncClient, base_url: str, token: str | None = None) -> None:
        self._client = client
        self._url = f"{base_url.rstrip('/')}/api/v1/query"
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}

    async def instant(self, expr: str) -> float | None:
        """The value of a query, or ``None`` if it has no data (or is NaN)."""
        response = await self._client.get(
            self._url, params={"query": expr}, headers=self._headers, timeout=15
        )
        response.raise_for_status()
        result = response.json()["data"]["result"]
        if not result:
            return None
        value = float(result[0]["value"][1])
        return None if value != value else value  # NaN: no traffic in the window

    async def read(self, pod: str, window: str) -> Reading:
        """All four numbers for a group of pods."""
        values = {name: await self.instant(expr) for name, expr in queries(pod, window).items()}
        return Reading(**values)


def judge(canary: Reading, stable: Reading, limits: Limits) -> list[str]:
    """The reasons why the canary is not good enough. Empty means it is."""
    if canary.requests_per_s is None or canary.requests_per_s < limits.min_requests_per_s:
        return ["not enough traffic on the canary to judge it"]
    problems: list[str] = []
    if canary.error_rate is not None:
        allowed = max(limits.max_error_rate, 0.0)
        if stable.error_rate is not None:
            allowed = min(allowed, max(stable.error_rate * limits.max_error_rate_ratio, 0.005))
        if canary.error_rate > allowed:
            problems.append(f"error rate {canary.error_rate:.4f} > {allowed:.4f}")
    if canary.p95_s is not None:
        allowed = limits.max_p95_s
        if stable.p95_s is not None:
            allowed = min(allowed, max(stable.p95_s * limits.max_p95_ratio, 0.05))
        if canary.p95_s > allowed:
            problems.append(f"search p95 {canary.p95_s:.3f}s > {allowed:.3f}s")
    if canary.fallback_rate is not None and canary.fallback_rate > limits.max_fallback_rate:
        problems.append(f"fallback rate {canary.fallback_rate:.4f} > {limits.max_fallback_rate}")
    return problems


async def watch(
    prom: Prometheus,
    *,
    canary_pod: str,
    stable_pod: str,
    window: str,
    duration_s: float,
    interval_s: float,
    limits: Limits,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> list[str]:
    """Check every ``interval_s`` for ``duration_s``. Returns the problems of the first bad check,
    or an empty list if every check was fine."""
    deadline = clock() + duration_s
    while True:
        canary = await prom.read(canary_pod, window)
        stable = await prom.read(stable_pod, window)
        problems = judge(canary, stable, limits)
        if problems:
            return problems
        if clock() + interval_s > deadline:
            return []
        await sleep(interval_s)


async def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="loadtest.canary_check", description=__doc__)
    parser.add_argument("--prometheus", required=True)
    parser.add_argument("--token")
    parser.add_argument("--canary-pod", required=True, help="regular expression for the pod label")
    parser.add_argument("--stable-pod", required=True)
    parser.add_argument("--window", default="5m")
    parser.add_argument("--duration", type=float, default=600)
    parser.add_argument("--interval", type=float, default=60)
    parser.add_argument("--max-error-rate", type=float, default=Limits.max_error_rate)
    parser.add_argument("--max-p95", type=float, default=Limits.max_p95_s)
    parser.add_argument("--max-fallback-rate", type=float, default=Limits.max_fallback_rate)
    args = parser.parse_args(argv)
    limits = Limits(
        max_error_rate=args.max_error_rate,
        max_p95_s=args.max_p95,
        max_fallback_rate=args.max_fallback_rate,
    )
    async with httpx.AsyncClient() as client:
        problems = await watch(
            Prometheus(client, args.prometheus, args.token),
            canary_pod=args.canary_pod,
            stable_pod=args.stable_pod,
            window=args.window,
            duration_s=args.duration,
            interval_s=args.interval,
            limits=limits,
        )
    for line in problems:
        print(f"canary is not good enough: {line}", file=sys.stderr)
    if not problems:
        print("canary ok")
    return 1 if problems else 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
