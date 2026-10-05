"""Load generator for the search and answer endpoints (task T5.2).

    python -m loadtest.run --api http://localhost:8080 --token ... \
        --questions eval/smoke_set.jsonl --endpoint search --rate 50 --duration 60 --p95-max 3.0

It sends requests at a fixed rate (open loop), not "as fast as the answers come back". A closed loop
slows down when the server slows down and hides the delay it caused (coordinated omission). If more
requests are in flight than ``--max-in-flight``, new ones are counted as ``dropped`` and the run is
reported as limited by the client, not by the service.

Questions come from the evaluation set (JSON Lines with ``question`` and ``as_user``) or a text file
with one question per line. The questions are test data: do not use real user questions.
"""

import argparse
import asyncio
import itertools
import json
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx

from loadtest.stats import Sample, Summary, check_slo, summarise


@dataclass(frozen=True)
class Query:
    """One request to send."""

    text: str
    user: str = "load-user"
    groups: tuple[str, ...] = ()


SendFn = Callable[[Query], Awaitable[Sample]]


def load_queries(path: Path) -> list[Query]:
    """Questions from a JSON Lines file (``question``, ``as_user``, ``groups``) or a text file."""
    queries: list[Query] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            row = json.loads(line)
            queries.append(
                Query(
                    row["question"],
                    row.get("as_user", "load-user"),
                    tuple(row.get("groups", [])),
                )
            )
        else:
            queries.append(Query(line))
    if not queries:
        raise ValueError("no questions to send")
    return queries


async def run_load(
    send: SendFn,
    queries: list[Query],
    *,
    rate: float,
    duration_s: float,
    max_in_flight: int = 500,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> Summary:
    """Send ``rate`` requests per second for ``duration_s`` seconds, then wait for the answers."""
    if rate <= 0 or duration_s <= 0:
        raise ValueError("rate and duration must be positive")
    total = int(rate * duration_s)
    interval = 1.0 / rate
    samples: list[Sample] = []
    pending: set[asyncio.Task[None]] = set()
    dropped = 0
    cycle = itertools.cycle(queries)
    started = clock()

    async def one(query: Query) -> None:
        samples.append(await send(query))

    for number in range(total):
        wait = started + number * interval - clock()
        if wait > 0:
            await sleep(wait)
        if len(pending) >= max_in_flight:
            dropped += 1
            continue
        task = asyncio.create_task(one(next(cycle)))
        pending.add(task)
        task.add_done_callback(pending.discard)
        await asyncio.sleep(0)  # let the request start now, not at the next scheduled slot
    if pending:
        await asyncio.gather(*pending)
    return summarise(samples, clock() - started, dropped=dropped)


_PATHS = {"search": "search", "answer": "answer", "stream": "answer/stream"}


class HttpSender:
    """Sends one request to the API and times it. ``stream`` also times the first token."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        token: str,
        endpoint: str,
        *,
        timeout_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._token = token
        self._endpoint = endpoint
        self._url = f"{base_url.rstrip('/')}/v1/{_PATHS[endpoint]}"
        self._timeout_s = timeout_s
        self._clock = clock

    def _body(self, query: Query) -> dict[str, object]:
        if self._endpoint == "search":
            return {"query": query.text, "top_k": 10}
        return {"question": query.text}

    def _headers(self, query: Query) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "X-User-Id": query.user,
            "X-User-Groups": ",".join(query.groups),
        }

    async def __call__(self, query: Query) -> Sample:
        if self._endpoint == "stream":
            return await self._stream(query)
        started = self._clock()
        try:
            response = await self._client.post(
                self._url,
                json=self._body(query),
                headers=self._headers(query),
                timeout=self._timeout_s,
            )
        except httpx.HTTPError:
            return Sample(latency_s=self._clock() - started, status=0)
        mode = str(response.json().get("mode_used", "")) if response.status_code == 200 else ""
        return Sample(
            latency_s=self._clock() - started, status=response.status_code, mode_used=mode
        )

    async def _stream(self, query: Query) -> Sample:
        started = self._clock()
        first: float | None = None
        status = 0
        mode = ""
        try:
            async with self._client.stream(
                "POST",
                self._url,
                json=self._body(query),
                headers=self._headers(query),
                timeout=self._timeout_s,
            ) as response:
                status = response.status_code
                async for line in response.aiter_lines():
                    if first is None and line.startswith("event: token"):
                        first = self._clock() - started
                    if line.startswith("data:") and '"mode_used"' in line:
                        mode = str(json.loads(line[5:]).get("mode_used", ""))
        except httpx.HTTPError:
            pass  # the status stays what it was: 0 if no answer began
        return Sample(
            latency_s=self._clock() - started, status=status, mode_used=mode, first_byte_s=first
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loadtest.run", description=__doc__)
    parser.add_argument("--api", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--questions", required=True, type=Path)
    parser.add_argument("--endpoint", choices=["search", "answer", "stream"], default="search")
    parser.add_argument("--rate", type=float, required=True, help="requests per second")
    parser.add_argument("--duration", type=float, default=60.0, help="seconds")
    parser.add_argument("--max-in-flight", type=int, default=500)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--label", default="run", help="e.g. the index size: 10pct, 50pct, 100pct")
    parser.add_argument("--out", type=Path, default=Path("loadtest/results"))
    parser.add_argument("--p95-max", type=float)
    parser.add_argument("--error-rate-max", type=float)
    parser.add_argument("--fallback-rate-max", type=float)
    parser.add_argument("--first-token-p95-max", type=float)
    return parser


async def _main(argv: list[str]) -> int:
    args = _parser().parse_args(argv)
    queries = load_queries(args.questions)
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=args.max_in_flight)) as client:
        sender = HttpSender(client, args.api, args.token, args.endpoint, timeout_s=args.timeout)
        summary = await run_load(
            sender,
            queries,
            rate=args.rate,
            duration_s=args.duration,
            max_in_flight=args.max_in_flight,
        )
    broken = check_slo(
        summary,
        p95_max_s=args.p95_max,
        error_rate_max=args.error_rate_max,
        fallback_rate_max=args.fallback_rate_max,
        first_byte_p95_max_s=args.first_token_p95_max,
    )
    report = {
        "label": args.label,
        "endpoint": args.endpoint,
        "target_rps": args.rate,
        "summary": asdict(summary),
        "slo_broken": broken,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{args.label}-{args.endpoint}-{int(args.rate)}rps.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))
    print(f"saved: {path}")
    for line in broken:
        print(f"limit broken: {line}", file=sys.stderr)
    return 1 if broken else 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
