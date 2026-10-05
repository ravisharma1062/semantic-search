"""LLM telemetry to Langfuse over its HTTP ingestion API (HLD sections 9 and 17).

Metadata only: model, prompt version, token counts, latency, outcome and IDs. The question, the
passages and the answer are never sent, because the policy for LLM traces is "masked, or metadata
only" and metadata is the safe default. The user ID is hashed.

``submit`` never blocks and never raises: events go into a bounded queue, a background task sends
them in batches, and a full queue or a failed send only counts ``telemetry_dropped_total``.
Telemetry must not slow down or break an answer.
"""

import asyncio
import hashlib
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from app.core.settings import LangfuseSettings
from app.observability.metrics import get_metrics

_log = structlog.get_logger(__name__)
_PATH = "/api/public/ingestion"


def pseudonym(user_id: str) -> str:
    """A stable short ID for a user, so traces can be grouped without naming the user."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:16]


class LangfuseSink:
    """Collects answer events and sends them in the background."""

    def __init__(self, settings: LangfuseSettings, client: httpx.AsyncClient) -> None:
        self._cfg = settings
        self._client = client
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(settings.queue_size)
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def submit(
        self,
        *,
        request_id: str | None,
        user_id: str,
        model: str,
        prompt_version: str,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        found: bool,
        reason: str,
        mode_used: str,
    ) -> None:
        """Queue one answer. Drops it if the queue is full."""
        trace_id = request_id or uuid.uuid4().hex
        now = datetime.now(UTC).isoformat()
        metadata = {
            "found": found,
            "reason": reason,
            "mode_used": mode_used,
            "prompt_version": prompt_version,
        }
        events = [
            {
                "id": uuid.uuid4().hex,
                "type": "trace-create",
                "timestamp": now,
                "body": {
                    "id": trace_id,
                    "name": "answer",
                    "userId": pseudonym(user_id),
                    "metadata": metadata,
                },
            },
            {
                "id": uuid.uuid4().hex,
                "type": "generation-create",
                "timestamp": now,
                "body": {
                    "id": uuid.uuid4().hex,
                    "traceId": trace_id,
                    "name": "answer-generation",
                    "model": model,
                    "metadata": {**metadata, "latency_ms": latency_ms},
                    "usage": {"input": input_tokens, "output": output_tokens, "unit": "TOKENS"},
                },
            },
        ]
        for event in events:
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                get_metrics().telemetry_dropped.labels("langfuse").inc()

    async def start(self) -> None:
        """Start the background sender."""
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        """Send what is queued, then stop."""
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not (self._stop.is_set() and self._queue.empty()):
            batch = await self._take()
            if batch:
                await self._send(batch)

    async def _take(self) -> list[dict[str, Any]]:
        batch: list[dict[str, Any]] = []
        try:
            first = await asyncio.wait_for(self._queue.get(), self._cfg.flush_interval_s)
        except TimeoutError:
            return batch
        batch.append(first)
        while len(batch) < self._cfg.batch_size and not self._queue.empty():
            batch.append(self._queue.get_nowait())
        return batch

    async def _send(self, batch: list[dict[str, Any]]) -> None:
        public, secret = self._cfg.public_key, self._cfg.secret_key
        if public is None or secret is None:
            get_metrics().telemetry_dropped.labels("langfuse").inc(len(batch))
            return
        try:
            response = await self._client.post(
                f"{self._cfg.host.rstrip('/')}{_PATH}",
                json={"batch": batch},
                auth=(public.get_secret_value(), secret.get_secret_value()),
                timeout=self._cfg.timeout_s,
            )
            if response.status_code >= 400:
                raise httpx.HTTPStatusError("rejected", request=response.request, response=response)
        except httpx.HTTPError as exc:
            _log.warning("langfuse_send_failed", error_type=type(exc).__name__)
            get_metrics().telemetry_dropped.labels("langfuse").inc(len(batch))
