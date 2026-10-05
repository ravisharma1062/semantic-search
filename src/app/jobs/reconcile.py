"""Reconciliation: compare the state index with the source and republish the differences
(HLD section 17, "Consistency and freshness").

Source to state, per scanned document:
    no record, FAILED, PENDING, or DELETED but still in the source    -> UPSERT (backfill topic)
    version in the source differs from the record, or other model/chunker -> UPSERT
    permission or filter fields differ from the record (``meta_hash``)    -> ACL_CHANGE (live topic)
State to source:
    record exists but the document is gone from the source                -> DELETE (live topic)

Deletes and permission fixes go to the live topic because they have the highest priority. A full
scan of 100 million documents is long, so the job works for one wave or a sample (``max_items``).
It is safe to run again at any time.
"""

import structlog
from pydantic import BaseModel

from app.core.settings import BackfillSettings, SourceSettings, WaveSpec
from app.ingestion.events import EventType
from app.ingestion.kafka_io import MessageProducer
from app.ingestion.source import map_source
from app.ingestion.state_admin import StateAdmin
from app.ingestion.state_store import IndexState, IndexStatus
from app.ingestion.worker import meta_hash
from app.jobs.events import build_event
from app.jobs.rate import RateLimiter
from app.jobs.scan import ScanItem, SourceScanner

_log = structlog.get_logger(__name__)


class ReconcileResult(BaseModel):
    """What the run found and published."""

    scanned: int = 0
    missing_state: int = 0
    failed: int = 0
    pending: int = 0
    resurrected: int = 0
    version_mismatch: int = 0
    stale_model: int = 0
    permission_mismatch: int = 0
    orphans_deleted: int = 0
    published: int = 0


class Reconciler:
    """Finds documents whose index state is wrong and sends events to fix them."""

    def __init__(
        self,
        *,
        scanner: SourceScanner,
        states: StateAdmin,
        producer: MessageProducer,
        live_topic: str,
        backfill_topic: str,
        limiter: RateLimiter,
        backfill: BackfillSettings,
        source: SourceSettings,
        embedding_model: str,
        chunker_version: str,
    ) -> None:
        self._scanner = scanner
        self._states = states
        self._producer = producer
        self._live_topic = live_topic
        self._backfill_topic = backfill_topic
        self._limiter = limiter
        self._cfg = backfill
        self._source = source
        self._model = embedding_model
        self._chunker_version = chunker_version

    async def run(
        self,
        scope: WaveSpec | None = None,
        *,
        max_items: int | None = None,
        check_orphans: bool = True,
    ) -> ReconcileResult:
        """Check the documents of the scope. Returns the counts."""
        limit = max_items if max_items is not None else self._cfg.reconcile_max_items
        result = ReconcileResult()
        await self._source_to_state(scope, limit, result)
        if check_orphans:
            await self._state_to_source(scope, limit, result)
        _log.info("reconcile_done", **result.model_dump())
        return result

    # --- source to state ----------------------------------------------------------------------

    async def _source_to_state(
        self, scope: WaveSpec | None, limit: int | None, result: ReconcileResult
    ) -> None:
        async for page in self._scanner.pages(scope, include_meta=True, max_items=limit):
            states = await self._states.get_many([i.item_id for i in page.items])
            upserts: list[str] = []
            permission_fixes: list[tuple[str, int | None]] = []
            for item in page.items:
                kind = self._difference(item, states.get(item.item_id), result)
                if kind == "upsert":
                    upserts.append(item.item_id)
                elif kind == "permissions":
                    permission_fixes.append((item.item_id, self._version(item)))
            result.scanned += len(page.items)
            await self._publish(
                self._backfill_topic,
                [
                    build_event(i, EventType.UPSERT, source="reconcile", priority="backfill")
                    for i in upserts
                ],
                result,
            )
            await self._publish(
                self._live_topic,
                [
                    build_event(
                        i, EventType.ACL_CHANGE, source="reconcile", priority="live", doc_version=v
                    )
                    for i, v in permission_fixes
                ],
                result,
            )

    def _version(self, item: ScanItem) -> int | None:
        return map_source(item.item_id, item.source, self._source).version

    def _difference(
        self, item: ScanItem, state: IndexState | None, result: ReconcileResult
    ) -> str | None:
        """What is wrong with this document: ``upsert``, ``permissions`` or nothing."""
        if state is None:
            result.missing_state += 1
            return "upsert"
        if state.status is IndexStatus.FAILED:
            result.failed += 1
            return "upsert"
        if state.status is IndexStatus.PENDING:
            result.pending += 1
            return "upsert"
        if state.status is IndexStatus.DELETED:
            result.resurrected += 1
            return "upsert"
        document = map_source(item.item_id, item.source, self._source)
        if (
            document.version is not None
            and state.doc_version is not None
            and document.version != state.doc_version
        ):
            result.version_mismatch += 1
            return "upsert"
        if state.status is IndexStatus.INDEXED:
            if (
                state.embedding_model != self._model
                or state.chunker_version != self._chunker_version
            ):
                result.stale_model += 1
                return "upsert"
            if state.meta_hash != meta_hash(document):
                result.permission_mismatch += 1
                return "permissions"
        return None

    # --- state to source ----------------------------------------------------------------------

    async def _state_to_source(
        self, scope: WaveSpec | None, limit: int | None, result: ReconcileResult
    ) -> None:
        wave = scope.number if scope else None
        after: str | None = None
        checked = 0
        while limit is None or checked < limit:
            size = (
                self._cfg.scan_size if limit is None else min(self._cfg.scan_size, limit - checked)
            )
            records, after = await self._states.scan(after=after, size=size, wave=wave)
            if not records:
                return
            checked += len(records)
            candidates = [r.item_id for r in records if r.status is not IndexStatus.DELETED]
            present = await self._scanner.existing(candidates)
            gone = [i for i in candidates if i not in present]
            result.orphans_deleted += len(gone)
            await self._publish(
                self._live_topic,
                [
                    build_event(i, EventType.DELETE, source="reconcile", priority="live")
                    for i in gone
                ],
                result,
            )

    # --- publishing ---------------------------------------------------------------------------

    async def _publish(
        self, topic: str, messages: list[tuple[bytes, bytes]], result: ReconcileResult
    ) -> None:
        if not messages:
            return
        await self._limiter.acquire(len(messages))
        await self._producer.send_batch(topic, messages)
        result.published += len(messages)
