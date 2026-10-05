"""Scans the existing document index in a stable order (HLD section 4, "Batch re-index").

This is the one place that sends a search request to the existing index. It is an internal job
with no user behind it, so it has no access filter (rule 1 is about user searches, see decision
0007). It returns IDs and, on request, permission and filter fields, never OCR text.

The scan sorts by one unique field (``backfill.scan_sort_field``, normally ITEM_ID as a keyword)
and continues with ``search_after``. The cursor is that field's last value, so a scan can stop for
any time and continue later. Documents without the field are not reached by the scan: they arrive
through live events.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch

from app.core.retry import RetryPolicy
from app.core.settings import BackfillSettings, ElasticsearchSettings, SourceSettings, WaveSpec
from app.store.calls import guarded

_log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ScanItem:
    """One document of the source index."""

    item_id: str
    source: dict[str, Any]  # permission and filter fields, only when asked for


@dataclass(frozen=True)
class ScanPage:
    """A page of the scan. ``cursor`` is where the next page starts."""

    items: list[ScanItem]
    cursor: str


class SourceScanner:
    """Reads the source index page by page."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        index: str,
        source: SourceSettings,
        backfill: BackfillSettings,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client.options(request_timeout=elasticsearch.source_read_timeout_s)
        self._index = index
        self._source = source
        self._cfg = backfill
        self._retry = retry
        self._sleep = sleep

    def query(self, scope: WaveSpec | None) -> dict[str, Any]:
        """The documents of a wave. No scope means all documents."""
        filters: list[dict[str, Any]] = []
        if scope and scope.doc_types:
            filters.append({"terms": {self._source.doc_type_field: scope.doc_types}})
        if scope and (scope.created_from or scope.created_to):
            bounds: dict[str, str] = {}
            if scope.created_from:
                bounds["gte"] = scope.created_from.isoformat()
            if scope.created_to:
                bounds["lte"] = scope.created_to.isoformat()
            filters.append({"range": {self._source.created_at_field: bounds}})
        if not filters:
            return {"match_all": {}}
        return {"bool": {"filter": filters}}

    def _meta_fields(self) -> list[str]:
        s = self._source
        return [
            s.doc_type_field,
            s.tags_field,
            s.created_at_field,
            s.owner_field,
            s.acl_users_field,
            s.acl_groups_field,
            s.version_field,
            s.language_field,
        ]

    async def pages(
        self,
        scope: WaveSpec | None = None,
        *,
        after: str | None = None,
        include_meta: bool = False,
        max_items: int | None = None,
    ) -> AsyncIterator[ScanPage]:
        """Pages in sort order, starting after ``after``. Stops after ``max_items`` documents."""
        query = self.query(scope)
        sort = [{self._cfg.scan_sort_field: {"order": "asc"}}]
        includes: bool | dict[str, Any] = (
            {"includes": self._meta_fields()} if include_meta else False
        )
        cursor = after
        seen = 0
        while max_items is None or seen < max_items:
            size = self._cfg.scan_size
            if max_items is not None:
                size = min(size, max_items - seen)

            async def fetch(size: int = size, cursor: str | None = cursor) -> list[dict[str, Any]]:
                response = await self._client.search(
                    index=self._index,
                    query=query,
                    sort=sort,
                    size=size,
                    search_after=[cursor] if cursor is not None else None,
                    source=includes,
                    track_total_hits=False,
                )
                return list(response["hits"]["hits"])

            hits = await guarded(fetch, self._retry, self._sleep)
            if not hits:
                return
            items: list[ScanItem] = []
            last: str | None = None
            for hit in hits:
                sort_value = hit["sort"][0] if hit.get("sort") else None
                if sort_value is None:
                    _log.warning("scan_stopped_at_documents_without_sort_field")
                    break
                items.append(ScanItem(str(hit["_id"]), hit.get("_source") or {}))
                last = str(sort_value)
            if not items or last is None:
                return
            seen += len(items)
            cursor = last
            yield ScanPage(items, last)
            if len(items) < len(hits):
                return

    async def existing(self, item_ids: list[str]) -> set[str]:
        """Which of these IDs are in the source index (no document text is read)."""
        if not item_ids:
            return set()

        async def fetch() -> list[dict[str, Any]]:
            response = await self._client.mget(index=self._index, ids=item_ids, source=False)
            return list(response["docs"])

        docs = await guarded(fetch, self._retry, self._sleep)
        return {str(d["_id"]) for d in docs if d.get("found")}
