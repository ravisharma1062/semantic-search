"""State index operations for the backfill and reconciliation jobs: bulk reads, wave
registration, counts per status and wave, and an ordered scan of the records.

These requests go to our own state index, not to user data. They are listed in decision 0007.
"""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from elasticsearch import AsyncElasticsearch

from app.core.errors import UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings
from app.ingestion.state_store import IndexState
from app.store.calls import guarded

NO_WAVE = -1  # in the counts: documents that were indexed by live events, outside any wave


class StateAdmin:
    """Reads and bulk-writes the state index."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        index: str,
        elasticsearch: ElasticsearchSettings,
        retry: RetryPolicy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client.options(request_timeout=elasticsearch.state_timeout_s * 5)
        self._index = index
        self._retry = retry
        self._sleep = sleep

    async def get_many(self, item_ids: Sequence[str]) -> dict[str, IndexState]:
        """The records that exist, by ``item_id``."""
        if not item_ids:
            return {}
        ids = list(item_ids)

        async def fetch() -> list[dict[str, Any]]:
            response = await self._client.mget(index=self._index, ids=ids)
            return list(response["docs"])

        found: dict[str, IndexState] = {}
        for doc in await guarded(fetch, self._retry, self._sleep):
            if doc.get("found"):
                found[str(doc["_id"])] = IndexState.model_validate(doc["_source"])
        return found

    async def register_wave(self, item_ids: Sequence[str], wave: int) -> None:
        """Put the documents into a wave. A document without a record gets a PENDING one. A
        record that exists keeps its status and only gets the wave number."""
        if not item_ids:
            return
        operations: list[dict[str, Any]] = []
        for item_id in item_ids:
            operations.append({"update": {"_index": self._index, "_id": item_id}})
            operations.append(
                {
                    "doc": {"wave": wave},
                    "upsert": {
                        "item_id": item_id,
                        "status": "PENDING",
                        "wave": wave,
                        "attempts": 0,
                    },
                }
            )

        async def send() -> None:
            response = await self._client.bulk(operations=operations, refresh=False)
            if response["errors"]:
                raise UpstreamUnavailableError("Elasticsearch could not take all records")

        await guarded(send, self._retry, self._sleep)

    async def status_counts(self) -> dict[int, dict[str, int]]:
        """Documents per wave and status. ``NO_WAVE`` collects records outside any wave."""
        aggs = {
            "by_wave": {
                "terms": {"field": "wave", "size": 1000, "missing": NO_WAVE},
                "aggs": {"by_status": {"terms": {"field": "status", "size": 10}}},
            }
        }

        async def fetch() -> dict[str, Any]:
            response = await self._client.search(
                index=self._index, size=0, aggs=aggs, track_total_hits=False
            )
            return dict(response["aggregations"])

        body = await guarded(fetch, self._retry, self._sleep)
        return {
            int(wave["key"]): {s["key"]: int(s["doc_count"]) for s in wave["by_status"]["buckets"]}
            for wave in body["by_wave"]["buckets"]
        }

    async def scan(
        self, *, after: str | None, size: int, wave: int | None = None
    ) -> tuple[list[IndexState], str | None]:
        """Records in ``item_id`` order after the given ID, and the cursor for the next page."""
        query: dict[str, Any] = {"term": {"wave": wave}} if wave is not None else {"match_all": {}}

        async def fetch() -> list[dict[str, Any]]:
            response = await self._client.search(
                index=self._index,
                query=query,
                sort=[{"item_id": {"order": "asc"}}],
                size=size,
                search_after=[after] if after is not None else None,
                track_total_hits=False,
            )
            return list(response["hits"]["hits"])

        hits = await guarded(fetch, self._retry, self._sleep)
        states = [IndexState.model_validate(h["_source"]) for h in hits]
        return states, (states[-1].item_id if states else None)
