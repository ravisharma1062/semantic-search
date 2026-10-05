from datetime import UTC, datetime
from typing import Any, cast

import pytest
from elastic_transport import ConnectionTimeout
from elasticsearch import AsyncElasticsearch

from app.core.errors import NonRetryableError, UpstreamTimeoutError, UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings
from app.ingestion.state_store import ElasticsearchStateStore, IndexStatus
from tests.fakes.es import FakeWriteEs, api_error

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


async def _no_sleep(_seconds: float) -> None:
    return None


def _store(es: FakeWriteEs, attempts: int = 3) -> ElasticsearchStateStore:
    return ElasticsearchStateStore(
        cast(AsyncElasticsearch, es),
        "state",
        ElasticsearchSettings(
            hosts=["http://es.test:9200"], state_index="state", state_timeout_s=2.0
        ),
        RetryPolicy(attempts=attempts),
        sleep=_no_sleep,
        clock=lambda: NOW,
    )


async def _indexed(store: ElasticsearchStateStore, item_id: str = "ITEM-1", **changes: Any) -> None:
    values: dict[str, Any] = {
        "content_hash": "sha256:a",
        "meta_hash": "sha256:m",
        "doc_version": 5,
        "chunk_count": 3,
        "chunker_version": "v1",
        "embedding_model": "bge-m3@1",
    }
    await store.mark_indexed(item_id, **{**values, **changes})


async def test_unknown_document_has_no_state() -> None:
    assert await _store(FakeWriteEs()).get("ITEM-1") is None


async def test_mark_indexed_creates_the_record() -> None:
    es = FakeWriteEs()
    store = _store(es)
    await _indexed(store)
    state = await store.get("ITEM-1")
    assert state is not None
    assert state.status is IndexStatus.INDEXED
    assert (state.content_hash, state.meta_hash, state.doc_version) == ("sha256:a", "sha256:m", 5)
    assert (state.chunk_count, state.chunker_version, state.embedding_model) == (
        3,
        "v1",
        "bge-m3@1",
    )
    assert state.indexed_at == NOW
    assert state.attempts == 0
    assert es.called("index")[0]["op_type"] == "create"  # a first write never overwrites


async def test_every_call_has_the_state_timeout() -> None:
    es = FakeWriteEs()
    await _indexed(_store(es))
    assert set(es.timeouts) == {2.0}


async def test_an_older_version_never_replaces_a_newer_record() -> None:
    store = _store(FakeWriteEs())
    await _indexed(store, doc_version=7)
    await _indexed(store, doc_version=6, content_hash="sha256:old")
    state = await store.get("ITEM-1")
    assert state is not None
    assert (state.doc_version, state.content_hash) == (7, "sha256:a")
    await store.mark_deleted("ITEM-1", 6)
    await store.mark_skipped("ITEM-1", "no text", 6)
    state = await store.get("ITEM-1")
    assert state is not None
    assert state.status is IndexStatus.INDEXED


async def test_same_or_newer_versions_and_unknown_versions_do_update() -> None:
    store = _store(FakeWriteEs())
    await _indexed(store, doc_version=5)
    await _indexed(store, doc_version=5, content_hash="sha256:same")
    await _indexed(store, doc_version=None, content_hash="sha256:none")
    state = await store.get("ITEM-1")
    assert state is not None
    assert state.content_hash == "sha256:none"


async def test_deleted_and_skipped_records() -> None:
    store = _store(FakeWriteEs())
    await store.mark_skipped("ITEM-1", "no usable text", 2)
    skipped = await store.get("ITEM-1")
    assert skipped is not None
    assert (skipped.status, skipped.reason) == (IndexStatus.SKIPPED, "no usable text")
    await store.mark_deleted("ITEM-1", 3)
    deleted = await store.get("ITEM-1")
    assert deleted is not None
    assert deleted.status is IndexStatus.DELETED


async def test_failures_are_counted_and_keep_the_earlier_hashes() -> None:
    store = _store(FakeWriteEs())
    await _indexed(store)
    await store.mark_failed("ITEM-1", "UpstreamTimeoutError: Timeout")
    await store.mark_failed("ITEM-1", "RuntimeError")
    state = await store.get("ITEM-1")
    assert state is not None
    assert (state.status, state.attempts, state.last_error) == (
        IndexStatus.FAILED,
        2,
        "RuntimeError",
    )
    assert state.content_hash == "sha256:a"


async def test_a_success_clears_the_failure_count() -> None:
    store = _store(FakeWriteEs())
    await store.mark_failed("ITEM-1", "RuntimeError")
    await _indexed(store)
    state = await store.get("ITEM-1")
    assert state is not None
    assert (state.status, state.attempts, state.last_error) == (IndexStatus.INDEXED, 0, None)


async def test_a_failure_on_a_never_seen_document_creates_a_record() -> None:
    store = _store(FakeWriteEs())
    await store.mark_failed("ITEM-9", "SourceNotReadyError")
    state = await store.get("ITEM-9")
    assert state is not None
    assert (state.status, state.attempts) == (IndexStatus.FAILED, 1)


async def test_the_error_text_is_cut() -> None:
    store = _store(FakeWriteEs())
    await store.mark_failed("ITEM-1", "x" * 1000)
    state = await store.get("ITEM-1")
    assert state is not None
    assert state.last_error is not None
    assert len(state.last_error) == 256


async def test_meta_hash_update_changes_only_that_field() -> None:
    store = _store(FakeWriteEs())
    await _indexed(store)
    await store.update_meta_hash("ITEM-1", "sha256:new")
    state = await store.get("ITEM-1")
    assert state is not None
    assert (state.meta_hash, state.content_hash, state.status) == (
        "sha256:new",
        "sha256:a",
        IndexStatus.INDEXED,
    )
    await store.update_meta_hash("ITEM-404", "sha256:new")  # nothing to update: no record is made
    assert await store.get("ITEM-404") is None


# --- concurrency and failures ---------------------------------------------------------------


async def test_a_conflicting_write_is_read_again_and_applied() -> None:
    es = FakeWriteEs()
    store = _store(es)
    await _indexed(store)
    # another worker writes between our read and our write
    es.before_write = lambda: es.seq.__setitem__("ITEM-1", es.seq["ITEM-1"] + 100)
    await store.mark_failed("ITEM-1", "RuntimeError")
    state = await store.get("ITEM-1")
    assert state is not None
    assert state.status is IndexStatus.FAILED
    assert len(es.called("index")) == 3  # create, the conflicting try, the second try


async def test_two_creates_at_the_same_time_do_not_lose_the_second() -> None:
    es = FakeWriteEs()
    store = _store(es)

    def other_worker_creates_first() -> None:
        es.docs["ITEM-1"] = {"item_id": "ITEM-1", "status": "FAILED", "attempts": 1}
        es.seq["ITEM-1"] = 50

    es.before_write = other_worker_creates_first
    await store.mark_failed("ITEM-1", "RuntimeError")
    state = await store.get("ITEM-1")
    assert state is not None
    assert state.attempts == 2  # both failures are counted


async def test_endless_conflicts_end_with_a_retryable_error() -> None:
    es = FakeWriteEs()
    store = _store(es)
    await _indexed(store)

    class Always:
        def __call__(self) -> None:
            es.seq["ITEM-1"] += 1
            es.before_write = self

    es.before_write = Always()
    with pytest.raises(UpstreamUnavailableError):
        await store.mark_failed("ITEM-1", "RuntimeError")


async def test_timeouts_are_retried() -> None:
    es = FakeWriteEs()
    es.errors = [ConnectionTimeout("slow")]
    await _indexed(_store(es))
    assert len(es.called("get")) == 2


async def test_a_dead_cluster_gives_a_typed_error() -> None:
    es = FakeWriteEs()
    es.errors = [ConnectionTimeout("slow")] * 5
    with pytest.raises(UpstreamTimeoutError):
        await _store(es).get("ITEM-1")


async def test_a_rejected_request_is_not_retried() -> None:
    es = FakeWriteEs()
    es.errors = [api_error(400)]
    with pytest.raises(NonRetryableError):
        await _store(es).get("ITEM-1")
    assert len(es.called("get")) == 1
