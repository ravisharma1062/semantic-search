from datetime import UTC, datetime
from typing import Any, cast

import pytest
from elastic_transport import ConnectionTimeout
from elasticsearch import AsyncElasticsearch

from app.core.errors import NonRetryableError, UpstreamTimeoutError, UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings, StoreSettings
from app.ingestion.chunker import Chunk, content_hash_of, make_chunk_id
from app.ingestion.indexer import ElasticsearchIndexer
from app.ingestion.source import SourceDocument
from tests.fakes.es import FakeWriteEs

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
DIMS = 4


async def _no_sleep(_seconds: float) -> None:
    return None


def _indexer(es: FakeWriteEs, *, batch: int = 200, attempts: int = 3) -> ElasticsearchIndexer:
    return ElasticsearchIndexer(
        cast(AsyncElasticsearch, es),
        "doc_chunks_v1_bgem3",
        StoreSettings(bulk_batch_size=batch, maintenance_timeout_s=90),
        ElasticsearchSettings(
            hosts=["http://es.test:9200"], state_index="state", bulk_timeout_s=30
        ),
        RetryPolicy(attempts=attempts),
        sleep=_no_sleep,
        clock=lambda: NOW,
    )


def _chunk(number: int, item_id: str = "ITEM-1") -> Chunk:
    content = f"chunk text number {number}"
    digest = content_hash_of(content)
    return Chunk(
        item_id=item_id,
        chunk_no=number,
        chunk_id=make_chunk_id(item_id, number, digest),
        content=content,
        page_start=1,
        page_end=2,
        section_title="TITLE",
        content_hash=digest,
        token_count=4,
        chunker_version="v1",
    )


def _doc() -> SourceDocument:
    return SourceDocument(
        item_id="ITEM-1",
        doc_type="contract",
        tags=["vendor"],
        created_at=NOW,
        acl_users=["u1"],
        acl_groups=["g1"],
    )


def _vectors(count: int) -> list[list[float]]:
    return [[1.0, 0.0, 0.0, 0.0] for _ in range(count)]


def _ids(op_list: list[dict[str, Any]]) -> list[str]:
    return [op["index"]["_id"] for op in op_list[::2]]


# --- bulk write -----------------------------------------------------------------------------


async def test_chunks_are_written_with_their_id_as_the_document_id() -> None:
    es = FakeWriteEs()
    chunks = [_chunk(0), _chunk(1)]
    await _indexer(es).bulk_write(_doc(), chunks, _vectors(2), "bge-m3@1")
    [call] = es.called("bulk")
    operations = call["operations"]
    assert operations[0] == {"index": {"_index": "doc_chunks_v1_bgem3", "_id": chunks[0].chunk_id}}
    source = operations[1]
    assert source["chunk_id"] == chunks[0].chunk_id
    assert source["doc_id"] == "ITEM-1"
    assert source["content"] == "chunk text number 0"
    assert source["embedding"] == [1.0, 0.0, 0.0, 0.0]
    assert source["embedding_model"] == "bge-m3@1"
    assert source["chunker_version"] == "v1"
    assert (source["page_start"], source["page_end"], source["section_title"]) == (1, 2, "TITLE")
    assert source["acl_users"] == ["u1"]
    assert source["acl_groups"] == ["g1"]
    assert source["doc_type"] == "contract"
    assert source["tags"] == ["vendor"]
    assert source["indexed_at"] == NOW.isoformat()


async def test_the_bulk_timeout_is_set() -> None:
    es = FakeWriteEs()
    await _indexer(es).bulk_write(_doc(), [_chunk(0)], _vectors(1), "m")
    assert 30 in es.timeouts


async def test_large_writes_are_split_into_batches() -> None:
    es = FakeWriteEs()
    await _indexer(es, batch=2).bulk_write(_doc(), [_chunk(i) for i in range(5)], _vectors(5), "m")
    assert [len(c["operations"]) // 2 for c in es.called("bulk")] == [2, 2, 1]


async def test_a_wrong_number_of_vectors_is_rejected_before_any_call() -> None:
    es = FakeWriteEs()
    with pytest.raises(NonRetryableError):
        await _indexer(es).bulk_write(_doc(), [_chunk(0), _chunk(1)], _vectors(1), "m")
    assert es.calls == []


async def test_no_chunks_means_no_call() -> None:
    es = FakeWriteEs()
    await _indexer(es).bulk_write(_doc(), [], [], "m")
    assert es.calls == []


def _answer(ids: list[str], failed: dict[str, int]) -> dict[str, Any]:
    items = []
    for chunk_id in ids:
        if chunk_id in failed:
            error = {
                "type": "es_rejected_execution_exception"
                if failed[chunk_id] == 429
                else "mapper_parsing_exception"
            }
            items.append({"index": {"_id": chunk_id, "status": failed[chunk_id], "error": error}})
        else:
            items.append({"index": {"_id": chunk_id, "status": 201}})
    return {"errors": bool(failed), "items": items}


async def test_only_the_failed_items_are_sent_again() -> None:
    es = FakeWriteEs()
    chunks = [_chunk(i) for i in range(4)]
    ids = [c.chunk_id for c in chunks]
    es.bulk_answers = [_answer(ids, {ids[1]: 429, ids[3]: 503}), _answer([ids[1], ids[3]], {})]
    await _indexer(es).bulk_write(_doc(), chunks, _vectors(4), "m")
    first, second = es.called("bulk")
    assert _ids(first["operations"]) == ids
    assert _ids(second["operations"]) == [ids[1], ids[3]]  # the other two are not sent again


async def test_a_rejected_item_stops_the_write_without_retrying() -> None:
    es = FakeWriteEs()
    chunks = [_chunk(0), _chunk(1)]
    ids = [c.chunk_id for c in chunks]
    es.bulk_answers = [_answer(ids, {ids[0]: 400})]
    with pytest.raises(NonRetryableError):
        await _indexer(es).bulk_write(_doc(), chunks, _vectors(2), "m")
    assert len(es.called("bulk")) == 1


async def test_items_that_keep_failing_end_in_a_retryable_error() -> None:
    es = FakeWriteEs()
    chunk = _chunk(0)
    es.bulk_answers = [_answer([chunk.chunk_id], {chunk.chunk_id: 429})] * 5
    with pytest.raises(UpstreamUnavailableError):
        await _indexer(es, attempts=3).bulk_write(_doc(), [chunk], _vectors(1), "m")
    assert len(es.called("bulk")) == 3


async def test_a_failed_whole_request_is_repeated() -> None:
    es = FakeWriteEs()
    es.errors = [ConnectionTimeout("slow")]
    await _indexer(es).bulk_write(_doc(), [_chunk(0)], _vectors(1), "m")
    assert len(es.called("bulk")) == 2


async def test_a_dead_cluster_gives_a_typed_error() -> None:
    es = FakeWriteEs()
    es.errors = [ConnectionTimeout("slow")] * 5
    with pytest.raises(UpstreamTimeoutError):
        await _indexer(es).bulk_write(_doc(), [_chunk(0)], _vectors(1), "m")


async def test_the_same_chunk_written_twice_has_the_same_id() -> None:
    es = FakeWriteEs()
    chunk = _chunk(0)
    await _indexer(es).bulk_write(_doc(), [chunk], _vectors(1), "m")
    await _indexer(es).bulk_write(_doc(), [chunk], _vectors(1), "m")
    first, second = es.called("bulk")
    assert _ids(first["operations"]) == _ids(second["operations"])  # so the second overwrites


# --- delete and update ----------------------------------------------------------------------


async def test_stale_chunks_are_the_documents_chunks_that_are_not_kept() -> None:
    es = FakeWriteEs()
    deleted = await _indexer(es).delete_stale_chunks("ITEM-1", ["a", "b"])
    assert deleted == 3
    assert es.called("delete_by_query")[0]["query"] == {
        "bool": {
            "filter": [{"term": {"doc_id": "ITEM-1"}}],
            "must_not": [{"ids": {"values": ["a", "b"]}}],
        }
    }
    assert 90 in es.timeouts


async def test_all_chunks_of_a_document_can_be_deleted() -> None:
    es = FakeWriteEs()
    assert await _indexer(es).delete_chunks("ITEM-1") == 3
    assert es.called("delete_by_query")[0]["query"] == {"term": {"doc_id": "ITEM-1"}}


async def test_refresh_comes_first_when_asked_and_not_otherwise() -> None:
    es = FakeWriteEs()
    await _indexer(es).delete_chunks("ITEM-1")
    assert [name for name, _ in es.calls] == ["delete_by_query"]
    es.calls.clear()
    await _indexer(es).delete_chunks("ITEM-1", refresh_first=True)
    assert [name for name, _ in es.calls] == ["refresh", "delete_by_query"]
    es.calls.clear()
    await _indexer(es).delete_stale_chunks("ITEM-1", [], refresh_first=True)
    await _indexer(es).refresh_metadata(_doc(), refresh_first=True)
    assert [name for name, _ in es.calls] == [
        "refresh",
        "delete_by_query",
        "refresh",
        "update_by_query",
    ]


async def test_a_delete_that_hits_a_conflict_is_tried_again_not_skipped() -> None:
    es = FakeWriteEs()
    es.by_query_conflicts = 1
    assert await _indexer(es).delete_chunks("ITEM-1") == 3
    assert len(es.called("delete_by_query")) == 2


async def test_permissions_are_updated_with_a_script_that_touches_only_filter_fields() -> None:
    es = FakeWriteEs()
    assert await _indexer(es).refresh_metadata(_doc()) == 4
    [call] = es.called("update_by_query")
    assert call["query"] == {"term": {"doc_id": "ITEM-1"}}
    script = call["script"]
    assert script["params"] == {
        "acl_users": ["u1"],
        "acl_groups": ["g1"],
        "doc_type": "contract",
        "tags": ["vendor"],
        "created_at": NOW.isoformat(),
    }
    changed_fields = {
        part.split("=")[0].strip() for part in script["source"].split(";") if part.strip()
    }
    assert changed_fields == {
        "ctx._source.acl_users",
        "ctx._source.acl_groups",
        "ctx._source.doc_type",
        "ctx._source.tags",
        "ctx._source.created_at",
    }  # content and the vector are not touched


async def test_a_permission_update_that_hits_a_conflict_is_never_skipped() -> None:
    es = FakeWriteEs()
    es.by_query_conflicts = 2
    await _indexer(es).refresh_metadata(_doc())
    assert len(es.called("update_by_query")) == 3


async def test_a_conflict_that_never_goes_away_is_an_error() -> None:
    es = FakeWriteEs()
    es.by_query_conflicts = 10
    with pytest.raises(UpstreamUnavailableError):
        await _indexer(es, attempts=2).refresh_metadata(_doc())
