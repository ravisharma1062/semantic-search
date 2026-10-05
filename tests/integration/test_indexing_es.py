"""Templates, indexer, state store, alias tools and the worker against a real Elasticsearch 8."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from elasticsearch import AsyncElasticsearch, BadRequestError

from app.core.errors import NonRetryableError
from app.core.settings import ChunkingSettings, IngestionSettings, Settings, StoreSettings
from app.ingestion.chunker import Chunk, Chunker, content_hash_of, make_chunk_id
from app.ingestion.consumer import HandlerContext
from app.ingestion.es_source_reader import ElasticsearchSourceReader
from app.ingestion.events import parse_event
from app.ingestion.indexer import ElasticsearchIndexer
from app.ingestion.source import SourceDocument
from app.ingestion.state_store import ElasticsearchStateStore, IndexStatus
from app.ingestion.tokens import WhitespaceTokenCounter
from app.ingestion.worker import IndexingWorker, Outcome
from app.store.aliases import (
    alias_targets,
    create_chunk_index,
    create_state_index,
    delete_chunk_index,
    install_templates,
    switch_alias,
)
from app.store.client import create_es_client
from app.store.templates import physical_chunk_index_name
from tests.fakes import FakeEmbedder

pytestmark = pytest.mark.integration

DIMS = 8
VECTOR = [1.0] + [0.0] * (DIMS - 1)


@dataclass
class Env:
    settings: Settings
    client: AsyncElasticsearch
    index: str  # the chunk index version
    alias: str
    state_index: str
    source_index: str


@pytest.fixture
async def env(es_url: str) -> AsyncIterator[Env]:
    suffix = uuid.uuid4().hex[:8]
    base = Settings()
    settings = base.model_copy(
        update={
            "elasticsearch": base.elasticsearch.model_copy(
                update={"hosts": [es_url], "state_index": f"state_{suffix}"}
            ),
            "store": StoreSettings(
                chunk_index_prefix=f"ch{suffix}", shards=1, replicas=0, refresh_interval="30s"
            ),
            "embedding": base.embedding.model_copy(update={"dims": DIMS}),
            "search": base.search.model_copy(
                update={"index_alias": f"alias_{suffix}", "source_index": f"src_{suffix}"}
            ),
        }
    )
    client = create_es_client(settings.elasticsearch)
    await install_templates(client, settings)
    index = physical_chunk_index_name(f"ch{suffix}", "v1", "bge-m3")
    await create_chunk_index(client, index, settings)
    await create_state_index(client, settings)
    yield Env(settings, client, index, f"alias_{suffix}", f"state_{suffix}", f"src_{suffix}")
    await client.close()


def _indexer(env: Env, index: str | None = None) -> ElasticsearchIndexer:
    s = env.settings
    return ElasticsearchIndexer(env.client, index or env.index, s.store, s.elasticsearch, s.retry)


def _chunk(number: int, item_id: str = "ITEM-1", text: str | None = None) -> Chunk:
    content = text or f"synthetic chunk text number {number}"
    digest = content_hash_of(content)
    return Chunk(
        item_id=item_id,
        chunk_no=number,
        chunk_id=make_chunk_id(item_id, number, digest),
        content=content,
        page_start=1,
        page_end=1,
        section_title=None,
        content_hash=digest,
        token_count=5,
        chunker_version="v1",
    )


def _doc(item_id: str = "ITEM-1", acl: str = "u1") -> SourceDocument:
    return SourceDocument(
        item_id=item_id,
        doc_type="contract",
        tags=["vendor"],
        created_at=datetime(2024, 3, 1, tzinfo=UTC),
        acl_users=[acl],
        acl_groups=["g1"],
    )


async def _count(env: Env, item_id: str | None = None, index: str | None = None) -> int:
    name = index or env.index
    await env.client.indices.refresh(index=name)
    query: dict[str, Any] = {"term": {"doc_id": item_id}} if item_id else {"match_all": {}}
    return int((await env.client.count(index=name, query=query))["count"])


async def _ids(env: Env, item_id: str) -> set[str]:
    await env.client.indices.refresh(index=env.index)
    found = await env.client.search(
        index=env.index, query={"term": {"doc_id": item_id}}, size=1000, source=False
    )
    return {hit["_id"] for hit in found["hits"]["hits"]}


# --- templates ------------------------------------------------------------------------------


async def test_the_template_gives_the_designed_mapping(env: Env) -> None:
    mapping = (await env.client.indices.get_mapping(index=env.index))[env.index]["mappings"]
    assert mapping["dynamic"] == "strict"
    embedding = mapping["properties"]["embedding"]
    assert (embedding["type"], embedding["dims"], embedding["similarity"]) == (
        "dense_vector",
        DIMS,
        "cosine",
    )
    assert embedding["index_options"]["type"] == "int8_hnsw"
    assert mapping["properties"]["acl_users"]["type"] == "keyword"
    state = (await env.client.indices.get_mapping(index=env.state_index))[env.state_index][
        "mappings"
    ]
    assert state["properties"]["doc_version"]["type"] == "long"


async def test_unplanned_fields_are_rejected(env: Env) -> None:
    with pytest.raises(BadRequestError):
        await env.client.index(index=env.index, id="x", document={"surprise": 1})


# --- indexer --------------------------------------------------------------------------------


async def test_writing_the_same_chunks_twice_overwrites_them(env: Env) -> None:
    indexer = _indexer(env)
    chunks = [_chunk(i) for i in range(5)]
    await indexer.bulk_write(_doc(), chunks, [VECTOR] * 5, "bge-m3@1")
    await indexer.bulk_write(_doc(), chunks, [VECTOR] * 5, "bge-m3@1")
    assert await _count(env) == 5
    stored = await env.client.get(index=env.index, id=chunks[0].chunk_id)
    assert stored["_source"]["content"] == chunks[0].content
    assert stored["_source"]["embedding"] == pytest.approx(VECTOR)
    assert stored["_source"]["embedding_model"] == "bge-m3@1"
    assert stored["_source"]["acl_users"] == ["u1"]


async def test_a_wrong_vector_size_is_rejected_for_good(env: Env) -> None:
    with pytest.raises(NonRetryableError):
        await _indexer(env).bulk_write(_doc(), [_chunk(0)], [[1.0, 0.0]], "m")


async def test_stale_chunks_are_deleted_and_new_ones_stay(env: Env) -> None:
    indexer = _indexer(env)
    old = [_chunk(i, text=f"old text {i}") for i in range(3)]
    await indexer.bulk_write(_doc(), old, [VECTOR] * 3, "m")
    await env.client.indices.refresh(index=env.index)
    new = [_chunk(i, text=f"new text {i}") for i in range(2)]
    await indexer.bulk_write(_doc(), new, [VECTOR] * 2, "m")
    other = [_chunk(0, item_id="ITEM-2")]
    await indexer.bulk_write(_doc("ITEM-2"), other, [VECTOR], "m")
    deleted = await indexer.delete_stale_chunks(
        "ITEM-1", [c.chunk_id for c in new], refresh_first=True
    )
    assert deleted == 3
    assert await _ids(env, "ITEM-1") == {c.chunk_id for c in new}
    assert await _count(env, "ITEM-2") == 1  # other documents are not touched


async def test_without_a_refresh_a_fresh_write_hides_its_old_chunks_from_the_delete(
    env: Env,
) -> None:
    """The reason for refresh_first: delete by query only sees refreshed documents."""
    indexer = _indexer(env)
    old = [_chunk(i, text=f"old text {i}") for i in range(3)]
    new = [_chunk(i, text=f"new text {i}") for i in range(2)]
    await indexer.bulk_write(_doc(), old, [VECTOR] * 3, "m")  # not refreshed yet (30 s interval)
    await indexer.bulk_write(_doc(), new, [VECTOR] * 2, "m")
    keep = [c.chunk_id for c in new]
    assert await indexer.delete_stale_chunks("ITEM-1", keep) == 0  # the ghosts are not seen
    assert await _count(env, "ITEM-1") == 5
    assert await indexer.delete_stale_chunks("ITEM-1", keep, refresh_first=True) == 3
    assert await _count(env, "ITEM-1") == 2


async def test_all_chunks_of_a_document_can_be_deleted_right_after_writing(env: Env) -> None:
    indexer = _indexer(env)
    await indexer.bulk_write(_doc(), [_chunk(i) for i in range(4)], [VECTOR] * 4, "m")
    assert await indexer.delete_chunks("ITEM-1", refresh_first=True) == 4
    assert await _count(env, "ITEM-1") == 0


async def test_a_permission_change_touches_only_the_filter_fields(env: Env) -> None:
    indexer = _indexer(env)
    chunk = _chunk(0)
    await indexer.bulk_write(_doc(acl="u1"), [chunk], [VECTOR], "m")
    before = (await env.client.get(index=env.index, id=chunk.chunk_id))["_source"]
    changed = _doc(acl="u2").model_copy(update={"tags": ["other"], "doc_type": "invoice"})
    assert await indexer.refresh_metadata(changed, refresh_first=True) == 1
    after = (await env.client.get(index=env.index, id=chunk.chunk_id))["_source"]
    assert after["acl_users"] == ["u2"]
    assert after["tags"] == ["other"]
    assert after["doc_type"] == "invoice"
    for untouched in (
        "content",
        "embedding",
        "indexed_at",
        "chunk_id",
        "embedding_model",
        "page_start",
    ):
        assert after[untouched] == before[untouched]


async def test_a_large_write_goes_in_batches(env: Env) -> None:
    s = env.settings
    small_batches = ElasticsearchIndexer(
        env.client,
        env.index,
        s.store.model_copy(update={"bulk_batch_size": 7}),
        s.elasticsearch,
        s.retry,
    )
    await small_batches.bulk_write(_doc(), [_chunk(i) for i in range(50)], [VECTOR] * 50, "m")
    assert await _count(env) == 50


# --- state store ----------------------------------------------------------------------------


def _states(env: Env) -> ElasticsearchStateStore:
    s = env.settings
    return ElasticsearchStateStore(env.client, env.state_index, s.elasticsearch, s.retry)


async def test_the_state_record_round_trips_and_never_goes_back_in_version(env: Env) -> None:
    states = _states(env)
    assert await states.get("ITEM-1") is None
    await states.mark_indexed(
        "ITEM-1",
        content_hash="sha256:a",
        meta_hash="sha256:m",
        doc_version=7,
        chunk_count=4,
        chunker_version="v1",
        embedding_model="bge-m3@1",
    )
    state = await states.get("ITEM-1")
    assert state is not None
    assert (state.status, state.doc_version, state.chunk_count) == (IndexStatus.INDEXED, 7, 4)
    assert state.indexed_at is not None
    await states.mark_deleted("ITEM-1", 6)  # an older event
    again = await states.get("ITEM-1")
    assert again is not None
    assert again.status is IndexStatus.INDEXED


async def test_workers_writing_the_same_record_at_once_lose_nothing(env: Env) -> None:
    states = _states(env)
    await asyncio.gather(*(states.mark_failed("ITEM-1", f"RuntimeError {i}") for i in range(4)))
    state = await states.get("ITEM-1")
    assert state is not None
    assert (state.status, state.attempts) == (IndexStatus.FAILED, 4)


# --- index versions and the alias -----------------------------------------------------------


async def test_the_alias_moves_atomically_and_old_versions_are_protected(env: Env) -> None:
    s = env.settings
    v1 = env.index
    v2 = physical_chunk_index_name(s.store.chunk_index_prefix, "v2", "bge-m3")
    assert await create_chunk_index(env.client, v2, s) is True
    await _indexer(env, v1).bulk_write(_doc(), [_chunk(0, text="in version one")], [VECTOR], "m")
    await _indexer(env, v2).bulk_write(_doc(), [_chunk(0, text="in version two")], [VECTOR], "m")
    await env.client.indices.refresh(index=f"{v1},{v2}")

    assert await switch_alias(env.client, env.alias, v1, s) == []
    found = await env.client.search(index=env.alias, query={"match_all": {}})
    assert found["hits"]["hits"][0]["_source"]["content"] == "in version one"

    assert await switch_alias(env.client, env.alias, v2, s) == [v1]
    assert await alias_targets(env.client, env.alias) == [v2]
    found = await env.client.search(index=env.alias, query={"match_all": {}})
    assert [h["_source"]["content"] for h in found["hits"]["hits"]] == ["in version two"]

    with pytest.raises(NonRetryableError, match="young"):
        await delete_chunk_index(env.client, v1, env.alias, s)  # kept for a rollback
    with pytest.raises(NonRetryableError, match="alias"):
        await delete_chunk_index(env.client, v2, env.alias, s, force=True)  # the live one
    await delete_chunk_index(env.client, v1, env.alias, s, force=True)
    assert not await env.client.indices.exists(index=v1)


async def test_an_empty_new_version_is_refused_and_the_existing_document_index_is_safe(
    env: Env,
) -> None:
    s = env.settings
    await env.client.index(index=env.source_index, id="ITEM-1", document={"ocr_text": "x"})
    empty = physical_chunk_index_name(s.store.chunk_index_prefix, "v3", "bge-m3")
    await create_chunk_index(env.client, empty, s)
    with pytest.raises(NonRetryableError, match="empty"):
        await switch_alias(env.client, env.alias, empty, s)
    with pytest.raises(NonRetryableError):
        await delete_chunk_index(env.client, env.source_index, env.alias, s, force=True)
    assert await env.client.indices.exists(index=env.source_index)


# --- the worker loop with real Elasticsearch ------------------------------------------------


def _worker(env: Env, embedder: FakeEmbedder) -> IndexingWorker:
    s = env.settings
    chunking = ChunkingSettings(
        version="v1",
        target_tokens=20,
        max_tokens=30,
        overlap_tokens=6,
        min_tokens=5,
        tokenizer="whitespace",
    )
    return IndexingWorker(
        source=ElasticsearchSourceReader(
            env.client, env.source_index, s.source, s.elasticsearch, s.retry
        ),
        indexer=_indexer(env),
        states=_states(env),
        embedder=embedder,
        chunker=Chunker(chunking, WhitespaceTokenCounter()),
        chunking=chunking,
        normalizer=s.normalizer,
        ingestion=IngestionSettings(window_size=4),
        store=s.store,
    )


def _text(start: int, sentences: int = 18) -> str:
    out = []
    for n in range(start, start + sentences):
        words = [f"w{n * 5 + i}" for i in range(5)]
        words[0] = words[0].capitalize()
        out.append(" ".join(words) + ".")
    return " ".join(out)


def _source(text: str, acl: list[str], version: int) -> dict[str, Any]:
    return {
        "pages": [{"page_no": 1, "text": text}, {"page_no": 2, "text": text.replace("w", "x")}],
        "doc_type": "contract",
        "acl_users": acl,
        "acl_groups": ["g1"],
        "version": version,
    }


def _event(event_type: str, version: int | None = None) -> Any:
    import json

    return parse_event(
        json.dumps(
            {
                "schema_version": 1,
                "event_id": uuid.uuid4().hex,
                "event_type": event_type,
                "item_id": "ITEM-1",
                "doc_version": version,
                "occurred_at": datetime.now(UTC).isoformat(),
                "source": "test",
            }
        ).encode()
    )


async def test_the_worker_keeps_the_index_exact_through_update_permission_change_and_delete(
    env: Env,
) -> None:
    context = HandlerContext(retry_count=0, max_retries=3)
    embedder = FakeEmbedder(dims=DIMS, model_name="bge-m3@1")
    worker = _worker(env, embedder)

    await env.client.index(
        index=env.source_index, id="ITEM-1", document=_source(_text(0), ["u1"], 1)
    )
    assert await worker.process(_event("UPSERT", 1), context) is Outcome.INDEXED
    first = await _ids(env, "ITEM-1")
    assert len(first) > 4
    state = await _states(env).get("ITEM-1")
    assert state is not None
    assert (state.status, state.chunk_count) == (IndexStatus.INDEXED, len(first))

    embeds = len(embedder.document_calls)
    assert await worker.process(_event("UPSERT", 1), context) is Outcome.UNCHANGED
    assert len(embedder.document_calls) == embeds

    # permission change right after the write (inside the refresh window): no chunk may be missed
    await env.client.index(
        index=env.source_index, id="ITEM-1", document=_source(_text(0), ["u2"], 2)
    )
    assert await worker.process(_event("ACL_CHANGE", 2), context) is Outcome.METADATA_REFRESHED
    await env.client.indices.refresh(index=env.index)
    users = await env.client.search(
        index=env.index,
        query={"term": {"doc_id": "ITEM-1"}},
        size=1000,
        source_includes=["acl_users"],
    )
    assert {tuple(h["_source"]["acl_users"]) for h in users["hits"]["hits"]} == {("u2",)}
    assert len(embedder.document_calls) == embeds  # no new vectors

    # the text changes right after: all old chunks must be gone, none left over
    await env.client.index(
        index=env.source_index, id="ITEM-1", document=_source(_text(500, 8), ["u2"], 3)
    )
    assert await worker.process(_event("UPSERT", 3), context) is Outcome.INDEXED
    second = await _ids(env, "ITEM-1")
    assert second
    assert not second & first
    assert len(second) < len(first)

    # and a delete right after that
    assert await worker.process(_event("DELETE", 4), context) is Outcome.DELETED
    assert await _count(env, "ITEM-1") == 0
    final = await _states(env).get("ITEM-1")
    assert final is not None
    assert final.status is IndexStatus.DELETED


async def test_the_worker_skips_a_scanned_document_without_text(env: Env) -> None:
    context = HandlerContext(retry_count=0, max_retries=3)
    worker = _worker(env, FakeEmbedder(dims=DIMS, model_name="bge-m3@1"))
    await env.client.index(index=env.source_index, id="ITEM-1", document={"doc_type": "scan"})
    assert await worker.process(_event("UPSERT"), context) is Outcome.SKIPPED
    state = await _states(env).get("ITEM-1")
    assert state is not None
    assert (state.status, state.reason) == (IndexStatus.SKIPPED, "no usable text")
