"""The real worker process against real Kafka and Elasticsearch, with the fake model server.

event -> read -> chunk -> embed (HTTP) -> index -> state -> commit, through ``run_worker``.
"""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from elasticsearch import AsyncElasticsearch

from app.core.settings import (
    BackfillSettings,
    ChunkingSettings,
    ConsumerSettings,
    IngestionSettings,
    KafkaSettings,
    Settings,
    StoreSettings,
    WaveSpec,
)
from app.ingestion.kafka_client import ConfluentProducer
from app.ingestion.runtime import run_worker
from app.ingestion.state_admin import StateAdmin
from app.ingestion.state_store import ElasticsearchStateStore, IndexState, IndexStatus
from app.jobs.backfill import BackfillJob
from app.jobs.job_store import ElasticsearchJobStore
from app.jobs.rate import RateLimiter
from app.jobs.scan import SourceScanner
from app.store.aliases import create_chunk_index, create_state_index, install_templates
from app.store.client import create_es_client
from app.store.templates import physical_chunk_index_name
from tests.integration.conftest import ModelServer, Topics
from tests.integration.kafka_helpers import committed_total, read_all

pytestmark = pytest.mark.integration


@dataclass
class Env:
    settings: Settings
    client: AsyncElasticsearch
    index: str
    source_index: str
    topics: Topics
    backfill_topic: str
    bootstrap: str
    heartbeat: Path
    model: ModelServer

    @property
    def states(self) -> ElasticsearchStateStore:
        s = self.settings
        return ElasticsearchStateStore(
            self.client, s.elasticsearch.state_index, s.elasticsearch, s.retry
        )


@pytest.fixture
async def env(
    es_url: str,
    kafka_bootstrap: str,
    topics: Topics,
    model_server: ModelServer,
    tmp_path: Path,
) -> AsyncIterator[Env]:
    suffix = uuid.uuid4().hex[:8]
    backfill_topic = f"backfill-{suffix}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    futures = admin.create_topics(
        [NewTopic(backfill_topic, num_partitions=4, replication_factor=1)]
    )
    futures[backfill_topic].result(timeout=30)
    base = Settings()
    prefix = f"ch{suffix}"
    index = physical_chunk_index_name(prefix, "v1", "bge-m3")
    heartbeat = tmp_path / "worker-alive"
    settings = base.model_copy(
        update={
            "elasticsearch": base.elasticsearch.model_copy(
                update={"hosts": [es_url], "state_index": f"state_{suffix}"}
            ),
            "store": StoreSettings(
                chunk_index_prefix=prefix,
                write_index=index,
                shards=1,
                replicas=0,
                refresh_interval="1s",
            ),
            "search": base.search.model_copy(
                update={"index_alias": f"alias_{suffix}", "source_index": f"src_{suffix}"}
            ),
            "kafka": KafkaSettings(
                bootstrap_servers=kafka_bootstrap,
                live_topic=topics.live,
                backfill_topic=backfill_topic,
                retry_topic=topics.retry,
                dlq_topic=topics.dlq,
                consumer_group=topics.group,
                backfill_consumer_group=f"{topics.group}-backfill",
            ),
            "consumer": ConsumerSettings(
                poll_timeout_s=0.2,
                commit_interval_s=0.2,
                quick_retries=1,
                quick_retry_initial_delay_s=0,
                retry_delay_s=1,
                max_retries=2,
                shutdown_timeout_s=15,
                rebalance_timeout_s=15,
                max_in_flight=4,
            ),
            "embedding": base.embedding.model_copy(
                update={
                    "endpoint": model_server.url,
                    "dims": model_server.dims,
                    "timeout_s": 5,
                    "document_timeout_s": 10,
                }
            ),
            "chunking": ChunkingSettings(
                version="v1",
                target_tokens=20,
                max_tokens=30,
                overlap_tokens=6,
                min_tokens=5,
                tokenizer="whitespace",
            ),
            "ingestion": IngestionSettings(
                window_size=4,
                backfill_max_in_flight=2,
                heartbeat_file=str(heartbeat),
                heartbeat_interval_s=0.2,
            ),
            "backfill": BackfillSettings(
                job_index=f"jobs_{suffix}", scan_size=5, waves=[WaveSpec(number=1, name="all")]
            ),
        }
    )
    client = create_es_client(settings.elasticsearch)
    await install_templates(client, settings)
    await create_chunk_index(client, index, settings)
    await create_state_index(client, settings)
    await client.indices.create(
        index=f"src_{suffix}",
        settings={"number_of_shards": 1, "number_of_replicas": 0},
        mappings={"properties": {"item_id": {"type": "keyword"}, "doc_type": {"type": "keyword"}}},
    )
    yield Env(
        settings,
        client,
        index,
        f"src_{suffix}",
        topics,
        backfill_topic,
        kafka_bootstrap,
        heartbeat,
        model_server,
    )
    await client.close()


def _text(start: int, sentences: int = 14) -> str:
    out = []
    for n in range(start, start + sentences):
        words = [f"w{n * 5 + i}" for i in range(5)]
        words[0] = words[0].capitalize()
        out.append(" ".join(words) + ".")
    return " ".join(out)


async def _put_source(
    env: Env, item_id: str, start: int, acl: str = "u1", version: int = 1
) -> None:
    await env.client.index(
        index=env.source_index,
        id=item_id,
        document={
            "item_id": item_id,
            "doc_type": "contract",
            "acl_users": [acl],
            "acl_groups": ["g1"],
            "version": version,
            "pages": [
                {"page_no": 1, "text": _text(start)},
                {"page_no": 2, "text": _text(start + 40)},
            ],
        },
    )


def _send(
    env: Env, event_type: str, item_id: str, version: int | None = None, topic: str | None = None
) -> None:
    producer = Producer({"bootstrap.servers": env.bootstrap})
    value = json.dumps(
        {
            "schema_version": 1,
            "event_id": uuid.uuid4().hex,
            "event_type": event_type,
            "item_id": item_id,
            "doc_version": version,
            "occurred_at": datetime.now(UTC).isoformat(),
            "source": "e2e-test",
        }
    ).encode()
    producer.produce(topic or env.topics.live, key=item_id.encode(), value=value)
    assert producer.flush(30) == 0


async def _until(
    condition: Callable[[], Awaitable[bool]], seconds: float = 60, what: str = ""
) -> None:
    deadline = time.monotonic() + seconds
    while not await condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"not reached in time: {what}")
        await asyncio.sleep(0.25)


async def _chunks(env: Env, item_id: str) -> list[dict[str, Any]]:
    await env.client.indices.refresh(index=env.index)
    found = await env.client.search(
        index=env.index,
        query={"term": {"doc_id": item_id}},
        sort=[{"chunk_no": "asc"}],
        size=1000,
        source_excludes=["embedding"],
    )
    return [hit["_source"] for hit in found["hits"]["hits"]]


class Worker:
    """The real worker process loop as a background task."""

    def __init__(self, env: Env) -> None:
        self.env = env
        self.stop = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.stop = asyncio.Event()
        self.task = asyncio.create_task(run_worker(self.env.settings, self.stop))

    async def shutdown(self) -> None:
        assert self.task is not None
        self.stop.set()
        await asyncio.wait_for(self.task, timeout=60)


@pytest.fixture
async def worker(env: Env) -> AsyncIterator[Worker]:
    w = Worker(env)
    w.start()
    yield w
    if w.task and not w.task.done():
        await w.shutdown()


async def _indexed(env: Env, item_id: str) -> IndexState | None:
    state = await env.states.get(item_id)
    return state if state and state.status is IndexStatus.INDEXED else None


# --- the tests ------------------------------------------------------------------------------


async def test_live_events_through_update_permission_change_and_delete(
    env: Env, worker: Worker
) -> None:
    for i in range(4):
        await _put_source(env, f"ITEM-{i}", start=i * 200)
        _send(env, "UPSERT", f"ITEM-{i}", 1)

    async def all_indexed() -> bool:
        return all([await _indexed(env, f"ITEM-{i}") for i in range(4)])

    await _until(all_indexed, what="four documents indexed")
    first = {i: await _chunks(env, f"ITEM-{i}") for i in range(4)}
    assert all(len(chunks) >= 4 for chunks in first.values())
    for chunks in first.values():
        assert [c["chunk_no"] for c in chunks] == list(range(len(chunks)))
        assert all(c["acl_users"] == ["u1"] and c["embedding_model"] == "bge-m3@1" for c in chunks)
        assert all(c["page_start"] in (1, 2) for c in chunks)
    total_chunks = sum(len(c) for c in first.values())
    assert env.model.stats()["embed_texts"] >= total_chunks
    assert env.heartbeat.exists()  # the liveness heartbeat is written
    await _until(
        lambda: _async(committed_total(env.bootstrap, env.topics.group, env.topics.live) == 4),
        what="offsets committed",
    )

    # an unchanged document: nothing is embedded again
    embedded = env.model.stats()["embed_texts"]
    _send(env, "UPSERT", "ITEM-0", 1)
    await asyncio.sleep(3)
    assert env.model.stats()["embed_texts"] == embedded

    # a permission change: permissions on all chunks, no new vectors
    await _put_source(env, "ITEM-1", start=200, acl="u2", version=2)
    _send(env, "ACL_CHANGE", "ITEM-1", 2)

    async def acl_changed() -> bool:
        return {tuple(c["acl_users"]) for c in await _chunks(env, "ITEM-1")} == {("u2",)}

    await _until(acl_changed, what="permissions refreshed")
    assert env.model.stats()["embed_texts"] == embedded

    # a text change: all old chunks are replaced, none left over
    old_ids = {c["chunk_id"] for c in await _chunks(env, "ITEM-2")}
    await _put_source(env, "ITEM-2", start=900, version=2)
    _send(env, "UPSERT", "ITEM-2", 2)

    async def replaced() -> bool:
        return not old_ids & {c["chunk_id"] for c in await _chunks(env, "ITEM-2")}

    await _until(replaced, what="chunks replaced")
    assert await _chunks(env, "ITEM-2")  # and the document is still searchable

    # a delete
    await env.client.delete(index=env.source_index, id="ITEM-3")
    _send(env, "DELETE", "ITEM-3", 2)

    async def deleted() -> bool:
        state = await env.states.get("ITEM-3")
        return bool(
            state and state.status is IndexStatus.DELETED and not await _chunks(env, "ITEM-3")
        )

    await _until(deleted, what="document deleted")
    assert env.model.stats()["embed_texts"] > embedded  # only the changed document was embedded


async def _async(value: bool) -> bool:
    return value


async def test_a_backfill_wave_reaches_the_worker_through_its_own_topic_and_group(
    env: Env, worker: Worker
) -> None:
    for i in range(14):
        await _put_source(env, f"ITEM-{i:02d}", start=i * 60)
    await env.client.indices.refresh(index=env.source_index)

    s = env.settings
    jobs = ElasticsearchJobStore(env.client, s.backfill.job_index, s.elasticsearch, s.retry)
    await jobs.ensure_index()
    producer = ConfluentProducer(s.kafka)
    try:
        job = BackfillJob(
            scanner=SourceScanner(
                env.client, env.source_index, s.source, s.backfill, s.elasticsearch, s.retry
            ),
            jobs=jobs,
            states=StateAdmin(env.client, s.elasticsearch.state_index, s.elasticsearch, s.retry),
            producer=producer,
            topic=env.backfill_topic,
            limiter=RateLimiter(1000),
            settings=s.backfill,
            embedding_model="bge-m3@1",
            chunker_version="v1",
        )
        record = await job.run("wave-1", WaveSpec(number=1), asyncio.Event())
    finally:
        await producer.close()
    assert record.published == 14

    async def all_indexed() -> bool:
        states = [await env.states.get(f"ITEM-{i:02d}") for i in range(14)]
        return all(st and st.status is IndexStatus.INDEXED and st.wave == 1 for st in states)

    await _until(all_indexed, what="the wave is indexed")
    admin = StateAdmin(env.client, s.elasticsearch.state_index, s.elasticsearch, s.retry)
    await env.client.indices.refresh(index=s.elasticsearch.state_index)
    assert await admin.status_counts() == {1: {"INDEXED": 14}}
    await _until(
        lambda: _async(
            committed_total(env.bootstrap, s.kafka.backfill_consumer_group, env.backfill_topic)
            == 14
        ),
        what="backfill offsets committed",
    )
    assert (
        committed_total(env.bootstrap, s.kafka.consumer_group, env.topics.live) == 0
    )  # the live group saw none of it


async def test_poison_messages_a_restart_and_no_reprocessing(env: Env, worker: Worker) -> None:
    await _put_source(env, "ITEM-1", start=0)
    producer = Producer({"bootstrap.servers": env.bootstrap})
    producer.produce(env.topics.live, key=b"ITEM-X", value=b"this is not an event")
    producer.produce(env.topics.live, key=b"ITEM-Y", value=b'{"schema_version": 99}')
    assert producer.flush(30) == 0
    _send(env, "UPSERT", "ITEM-1", 1)

    async def indexed() -> bool:
        return await _indexed(env, "ITEM-1") is not None

    await _until(indexed, what="the good document is indexed")
    dead = await asyncio.to_thread(read_all, env.bootstrap, env.topics.dlq, 2)
    assert {value for value, _ in dead} == {b'{"schema_version": 99}', b"this is not an event"}
    await _until(
        lambda: _async(committed_total(env.bootstrap, env.topics.group, env.topics.live) == 3),
        what="all three messages committed",
    )

    # graceful stop: the loop ends, the heartbeat stops, the offsets stay committed
    await worker.shutdown()
    stopped_at = env.heartbeat.stat().st_mtime
    await asyncio.sleep(1)
    assert env.heartbeat.stat().st_mtime == stopped_at
    embedded = env.model.stats()["embed_texts"]

    # restart: committed events are not processed again, and a new event is
    worker.start()
    await _put_source(env, "ITEM-2", start=500)
    _send(env, "UPSERT", "ITEM-2", 1)

    async def second_indexed() -> bool:
        return await _indexed(env, "ITEM-2") is not None

    await _until(second_indexed, what="the new document after the restart")
    new_embeds = env.model.stats()["embed_texts"] - embedded
    assert new_embeds == len(await _chunks(env, "ITEM-2"))  # only the new document was embedded
    assert (
        len(await asyncio.to_thread(read_all, env.bootstrap, env.topics.dlq, 3, 3)) == 2
    )  # no new DLQ entries


async def test_a_document_that_never_appears_is_retried_and_then_treated_as_deleted(
    env: Env, worker: Worker
) -> None:
    _send(env, "UPSERT", "GHOST", 1)  # an event for a document that is not in the source index

    async def deleted() -> bool:
        state = await env.states.get("GHOST")
        return bool(state and state.status is IndexStatus.DELETED)

    await _until(
        deleted, seconds=90, what="the missing document is treated as deleted after the retries"
    )
    retries = await asyncio.to_thread(read_all, env.bootstrap, env.topics.retry, 2)
    assert len(retries) == 2  # two trips through the retry topic (retry limit is 2)
    assert await _chunks(env, "GHOST") == []
    assert await asyncio.to_thread(read_all, env.bootstrap, env.topics.dlq, 1, 2) == []
