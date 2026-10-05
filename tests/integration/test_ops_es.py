"""Operations on real Elasticsearch: snapshots (create, list, prune, restore) and queued jobs."""

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from elasticsearch import AsyncElasticsearch

from app.core.errors import NonRetryableError
from app.core.retry import RetryPolicy
from app.core.settings import SearchSettings, Settings, SnapshotSettings, StoreSettings
from app.jobs.job_store import ElasticsearchJobStore, JobStatus
from app.jobs.snapshot import SnapshotManager
from app.store.aliases import create_chunk_index, create_state_index, install_templates
from app.store.client import create_es_client
from app.store.templates import physical_chunk_index_name
from tests.fakes.search_es import chunk_doc

pytestmark = pytest.mark.integration

DIMS = 8


@dataclass
class Env:
    settings: Settings
    client: AsyncElasticsearch
    index: str
    repository: str
    clock_at: datetime

    def manager(self) -> SnapshotManager:
        def tick() -> datetime:
            self.clock_at += timedelta(minutes=1)  # every snapshot gets its own name
            return self.clock_at

        return SnapshotManager(self.client, self.settings, RetryPolicy(attempts=1), clock=tick)


async def _make_env(url: str, keep_last: int = 14) -> Env:
    suffix = uuid.uuid4().hex[:8]
    base = Settings()
    repository = f"repo_{suffix}"
    settings = base.model_copy(
        update={
            "elasticsearch": base.elasticsearch.model_copy(
                update={"hosts": [url], "state_index": f"state_{suffix}"}
            ),
            "store": StoreSettings(
                chunk_index_prefix=f"ch{suffix}", shards=1, replicas=0, refresh_interval="1s"
            ),
            "embedding": base.embedding.model_copy(update={"dims": DIMS}),
            "search": SearchSettings(
                index_alias=f"alias_{suffix}", source_index="src", candidates=20
            ),
            "snapshot": SnapshotSettings(
                repository=repository, keep_last=keep_last, restore_prefix="restored_"
            ),
        }
    )
    client = create_es_client(settings.elasticsearch)
    await install_templates(client, settings)
    index = physical_chunk_index_name(f"ch{suffix}", "v1", "bge-m3")
    await create_chunk_index(client, index, settings)
    await create_state_index(client, settings)
    await client.indices.put_alias(index=index, name=settings.search.index_alias)
    for n in range(5):
        doc = chunk_doc(f"D{n}:0", f"D{n}", f"snapshot test text {n}", users=["u"])
        doc["embedding"] = [1.0] + [0.0] * (DIMS - 1)
        await client.index(index=index, id=doc["chunk_id"], document=doc)
    await client.indices.refresh(index=index)
    return Env(settings, client, index, repository, datetime(2026, 1, 1, tzinfo=UTC))


async def _register(env: Env) -> None:
    await env.client.snapshot.create_repository(
        name=env.repository,
        repository={
            "type": "fs",
            "settings": {"location": f"/tmp/es-snapshots/{env.repository}"},  # noqa: S108
        },
    )


@pytest.fixture
async def env(es_url: str) -> AsyncIterator[Env]:
    e = await _make_env(es_url)
    yield e
    await e.client.close()


async def test_a_missing_repository_is_a_clear_error(env: Env) -> None:
    with pytest.raises(NonRetryableError):
        await env.manager().check()


async def test_a_snapshot_holds_the_chunk_index_and_the_state_index(env: Env) -> None:
    await _register(env)
    manager = env.manager()
    await manager.check()
    info = await manager.create()
    assert info.state == "SUCCESS" and info.failed_shards == 0
    assert set(info.indices) == {env.index, env.settings.elasticsearch.state_index}
    assert [s.name for s in await manager.snapshots()] == [info.name]


async def test_restore_goes_to_new_names_and_overwrites_nothing(env: Env) -> None:
    await _register(env)
    manager = env.manager()
    info = await manager.create()
    await env.client.delete_by_query(
        index=env.index, query={"match_all": {}}, refresh=True, wait_for_completion=True
    )
    assert (await env.client.count(index=env.index))["count"] == 0  # the "disaster"
    restored = await manager.restore(info.name, [env.index])
    assert restored == [f"restored_{env.index}"]
    counts = await manager.counts(restored)
    assert counts == {f"restored_{env.index}": 5}
    # The live index was not touched, and the restored one has no alias yet.
    assert (await env.client.count(index=env.index))["count"] == 0
    aliases = await env.client.indices.get_alias(index=restored[0])
    assert aliases[restored[0]]["aliases"] == {}


async def test_restoring_something_that_does_not_exist_is_refused(env: Env) -> None:
    await _register(env)
    manager = env.manager()
    info = await manager.create()
    with pytest.raises(NonRetryableError):
        await manager.restore("semsearch-19990101t000000z")
    with pytest.raises(NonRetryableError):
        await manager.restore(info.name, ["some_other_index"])


async def test_prune_keeps_the_newest_snapshots(es_url: str) -> None:
    env = await _make_env(es_url, keep_last=2)
    try:
        await _register(env)
        manager = env.manager()
        names = [(await manager.create()).name for _ in range(4)]
        deleted = await manager.prune()
        assert sorted(deleted) == sorted(names[:2])
        assert [s.name for s in await manager.snapshots()] == [names[3], names[2]]
        assert await manager.prune() == []
    finally:
        await env.client.close()


async def test_an_alias_that_points_nowhere_is_not_snapshotted(env: Env) -> None:
    await _register(env)
    await env.client.indices.delete_alias(index=env.index, name=env.settings.search.index_alias)
    with pytest.raises(NonRetryableError):
        await env.manager().create()


# --- queued jobs ----------------------------------------------------------------------------


async def test_a_requested_job_is_found_by_the_job_process_and_runs_once(env: Env) -> None:
    store = ElasticsearchJobStore(
        env.client,
        f"jobs_{uuid.uuid4().hex[:8]}",
        env.settings.elasticsearch,
        RetryPolicy(attempts=1),
    )
    await store.ensure_index()
    first = await store.request_job("backfill-wave1", "backfill", 1)
    assert first.status is JobStatus.REQUESTED
    again = await store.request_job("backfill-wave1", "backfill", 1)
    assert again.status is JobStatus.REQUESTED
    await env.client.indices.refresh(index=store._index)
    assert [j.job_id for j in await store.list_requested()] == ["backfill-wave1"]

    await store.start("backfill-wave1", "backfill", 1)  # the job process takes it
    await env.client.indices.refresh(index=store._index)
    assert await store.list_requested() == []
    running = await store.request_job("backfill-wave1", "backfill", 1)
    assert running.status is JobStatus.RUNNING  # asking again does not queue a second run

    await store.save_progress(
        "backfill-wave1", cursor="c", scanned=7, published=6, skipped_up_to_date=1
    )
    await store.finish("backfill-wave1", JobStatus.COMPLETED)
    queued = await store.request_job("backfill-wave1", "backfill", 1, restart=True)
    assert (queued.status, queued.scanned, queued.cursor) == (JobStatus.REQUESTED, 0, None)
    stored = await store.get("backfill-wave1")
    assert stored is not None
    assert (stored.status, stored.scanned, stored.cursor) == (JobStatus.REQUESTED, 0, None)
