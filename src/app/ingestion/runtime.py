"""Builds and runs the indexing worker process (``APP_MODE=worker``).

Two consumer loops run side by side: live events (and the retry topic) with one consumer group,
and backfill with its own group and fewer parallel documents, so a backfill never starves live
updates. A heartbeat file shows that the event loop is alive (the Helm liveness probe reads it).
"""

import asyncio
import contextlib
import time
from pathlib import Path

import structlog
from prometheus_client import start_http_server

from app.core.settings import Settings
from app.embeddings.factory import create_embedder, create_http_client
from app.ingestion.chunker import Chunker
from app.ingestion.consumer import ConsumerLoop
from app.ingestion.es_source_reader import ElasticsearchSourceReader
from app.ingestion.indexer import ElasticsearchIndexer
from app.ingestion.kafka_client import ConfluentConsumer, ConfluentProducer
from app.ingestion.state_store import ElasticsearchStateStore
from app.ingestion.tokens import create_token_counter
from app.ingestion.worker import IndexingWorker
from app.observability.metrics import get_metrics
from app.observability.tracing import build_provider, configure_tracing
from app.store.client import create_es_client

_log = structlog.get_logger(__name__)


def _touch(path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(str(time.time()))


async def heartbeat(path: str, interval_s: float, stop: asyncio.Event) -> None:
    """Touch the file until ``stop``. If the event loop is blocked, the file gets old."""
    while not stop.is_set():
        await asyncio.to_thread(_touch, path)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), interval_s)


async def run_worker(settings: Settings, stop: asyncio.Event) -> None:
    """Run the live and backfill loops until ``stop`` is set."""
    es_client = create_es_client(settings.elasticsearch)
    http_client = create_http_client(settings.embedding)
    obs = settings.observability
    if obs.metrics_enabled:
        start_http_server(obs.worker_metrics_port, registry=get_metrics().registry)
    configure_tracing(build_provider(obs))
    try:
        worker = IndexingWorker(
            source=ElasticsearchSourceReader(
                es_client,
                settings.search.source_index,
                settings.source,
                settings.elasticsearch,
                settings.retry,
            ),
            indexer=ElasticsearchIndexer(
                es_client,
                settings.store.write_index or settings.search.index_alias,
                settings.store,
                settings.elasticsearch,
                settings.retry,
            ),
            states=ElasticsearchStateStore(
                es_client,
                settings.elasticsearch.state_index,
                settings.elasticsearch,
                settings.retry,
            ),
            embedder=create_embedder(settings.embedding, http_client, settings.retry),
            chunker=Chunker(settings.chunking, create_token_counter(settings.chunking)),
            chunking=settings.chunking,
            normalizer=settings.normalizer,
            ingestion=settings.ingestion,
            store=settings.store,
        )
        kafka = settings.kafka
        rebalance_s = settings.consumer.rebalance_timeout_s
        live = ConsumerLoop(
            consumer=ConfluentConsumer(kafka, kafka.consumer_group, rebalance_s),
            producer=ConfluentProducer(kafka),
            handler=worker.handle,
            topics=[kafka.live_topic, kafka.retry_topic],
            retry_topic=kafka.retry_topic,
            dlq_topic=kafka.dlq_topic,
            settings=settings.consumer,
        )
        # The backfill loop can be switched off (runbook 2): events stay in Kafka, nothing is lost,
        # and live updates are not affected.
        backfill: ConsumerLoop | None = None
        if settings.ingestion.backfill_consumer_enabled:
            backfill = ConsumerLoop(
                consumer=ConfluentConsumer(kafka, kafka.backfill_consumer_group, rebalance_s),
                producer=ConfluentProducer(kafka),
                handler=worker.handle,
                topics=[kafka.backfill_topic],
                retry_topic=kafka.retry_topic,
                dlq_topic=kafka.dlq_topic,
                settings=settings.consumer.model_copy(
                    update={"max_in_flight": settings.ingestion.backfill_max_in_flight}
                ),
            )
        else:
            _log.warning("backfill_consumer_disabled")

        async def stop_loops() -> None:
            await stop.wait()
            live.stop()
            if backfill is not None:
                backfill.stop()

        _log.info("worker_started")
        async with asyncio.TaskGroup() as group:
            group.create_task(live.run())
            if backfill is not None:
                group.create_task(backfill.run())
            group.create_task(stop_loops())
            beat = settings.ingestion
            group.create_task(heartbeat(beat.heartbeat_file, beat.heartbeat_interval_s, stop))
    finally:
        await http_client.aclose()
        await es_client.close()
        _log.info("worker_stopped")
