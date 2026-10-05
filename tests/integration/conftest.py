"""Real Kafka (and later Elasticsearch) in Docker through Testcontainers."""

import socket
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

import httpx
import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from elasticsearch import AsyncElasticsearch
from testcontainers.core.container import DockerContainer

from app.core.settings import ElasticsearchSettings, KafkaSettings
from app.store.client import create_es_client

KAFKA_IMAGE = "apache/kafka:3.8.0"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    """A single-node Kafka. The advertised address is the mapped host port."""
    port = _free_port()
    container = (
        DockerContainer(KAFKA_IMAGE)
        .with_bind_ports(9092, port)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
        )
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
    )
    with container:
        bootstrap = f"127.0.0.1:{port}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        deadline = time.monotonic() + 60
        while True:
            try:
                admin.list_topics(timeout=3)
                break
            except Exception:  # not ready yet
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        yield bootstrap


@dataclass(frozen=True)
class Topics:
    """Unique topic and group names for one test."""

    live: str
    retry: str
    dlq: str
    group: str


@pytest.fixture
def topics(kafka_bootstrap: str) -> Topics:
    """Four topics with 4 partitions each, named per test so tests do not influence each other."""
    suffix = uuid.uuid4().hex[:8]
    names = Topics(f"live-{suffix}", f"retry-{suffix}", f"dlq-{suffix}", f"group-{suffix}")
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    futures = admin.create_topics(
        [
            NewTopic(t, num_partitions=4, replication_factor=1)
            for t in (names.live, names.retry, names.dlq)
        ]
    )
    for future in futures.values():
        future.result(timeout=30)
    return names


@pytest.fixture
def kafka_settings(kafka_bootstrap: str, topics: Topics) -> KafkaSettings:
    """Kafka settings that point at the test topics."""
    return KafkaSettings(
        bootstrap_servers=kafka_bootstrap,
        live_topic=topics.live,
        backfill_topic=f"backfill-{topics.group}",
        retry_topic=topics.retry,
        dlq_topic=topics.dlq,
        consumer_group=topics.group,
        backfill_consumer_group=f"{topics.group}-backfill",
        request_timeout_s=10,
        producer_timeout_s=30,
    )


ES_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.15.3"


@pytest.fixture(scope="session")
def es_url() -> Iterator[str]:
    """A single-node Elasticsearch 8 without security."""
    port = _free_port()
    container = (
        DockerContainer(ES_IMAGE)
        .with_bind_ports(9200, port)
        .with_env("discovery.type", "single-node")
        .with_env("xpack.security.enabled", "false")
        .with_env("xpack.ml.enabled", "false")
        .with_env("path.repo", "/tmp/es-snapshots")  # noqa: S108 (inside the container)
        .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
    )
    with container:
        url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 120
        while True:
            try:
                response = httpx.get(f"{url}/_cluster/health", params={"wait_for_status": "yellow"})
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError("Elasticsearch did not become ready")
            time.sleep(2)
        yield url


@pytest.fixture
async def es_client(es_url: str) -> AsyncIterator[AsyncElasticsearch]:
    """An async client for the test container, closed after the test."""
    client = create_es_client(ElasticsearchSettings(hosts=[es_url], state_index="state_test"))
    yield client
    await client.close()


REDIS_IMAGE = "redis:7-alpine"


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A single Redis."""
    port = _free_port()
    container = DockerContainer(REDIS_IMAGE).with_bind_ports(6379, port)
    with container:
        deadline = time.monotonic() + 30
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1) as conn:
                    conn.sendall(b"PING" + bytes([13, 10]))  # the port opens before Redis is ready
                    if conn.recv(16).startswith(b"+PONG"):
                        break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError("Redis did not become ready")
            time.sleep(0.5)
        yield f"redis://127.0.0.1:{port}/0"


@dataclass(frozen=True)
class ModelServer:
    """The fake model server, running on a real port."""

    url: str
    dims: int

    def stats(self) -> dict[str, int]:
        """Counters of served embedding calls and texts."""
        data: dict[str, int] = httpx.get(f"{self.url}/stats").json()
        return data


@pytest.fixture(scope="session")
def model_server() -> Iterator[ModelServer]:
    """deploy/local/fake-model-server on a free port (8-dimensional vectors)."""
    import importlib.util
    import os
    import threading
    from pathlib import Path

    import uvicorn

    path = Path(__file__).resolve().parents[2] / "deploy/local/fake-model-server/server.py"
    os.environ["FAKE_DIMS"] = "8"
    spec = importlib.util.spec_from_file_location("fake_model_server_e2e", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(module.app, host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("fake model server did not start")
        time.sleep(0.05)
    yield ModelServer(f"http://127.0.0.1:{port}", 8)
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(scope="session")
def es_trial_url() -> Iterator[str]:
    """Elasticsearch 8 with a trial license, so the rrf retriever is available."""
    port = _free_port()
    container = (
        DockerContainer(ES_IMAGE)
        .with_bind_ports(9200, port)
        .with_env("discovery.type", "single-node")
        .with_env("xpack.security.enabled", "false")
        .with_env("xpack.ml.enabled", "false")
        .with_env("xpack.license.self_generated.type", "trial")
        .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
    )
    with container:
        url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 120
        while True:
            try:
                response = httpx.get(f"{url}/_cluster/health", params={"wait_for_status": "yellow"})
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise RuntimeError("Elasticsearch did not become ready")
            time.sleep(2)
        yield url
