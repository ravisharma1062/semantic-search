"""Real Kafka (and later Elasticsearch) in Docker through Testcontainers."""

import socket
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from testcontainers.core.container import DockerContainer

from app.core.settings import KafkaSettings

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
