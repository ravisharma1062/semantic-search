"""Helpers for integration tests that read Kafka topics."""

import time
import uuid
from typing import cast

from confluent_kafka import Consumer


def read_all(
    bootstrap: str, topic: str, expected: int, wait_s: float = 20
) -> list[tuple[bytes, dict[str, bytes]]]:
    """Read a topic from the start with a throw-away consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"reader-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    found: list[tuple[bytes, dict[str, bytes]]] = []
    deadline = time.monotonic() + wait_s
    while len(found) < expected and time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        raw_headers = cast(list[tuple[str, bytes | str | None]], message.headers() or [])
        headers = {k: v for k, v in raw_headers if isinstance(v, bytes)}
        found.append((message.value() or b"", headers))
    consumer.close()
    return found
