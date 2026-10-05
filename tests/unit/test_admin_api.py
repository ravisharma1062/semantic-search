"""The admin API: manual indexing, deleting, re-index jobs, admin tokens and audit lines."""

import json
from collections.abc import Iterator
from typing import Any

import pytest
import structlog
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.core.settings import ApiSettings, Settings, WaveSpec
from app.jobs.admin import AdminService
from app.jobs.job_store import JobStatus
from app.main import create_app
from app.services import Services
from tests.fakes import FakeLLMClient
from tests.fakes.jobs import FakeJobStore
from tests.fakes.kafka import FakeBroker, FakeProducer
from tests.unit.test_answer_service import make_service

ADMIN = {"Authorization": "Bearer admin-token"}
SERVICE = {"Authorization": "Bearer token-java", "X-User-Id": "alice"}


class Rig:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings.model_copy(
            update={
                "api": ApiSettings(
                    service_tokens={"java-search": SecretStr("token-java")},
                    admin_tokens={"ops": SecretStr("admin-token")},
                    identity_services=["java-search"],
                ),
                "backfill": settings.backfill.model_copy(
                    update={"waves": [WaveSpec(number=1, name="first")]}
                ),
            }
        )
        self.broker = FakeBroker()
        self.producer = FakeProducer(self.broker)
        self.jobs = FakeJobStore()
        service, _ = make_service(self.settings, FakeLLMClient(["x"]))
        self.admin = AdminService(producer=self.producer, jobs=self.jobs, settings=self.settings)
        self.app = create_app(
            self.settings, Services(search=service._search, answer=service, admin=self.admin)
        )


@pytest.fixture
def rig(settings: Settings) -> Iterator[tuple[Rig, TestClient]]:
    r = Rig(settings)
    with TestClient(r.app, raise_server_exceptions=False) as client:
        yield r, client


def _events(rig: Rig) -> list[dict[str, Any]]:
    topic = rig.settings.kafka.live_topic
    return [json.loads(m.value or b"{}") for m in rig.producer.sent if m.topic == topic]


# --- indexing and deleting ------------------------------------------------------------------


def test_a_document_is_queued_as_an_event_with_the_id_only(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    response = client.post("/v1/index/documents", json={"item_id": "ITEM-1"}, headers=ADMIN)
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued" and body["item_id"] == "ITEM-1"
    [event] = _events(r)
    assert event["event_id"] == body["event_id"]
    assert (event["event_type"], event["item_id"], event["source"]) == (
        "UPSERT",
        "ITEM-1",
        "admin-api",
    )
    assert set(event) <= {
        "schema_version",
        "event_id",
        "event_type",
        "item_id",
        "occurred_at",
        "source",
        "priority",
    }
    assert r.producer.sent[0].key == b"ITEM-1"


def test_a_delete_is_queued_as_a_delete_event(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    response = client.delete("/v1/documents/DOC-9", headers=ADMIN)
    assert response.status_code == 202
    [event] = _events(r)
    assert (event["event_type"], event["item_id"]) == ("DELETE", "DOC-9")


@pytest.mark.parametrize("bad", ["a b", "x" * 129, "../etc", "id;drop", "a/b"])
def test_odd_ids_are_refused(rig: tuple[Rig, TestClient], bad: str) -> None:
    r, client = rig
    assert (
        client.post("/v1/index/documents", json={"item_id": bad}, headers=ADMIN).status_code == 400
    )
    assert client.delete(f"/v1/documents/{bad}", headers=ADMIN).status_code in (400, 404)
    assert r.producer.sent == []


def test_a_broker_problem_is_a_clear_error_and_audited(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    r.producer.fail_times = 1
    with structlog.testing.capture_logs() as logs:
        response = client.post("/v1/index/documents", json={"item_id": "ITEM-1"}, headers=ADMIN)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "UPSTREAM_UNAVAILABLE"
    [line] = [e for e in logs if e["event"] == "admin_action"]
    assert (line["action"], line["outcome"]) == ("index_document", "failed")


# --- re-index jobs --------------------------------------------------------------------------


def test_a_reindex_is_requested_and_can_be_followed(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    response = client.post("/v1/admin/reindex", json={"wave": 1}, headers=ADMIN)
    assert response.status_code == 202
    job = response.json()["job"]
    assert (job["job_id"], job["status"], job["wave"]) == ("backfill-wave1", "REQUESTED", 1)
    assert "cursor" not in job
    status = client.get("/v1/admin/reindex/backfill-wave1", headers=ADMIN)
    assert status.status_code == 200 and status.json()["job"]["status"] == "REQUESTED"


def test_asking_twice_returns_the_same_job(rig: tuple[Rig, TestClient]) -> None:
    r, client = rig
    first = client.post("/v1/admin/reindex", json={"wave": 1}, headers=ADMIN).json()["job"]
    second = client.post("/v1/admin/reindex", json={"wave": 1}, headers=ADMIN).json()["job"]
    assert first["job_id"] == second["job_id"] and len(r.jobs.records) == 1


async def test_a_finished_job_is_queued_again_and_restart_clears_the_cursor(
    settings: Settings,
) -> None:
    r = Rig(settings)
    await r.jobs.start("backfill-wave1", "backfill", 1)
    await r.jobs.save_progress(
        "backfill-wave1", cursor="c", scanned=5, published=4, skipped_up_to_date=1
    )
    await r.jobs.finish("backfill-wave1", JobStatus.COMPLETED)
    again = await r.admin.request_reindex(1, restart=False)
    assert (again.status, again.scanned, again.cursor) == (JobStatus.REQUESTED, 5, "c")
    await r.jobs.finish("backfill-wave1", JobStatus.PAUSED)
    fresh = await r.admin.request_reindex(1, restart=True)
    assert (fresh.status, fresh.scanned, fresh.cursor) == (JobStatus.REQUESTED, 0, None)


def test_unknown_waves_and_jobs(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    assert client.post("/v1/admin/reindex", json={"wave": 9}, headers=ADMIN).status_code == 400
    assert client.post("/v1/admin/reindex", json={"wave": 0}, headers=ADMIN).status_code == 400
    missing = client.get("/v1/admin/reindex/nope", headers=ADMIN)
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "NOT_FOUND"


def test_the_requested_jobs_wait_for_a_job_process(rig: tuple[Rig, TestClient]) -> None:
    import asyncio

    r, client = rig
    client.post("/v1/admin/reindex", json={"wave": 1}, headers=ADMIN)
    queued = asyncio.run(r.jobs.list_requested())
    assert [j.job_id for j in queued] == ["backfill-wave1"]
    asyncio.run(r.jobs.start("backfill-wave1", "backfill", 1))  # a job process takes it
    assert asyncio.run(r.jobs.list_requested()) == []


# --- who may call ---------------------------------------------------------------------------

ADMIN_CALLS = [
    ("post", "/v1/index/documents", {"item_id": "ITEM-1"}),
    ("delete", "/v1/documents/ITEM-1", None),
    ("post", "/v1/admin/reindex", {"wave": 1}),
    ("get", "/v1/admin/reindex/backfill-wave1", None),
]


@pytest.mark.parametrize("method,path,body", ADMIN_CALLS)
def test_a_service_token_is_not_an_admin_token(
    rig: tuple[Rig, TestClient], method: str, path: str, body: Any
) -> None:
    r, client = rig
    response = client.request(method, path, json=body, headers=SERVICE)
    assert response.status_code in (401, 403)
    assert r.producer.sent == [] and r.jobs.records == {}


@pytest.mark.parametrize("method,path,body", ADMIN_CALLS)
def test_no_token_and_a_wrong_token_are_refused(
    rig: tuple[Rig, TestClient], method: str, path: str, body: Any
) -> None:
    _, client = rig
    assert client.request(method, path, json=body).status_code == 401
    wrong = {"Authorization": "Bearer nope"}
    assert client.request(method, path, json=body, headers=wrong).status_code in (401, 403)


def test_an_admin_token_does_not_open_the_user_api(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    response = client.post(
        "/v1/search", json={"query": "late"}, headers={**ADMIN, "X-User-Id": "alice"}
    )
    assert response.status_code in (401, 403)


def test_the_admin_api_is_off_without_a_service(settings: Settings) -> None:
    s = settings.model_copy(
        update={"api": ApiSettings(admin_tokens={"ops": SecretStr("admin-token")})}
    )
    service, _ = make_service(s, FakeLLMClient(["x"]))
    with TestClient(
        create_app(s, Services(search=service._search)), raise_server_exceptions=False
    ) as client:
        response = client.post("/v1/index/documents", json={"item_id": "I"}, headers=ADMIN)
    assert response.status_code == 503


# --- audit ----------------------------------------------------------------------------------


def test_every_admin_action_leaves_an_audit_line_with_ids_only(
    rig: tuple[Rig, TestClient],
) -> None:
    _, client = rig
    with structlog.testing.capture_logs() as logs:
        client.post("/v1/index/documents", json={"item_id": "ITEM-1"}, headers=ADMIN)
        client.delete("/v1/documents/ITEM-2", headers=ADMIN)
        client.post("/v1/admin/reindex", json={"wave": 1, "restart": True}, headers=ADMIN)
        client.get("/v1/admin/reindex/backfill-wave1", headers=ADMIN)
        client.get("/v1/admin/reindex/missing", headers=ADMIN)
    lines = [e for e in logs if e["event"] == "admin_action"]
    assert [(e["action"], e["outcome"]) for e in lines] == [
        ("index_document", "accepted"),
        ("delete_document", "accepted"),
        ("request_reindex", "accepted"),
        ("reindex_status", "accepted"),
        ("reindex_status", "rejected"),
    ]
    for line in lines:
        assert line["audit"] is True and line["actor"] == "ops"
    assert lines[0]["item_id"] == "ITEM-1" and lines[1]["item_id"] == "ITEM-2"
    assert lines[2]["job_id"] == "backfill-wave1" and lines[2]["restart"] is True
    assert "admin-token" not in str(lines)


def test_a_refused_admin_call_is_audited_without_the_token(rig: tuple[Rig, TestClient]) -> None:
    _, client = rig
    with structlog.testing.capture_logs() as logs:
        client.post(
            "/v1/index/documents",
            json={"item_id": "ITEM-1"},
            headers={"Authorization": "Bearer guess-secret"},
        )
    [line] = [e for e in logs if e["event"] == "admin_action"]
    assert (line["actor"], line["outcome"]) == ("unknown", "rejected")
    assert "guess-secret" not in str(logs)


def test_the_admin_endpoints_are_in_the_contract_with_the_admin_scheme(
    rig: tuple[Rig, TestClient],
) -> None:
    _, client = rig
    paths = client.get("/openapi.json").json()["paths"]
    for path in ("/v1/index/documents", "/v1/documents/{doc_id}", "/v1/admin/reindex"):
        for operation in paths[path].values():
            assert operation["security"] == [{"adminToken": []}]
    assert "/v1/admin/reindex/{job_id}" in paths
