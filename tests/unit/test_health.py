from fastapi.testclient import TestClient

from app.core.settings import Settings
from app.main import create_app


def test_live_returns_ok(client: TestClient) -> None:
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_ready_after_startup(client: TestClient) -> None:
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_ready_is_503_before_startup(settings: Settings) -> None:
    # No `with`: the lifespan has not run, so the app is not ready.
    client = TestClient(create_app(settings))
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}


def test_ready_is_503_after_shutdown(settings: Settings) -> None:
    app = create_app(settings)
    with TestClient(app):
        assert app.state.ready is True
    assert app.state.ready is False
