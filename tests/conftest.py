"""Shared fixtures. Every test starts with a clean APP_* environment and the test config."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.settings import Settings
from app.main import create_app

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove stray APP_* variables and point settings at config/test.yaml."""
    import os

    for name in [n for n in os.environ if n.startswith("APP_")]:
        monkeypatch.delenv(name)
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("APP_CONFIG_DIR", str(CONFIG_DIR))


@pytest.fixture
def settings() -> Settings:
    """Settings loaded from config/base.yaml and config/test.yaml."""
    return Settings()


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    """Test client with startup and shutdown run. Unhandled errors become 500 responses."""
    with TestClient(create_app(settings), raise_server_exceptions=False) as test_client:
        yield test_client
