from pathlib import Path

import pytest
from pydantic import ValidationError

from app.core.settings import AppMode, Settings, get_settings

_BASE = """
search: {index_alias: alias_from_base, source_index: source_from_base}
elasticsearch: {hosts: ["http://es.test:9200"], state_index: state_from_base}
kafka:
  bootstrap_servers: base.test:9092
  live_topic: live
  backfill_topic: backfill
  retry_topic: retry
  dlq_topic: dlq
  consumer_group: group
  backfill_consumer_group: backfill-group
redis: {url: "redis://base.test:6379/0"}
embedding: {model: m, endpoint: "http://e.test"}
reranker: {model: r, endpoint: "http://r.test"}
llm: {model: l, endpoint: "http://l.test"}
chunking: {version: v1, tokenizer: whitespace}
"""


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "base.yaml").write_text(_BASE)
    monkeypatch.setenv("APP_CONFIG_DIR", str(tmp_path))
    return tmp_path


def test_yaml_values_are_loaded(config_dir: Path) -> None:
    settings = Settings()
    assert settings.search.index_alias == "alias_from_base"
    assert settings.kafka.bootstrap_servers == "base.test:9092"
    assert settings.mode is AppMode.API
    assert settings.chunking.target_tokens == 400


def test_env_yaml_overrides_base_yaml(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (config_dir / "test.yaml").write_text("search: {source_index: source_from_env_file}")
    settings = Settings()
    assert settings.search.source_index == "source_from_env_file"
    assert settings.search.index_alias == "alias_from_base"  # untouched keys survive the merge


def test_environment_variable_overrides_yaml(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (config_dir / "test.yaml").write_text("search: {source_index: source_from_env_file}")
    monkeypatch.setenv("APP_SEARCH__SOURCE_INDEX", "source_from_env_var")
    monkeypatch.setenv("APP_ELASTICSEARCH__HOSTS", '["http://a.test:9200","http://b.test:9200"]')
    monkeypatch.setenv("APP_MODE", "worker")
    settings = Settings()
    assert settings.search.source_index == "source_from_env_var"
    assert settings.search.index_alias == "alias_from_base"
    assert settings.elasticsearch.hosts == ["http://a.test:9200", "http://b.test:9200"]
    assert settings.mode is AppMode.WORKER


def test_missing_required_value_fails_and_names_the_field(
    config_dir: Path,
) -> None:
    (config_dir / "base.yaml").write_text("search: {index_alias: only_this}")
    with pytest.raises(ValidationError) as error:
        Settings()
    assert "kafka" in str(error.value)


def test_invalid_value_is_rejected(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_SERVICE__PORT", "70000")
    with pytest.raises(ValidationError):
        Settings()


def test_unknown_mode_is_rejected(config_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_MODE", "sidecar")
    with pytest.raises(ValidationError):
        Settings()


def test_repository_test_config_is_complete(settings: Settings) -> None:
    assert settings.env == "test"
    assert settings.search.index_alias == "doc_chunks_current"
    assert settings.embedding.model == "bge-m3"
    assert settings.kafka.dlq_topic == "doc-index-dlq"


def test_get_settings_is_cached() -> None:
    get_settings.cache_clear()
    assert get_settings() is get_settings()
    get_settings.cache_clear()
