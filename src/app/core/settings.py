"""Application settings.

Values come from, in order of priority: init arguments, environment variables,
``config/<APP_ENV>.yaml`` and ``config/base.yaml``. Environment variables use the
``APP_`` prefix and ``__`` for nesting, for example ``APP_KAFKA__BOOTSTRAP_SERVERS``.
Lists are written as JSON.
"""

import os
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from app.core.retry import RetryPolicy

_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


class AppMode(StrEnum):
    """Run mode of the single Docker image."""

    API = "api"
    WORKER = "worker"
    BATCH = "batch"


class ServiceSettings(BaseModel):
    """HTTP server and logging."""

    host: str = "127.0.0.1"
    port: int = Field(8080, ge=1, le=65535)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_json: bool = True


class SearchSettings(BaseModel):
    """Search and index names."""

    index_alias: str
    source_index: str
    top_k_default: int = Field(10, ge=1)
    candidates: int = Field(100, ge=1)
    rerank_top_n: int = Field(50, ge=1)
    rrf_rank_constant: int = Field(60, ge=1)
    timeout_ms: int = Field(3000, ge=1)


class ElasticsearchSettings(BaseModel):
    """Elasticsearch connection, state index and per-interface timeouts."""

    hosts: list[str] = Field(min_length=1)
    state_index: str
    source_read_timeout_s: float = Field(5.0, gt=0)
    bulk_timeout_s: float = Field(30.0, gt=0)
    state_timeout_s: float = Field(2.0, gt=0)
    search_timeout_s: float = Field(1.5, gt=0)


class KafkaSettings(BaseModel):
    """Kafka connection and topic names."""

    bootstrap_servers: str
    live_topic: str
    backfill_topic: str
    retry_topic: str
    dlq_topic: str
    consumer_group: str
    backfill_consumer_group: str
    request_timeout_s: float = Field(10.0, gt=0)
    producer_timeout_s: float = Field(30.0, gt=0)


class ConsumerSettings(BaseModel):
    """Behaviour of the indexing consumer (HLD section 15, Kafka design)."""

    max_in_flight: int = Field(8, ge=1)
    poll_batch_size: int = Field(100, ge=1)
    poll_timeout_s: float = Field(1.0, gt=0)
    commit_interval_s: float = Field(1.0, gt=0)
    quick_retries: int = Field(3, ge=1)
    quick_retry_initial_delay_s: float = Field(0.2, ge=0)
    retry_delay_s: float = Field(60.0, ge=0)
    max_retries: int = Field(5, ge=0)
    shutdown_timeout_s: float = Field(30.0, gt=0)
    rebalance_timeout_s: float = Field(20.0, gt=0)


class RedisSettings(BaseModel):
    """Redis connection."""

    url: str
    timeout_ms: int = Field(50, ge=1)


class EmbeddingSettings(BaseModel):
    """Embedding provider."""

    provider: Literal["inhouse", "openai"] = "inhouse"
    model: str
    endpoint: str
    dims: int = Field(1024, ge=1)
    timeout_s: float = Field(0.5, gt=0)


class RerankerSettings(BaseModel):
    """Reranker provider."""

    provider: Literal["inhouse"] = "inhouse"
    model: str
    endpoint: str
    enabled: bool = True
    timeout_s: float = Field(1.0, gt=0)


class LlmSettings(BaseModel):
    """LLM provider."""

    provider: Literal["inhouse", "openai"] = "inhouse"
    model: str
    endpoint: str
    max_output_tokens: int = Field(800, ge=1)
    temperature: float = Field(0.1, ge=0, le=2)
    timeout_s: float = Field(30.0, gt=0)


class ChunkingSettings(BaseModel):
    """Chunker parameters. Changing them means a re-index."""

    version: str
    target_tokens: int = Field(400, ge=1)
    max_tokens: int = Field(512, ge=1)
    overlap_tokens: int = Field(60, ge=0)


class FeatureFlags(BaseModel):
    """Feature switches."""

    semantic_search: bool = True
    rag: bool = False


class Settings(BaseSettings):
    """Root settings object."""

    model_config = SettingsConfigDict(
        env_prefix="APP_",
        env_nested_delimiter="__",
        populate_by_name=True,
        extra="ignore",
    )

    env: str = Field("dev", validation_alias="APP_ENV", pattern=r"^[a-z][a-z0-9_-]*$")
    mode: AppMode = Field(AppMode.API, validation_alias="APP_MODE")
    service: ServiceSettings = ServiceSettings()
    search: SearchSettings
    elasticsearch: ElasticsearchSettings
    kafka: KafkaSettings
    consumer: ConsumerSettings = ConsumerSettings()
    retry: RetryPolicy = RetryPolicy()
    redis: RedisSettings
    embedding: EmbeddingSettings
    reranker: RerankerSettings
    llm: LlmSettings
    chunking: ChunkingSettings
    feature_flags: FeatureFlags = FeatureFlags()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order the sources. The environment yaml overrides the base yaml."""
        config_dir = Path(os.environ.get("APP_CONFIG_DIR", _DEFAULT_CONFIG_DIR))
        env_name = os.environ.get("APP_ENV", "dev")
        # One source per file: sources are merged key by key, a list of files is not.
        env_yaml = YamlConfigSettingsSource(settings_cls, yaml_file=config_dir / f"{env_name}.yaml")
        base_yaml = YamlConfigSettingsSource(settings_cls, yaml_file=config_dir / "base.yaml")
        return (init_settings, env_settings, env_yaml, base_yaml)


@lru_cache
def get_settings() -> Settings:
    """Load the settings once per process."""
    return Settings()
