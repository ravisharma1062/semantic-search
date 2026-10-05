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

from pydantic import BaseModel, Field, SecretStr, model_validator
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
    api_key: SecretStr | None = None
    ca_certs: str | None = None
    verify_certs: bool = True
    state_index: str
    source_read_timeout_s: float = Field(5.0, gt=0)
    bulk_timeout_s: float = Field(30.0, gt=0)
    state_timeout_s: float = Field(2.0, gt=0)
    search_timeout_s: float = Field(1.5, gt=0)


class SourceSettings(BaseModel):
    """How to read the existing document index. Field names must be confirmed with the Java team.

    ``ITEM_ID`` is assumed to be the Elasticsearch ``_id``. Pages are an array of objects. When
    a document has no pages, the document-level text field is used.
    """

    max_get_many: int = Field(100, ge=1)
    max_pages: int = Field(5000, ge=1)
    max_chars: int = Field(40_000_000, ge=1)
    pages_field: str = "pages"
    page_no_field: str = "page_no"
    page_text_field: str = "text"
    text_field: str = "ocr_text"
    doc_type_field: str = "doc_type"
    tags_field: str = "tags"
    created_at_field: str = "created_at"
    owner_field: str = "owner"
    acl_users_field: str = "acl_users"
    acl_groups_field: str = "acl_groups"
    version_field: str = "version"
    language_field: str = "language"


class NormalizerSettings(BaseModel):
    """OCR text clean-up."""

    edge_lines: int = Field(3, ge=1)
    repeat_ratio: float = Field(0.4, gt=0, le=1)
    repeat_min_pages: int = Field(4, ge=2)
    short_line_chars: int = Field(50, ge=1)


class StoreSettings(BaseModel):
    """The chunk index (HLD section 5) and how it is written."""

    chunk_index_prefix: str = "doc_chunks"
    # Where the worker writes. Empty means the read alias. During a re-index it is the new version.
    write_index: str | None = None
    shards: int = Field(3, ge=1)
    replicas: int = Field(1, ge=0)
    refresh_interval: str = "30s"
    vector_index_type: Literal["hnsw", "int8_hnsw", "bbq_hnsw"] = "int8_hnsw"
    bulk_batch_size: int = Field(200, ge=1)
    maintenance_timeout_s: float = Field(120.0, gt=0)  # delete or update by query
    min_index_age_days: int = Field(14, ge=0)  # an old version is kept at least this long


class IngestionSettings(BaseModel):
    """The indexing worker."""

    window_size: int = Field(256, ge=1)  # chunks embedded and written together
    backfill_max_in_flight: int = Field(2, ge=1)
    heartbeat_file: str = "/tmp/worker-alive"  # noqa: S108 (the pod has its own /tmp)
    heartbeat_interval_s: float = Field(10.0, gt=0)


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
    """Redis connection. Redis is only a cache: an outage must never fail a request."""

    url: str
    timeout_ms: int = Field(50, ge=1)
    breaker_failures: int = Field(3, ge=1)
    breaker_cooldown_s: float = Field(10.0, gt=0)


class EmbeddingSettings(BaseModel):
    """Embedding provider. The in-house TEI-style server is the default."""

    provider: Literal["inhouse", "openai"] = "inhouse"
    model: str
    model_version: str = "1"
    endpoint: str
    dims: int = Field(1024, ge=1)
    timeout_s: float = Field(0.5, gt=0)  # one query embedding
    document_timeout_s: float = Field(10.0, gt=0)  # one batch of chunks
    batch_size: int = Field(32, ge=1)
    max_concurrency: int = Field(4, ge=1)
    truncate: bool = False
    cache_ttl_s: int = Field(3600, ge=1)
    # OpenAI is optional and off by default. It needs provider "openai" AND allow_external,
    # and may only be used for data classes that security has approved (HLD section 9).
    allow_external: bool = False
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: SecretStr | None = None
    proxy: str | None = None


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
    min_tokens: int = Field(50, ge=0)
    max_chunks_per_document: int = Field(20_000, ge=1)
    # "hf" counts with the embedding model's tokenizer file (no download at runtime).
    # "whitespace" counts words and is only for local development and tests.
    tokenizer: Literal["hf", "whitespace"] = "hf"
    tokenizer_file: str | None = None

    @model_validator(mode="after")
    def _check(self) -> "ChunkingSettings":
        if not self.overlap_tokens < self.target_tokens <= self.max_tokens:
            raise ValueError("need overlap_tokens < target_tokens <= max_tokens")
        if self.min_tokens > self.target_tokens:
            raise ValueError("min_tokens must not be larger than target_tokens")
        if self.tokenizer == "hf" and not self.tokenizer_file:
            raise ValueError("tokenizer 'hf' needs tokenizer_file")
        return self


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
    source: SourceSettings = SourceSettings()
    store: StoreSettings = StoreSettings()
    ingestion: IngestionSettings = IngestionSettings()
    normalizer: NormalizerSettings = NormalizerSettings()
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
