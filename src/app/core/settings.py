"""Application settings.

Values come from, in order of priority: init arguments, environment variables,
``config/<APP_ENV>.yaml`` and ``config/base.yaml``. Environment variables use the
``APP_`` prefix and ``__`` for nesting, for example ``APP_KAFKA__BOOTSTRAP_SERVERS``.
Lists are written as JSON.
"""

import os
from datetime import date
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
    # "python": two searches merged here (works on every license). "retriever": one request with
    # the Elasticsearch rrf retriever (needs a license tier that has it, falls back to "python").
    rrf_mode: Literal["python", "retriever"] = "python"
    num_candidates_factor: int = Field(3, ge=1)
    max_top_k: int = Field(50, ge=1)
    snippet_chars: int = Field(300, ge=50)


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


class SnapshotSettings(BaseModel):
    """Snapshots of the chunk and state indices (HLD section 17, "Backup and recovery"). The
    repository itself (object storage) is registered by the platform team."""

    repository: str = "semantic-search-snapshots"
    keep_last: int = Field(14, ge=1)  # daily snapshots: two weeks
    restore_prefix: str = "restored_"
    wait_timeout_s: float = Field(7200.0, gt=0)  # snapshots of big indices take a long time


class IngestionSettings(BaseModel):
    """The indexing worker."""

    window_size: int = Field(256, ge=1)  # chunks embedded and written together
    backfill_max_in_flight: int = Field(2, ge=1)
    # False: this worker does not read the backfill topic (runbook 2). Live updates continue.
    backfill_consumer_enabled: bool = True
    heartbeat_file: str = "/tmp/worker-alive"  # noqa: S108 (the pod has its own /tmp)
    heartbeat_interval_s: float = Field(10.0, gt=0)


class WaveSpec(BaseModel):
    """One backfill wave (HLD section 22): which documents. Waves may overlap: a document that
    is already indexed is skipped cheaply."""

    number: int = Field(ge=1)
    name: str = ""
    doc_types: list[str] = []
    created_from: date | None = None
    created_to: date | None = None


class BackfillSettings(BaseModel):
    """The backfill producer and the reconciliation job (task T1.7)."""

    job_index: str = "doc_index_jobs"
    # A field of the source index with a unique value per document, normally ITEM_ID as a keyword.
    # The scan sorts by it, so it can continue after a pause of any length. To confirm with the
    # Java team: if ITEM_ID is only the document _id, a keyword copy is needed.
    scan_sort_field: str = "item_id"
    scan_size: int = Field(500, ge=1)
    rate_per_second: float = Field(200.0, gt=0)
    skip_up_to_date: bool = True
    waves: list[WaveSpec] = []
    reconcile_max_items: int | None = Field(None, ge=1)


class ApiSettings(BaseModel):
    """Who may call the API, and how much (HLD sections 8 and 9)."""

    # Service name to token. Tokens come from the secret store (APP_API__SERVICE_TOKENS as JSON).
    service_tokens: dict[str, SecretStr] = {}
    admin_tokens: dict[str, SecretStr] = {}
    # Only these services may send the end-user identity headers (X-User-Id, X-User-Groups).
    identity_services: list[str] = []
    auth_disabled: bool = False  # local development and tests only. Refused in prod.
    max_query_chars: int = Field(1000, ge=1)
    max_groups: int = Field(200, ge=1)
    max_filter_values: int = Field(50, ge=1)
    rate_limit_user_per_min: int = Field(120, ge=0)  # 0 = off
    rate_limit_service_per_min: int = Field(6000, ge=0)
    # Calls on the request path must not wait for retries: the budget is 3 seconds in total.
    request_retry: RetryPolicy = RetryPolicy(attempts=1)


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
    """Reranker provider (HLD section 6). A cross-encoder re-orders the top candidates."""

    provider: Literal["inhouse"] = "inhouse"
    model: str
    endpoint: str
    enabled: bool = True
    timeout_s: float = Field(1.0, gt=0)
    truncate: bool = False
    max_concurrency: int = Field(4, ge=1)
    breaker_failures: int = Field(3, ge=1)
    breaker_cooldown_s: float = Field(10.0, gt=0)


class LlmSettings(BaseModel):
    """LLM provider. The in-house server speaks the OpenAI chat API (for example vLLM)."""

    provider: Literal["inhouse", "openai"] = "inhouse"
    model: str
    model_version: str = "1"
    endpoint: str
    max_output_tokens: int = Field(800, ge=1)
    temperature: float = Field(0.1, ge=0, le=2)
    timeout_s: float = Field(30.0, gt=0)  # whole answer
    first_token_timeout_s: float = Field(3.0, gt=0)  # streaming: wait for the first piece
    max_concurrency: int = Field(8, ge=1)
    breaker_failures: int = Field(3, ge=1)
    breaker_cooldown_s: float = Field(15.0, gt=0)
    # OpenAI is optional and off by default: provider "openai" AND allow_external, and only for
    # data classes that security has approved (HLD section 9).
    allow_external: bool = False
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: SecretStr | None = None
    proxy: str | None = None


class GuardrailSettings(BaseModel):
    """In-house guardrails (HLD section 7): PII masking and denied topics."""

    mask_pii: bool = True
    # Regular expressions. A question that matches one is refused before anything is searched.
    denied_question_patterns: list[str] = []
    # Regular expressions. A match in the answer replaces the answer by a refusal.
    denied_answer_patterns: list[str] = []


class RagSettings(BaseModel):
    """Answer pipeline (HLD section 7)."""

    prompt_version: str = "v1"
    prompts_dir: str = "prompts"
    max_chunks: int = Field(8, ge=1, le=20)
    max_chunks_per_document: int = Field(3, ge=1)
    context_token_budget: int = Field(3000, ge=100)
    duplicate_similarity: float = Field(0.85, gt=0, le=1)
    # The gate: if the best passage scored below this after reranking, the answer is NOT_FOUND
    # and the LLM is not called. The value comes from the evaluation set. 0 switches the gate off.
    # It is applied only when the reranker ran, because RRF scores have no absolute meaning.
    min_score: float = Field(0.0, ge=0)
    # "reject": an answer without valid citations is not shown. "flag": it is shown with a warning.
    on_bad_citations: Literal["reject", "flag"] = "reject"
    guardrails: GuardrailSettings = GuardrailSettings()


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


class LangfuseSettings(BaseModel):
    """Langfuse LLM telemetry over its HTTP API. Metadata only: never the question or the answer."""

    enabled: bool = False
    host: str = ""
    public_key: SecretStr | None = None
    secret_key: SecretStr | None = None
    queue_size: int = Field(1000, ge=1)
    batch_size: int = Field(20, ge=1)
    flush_interval_s: float = Field(5.0, gt=0)
    timeout_s: float = Field(2.0, gt=0)


class ObservabilitySettings(BaseModel):
    """Metrics, traces and LLM telemetry (HLD section 18)."""

    metrics_enabled: bool = True
    worker_metrics_port: int = Field(9100, ge=1, le=65535)
    service_name: str = "semantic-search"
    # Traces go to an OTLP/HTTP collector. Empty means no export (spans are no-ops).
    otlp_endpoint: str = ""
    trace_sample_ratio: float = Field(0.1, ge=0, le=1)
    langfuse: LangfuseSettings = LangfuseSettings()


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
    snapshot: SnapshotSettings = SnapshotSettings()
    api: ApiSettings = ApiSettings()
    backfill: BackfillSettings = BackfillSettings()
    ingestion: IngestionSettings = IngestionSettings()
    normalizer: NormalizerSettings = NormalizerSettings()
    kafka: KafkaSettings
    consumer: ConsumerSettings = ConsumerSettings()
    retry: RetryPolicy = RetryPolicy()
    redis: RedisSettings
    embedding: EmbeddingSettings
    reranker: RerankerSettings
    llm: LlmSettings
    rag: RagSettings = RagSettings()
    chunking: ChunkingSettings
    feature_flags: FeatureFlags = FeatureFlags()
    observability: ObservabilitySettings = ObservabilitySettings()

    @model_validator(mode="after")
    def _prod_needs_auth(self) -> "Settings":
        if self.env == "prod" and self.api.auth_disabled:
            raise ValueError("api.auth_disabled is not allowed in prod")
        return self

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
