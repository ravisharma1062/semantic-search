from typing import cast

import pytest
from elasticsearch import AsyncElasticsearch

from app.core.errors import NonRetryableError
from app.core.settings import Settings, StoreSettings
from app.jobs.index_admin import build_parser, run_command
from app.store.aliases import (
    alias_targets,
    create_chunk_index,
    create_state_index,
    delete_chunk_index,
    ensure_managed,
    install_templates,
    switch_alias,
)
from app.store.templates import (
    chunk_index_template,
    chunk_mappings,
    physical_chunk_index_name,
    state_index_template,
    state_mappings,
)
from tests.fakes.es_admin import FakeAdminEs

ALIAS = "doc_chunks_current"


def _client(es: FakeAdminEs) -> AsyncElasticsearch:
    return cast(AsyncElasticsearch, es)


# --- templates ------------------------------------------------------------------------------


def test_chunk_mapping_matches_the_design() -> None:
    mapping = chunk_mappings(1024, StoreSettings())
    props = mapping["properties"]
    assert mapping["dynamic"] == "strict"
    assert props["embedding"] == {
        "type": "dense_vector",
        "dims": 1024,
        "index": True,
        "similarity": "cosine",
        "index_options": {"type": "int8_hnsw"},
    }
    for keyword in ("chunk_id", "doc_id", "embedding_model", "chunker_version", "doc_type", "tags"):
        assert props[keyword] == {"type": "keyword"}
    assert props["acl_users"] == {"type": "keyword"}
    assert props["acl_groups"] == {"type": "keyword"}
    assert props["content"] == {"type": "text", "analyzer": "standard"}
    assert props["section_title"] == {"type": "text"}
    assert props["created_at"] == {"type": "date"}
    assert props["indexed_at"] == {"type": "date"}
    assert props["chunk_no"] == {"type": "integer"}
    assert props["page_start"] == {"type": "integer"}
    assert props["page_end"] == {"type": "integer"}


def test_vector_size_and_quantization_come_from_settings() -> None:
    assert chunk_mappings(8, StoreSettings())["properties"]["embedding"]["dims"] == 8
    plain = chunk_mappings(8, StoreSettings(vector_index_type="hnsw"))["properties"]["embedding"]
    assert "index_options" not in plain
    binary = chunk_mappings(8, StoreSettings(vector_index_type="bbq_hnsw"))["properties"][
        "embedding"
    ]
    assert binary["index_options"] == {"type": "bbq_hnsw"}


def test_chunk_template_covers_all_versions_and_sets_shards() -> None:
    template = chunk_index_template(
        1024, StoreSettings(shards=5, replicas=2, refresh_interval="10s")
    )
    assert template["index_patterns"] == ["doc_chunks_v*"]
    assert template["template"]["settings"] == {
        "number_of_shards": 5,
        "number_of_replicas": 2,
        "refresh_interval": "10s",
    }


def test_state_mapping_matches_the_design() -> None:
    props = state_mappings()["properties"]
    assert {
        "item_id",
        "status",
        "content_hash",
        "doc_version",
        "chunk_count",
        "chunker_version",
        "embedding_model",
        "indexed_at",
        "attempts",
        "last_error",
        "wave",
    } <= set(props)
    assert props["status"] == {"type": "keyword"}
    assert props["doc_version"] == {"type": "long"}
    assert state_index_template("doc_index_state", StoreSettings())["index_patterns"] == [
        "doc_index_state"
    ]


@pytest.mark.parametrize(
    ("version", "model", "expected"),
    [("v1", "bge-m3", "doc_chunks_v1_bgem3"), ("v2", "BGE_M3-large", "doc_chunks_v2_bgem3large")],
)
def test_physical_index_name_has_the_version_and_the_model(
    version: str, model: str, expected: str
) -> None:
    assert physical_chunk_index_name("doc_chunks", version, model) == expected


# --- rule 9: the existing document index is never touched -----------------------------------


@pytest.mark.parametrize(
    "name", ["documents_sample", "documents", "doc_index_state", "doc_chunks", "chunks_v1"]
)
def test_only_chunk_index_versions_can_be_managed(settings: Settings, name: str) -> None:
    with pytest.raises(NonRetryableError):
        ensure_managed(name, settings)


def test_chunk_index_versions_are_accepted(settings: Settings) -> None:
    ensure_managed("doc_chunks_v1_bgem3", settings)


async def test_nothing_can_be_created_or_deleted_outside_the_chunk_prefix(
    settings: Settings,
) -> None:
    es = FakeAdminEs()
    es.add_index("documents_sample")
    with pytest.raises(NonRetryableError):
        await create_chunk_index(_client(es), "documents_sample2", settings)
    with pytest.raises(NonRetryableError):
        await delete_chunk_index(_client(es), "documents_sample", ALIAS, settings, force=True)
    with pytest.raises(NonRetryableError):
        await switch_alias(_client(es), ALIAS, "documents_sample", settings)
    assert "documents_sample" in es.created


# --- tools ----------------------------------------------------------------------------------


async def test_templates_are_installed(settings: Settings) -> None:
    es = FakeAdminEs()
    await install_templates(_client(es), settings)
    assert set(es.templates) == {"doc_chunks_template", "doc_index_state_template"}
    assert (
        es.templates["doc_chunks_template"]["template"]["mappings"]["properties"]["embedding"][
            "dims"
        ]
        == 1024
    )


async def test_create_index_and_state_index_are_safe_to_repeat(settings: Settings) -> None:
    es = FakeAdminEs()
    assert await create_chunk_index(_client(es), "doc_chunks_v1_bgem3", settings) is True
    assert await create_chunk_index(_client(es), "doc_chunks_v1_bgem3", settings) is False
    assert await create_state_index(_client(es), settings) is True
    assert await create_state_index(_client(es), settings) is False


async def test_switching_the_alias_moves_it_in_one_step(settings: Settings) -> None:
    es = FakeAdminEs()
    es.add_index("doc_chunks_v1_bgem3", docs=10)
    es.add_index("doc_chunks_v2_bgem3", docs=10)
    es.aliases[ALIAS] = {"doc_chunks_v1_bgem3"}
    previous = await switch_alias(_client(es), ALIAS, "doc_chunks_v2_bgem3", settings)
    assert previous == ["doc_chunks_v1_bgem3"]
    assert await alias_targets(_client(es), ALIAS) == ["doc_chunks_v2_bgem3"]


async def test_the_first_alias_has_nothing_to_leave(settings: Settings) -> None:
    es = FakeAdminEs()
    es.add_index("doc_chunks_v1_bgem3")
    assert await switch_alias(_client(es), ALIAS, "doc_chunks_v1_bgem3", settings) == []
    assert await alias_targets(_client(es), "no_such_alias") == []


async def test_switching_to_an_empty_or_missing_index_is_refused(settings: Settings) -> None:
    es = FakeAdminEs()
    es.add_index("doc_chunks_v2_bgem3", docs=0)
    es.aliases[ALIAS] = {"doc_chunks_v1_bgem3"}
    with pytest.raises(NonRetryableError, match="empty"):
        await switch_alias(_client(es), ALIAS, "doc_chunks_v2_bgem3", settings)
    with pytest.raises(NonRetryableError, match="exist"):
        await switch_alias(_client(es), ALIAS, "doc_chunks_v9_bgem3", settings)
    assert es.aliases[ALIAS] == {"doc_chunks_v1_bgem3"}  # nothing moved
    await switch_alias(_client(es), ALIAS, "doc_chunks_v2_bgem3", settings, allow_empty=True)


async def test_the_live_index_cannot_be_deleted(settings: Settings) -> None:
    es = FakeAdminEs()
    es.add_index("doc_chunks_v1_bgem3", age_days=100)
    es.aliases[ALIAS] = {"doc_chunks_v1_bgem3"}
    with pytest.raises(NonRetryableError, match="alias"):
        await delete_chunk_index(_client(es), "doc_chunks_v1_bgem3", ALIAS, settings, force=True)
    assert "doc_chunks_v1_bgem3" in es.created


async def test_a_young_index_is_kept_for_a_rollback(settings: Settings) -> None:
    es = FakeAdminEs()
    es.add_index("doc_chunks_v1_bgem3", age_days=3)
    now = es.now_ms
    with pytest.raises(NonRetryableError, match="young"):
        await delete_chunk_index(
            _client(es), "doc_chunks_v1_bgem3", ALIAS, settings, now_ms=lambda: now
        )
    await delete_chunk_index(_client(es), "doc_chunks_v1_bgem3", ALIAS, settings, force=True)
    assert "doc_chunks_v1_bgem3" not in es.created


async def test_an_old_index_can_be_deleted(settings: Settings) -> None:
    es = FakeAdminEs()
    es.add_index("doc_chunks_v1_bgem3", age_days=15)
    now = es.now_ms
    await delete_chunk_index(
        _client(es), "doc_chunks_v1_bgem3", ALIAS, settings, now_ms=lambda: now
    )
    assert "doc_chunks_v1_bgem3" not in es.created


# --- the command line -----------------------------------------------------------------------


async def _run(es: FakeAdminEs, settings: Settings, *argv: str) -> str:
    return await run_command(build_parser().parse_args(argv), _client(es), settings)


async def test_command_line_end_to_end(settings: Settings) -> None:
    es = FakeAdminEs()
    assert await _run(es, settings, "install-templates") == "templates installed"
    assert (
        await _run(es, settings, "create-index", "--version", "v2") == "doc_chunks_v2_bgem3 created"
    )
    es.counts["doc_chunks_v2_bgem3"] = 5
    assert "doc_chunks_current ->" in await _run(es, settings, "show-alias")
    result = await _run(es, settings, "switch-alias", "--index", "doc_chunks_v2_bgem3")
    assert result.startswith("doc_chunks_current -> doc_chunks_v2_bgem3")


def test_command_line_needs_a_command() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])
