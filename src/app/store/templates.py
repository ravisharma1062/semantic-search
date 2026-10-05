"""Index templates for the chunk index (HLD section 5) and the state index (HLD section 17).

The mapping of the existing document index is never touched (rule 9). Chunk index changes make a
new versioned index behind an alias.
"""

import re
from typing import Any

from app.core.settings import StoreSettings


def chunk_template_name(prefix: str) -> str:
    """Name of the composable template for all chunk index versions."""
    return f"{prefix}_template"


def state_template_name(state_index: str) -> str:
    """Name of the template for the state index."""
    return f"{state_index}_template"


def physical_chunk_index_name(prefix: str, version: str, model: str) -> str:
    """For example ``doc_chunks_v1_bgem3``: the name carries the version and the model."""
    slug = re.sub(r"[^a-z0-9]", "", model.lower())
    return f"{prefix}_{version}_{slug}"


def chunk_mappings(dims: int, store: StoreSettings) -> dict[str, Any]:
    """Field mappings of a chunk. ``dynamic: strict`` catches fields that were not planned."""
    vector: dict[str, Any] = {
        "type": "dense_vector",
        "dims": dims,
        "index": True,
        "similarity": "cosine",
    }
    if store.vector_index_type != "hnsw":
        vector["index_options"] = {"type": store.vector_index_type}
    return {
        "dynamic": "strict",
        "properties": {
            "chunk_id": {"type": "keyword"},
            "doc_id": {"type": "keyword"},
            "chunk_no": {"type": "integer"},
            "page_start": {"type": "integer"},
            "page_end": {"type": "integer"},
            "section_title": {"type": "text"},
            "content": {"type": "text", "analyzer": "standard"},
            "content_hash": {"type": "keyword"},
            "is_table": {"type": "boolean"},
            "embedding": vector,
            "embedding_model": {"type": "keyword"},
            "chunker_version": {"type": "keyword"},
            "doc_type": {"type": "keyword"},
            "tags": {"type": "keyword"},
            "created_at": {"type": "date"},
            "acl_users": {"type": "keyword"},
            "acl_groups": {"type": "keyword"},
            "indexed_at": {"type": "date"},
        },
    }


def chunk_index_settings(store: StoreSettings) -> dict[str, Any]:
    """Shards, replicas and refresh interval."""
    return {
        "number_of_shards": store.shards,
        "number_of_replicas": store.replicas,
        "refresh_interval": store.refresh_interval,
    }


def chunk_index_template(dims: int, store: StoreSettings) -> dict[str, Any]:
    """The composable template for ``{prefix}_v*``."""
    return {
        "index_patterns": [f"{store.chunk_index_prefix}_v*"],
        "priority": 200,
        "template": {
            "settings": chunk_index_settings(store),
            "mappings": chunk_mappings(dims, store),
        },
    }


def state_mappings() -> dict[str, Any]:
    """One record per ``ITEM_ID`` (HLD section 17). Plus ``meta_hash`` and ``reason``."""
    return {
        "dynamic": "strict",
        "properties": {
            "item_id": {"type": "keyword"},
            "status": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "meta_hash": {"type": "keyword"},
            "doc_version": {"type": "long"},
            "chunk_count": {"type": "integer"},
            "chunker_version": {"type": "keyword"},
            "embedding_model": {"type": "keyword"},
            "indexed_at": {"type": "date"},
            "attempts": {"type": "integer"},
            "last_error": {"type": "keyword", "ignore_above": 256},
            "reason": {"type": "keyword", "ignore_above": 256},
            "wave": {"type": "integer"},
        },
    }


def state_index_template(state_index: str, store: StoreSettings) -> dict[str, Any]:
    """The template for the state index. Small: one shard is enough for the records."""
    return {
        "index_patterns": [state_index],
        "priority": 200,
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": store.replicas},
            "mappings": state_mappings(),
        },
    }
