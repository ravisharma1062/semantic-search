"""The source reader against a real Elasticsearch 8."""

import uuid
from typing import Any

import pytest
from elasticsearch import AsyncElasticsearch

from app.core.errors import NonRetryableError, UpstreamTimeoutError, UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings, SourceSettings
from app.ingestion.es_source_reader import ElasticsearchSourceReader
from app.ingestion.source import source_includes
from app.store.client import create_es_client

pytestmark = pytest.mark.integration

_HOSTS_PLACEHOLDER = ["http://placeholder.test:9200"]


def _page(number: int) -> dict[str, Any]:
    return {"page_no": number, "text": f"synthetic text of page {number}"}


@pytest.fixture
async def index(es_client: AsyncElasticsearch) -> str:
    """A source index with a few synthetic documents."""
    name = f"documents-{uuid.uuid4().hex[:8]}"
    documents: dict[str, dict[str, Any]] = {
        "ITEM-1": {
            "pages": [_page(1), _page(2)],
            "doc_type": "contract",
            "tags": ["vendor"],
            "created_at": "2024-03-01T10:00:00Z",
            "acl_users": ["user-1"],
            "acl_groups": ["group-a"],
            "version": 5,
            "unrelated_field": "must not be fetched",
        },
        "ITEM-2": {"ocr_text": "document level text only", "acl_groups": ["group-b"]},
        "ITEM-3": {"doc_type": "scan"},  # no text at all
        "ITEM-BIG": {"pages": [_page(n) for n in range(1, 3001)]},  # 3,000 pages
    }
    for item_id, source in documents.items():
        await es_client.index(index=name, id=item_id, document=source)
    await es_client.indices.refresh(index=name)
    return name


def _reader(
    client: AsyncElasticsearch, index: str, *, source: SourceSettings | None = None
) -> ElasticsearchSourceReader:
    return ElasticsearchSourceReader(
        client,
        index,
        source or SourceSettings(),
        ElasticsearchSettings(
            hosts=_HOSTS_PLACEHOLDER, state_index="state", source_read_timeout_s=5
        ),
        RetryPolicy(attempts=1),
    )


async def test_get_reads_pages_metadata_and_permissions(
    es_client: AsyncElasticsearch, index: str
) -> None:
    doc = await _reader(es_client, index).get("ITEM-1")
    assert doc is not None
    assert [p.page_no for p in doc.pages] == [1, 2]
    assert doc.pages[1].text == "synthetic text of page 2"
    assert doc.doc_type == "contract"
    assert doc.tags == ["vendor"]
    assert doc.acl_users == ["user-1"]
    assert doc.acl_groups == ["group-a"]
    assert doc.version == 5
    assert doc.created_at is not None


async def test_get_reads_document_level_text(es_client: AsyncElasticsearch, index: str) -> None:
    doc = await _reader(es_client, index).get("ITEM-2")
    assert doc is not None
    assert doc.pages == []
    assert doc.text == "document level text only"
    assert doc.acl_groups == ["group-b"]


async def test_document_without_text_is_returned_and_flagged_by_has_text(
    es_client: AsyncElasticsearch, index: str
) -> None:
    doc = await _reader(es_client, index).get("ITEM-3")
    assert doc is not None
    assert not doc.has_text


async def test_missing_document_is_none(es_client: AsyncElasticsearch, index: str) -> None:
    assert await _reader(es_client, index).get("ITEM-404") is None


async def test_missing_index_is_an_error_and_not_a_missing_document(
    es_client: AsyncElasticsearch,
) -> None:
    with pytest.raises(NonRetryableError):
        await _reader(es_client, "no-such-index").get("ITEM-1")
    with pytest.raises(NonRetryableError):
        await _reader(es_client, "no-such-index").get_many(["ITEM-1"])


async def test_get_many_mixed_ids_batches_and_duplicates(
    es_client: AsyncElasticsearch, index: str
) -> None:
    reader = _reader(es_client, index, source=SourceSettings(max_get_many=2))
    result = await reader.get_many(["ITEM-1", "ITEM-404", "ITEM-2", "ITEM-1", "ITEM-3"])
    assert set(result) == {"ITEM-1", "ITEM-2", "ITEM-3"}


async def test_only_the_configured_fields_are_fetched(
    es_client: AsyncElasticsearch, index: str
) -> None:
    raw = await es_client.get(
        index=index, id="ITEM-1", source_includes=source_includes(SourceSettings())
    )
    assert "unrelated_field" not in raw["_source"]


async def test_a_3000_page_document_is_read_complete(
    es_client: AsyncElasticsearch, index: str
) -> None:
    doc = await _reader(es_client, index).get("ITEM-BIG")
    assert doc is not None
    assert len(doc.pages) == 3000
    assert doc.pages[-1].page_no == 3000
    assert not doc.truncated


async def test_the_page_limit_cuts_a_huge_document_and_flags_it(
    es_client: AsyncElasticsearch, index: str
) -> None:
    reader = _reader(es_client, index, source=SourceSettings(max_pages=100))
    doc = await reader.get("ITEM-BIG")
    assert doc is not None
    assert len(doc.pages) == 100
    assert doc.truncated


async def test_an_unreachable_cluster_is_upstream_unavailable() -> None:
    settings = ElasticsearchSettings(
        hosts=["http://127.0.0.1:1"], state_index="state", source_read_timeout_s=1
    )
    client = create_es_client(settings)
    try:
        reader = ElasticsearchSourceReader(
            client, "any", SourceSettings(), settings, RetryPolicy(attempts=1)
        )
        with pytest.raises((UpstreamUnavailableError, UpstreamTimeoutError)):
            await reader.get("ITEM-1")
    finally:
        await client.close()
