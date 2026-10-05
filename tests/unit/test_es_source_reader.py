from typing import Any

import pytest
from elastic_transport import ConnectionError as TransportConnectionError
from elastic_transport import ConnectionTimeout

from app.core.errors import NonRetryableError, UpstreamTimeoutError, UpstreamUnavailableError
from app.core.retry import RetryPolicy
from app.core.settings import ElasticsearchSettings, SourceSettings
from app.ingestion.es_source_reader import ElasticsearchSourceReader
from app.ingestion.source import SourceReader
from tests.fakes.es import FakeElasticsearch, api_error

DOCS: dict[str, dict[str, Any]] = {
    "ITEM-1": {"pages": [{"page_no": 1, "text": "one"}], "acl_users": ["u1"]},
    "ITEM-2": {"ocr_text": "two"},
}


async def _no_sleep(_seconds: float) -> None:
    return None


def _reader(
    es: FakeElasticsearch, *, attempts: int = 3, max_get_many: int = 100, timeout_s: float = 5.0
) -> ElasticsearchSourceReader:
    return ElasticsearchSourceReader(
        es.as_client(),
        "documents_test",
        SourceSettings(max_get_many=max_get_many),
        ElasticsearchSettings(
            hosts=["http://es.test:9200"], state_index="state", source_read_timeout_s=timeout_s
        ),
        RetryPolicy(attempts=attempts),
        sleep=_no_sleep,
    )


def test_reader_satisfies_the_source_reader_interface() -> None:
    assert isinstance(_reader(FakeElasticsearch()), SourceReader)


# --- get ------------------------------------------------------------------------------------


async def test_get_returns_the_document() -> None:
    doc = await _reader(FakeElasticsearch(DOCS)).get("ITEM-1")
    assert doc is not None
    assert doc.pages[0].text == "one"
    assert doc.acl_users == ["u1"]


async def test_get_returns_none_for_a_missing_document() -> None:
    assert await _reader(FakeElasticsearch(DOCS)).get("ITEM-404") is None


async def test_a_missing_index_is_an_error_not_a_missing_document() -> None:
    es = FakeElasticsearch(DOCS)
    es.index_exists = False
    with pytest.raises(NonRetryableError):
        await _reader(es).get("ITEM-1")


async def test_every_call_has_a_timeout() -> None:
    es = FakeElasticsearch(DOCS)
    await _reader(es, timeout_s=2.5).get("ITEM-1")
    await _reader(es, timeout_s=2.5).get_many(["ITEM-1"])
    assert es.timeouts == [2.5, 2.5]


async def test_get_retries_a_timeout_and_then_succeeds() -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [ConnectionTimeout("slow"), TransportConnectionError("down")]
    doc = await _reader(es).get("ITEM-1")
    assert doc is not None
    assert len(es.calls) == 3


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
async def test_get_retries_busy_statuses(status: int) -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [api_error(status)]
    assert await _reader(es).get("ITEM-1") is not None


async def test_get_gives_up_after_the_retry_limit_with_a_typed_error() -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [ConnectionTimeout("slow")] * 5
    with pytest.raises(UpstreamTimeoutError):
        await _reader(es, attempts=3).get("ITEM-1")
    assert len(es.calls) == 3


async def test_get_connection_failure_is_upstream_unavailable() -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [TransportConnectionError("down")] * 3
    with pytest.raises(UpstreamUnavailableError):
        await _reader(es).get("ITEM-1")


@pytest.mark.parametrize("status", [400, 401, 403])
async def test_get_does_not_retry_a_rejected_request(status: int) -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [api_error(status)]
    with pytest.raises(NonRetryableError):
        await _reader(es).get("ITEM-1")
    assert len(es.calls) == 1


async def test_error_messages_are_generic() -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [api_error(400)]
    with pytest.raises(NonRetryableError) as error:
        await _reader(es).get("ITEM-1")
    assert "synthetic" not in str(error.value)  # the client's own text is not passed on


# --- get_many -------------------------------------------------------------------------------


async def test_get_many_returns_found_documents_and_leaves_out_missing_ones() -> None:
    result = await _reader(FakeElasticsearch(DOCS)).get_many(["ITEM-2", "ITEM-404", "ITEM-1"])
    assert set(result) == {"ITEM-1", "ITEM-2"}
    assert result["ITEM-2"].text == "two"


async def test_get_many_with_no_ids_makes_no_call() -> None:
    es = FakeElasticsearch(DOCS)
    assert await _reader(es).get_many([]) == {}
    assert es.calls == []


async def test_get_many_reads_duplicates_once() -> None:
    es = FakeElasticsearch(DOCS)
    await _reader(es).get_many(["ITEM-1", "ITEM-1", "ITEM-2"])
    assert es.calls == [("mget", ["ITEM-1", "ITEM-2"])]


async def test_get_many_splits_large_requests() -> None:
    es = FakeElasticsearch({f"ITEM-{i}": {"ocr_text": "t"} for i in range(5)})
    result = await _reader(es, max_get_many=2).get_many([f"ITEM-{i}" for i in range(5)])
    assert len(result) == 5
    assert [len(ids) for _, ids in es.calls] == [2, 2, 1]


async def test_get_many_retries_a_failed_batch() -> None:
    es = FakeElasticsearch(DOCS)
    es.errors = [ConnectionTimeout("slow")]
    assert len(await _reader(es).get_many(["ITEM-1", "ITEM-2"])) == 2


async def test_get_many_shard_error_is_retried_and_then_raised() -> None:
    es = FakeElasticsearch(DOCS)
    es.mget_errors_for = {"ITEM-2"}  # a shard error is not the same as "missing"
    with pytest.raises(UpstreamUnavailableError):
        await _reader(es).get_many(["ITEM-1", "ITEM-2"])
    assert len(es.calls) == 3


async def test_get_many_on_a_missing_index_is_an_error() -> None:
    es = FakeElasticsearch(DOCS)
    es.index_exists = False
    with pytest.raises(NonRetryableError):
        await _reader(es).get_many(["ITEM-1"])
