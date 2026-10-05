"""Fake Elasticsearch client for the source reader: get and mget over a dict."""

from typing import Any, Self

from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig, ObjectApiResponse
from elasticsearch import ApiError, AsyncElasticsearch, NotFoundError


def meta(status: int = 200) -> ApiResponseMeta:
    """Response metadata with the given status."""
    return ApiResponseMeta(
        status=status,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig("http", "localhost", 9200),
    )


def api_error(status: int) -> ApiError:
    """An error with this HTTP status."""
    return ApiError("synthetic", meta(status), {"error": {"type": "synthetic"}})


class FakeElasticsearch:
    """Serves ``get`` and ``mget`` from ``documents``. Queued errors are raised first."""

    def __init__(self, documents: dict[str, dict[str, Any]] | None = None) -> None:
        self.documents = documents or {}
        self.index_exists = True
        self.errors: list[Exception] = []
        self.mget_errors_for: set[str] = set()
        self.calls: list[tuple[str, Any]] = []
        self.timeouts: list[Any] = []

    def options(self, **kwargs: Any) -> Self:
        """Records per-call options such as the timeout."""
        self.timeouts.append(kwargs.get("request_timeout"))
        return self

    def _maybe_fail(self) -> None:
        if self.errors:
            raise self.errors.pop(0)
        if not self.index_exists:
            raise NotFoundError(
                "no such index",
                meta(404),
                {"error": {"type": "index_not_found_exception"}, "status": 404},
            )

    async def get(
        self, *, index: str, id: str, source_includes: list[str]
    ) -> ObjectApiResponse[Any]:
        """One document, or a 404 with ``found: false``."""
        self.calls.append(("get", id))
        self._maybe_fail()
        if id not in self.documents:
            raise NotFoundError(
                "not found", meta(404), {"_index": index, "_id": id, "found": False}
            )
        body = {"_index": index, "_id": id, "found": True, "_source": self.documents[id]}
        return ObjectApiResponse(body=body, meta=meta())

    async def mget(
        self, *, index: str, ids: list[str], source_includes: list[str]
    ) -> ObjectApiResponse[Any]:
        """Several documents. Unknown IDs come back with ``found: false``."""
        self.calls.append(("mget", list(ids)))
        if self.errors:
            raise self.errors.pop(0)
        docs: list[dict[str, Any]] = []
        if not self.index_exists:  # like the real server: 200, with an error per ID
            error = {"type": "index_not_found_exception"}
            docs = [{"_id": item_id, "error": error} for item_id in ids]
            return ObjectApiResponse(body={"docs": docs}, meta=meta())
        for item_id in ids:
            if item_id in self.mget_errors_for:
                docs.append({"_id": item_id, "error": {"type": "shard_failure"}})
            elif item_id in self.documents:
                docs.append({"_id": item_id, "found": True, "_source": self.documents[item_id]})
            else:
                docs.append({"_id": item_id, "found": False})
        return ObjectApiResponse(body={"docs": docs}, meta=meta())

    def as_client(self) -> AsyncElasticsearch:
        """For code that is typed against the real client."""
        return self  # type: ignore[return-value]
