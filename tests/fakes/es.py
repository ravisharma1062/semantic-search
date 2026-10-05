"""Fake Elasticsearch client for the source reader: get and mget over a dict."""

from typing import Any, Self

from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig, ObjectApiResponse
from elasticsearch import ApiError, AsyncElasticsearch, ConflictError, NotFoundError


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


class FakeWriteEs:
    """Fake client for the state store, the indexer and the alias tools.

    Keeps documents with sequence numbers (optimistic concurrency), scripted bulk answers, and
    records every call. Queued errors are raised first.
    """

    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}
        self.seq: dict[str, int] = {}
        self.errors: list[Exception] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.timeouts: list[Any] = []
        self.bulk_answers: list[dict[str, Any]] = []
        self.before_write: Any = None  # a hook that runs just before an index call
        self.by_query_conflicts = 0
        self._next_seq = 0
        self.indices = self

    def options(self, **kwargs: Any) -> Self:
        """Records per-call options."""
        self.timeouts.append(kwargs.get("request_timeout"))
        return self

    def _fail(self) -> None:
        if self.errors:
            raise self.errors.pop(0)

    # state store ------------------------------------------------------------------------

    async def get(self, *, index: str, id: str) -> ObjectApiResponse[Any]:
        """A document with its sequence number, or a 404 with found false."""
        self.calls.append(("get", {"id": id}))
        self._fail()
        if id not in self.docs:
            raise NotFoundError("nf", meta(404), {"_id": id, "found": False})
        body = {"_id": id, "_source": self.docs[id], "_seq_no": self.seq[id], "_primary_term": 1}
        return ObjectApiResponse(body=body, meta=meta())

    async def index(
        self,
        *,
        index: str,
        id: str,
        document: dict[str, Any],
        op_type: str | None = None,
        if_seq_no: int | None = None,
        if_primary_term: int | None = None,
    ) -> ObjectApiResponse[Any]:
        """Write with a version check."""
        if self.before_write:
            hook, self.before_write = self.before_write, None
            hook()
        self.calls.append(("index", {"id": id, "op_type": op_type, "if_seq_no": if_seq_no}))
        self._fail()
        if op_type == "create" and id in self.docs:
            raise ConflictError("exists", meta(409), {})
        if if_seq_no is not None and self.seq.get(id) != if_seq_no:
            raise ConflictError("conflict", meta(409), {})
        self.docs[id] = document
        self._next_seq += 1
        self.seq[id] = self._next_seq
        return ObjectApiResponse(body={"result": "created"}, meta=meta())

    # indexer ----------------------------------------------------------------------------

    async def bulk(
        self, *, operations: list[dict[str, Any]], refresh: bool
    ) -> ObjectApiResponse[Any]:
        """A scripted answer if there is one, otherwise everything succeeds."""
        self.calls.append(("bulk", {"operations": operations}))
        self._fail()
        if self.bulk_answers:
            return ObjectApiResponse(body=self.bulk_answers.pop(0), meta=meta())
        items = [{"index": {"_id": op["index"]["_id"], "status": 201}} for op in operations[::2]]
        return ObjectApiResponse(body={"errors": False, "items": items}, meta=meta())

    async def refresh(self, *, index: str) -> ObjectApiResponse[Any]:
        """Records the refresh."""
        self.calls.append(("refresh", {"index": index}))
        self._fail()
        return ObjectApiResponse(body={}, meta=meta())

    async def delete_by_query(
        self, *, index: str, query: dict[str, Any], refresh: bool
    ) -> ObjectApiResponse[Any]:
        """Records the query."""
        self.calls.append(("delete_by_query", {"query": query}))
        self._fail()
        if self.by_query_conflicts:
            self.by_query_conflicts -= 1
            raise ConflictError("conflict", meta(409), {})
        return ObjectApiResponse(body={"deleted": 3}, meta=meta())

    async def update_by_query(
        self, *, index: str, query: dict[str, Any], script: dict[str, Any], refresh: bool
    ) -> ObjectApiResponse[Any]:
        """Records the query and the script."""
        self.calls.append(("update_by_query", {"query": query, "script": script}))
        self._fail()
        if self.by_query_conflicts:
            self.by_query_conflicts -= 1
            raise ConflictError("conflict", meta(409), {})
        return ObjectApiResponse(body={"updated": 4}, meta=meta())

    def called(self, name: str) -> list[dict[str, Any]]:
        """The arguments of all calls of one kind."""
        return [args for call, args in self.calls if call == name]
