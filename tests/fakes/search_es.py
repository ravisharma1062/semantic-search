"""A small in-memory Elasticsearch for search tests.

It really evaluates the query shapes of ``QueryBuilder``: ``bool`` with ``must``, ``filter`` and
``should``, ``term``, ``terms``, ``range``, ``multi_match`` (words in content or title), ``knn`` with its own
filter (cosine similarity) and the ``rrf`` retriever. So a test can run the same request as different
users and see what each of them gets. Highlights are built like the real ones (HTML encoded, ``<em>``).
"""

import html
import math
import re
from typing import Any, Self

from elastic_transport import ObjectApiResponse

from tests.fakes.es import api_error, meta


def _words(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


class FakeSearchEs:
    """Chunk documents as dicts with ``chunk_id``, ``doc_id``, ``content``, ``embedding``, ``acl_*``."""

    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs
        self.requests: list[dict[str, Any]] = []
        self.timeouts: list[Any] = []
        self.fail: dict[str, Exception] = {}  # "bm25", "knn" or "retriever" -> error to raise
        self.delay_s = 0.0

    def options(self, **kwargs: Any) -> Self:
        """Records the per-call timeout."""
        self.timeouts.append(kwargs.get("request_timeout"))
        return self

    # --- evaluation -----------------------------------------------------------------------

    def _clause(self, doc: dict[str, Any], clause: dict[str, Any]) -> bool:
        if "bool" in clause:
            return self._bool(doc, clause["bool"])
        if "term" in clause:
            ((field, value),) = clause["term"].items()
            stored = doc.get(field)
            return value in stored if isinstance(stored, list) else stored == value
        if "terms" in clause:
            ((field, values),) = clause["terms"].items()
            stored = doc.get(field)
            have = stored if isinstance(stored, list) else [stored]
            return any(v in have for v in values)
        if "range" in clause:
            ((field, bounds),) = clause["range"].items()
            value = str(doc.get(field) or "")[:10]
            return ("gte" not in bounds or value >= bounds["gte"][:10]) and (
                "lte" not in bounds or value <= bounds["lte"][:10]
            )
        if "multi_match" in clause:
            return self._text_score(doc, clause["multi_match"]["query"]) > 0
        raise AssertionError(f"the fake does not know this clause: {clause}")

    def _bool(self, doc: dict[str, Any], node: dict[str, Any]) -> bool:
        if not all(self._clause(doc, c) for c in node.get("must", [])):
            return False
        if not all(self._clause(doc, c) for c in node.get("filter", [])):
            return False
        should = node.get("should", [])
        return not should or any(self._clause(doc, c) for c in should)

    @staticmethod
    def _text_score(doc: dict[str, Any], query: str) -> float:
        words = _words(doc.get("content", "") + " " + (doc.get("section_title") or ""))
        return float(sum(words.count(w) for w in set(_words(query))))

    @staticmethod
    def _cosine(a: list[float], b: list[float]) -> float:
        na, nb = math.sqrt(sum(x * x for x in a)), math.sqrt(sum(x * x for x in b))
        return sum(x * y for x, y in zip(a, b, strict=True)) / ((na * nb) or 1.0)

    def _highlight(self, doc: dict[str, Any], query: str, spec: dict[str, Any] | None) -> list[str]:
        if not spec:
            return []
        terms = set(_words(query))
        text = doc.get("content", "")
        out = []
        for token in re.split(r"(\W+)", text):
            out.append(
                f"<em>{html.escape(token)}</em>" if token.lower() in terms else html.escape(token)
            )
        fragment = "".join(out)
        return [fragment] if "<em>" in fragment else []

    # --- the legs -------------------------------------------------------------------------

    def _bm25(
        self, query: dict[str, Any], size: int, spec: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        text = query["bool"]["must"][0]["multi_match"]["query"]
        found = [d for d in self.docs if self._bool(d, query["bool"])]
        found.sort(key=lambda d: (-self._text_score(d, text), d["chunk_id"]))
        return [
            self._hit(d, self._text_score(d, text), self._highlight(d, text, spec))
            for d in found[:size]
        ]

    def _knn(self, knn: dict[str, Any], size: int) -> list[dict[str, Any]]:
        allowed = [d for d in self.docs if all(self._clause(d, c) for c in knn.get("filter", []))]
        scored = sorted(
            ((self._cosine(knn["query_vector"], d["embedding"]), d) for d in allowed),
            key=lambda pair: (-pair[0], pair[1]["chunk_id"]),
        )
        return [self._hit(d, s, []) for s, d in scored[: min(size, knn["k"])]]

    @staticmethod
    def _hit(doc: dict[str, Any], score: float, highlight: list[str]) -> dict[str, Any]:
        source = {k: v for k, v in doc.items() if k not in {"embedding", "acl_users", "acl_groups"}}
        hit: dict[str, Any] = {"_id": doc["chunk_id"], "_score": score, "_source": source}
        if highlight:
            hit["highlight"] = {"content": highlight}
        return hit

    # --- the API --------------------------------------------------------------------------

    async def search(
        self,
        *,
        index: str,
        query: dict[str, Any] | None = None,
        knn: dict[str, Any] | None = None,
        retriever: dict[str, Any] | None = None,
        size: int = 10,
        source: list[str] | None = None,
        highlight: dict[str, Any] | None = None,
    ) -> ObjectApiResponse[Any]:
        """Evaluate one request."""
        self.requests.append(
            {
                "index": index,
                "query": query,
                "knn": knn,
                "retriever": retriever,
                "size": size,
                "highlight": highlight,
            }
        )
        kind = "retriever" if retriever else "knn" if knn else "bm25"
        if self.delay_s:
            import asyncio

            await asyncio.sleep(self.delay_s)
        if kind in self.fail:
            raise self.fail[kind]
        if retriever:
            if highlight:  # like Elasticsearch 8.15: [rank] cannot be used with [highlighter]
                raise api_error(400)
            hits = self._rrf(retriever["rrf"], size, None)
        elif knn:
            hits = self._knn(knn, size)
        else:
            assert query is not None
            hits = self._bm25(query, size, highlight)
        return ObjectApiResponse(body={"hits": {"hits": hits}}, meta=meta())

    def _rrf(
        self, rrf: dict[str, Any], size: int, spec: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        legs: list[list[dict[str, Any]]] = []
        for leg in rrf["retrievers"]:
            if "standard" in leg:
                legs.append(self._bm25(leg["standard"]["query"], rrf["rank_window_size"], spec))
            else:
                legs.append(self._knn(leg["knn"], rrf["rank_window_size"]))
        scores: dict[str, float] = {}
        best: dict[str, dict[str, Any]] = {}
        for hits in legs:
            for rank, hit in enumerate(hits, start=1):
                cid = hit["_id"]
                scores[cid] = scores.get(cid, 0.0) + 1.0 / (rrf["rank_constant"] + rank)
                best.setdefault(cid, hit)
                if "highlight" in hit:
                    best[cid] = hit
        ordered = sorted(scores, key=lambda c: (-scores[c], c))[:size]
        return [{**best[c], "_score": scores[c]} for c in ordered]


def chunk_doc(
    chunk_id: str,
    doc_id: str,
    content: str,
    *,
    users: list[str] | None = None,
    groups: list[str] | None = None,
    embedding: list[float] | None = None,
    doc_type: str = "contract",
    created_at: str = "2024-05-01",
    pages: tuple[int, int] = (1, 1),
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """A chunk document for the fake index."""
    return {
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "chunk_no": 0,
        "page_start": pages[0],
        "page_end": pages[1],
        "section_title": None,
        "content": content,
        "doc_type": doc_type,
        "created_at": created_at,
        "tags": tags or [],
        "embedding": embedding or [1.0, 0.0, 0.0, 0.0],
        "acl_users": users or [],
        "acl_groups": groups or [],
    }
