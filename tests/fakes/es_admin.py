"""Fake Elasticsearch index administration: indices, aliases, templates and counts."""

from typing import Any, Self

from elastic_transport import ObjectApiResponse
from elasticsearch import NotFoundError

from tests.fakes.es import meta

DAY_MS = 86_400_000


class FakeAdminEs:
    """Just enough of the indices API for the alias and index tools."""

    def __init__(self, now_ms: float = 1_000 * DAY_MS) -> None:
        self.now_ms = now_ms
        self.created: dict[str, float] = {}
        self.counts: dict[str, int] = {}
        self.aliases: dict[str, set[str]] = {}
        self.templates: dict[str, dict[str, Any]] = {}
        self.indices = self

    def add_index(self, name: str, *, age_days: float = 0, docs: int = 1) -> None:
        """Make an index that exists already."""
        self.created[name] = self.now_ms - age_days * DAY_MS
        self.counts[name] = docs

    def options(self, **_kwargs: Any) -> Self:
        """Ignored."""
        return self

    async def put_index_template(self, *, name: str, **body: Any) -> ObjectApiResponse[Any]:
        """Store the template."""
        self.templates[name] = body
        return ObjectApiResponse(body={"acknowledged": True}, meta=meta())

    async def exists(self, *, index: str) -> bool:
        """Does the index exist?"""
        return index in self.created

    async def create(self, *, index: str) -> ObjectApiResponse[Any]:
        """Create an empty index."""
        self.add_index(index, docs=0)
        return ObjectApiResponse(body={"acknowledged": True}, meta=meta())

    async def count(self, *, index: str) -> dict[str, int]:
        """Number of documents."""
        return {"count": self.counts[index]}

    async def get_alias(self, *, name: str) -> ObjectApiResponse[Any]:
        """Indices behind the alias, or a 404."""
        targets = self.aliases.get(name)
        if not targets:
            raise NotFoundError("no alias", meta(404), {})
        return ObjectApiResponse(body={i: {"aliases": {name: {}}} for i in targets}, meta=meta())

    async def update_aliases(self, *, actions: list[dict[str, Any]]) -> ObjectApiResponse[Any]:
        """Apply all actions together."""
        new = {alias: set(targets) for alias, targets in self.aliases.items()}
        for action in actions:
            if "remove" in action:
                new.get(action["remove"]["alias"], set()).discard(action["remove"]["index"])
            if "add" in action:
                new.setdefault(action["add"]["alias"], set()).add(action["add"]["index"])
        self.aliases = new
        return ObjectApiResponse(body={"acknowledged": True}, meta=meta())

    async def get_settings(self, *, index: str, name: str) -> dict[str, Any]:
        """The creation date of the index."""
        date = str(int(self.created[index]))
        return {index: {"settings": {"index": {"creation_date": date}}}}

    async def delete(self, *, index: str) -> ObjectApiResponse[Any]:
        """Delete the index."""
        del self.created[index]
        return ObjectApiResponse(body={"acknowledged": True}, meta=meta())
