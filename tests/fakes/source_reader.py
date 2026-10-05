"""Fake SourceReader: an in-memory dict of documents."""

from app.ingestion.source import SourceDocument


class FakeSourceReader:
    """Documents by item ID. Can be told to fail."""

    def __init__(self, documents: list[SourceDocument] | None = None) -> None:
        self.documents = {doc.item_id: doc for doc in documents or []}
        self.fail_with: Exception | None = None
        self.get_calls: list[str] = []

    async def get(self, item_id: str) -> SourceDocument | None:
        """The document, or ``None`` if it is not there."""
        if self.fail_with:
            raise self.fail_with
        self.get_calls.append(item_id)
        return self.documents.get(item_id)

    async def get_many(self, item_ids: list[str]) -> dict[str, SourceDocument]:
        """The documents found. Missing IDs are left out."""
        if self.fail_with:
            raise self.fail_with
        self.get_calls.extend(item_ids)
        return {i: self.documents[i] for i in item_ids if i in self.documents}
