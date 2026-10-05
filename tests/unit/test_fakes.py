"""The fakes satisfy the interfaces (checked by mypy) and behave as documented."""

import math

import pytest

from app.core.errors import UpstreamTimeoutError, UpstreamUnavailableError
from app.embeddings.base import Embedder
from app.ingestion.source import SourceDocument, SourcePage, SourceReader
from app.llm.base import ChatMessage, LLMClient, LLMOptions
from app.rerank.base import Reranker
from tests.fakes import FakeEmbedder, FakeLLMClient, FakeReranker, FakeSourceReader

# mypy checks these assignments against the Protocols.
_EMBEDDER: Embedder = FakeEmbedder()
_RERANKER: Reranker = FakeReranker()
_LLM: LLMClient = FakeLLMClient()
_READER: SourceReader = FakeSourceReader()

_MESSAGES = [ChatMessage(role="user", content="a synthetic question")]


def test_fakes_match_the_runtime_protocols() -> None:
    assert isinstance(FakeEmbedder(), Embedder)
    assert isinstance(FakeReranker(), Reranker)
    assert isinstance(FakeLLMClient(), LLMClient)
    assert isinstance(FakeSourceReader(), SourceReader)


class TestFakeEmbedder:
    async def test_vectors_are_deterministic_normalized_and_sized(self) -> None:
        embedder = FakeEmbedder(dims=16)
        first = await embedder.embed_documents(["alpha", "beta"])
        again = await embedder.embed_documents(["alpha"])
        assert first[0] == again[0]
        assert first[0] != first[1]
        assert all(len(v) == 16 for v in first)
        assert math.isclose(math.sqrt(sum(x * x for x in first[0])), 1.0)

    async def test_query_and_document_vectors_match_for_same_text(self) -> None:
        embedder = FakeEmbedder()
        assert await embedder.embed_query("alpha") == (await embedder.embed_documents(["alpha"]))[0]

    async def test_records_calls(self) -> None:
        embedder = FakeEmbedder()
        await embedder.embed_documents(["a", "b"])
        await embedder.embed_query("q")
        assert embedder.document_calls == [["a", "b"]]
        assert embedder.query_calls == ["q"]

    async def test_can_fail(self) -> None:
        embedder = FakeEmbedder()
        embedder.fail_with = UpstreamTimeoutError()
        with pytest.raises(UpstreamTimeoutError):
            await embedder.embed_documents(["a"])
        with pytest.raises(UpstreamTimeoutError):
            await embedder.embed_query("a")


class TestFakeReranker:
    async def test_orders_by_word_overlap_and_applies_top_n(self) -> None:
        reranker = FakeReranker()
        passages = ["nothing relevant", "late delivery penalty", "penalty clause"]
        result = await reranker.rerank("penalty for late delivery", passages, top_n=2)
        assert [index for index, _ in result] == [1, 2]
        assert result[0][1] > result[1][1]

    async def test_ties_keep_input_order(self) -> None:
        result = await FakeReranker().rerank("zzz", ["a", "b", "c"], top_n=3)
        assert [index for index, _ in result] == [0, 1, 2]

    async def test_empty_passages_and_empty_query(self) -> None:
        reranker = FakeReranker()
        assert await reranker.rerank("q", [], top_n=5) == []
        assert await reranker.rerank("", ["a"], top_n=5) == [(0, 0.0)]

    async def test_can_fail(self) -> None:
        reranker = FakeReranker()
        reranker.fail_with = UpstreamUnavailableError()
        with pytest.raises(UpstreamUnavailableError):
            await reranker.rerank("q", ["a"], top_n=1)


class TestFakeLLMClient:
    async def test_replies_in_order_then_repeats_the_last(self) -> None:
        llm = FakeLLMClient(["first [1]", "second"])
        assert await llm.generate(_MESSAGES) == "first [1]"
        assert await llm.generate(_MESSAGES) == "second"
        assert await llm.generate(_MESSAGES, LLMOptions(temperature=0)) == "second"
        assert len(llm.calls) == 3

    async def test_stream_joins_to_the_reply(self) -> None:
        llm = FakeLLMClient(["The penalty is one percent [1]."])
        pieces = [piece async for piece in llm.stream(_MESSAGES)]
        assert len(pieces) > 1
        assert "".join(pieces) == "The penalty is one percent [1]."

    async def test_can_fail_in_generate_and_stream(self) -> None:
        llm = FakeLLMClient()
        llm.fail_with = UpstreamUnavailableError()
        with pytest.raises(UpstreamUnavailableError):
            await llm.generate(_MESSAGES)
        with pytest.raises(UpstreamUnavailableError):
            _ = [piece async for piece in llm.stream(_MESSAGES)]

    def test_needs_at_least_one_reply(self) -> None:
        with pytest.raises(ValueError, match="replies"):
            FakeLLMClient([])


class TestFakeSourceReader:
    @staticmethod
    def _reader() -> FakeSourceReader:
        return FakeSourceReader(
            [
                SourceDocument(item_id="ITEM-1", pages=[SourcePage(page_no=1, text="page one")]),
                SourceDocument(item_id="ITEM-2", text="document level text"),
            ]
        )

    async def test_get_returns_document_or_none(self) -> None:
        reader = self._reader()
        found = await reader.get("ITEM-1")
        assert found is not None
        assert found.pages[0].text == "page one"
        assert await reader.get("ITEM-404") is None

    async def test_get_many_leaves_out_missing_ids(self) -> None:
        result = await self._reader().get_many(["ITEM-2", "ITEM-404", "ITEM-1"])
        assert set(result) == {"ITEM-1", "ITEM-2"}

    async def test_empty_reader_and_empty_request(self) -> None:
        reader = FakeSourceReader()
        assert await reader.get("ITEM-1") is None
        assert await reader.get_many([]) == {}

    async def test_can_fail(self) -> None:
        reader = self._reader()
        reader.fail_with = UpstreamTimeoutError()
        with pytest.raises(UpstreamTimeoutError):
            await reader.get("ITEM-1")
        with pytest.raises(UpstreamTimeoutError):
            await reader.get_many(["ITEM-1"])
