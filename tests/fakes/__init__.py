"""Fakes for unit tests. No network, no Docker, synthetic data only."""

from tests.fakes.embedder import FakeEmbedder
from tests.fakes.llm import FakeLLMClient
from tests.fakes.reranker import FakeReranker
from tests.fakes.source_reader import FakeSourceReader

__all__ = ["FakeEmbedder", "FakeLLMClient", "FakeReranker", "FakeSourceReader"]
