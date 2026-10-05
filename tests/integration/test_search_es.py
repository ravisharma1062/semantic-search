"""Search against real Elasticsearch: access isolation, pre-filtering in kNN, the rrf retriever, and
the evaluation smoke run."""

import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from elasticsearch import AsyncElasticsearch
from eval.dataset import load_questions
from eval.runner import ServiceSearch, evaluate

from app.core.errors import AppError
from app.core.retry import RetryPolicy
from app.core.security import Identity
from app.core.settings import (
    ChunkingSettings,
    IngestionSettings,
    NormalizerSettings,
    SearchSettings,
    Settings,
    StoreSettings,
)
from app.ingestion.chunker import Chunker
from app.ingestion.consumer import HandlerContext
from app.ingestion.events import parse_event
from app.ingestion.indexer import ElasticsearchIndexer
from app.ingestion.source import SourceDocument, SourcePage
from app.ingestion.state_store import ElasticsearchStateStore
from app.ingestion.tokens import WhitespaceTokenCounter
from app.ingestion.worker import IndexingWorker
from app.retrieval.filters import SearchFilters
from app.retrieval.query import QueryBuilder
from app.retrieval.searcher import HybridSearcher
from app.retrieval.service import RequestMode, SearchService
from app.store.aliases import create_chunk_index, create_state_index, install_templates
from app.store.client import create_es_client
from app.store.templates import physical_chunk_index_name
from tests.fakes import FakeEmbedder, FakeSourceReader
from tests.fakes.search_es import chunk_doc

pytestmark = [pytest.mark.integration, pytest.mark.access]

DIMS = 8
ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Env:
    settings: Settings
    client: AsyncElasticsearch
    index: str

    def service(self, **search: Any) -> SearchService:
        s = self.settings.model_copy(
            update={
                "search": self.settings.search.model_copy(
                    update={"index_alias": self.index, **search}
                )
            }
        )
        searcher = HybridSearcher(
            self.client,
            s.search.index_alias,
            QueryBuilder(s.search),
            s.search,
            s.elasticsearch,
            RetryPolicy(attempts=1),
        )
        return SearchService(embedder=FakeEmbedder(dims=DIMS), searcher=searcher, settings=s)


async def _make_env(url: str) -> Env:
    suffix = uuid.uuid4().hex[:8]
    base = Settings()
    settings = base.model_copy(
        update={
            "elasticsearch": base.elasticsearch.model_copy(
                update={"hosts": [url], "state_index": f"state_{suffix}"}
            ),
            "store": StoreSettings(
                chunk_index_prefix=f"ch{suffix}", shards=1, replicas=0, refresh_interval="1s"
            ),
            "embedding": base.embedding.model_copy(update={"dims": DIMS}),
            "search": SearchSettings(
                index_alias=f"alias_{suffix}", source_index="src", candidates=20
            ),
        }
    )
    client = create_es_client(settings.elasticsearch)
    await install_templates(client, settings)
    index = physical_chunk_index_name(f"ch{suffix}", "v1", "bge-m3")
    await create_chunk_index(client, index, settings)
    return Env(settings, client, index)


@pytest.fixture
async def env(es_url: str) -> AsyncIterator[Env]:
    e = await _make_env(es_url)
    yield e
    await e.client.close()


@pytest.fixture
async def trial_env(es_trial_url: str) -> AsyncIterator[Env]:
    e = await _make_env(es_trial_url)
    yield e
    await e.client.close()


def _vector(*values: float) -> list[float]:
    padded = [*values, *([0.0] * (DIMS - len(values)))]
    return padded[:DIMS]


async def _index(env: Env, docs: list[dict[str, Any]]) -> None:
    for doc in docs:
        await env.client.index(index=env.index, id=doc["chunk_id"], document=doc)
    await env.client.indices.refresh(index=env.index)


CORPUS: dict[str, tuple[dict[str, Any], str]] = {
    "LEGAL": ({"groups": ["g-legal"]}, "legalsecret"),
    "HR": ({"groups": ["g-hr"]}, "hrsecret"),
    "CAROL": ({"users": ["carol"]}, "carolsecret"),
    "ALICE": ({"users": ["alice"]}, "alicesecret"),
    "NOBODY": ({}, "nobodysecret"),
    "SHARED": ({"groups": ["g-legal", "g-hr"], "users": ["carol"]}, "sharedsecret"),
}
USERS = {
    "alice": Identity("alice", ("g-legal",), "svc"),
    "bob": Identity("bob", ("g-hr",), "svc"),
    "carol": Identity("carol", (), "svc"),
    "dave": Identity("dave", ("g-legal", "g-hr"), "svc"),
    "eve": Identity("eve", (), "svc"),
}
VISIBLE = {
    "alice": {"LEGAL", "ALICE", "SHARED"},
    "bob": {"HR", "SHARED"},
    "carol": {"CAROL", "SHARED"},
    "dave": {"LEGAL", "HR", "SHARED"},
    "eve": set(),
}


def _corpus_docs() -> list[dict[str, Any]]:
    return [
        chunk_doc(
            f"{name}:0",
            name,
            f"quarterly report {secret} appendix",
            users=acl.get("users"),
            groups=acl.get("groups"),
            embedding=_vector(1.0, 0.1),
        )
        for name, (acl, secret) in CORPUS.items()
    ]


async def _isolation_check(service: SearchService, mode: RequestMode) -> None:
    for user, identity in USERS.items():
        result = await service.search(
            query="quarterly report appendix", identity=identity, mode=mode, top_k=50
        )
        assert {h.doc_id for h in result.hits} == VISIBLE[user], (user, mode)
        answer = json.dumps(
            [[h.snippet, h.highlights, h.doc_id, h.chunk_id, h.content] for h in result.hits]
        ).lower()
        for name, (_, secret) in CORPUS.items():
            if name not in VISIBLE[user]:
                assert secret not in answer, (user, mode, secret)


@pytest.mark.parametrize("mode", ["bm25", "hybrid", "vector"])
async def test_every_user_gets_exactly_their_documents_on_real_elasticsearch(
    env: Env, mode: RequestMode
) -> None:
    await _index(env, _corpus_docs())
    await _isolation_check(env.service(), mode)


async def test_the_same_holds_with_the_rrf_retriever_on_a_trial_license(trial_env: Env) -> None:
    await _index(trial_env, _corpus_docs())
    service = trial_env.service(rrf_mode="retriever")
    await _isolation_check(service, "hybrid")
    searcher = service._searcher
    assert searcher._retriever_unsupported is False  # the real retriever answered, no fallback


async def test_a_basic_license_falls_back_to_the_python_merge_and_stays_isolated(env: Env) -> None:
    await _index(env, _corpus_docs())
    service = env.service(rrf_mode="retriever")
    await _isolation_check(service, "hybrid")
    assert service._searcher._retriever_unsupported is True  # the cluster has no rrf retriever


async def test_knn_filters_before_it_picks_neighbours(env: Env) -> None:
    """Pre-filter, not post-filter (HLD section 9): forbidden documents that are much closer to the
    query must not use up the candidates and push the allowed ones out."""
    near = [
        chunk_doc(
            f"FORBIDDEN-{i}:0",
            f"FORBIDDEN-{i}",
            f"forbidden {i}",
            users=["other"],
            embedding=_vector(1.0, 0.001 * i),
        )
        for i in range(30)
    ]
    far = [
        chunk_doc(
            f"ALLOWED-{i}:0",
            f"ALLOWED-{i}",
            f"allowed {i}",
            users=["alice"],
            embedding=_vector(0.0, 1.0, 0.1 * i),
        )
        for i in range(3)
    ]
    await _index(env, near + far)
    service = env.service(candidates=5)
    result = await service.search(
        query="anything", identity=Identity("alice", (), "svc"), mode="vector", top_k=5
    )
    assert {h.doc_id for h in result.hits} == {"ALLOWED-0", "ALLOWED-1", "ALLOWED-2"}


async def test_filters_and_highlights_work_and_the_snippet_is_plain_text(env: Env) -> None:
    docs = [
        chunk_doc(
            "A:0",
            "A",
            "the <b>penalty</b> for late delivery & more",
            users=["alice"],
            doc_type="contract",
            tags=["vendor"],
            pages=(4, 5),
            embedding=_vector(1.0),
        ),
        chunk_doc(
            "B:0",
            "B",
            "penalty in an invoice",
            users=["alice"],
            doc_type="invoice",
            embedding=_vector(1.0),
        ),
    ]
    await _index(env, docs)
    service = env.service()
    alice = Identity("alice", (), "svc")
    only_contracts = await service.search(
        query="penalty", identity=alice, mode="bm25", filters=SearchFilters(doc_type=["contract"])
    )
    assert [h.doc_id for h in only_contracts.hits] == ["A"]
    hit = only_contracts.hits[0]
    assert "penalty" in hit.highlights
    assert "<em>" not in hit.snippet
    assert hit.pages == [4, 5]
    tagged = await service.search(
        query="penalty", identity=alice, mode="bm25", filters=SearchFilters(tags=["vendor"])
    )
    assert [h.doc_id for h in tagged.hits] == ["A"]


async def test_a_missing_index_is_an_upstream_error_for_the_fallback(env: Env) -> None:
    service = env.service(index_alias="no-such-index")
    with pytest.raises(AppError):
        await service.search(query="x", identity=Identity("alice", (), "svc"), mode="bm25")


# --- the evaluation smoke run (task T2.5) ----------------------------------------------------


async def _index_smoke_corpus(env: Env) -> None:
    lines = (ROOT / "eval/smoke_corpus.jsonl").read_text(encoding="utf-8").splitlines()
    documents = [
        SourceDocument(
            item_id=row["item_id"],
            pages=[
                SourcePage(page_no=n, text=text) for n, text in enumerate(row["pages"], start=1)
            ],
            doc_type=row["doc_type"],
            acl_groups=row["acl_groups"],
            version=1,
        )
        for row in map(json.loads, lines)
    ]
    s = env.settings
    chunking = ChunkingSettings(
        version="v1",
        target_tokens=40,
        max_tokens=60,
        overlap_tokens=8,
        min_tokens=5,
        tokenizer="whitespace",
    )
    embedder = FakeEmbedder(dims=DIMS, model_name="bge-m3@1")
    worker = IndexingWorker(
        source=FakeSourceReader(documents),
        indexer=ElasticsearchIndexer(env.client, env.index, s.store, s.elasticsearch, s.retry),
        states=ElasticsearchStateStore(
            env.client, s.elasticsearch.state_index, s.elasticsearch, s.retry
        ),
        embedder=embedder,
        chunker=Chunker(chunking, WhitespaceTokenCounter()),
        chunking=chunking,
        normalizer=NormalizerSettings(),
        ingestion=IngestionSettings(window_size=16),
        store=s.store,
    )
    await create_state_index(env.client, s)
    for doc in documents:
        event = parse_event(
            json.dumps(
                {
                    "schema_version": 1,
                    "event_id": "e",
                    "event_type": "UPSERT",
                    "item_id": doc.item_id,
                    "doc_version": 1,
                    "occurred_at": "2026-10-05T10:00:00Z",
                    "source": "smoke",
                }
            ).encode()
        )
        await worker.process(event, HandlerContext(0, 3))
    await env.client.indices.refresh(index=env.index)


@pytest.mark.parametrize(
    ("mode", "min_recall", "min_mrr"), [("bm25", 0.9, 0.8), ("hybrid", 0.9, 0.5)]
)
async def test_the_smoke_set_meets_its_quality_gate(
    env: Env, mode: RequestMode, min_recall: float, min_mrr: float
) -> None:
    await _index_smoke_corpus(env)
    questions = load_questions(ROOT / "eval/smoke_set.jsonl")
    report = await evaluate(
        questions, ServiceSearch(env.service(), mode), name="smoke", config={"mode": mode}
    )
    assert report.metrics["recall@10"] >= min_recall, report.metrics
    assert report.metrics["mrr"] >= min_mrr, report.metrics
    service = env.service()
    analyst = Identity("analyst", ("g-all",), "eval")
    result = await service.search(
        query="Orion arbitration settlement ceiling", identity=analyst, mode="bm25"
    )
    assert "DOC-008" not in {h.doc_id for h in result.hits}  # the restricted memo is never found
    lawyer = Identity("lawyer", ("g-legal",), "eval")
    found = await service.search(
        query="Orion arbitration settlement ceiling", identity=lawyer, mode="bm25"
    )
    assert found.hits[0].doc_id == "DOC-008"


# --- RAG on real Elasticsearch ---------------------------------------------------------------


async def test_the_model_only_sees_and_cites_what_each_user_may_read(env: Env) -> None:
    from app.rag.prompts import load_prompt
    from app.rag.service import AnswerService
    from tests.fakes import FakeLLMClient

    await _index(env, _corpus_docs())
    settings = env.settings.model_copy(
        update={"feature_flags": env.settings.feature_flags.model_copy(update={"rag": True})}
    )
    for user, identity in USERS.items():
        llm = FakeLLMClient(["See [1][2][3][4][5][6]."])
        search = env.service()
        service = AnswerService(
            search=search,
            llm=llm,
            prompt=load_prompt(ROOT / "prompts", "answer", "v1"),
            settings=settings,
            counter=WhitespaceTokenCounter(),
        )
        result = await service.answer(question="quarterly report appendix", identity=identity)
        sent = " ".join(m.content for call in llm.calls for m in call).lower()
        for name, (_, secret) in CORPUS.items():
            if name not in VISIBLE[user]:
                assert secret not in sent, (user, secret)
        assert {c.doc_id for c in result.citations} <= VISIBLE[user], user
        if not VISIBLE[user]:
            assert llm.calls == [] and result.reason == "no_context"
