"""Fake model server for local development. Never use it outside a developer machine.

Mimics the shape of Hugging Face TEI (``/embed``, ``/rerank``) and an OpenAI-style chat
endpoint. The real contracts are fixed in tasks T1.5 (embeddings), T3.1 (reranker) and
T4.2 (LLM), and this file follows them then. Vectors come from a hash of the text, so the
same text always gives the same vector. Texts are never logged.
"""

import hashlib
import math
import os

from fastapi import FastAPI
from pydantic import BaseModel

DIMS = int(os.environ.get("FAKE_DIMS", "1024"))

app = FastAPI(title="Fake model server")
STATS = {"embed_requests": 0, "embed_texts": 0}


class EmbedRequest(BaseModel):
    inputs: list[str]


class RerankRequest(BaseModel):
    query: str
    texts: list[str]


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    model: str = "fake-llm"
    messages: list[ChatMessage]


def _vector(text: str) -> list[float]:
    digest = hashlib.sha256(text.encode()).digest()
    raw = [digest[i % len(digest)] - 127.5 for i in range(DIMS)]
    norm = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [x / norm for x in raw]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
async def stats() -> dict[str, int]:
    """Counters for tests: how many embedding calls and texts were served."""
    return dict(STATS)


@app.post("/embed")
async def embed(request: EmbedRequest) -> list[list[float]]:
    STATS["embed_requests"] += 1
    STATS["embed_texts"] += len(request.inputs)
    return [_vector(text) for text in request.inputs]


@app.post("/rerank")
async def rerank(request: RerankRequest) -> list[dict[str, float | int]]:
    words = set(request.query.lower().split())
    scored = [
        {"index": i, "score": len(words & set(text.lower().split())) / (len(words) or 1)}
        for i, text in enumerate(request.texts)
    ]
    return sorted(scored, key=lambda item: (-item["score"], item["index"]))


@app.post("/v1/chat/completions")
async def chat(request: ChatRequest) -> dict[str, object]:
    return {
        "model": request.model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "NOT_FOUND"}}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 1},
    }
