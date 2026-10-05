"""Retrieval metrics: recall@k, MRR and nDCG@k with binary relevance (HLD section 10).

A hit is correct when its document is one of the correct documents and, if pages are given for that
document, the hit's pages overlap them.
"""

import math
from dataclasses import dataclass

from eval.dataset import Relevant


@dataclass(frozen=True)
class RankedHit:
    """One result of a search, in rank order."""

    doc_id: str
    chunk_id: str = ""
    pages: tuple[int, ...] = ()


def matches(hit: RankedHit, item: Relevant) -> bool:
    """Does the hit answer this correct item?"""
    if hit.doc_id != item.doc_id:
        return False
    return not item.pages or bool(set(item.pages) & set(hit.pages))


def _is_correct(hit: RankedHit, relevant: list[Relevant]) -> bool:
    return any(matches(hit, item) for item in relevant)


def recall_at_k(ranked: list[RankedHit], relevant: list[Relevant], k: int) -> float:
    """The share of correct items that some hit in the top ``k`` answers."""
    if not relevant:
        return 0.0
    top = ranked[:k]
    found = sum(1 for item in relevant if any(matches(hit, item) for hit in top))
    return found / len(relevant)


def reciprocal_rank(ranked: list[RankedHit], relevant: list[Relevant]) -> float:
    """``1 / rank`` of the first correct hit, 0 if there is none."""
    for rank, hit in enumerate(ranked, start=1):
        if _is_correct(hit, relevant):
            return 1.0 / rank
    return 0.0


def ndcg_at_k(ranked: list[RankedHit], relevant: list[Relevant], k: int) -> float:
    """Normalized discounted cumulative gain. A document counts once, at its best rank."""
    seen: set[str] = set()
    gains: list[int] = []
    for hit in ranked[:k]:
        correct = _is_correct(hit, relevant) and hit.doc_id not in seen
        gains.append(1 if correct else 0)
        if correct:
            seen.add(hit.doc_id)
    dcg = sum(g / math.log2(i + 2) for i, g in enumerate(gains))
    distinct_docs = len({item.doc_id for item in relevant})
    ideal = sum(1 / math.log2(i + 2) for i in range(min(distinct_docs, k)))
    return dcg / ideal if ideal else 0.0
