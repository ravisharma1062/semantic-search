"""Builds the reranker chosen by settings (rule 5)."""

import httpx

from app.core.breaker import CircuitBreaker
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import RerankerSettings
from app.rerank.base import Reranker
from app.rerank.guarded import GuardedReranker
from app.rerank.inhouse import InHouseReranker


def create_reranker(
    settings: RerankerSettings, http_client: httpx.AsyncClient, retry: RetryPolicy
) -> Reranker:
    """The in-house reranker with a timeout and a circuit breaker around it."""
    inner = InHouseReranker(settings, JsonHttpClient(http_client, retry))
    breaker = CircuitBreaker(settings.breaker_failures, settings.breaker_cooldown_s)
    return GuardedReranker(inner, breaker, settings.timeout_s + 0.1)
