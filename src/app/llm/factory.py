"""Builds the LLM client chosen by settings (rule 5: pipeline code never picks a provider)."""

import httpx

from app.core.breaker import CircuitBreaker
from app.core.errors import NonRetryableError
from app.core.http import JsonHttpClient
from app.core.retry import RetryPolicy
from app.core.settings import LlmSettings
from app.llm.base import LLMClient
from app.llm.guarded import GuardedLLM
from app.llm.openai_compat import OpenAICompatibleLLM


def create_llm_http_client(settings: LlmSettings) -> httpx.AsyncClient:
    """The HTTP client for the LLM. The egress proxy applies to OpenAI only."""
    proxy = settings.proxy if settings.provider == "openai" else None
    return httpx.AsyncClient(proxy=proxy)


def create_llm(
    settings: LlmSettings, http_client: httpx.AsyncClient, retry: RetryPolicy
) -> LLMClient:
    """The LLM for ``llm.provider`` with a breaker around it. OpenAI needs ``allow_external``."""
    http = JsonHttpClient(http_client, retry)
    if settings.provider == "openai":
        if not settings.allow_external:
            raise NonRetryableError(
                "OpenAI LLM is off: set llm.allow_external after security approval"
            )
        if settings.openai_api_key is None:
            raise NonRetryableError("OpenAI LLM needs an API key")
        inner = OpenAICompatibleLLM(
            settings,
            http,
            http_client,
            url=f"{settings.openai_base_url.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {settings.openai_api_key.get_secret_value()}"},
        )
    else:
        inner = OpenAICompatibleLLM(
            settings, http, http_client, url=f"{settings.endpoint.rstrip('/')}/v1/chat/completions"
        )
    breaker = CircuitBreaker(settings.breaker_failures, settings.breaker_cooldown_s)
    return GuardedLLM(
        inner, breaker, settings.timeout_s + 0.5, settings.first_token_timeout_s + 0.25
    )
