"""A Redis cache for results that depend on the user's rights (task T2.4, HLD sections 7 and 11).

The key always contains the access scope of the user (``AclFilter.scope_key``), so a result cached
for one set of rights is never served to another. The key holds hashes only, never the question, the
user ID or document text. Redis is an optimization: every call has a short timeout, errors count as
a miss, and a circuit breaker stops a dead Redis from slowing every request.
"""

import asyncio
import hashlib
from collections.abc import Awaitable
from typing import Any, Protocol

import structlog

from app.core.breaker import BreakerOpenError, CircuitBreaker
from app.core.errors import UpstreamUnavailableError
from app.retrieval.acl import AclFilter

_log = structlog.get_logger(__name__)


class TextRedis(Protocol):
    """The two Redis calls used. ``redis.asyncio.Redis`` fits."""

    def get(self, name: str) -> Awaitable[Any]:
        """The stored value, or ``None``."""
        ...

    def set(self, name: str, value: str, ex: int | None = None) -> Awaitable[Any]:
        """Store a value that expires after ``ex`` seconds."""
        ...


class ScopedCache:
    """Cached text values per access scope."""

    def __init__(
        self,
        redis: TextRedis,
        *,
        namespace: str,
        ttl_s: int,
        timeout_s: float,
        breaker: CircuitBreaker,
    ) -> None:
        self._redis = redis
        self._namespace = namespace
        self._ttl_s = ttl_s
        self._timeout_s = timeout_s
        self._breaker = breaker

    async def _redis_call(self, call: Awaitable[Any]) -> Any:
        """One Redis call with a timeout. Any Redis error becomes ours, so the breaker counts it."""
        try:
            return await asyncio.wait_for(call, self._timeout_s)
        except TimeoutError:
            raise
        except Exception as exc:
            raise UpstreamUnavailableError("Redis failed") from exc

    def key(self, scope: AclFilter, *parts: str) -> str:
        """``namespace:scope:hash(parts)``. The parts (query, filters, model, prompt) are hashed."""
        digest = hashlib.sha256("\x1f".join(parts).encode()).hexdigest()
        return f"{self._namespace}:{scope.scope_key()}:{digest}"

    async def get(self, scope: AclFilter, *parts: str) -> str | None:
        """The cached value for exactly this scope, or ``None`` on a miss or any problem."""
        key = self.key(scope, *parts)
        try:
            raw = await self._breaker.call(lambda: self._redis_call(self._redis.get(key)))
        except BreakerOpenError:
            return None
        except (Exception, TimeoutError) as exc:
            _log.warning("scoped_cache_read_failed", error_type=type(exc).__name__)
            return None
        if isinstance(raw, bytes):
            return raw.decode(errors="replace")
        return raw if isinstance(raw, str) else None

    async def set(self, scope: AclFilter, value: str, *parts: str) -> None:
        """Store the value for this scope. Problems are ignored."""
        key = self.key(scope, *parts)
        try:
            await self._breaker.call(
                lambda: self._redis_call(self._redis.set(key, value, ex=self._ttl_s))
            )
        except BreakerOpenError:
            return
        except (Exception, TimeoutError) as exc:
            _log.warning("scoped_cache_write_failed", error_type=type(exc).__name__)
