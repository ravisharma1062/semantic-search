"""Index versions behind an alias (HLD sections 5 and 13, runbook "switch the alias").

Chunk index changes always create a new versioned index. The alias moves to it in one atomic step,
and the previous index is kept for at least ``store.min_index_age_days`` for a rollback. Only
indices of this service can be touched: the existing document index is never changed (rule 9).
"""

import time
from collections.abc import Awaitable, Callable
from typing import Any

from elasticsearch import AsyncElasticsearch, NotFoundError

from app.core.errors import NonRetryableError
from app.core.settings import Settings
from app.store.templates import (
    chunk_index_template,
    chunk_template_name,
    state_index_template,
    state_template_name,
)

_MS_PER_DAY = 86_400_000


def ensure_managed(name: str, settings: Settings) -> None:
    """Only chunk index versions (and the state index) may be managed here."""
    prefix = settings.store.chunk_index_prefix
    if not name.startswith(f"{prefix}_v"):
        raise NonRetryableError("Not an index of this service: refusing to touch it")


async def install_templates(client: AsyncElasticsearch, settings: Settings) -> None:
    """Create or update the chunk and state index templates."""
    store = settings.store
    chunk = chunk_index_template(settings.embedding.dims, store)
    await client.indices.put_index_template(
        name=chunk_template_name(store.chunk_index_prefix), **chunk
    )
    state = state_index_template(settings.elasticsearch.state_index, store)
    await client.indices.put_index_template(
        name=state_template_name(settings.elasticsearch.state_index), **state
    )


async def create_chunk_index(client: AsyncElasticsearch, name: str, settings: Settings) -> bool:
    """Create a chunk index version from the template. False if it already exists."""
    ensure_managed(name, settings)
    if await client.indices.exists(index=name):
        return False
    await client.indices.create(index=name)
    return True


async def create_state_index(client: AsyncElasticsearch, settings: Settings) -> bool:
    """Create the state index from its template. False if it already exists."""
    name = settings.elasticsearch.state_index
    if await client.indices.exists(index=name):
        return False
    await client.indices.create(index=name)
    return True


async def alias_targets(client: AsyncElasticsearch, alias: str) -> list[str]:
    """The indices the alias points to now."""
    try:
        response = await client.indices.get_alias(name=alias)
    except NotFoundError:
        return []
    return sorted(response.body)


async def switch_alias(
    client: AsyncElasticsearch,
    alias: str,
    new_index: str,
    settings: Settings,
    *,
    allow_empty: bool = False,
) -> list[str]:
    """Point the alias at ``new_index`` in one atomic step. Returns the indices it left."""
    ensure_managed(new_index, settings)
    if not await client.indices.exists(index=new_index):
        raise NonRetryableError("The new index does not exist")
    if not allow_empty:
        count = await client.count(index=new_index)
        if count["count"] == 0:
            raise NonRetryableError("The new index is empty: check the re-index first")
    previous = [name for name in await alias_targets(client, alias) if name != new_index]
    actions: list[dict[str, Any]] = [
        {"remove": {"index": name, "alias": alias}} for name in previous
    ]
    actions.append({"add": {"index": new_index, "alias": alias}})
    await client.indices.update_aliases(actions=actions)
    return previous


async def delete_chunk_index(
    client: AsyncElasticsearch,
    name: str,
    alias: str,
    settings: Settings,
    *,
    force: bool = False,
    now_ms: Callable[[], float] = lambda: time.time() * 1000,
) -> None:
    """Delete an old index version. Refuses the live one, and one that is too young to need
    for a rollback, unless ``force``."""
    ensure_managed(name, settings)
    if name in await alias_targets(client, alias):
        raise NonRetryableError("The index is behind the alias: switch the alias first")
    if not force:
        info = await client.indices.get_settings(index=name, name="index.creation_date")
        created = float(info[name]["settings"]["index"]["creation_date"])
        age_days = (now_ms() - created) / _MS_PER_DAY
        if age_days < settings.store.min_index_age_days:
            raise NonRetryableError("The index is too young to delete: keep it for a rollback")
    await client.indices.delete(index=name)


Operation = Callable[[], Awaitable[Any]]
