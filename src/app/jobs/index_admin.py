"""Index version tools (HLD runbook "switch the alias"):

    python -m app.jobs.index_admin install-templates
    python -m app.jobs.index_admin create-state-index
    python -m app.jobs.index_admin create-index --version v2 [--model bge-m3]
    python -m app.jobs.index_admin show-alias
    python -m app.jobs.index_admin switch-alias --index doc_chunks_v2_bgem3 [--allow-empty]
    python -m app.jobs.index_admin delete-index --index doc_chunks_v1_bgem3 [--force]

Only chunk index versions and the state index can be changed. The existing document index is
never touched (rule 9).
"""

import argparse
import asyncio
import sys
from collections.abc import Sequence

from elasticsearch import AsyncElasticsearch

from app.core.errors import AppError
from app.core.settings import Settings, get_settings
from app.store.aliases import (
    alias_targets,
    create_chunk_index,
    create_state_index,
    delete_chunk_index,
    install_templates,
    switch_alias,
)
from app.store.client import create_es_client
from app.store.templates import physical_chunk_index_name


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(prog="index_admin", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("install-templates", help="create or update the index templates")
    sub.add_parser("create-state-index", help="create the state index")
    create = sub.add_parser("create-index", help="create a chunk index version")
    create.add_argument("--version", required=True, help="for example v2")
    create.add_argument("--model", help="model name for the index name (default: settings)")
    sub.add_parser("show-alias", help="show where the alias points")
    switch = sub.add_parser("switch-alias", help="point the alias at an index, atomically")
    switch.add_argument("--index", required=True)
    switch.add_argument("--allow-empty", action="store_true")
    delete = sub.add_parser("delete-index", help="delete an old chunk index version")
    delete.add_argument("--index", required=True)
    delete.add_argument("--force", action="store_true", help="ignore the minimum age")
    return parser


async def run_command(
    args: argparse.Namespace, client: AsyncElasticsearch, settings: Settings
) -> str:
    """Run one command and return what to print."""
    alias = settings.search.index_alias
    match args.command:
        case "install-templates":
            await install_templates(client, settings)
            return "templates installed"
        case "create-state-index":
            created = await create_state_index(client, settings)
            return "state index created" if created else "state index already exists"
        case "create-index":
            name = physical_chunk_index_name(
                settings.store.chunk_index_prefix,
                args.version,
                args.model or settings.embedding.model,
            )
            created = await create_chunk_index(client, name, settings)
            return f"{name} created" if created else f"{name} already exists"
        case "show-alias":
            targets = await alias_targets(client, alias)
            return f"{alias} -> {', '.join(targets) or '(nothing)'}"
        case "switch-alias":
            previous = await switch_alias(
                client, alias, args.index, settings, allow_empty=args.allow_empty
            )
            return f"{alias} -> {args.index} (was: {', '.join(previous) or 'nothing'})"
        case "delete-index":
            await delete_chunk_index(client, args.index, alias, settings, force=args.force)
            return f"{args.index} deleted"
    raise AssertionError(args.command)


async def _main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    client = create_es_client(settings.elasticsearch)
    try:
        print(await run_command(args, client, settings))
    except AppError as error:
        print(f"error: {error.message}", file=sys.stderr)
        return 1
    finally:
        await client.close()
    return 0


def main() -> None:
    """Entry point."""
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
