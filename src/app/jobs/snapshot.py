"""Snapshots of the chunk and state indices (HLD section 17, "Backup and recovery"; runbook 7).

    python -m app.jobs.snapshot check
    python -m app.jobs.snapshot create
    python -m app.jobs.snapshot list
    python -m app.jobs.snapshot prune
    python -m app.jobs.snapshot restore --snapshot semsearch-20260101t020000z

A snapshot holds the indices behind the read alias and the state index, not the cluster state and
not the existing document index (that one belongs to the existing application and has its own
backup). ``restore`` never writes over a live index: every restored index gets a prefix
(``restored_``). The operator then checks the document counts and moves the alias with
``app.jobs.index_admin switch-alias`` (runbook 7). The repository (object storage) is registered by
the platform team: ``check`` verifies that it works.
"""

import argparse
import asyncio
import sys
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

import structlog
from elasticsearch import AsyncElasticsearch, NotFoundError
from pydantic import BaseModel

from app.core.errors import AppError, NonRetryableError
from app.core.retry import RetryPolicy
from app.core.settings import Settings, get_settings
from app.store.aliases import alias_targets
from app.store.calls import guarded
from app.store.client import create_es_client

_log = structlog.get_logger(__name__)
_NAME_PREFIX = "semsearch-"


class SnapshotInfo(BaseModel):
    """What the cluster says about one snapshot."""

    name: str
    state: str
    indices: list[str]
    failed_shards: int = 0
    started_at: datetime | None = None


def _info(raw: dict[str, Any]) -> SnapshotInfo:
    started = raw.get("start_time_in_millis")
    shards = raw.get("shards") or {}
    return SnapshotInfo(
        name=str(raw["snapshot"]),
        state=str(raw.get("state", "UNKNOWN")),
        indices=sorted(str(i) for i in raw.get("indices", [])),
        failed_shards=int(shards.get("failed", 0)),
        started_at=datetime.fromtimestamp(started / 1000, UTC) if started else None,
    )


class SnapshotManager:
    """Creates, lists, prunes and restores snapshots."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        settings: Settings,
        retry: RetryPolicy | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._settings = settings
        self._cfg = settings.snapshot
        self._retry = retry or settings.retry
        self._clock = clock
        self._sleep = sleep
        # A snapshot call waits for the snapshot to finish, which takes far longer than a search.
        self._client = client.options(request_timeout=self._cfg.wait_timeout_s)
        self._meta = client

    async def check(self) -> None:
        """Verify that the repository exists and that every node can write to it."""

        async def run() -> None:
            try:
                await self._meta.snapshot.verify_repository(name=self._cfg.repository)
            except NotFoundError as exc:
                raise NonRetryableError("The snapshot repository is not registered") from exc

        await guarded(run, self._retry, self._sleep)

    async def indices_to_save(self) -> list[str]:
        """The chunk indices behind the read alias, and the state index."""
        targets = await alias_targets(self._meta, self._settings.search.index_alias)
        if not targets:
            raise NonRetryableError("The read alias points nowhere: nothing to snapshot")
        return [*targets, self._settings.elasticsearch.state_index]

    async def create(self) -> SnapshotInfo:
        """Take a snapshot and wait for it. A snapshot that is not SUCCESS is an error."""
        indices = await self.indices_to_save()
        # Elasticsearch accepts lowercase snapshot names only.
        name = f"{_NAME_PREFIX}{self._clock().strftime('%Y%m%dt%H%M%Sz')}"

        async def run() -> dict[str, Any]:
            response = await self._client.snapshot.create(
                repository=self._cfg.repository,
                snapshot=name,
                indices=",".join(indices),
                include_global_state=False,
                wait_for_completion=True,
            )
            return dict(response.body["snapshot"])

        info = _info(await guarded(run, self._retry, self._sleep))
        _log.info("snapshot_created", snapshot=info.name, state=info.state, indices=info.indices)
        if info.state != "SUCCESS" or info.failed_shards:
            raise NonRetryableError(f"Snapshot {name} is {info.state}, not SUCCESS")
        return info

    async def snapshots(self) -> list[SnapshotInfo]:
        """Our snapshots, newest first."""

        async def run() -> list[dict[str, Any]]:
            response = await self._meta.snapshot.get(
                repository=self._cfg.repository, snapshot=f"{_NAME_PREFIX}*"
            )
            return list(response.body["snapshots"])

        found = [_info(raw) for raw in await guarded(run, self._retry, self._sleep)]
        return sorted(found, key=lambda s: s.name, reverse=True)

    async def prune(self) -> list[str]:
        """Delete the oldest snapshots beyond ``snapshot.keep_last``. A failed snapshot is never
        counted as one of the kept ones. Returns the deleted names."""
        good = [s for s in await self.snapshots() if s.state == "SUCCESS"]
        doomed = [s.name for s in good[self._cfg.keep_last :]]
        for name in doomed:

            async def run(name: str = name) -> None:
                await self._meta.snapshot.delete(repository=self._cfg.repository, snapshot=name)

            await guarded(run, self._retry, self._sleep)
            _log.info("snapshot_deleted", snapshot=name)
        return doomed

    async def restore(self, snapshot: str, indices: Sequence[str] | None = None) -> list[str]:
        """Restore indices of a snapshot under new names (``restore_prefix`` + the old name).
        Returns the new names. Nothing live is overwritten."""
        known = {s.name: s for s in await self.snapshots()}
        if snapshot not in known:
            raise NonRetryableError("No such snapshot")
        wanted = list(indices) if indices else known[snapshot].indices
        missing = set(wanted) - set(known[snapshot].indices)
        if missing:
            raise NonRetryableError("The snapshot does not hold the requested indices")
        prefix = self._cfg.restore_prefix

        async def run() -> None:
            await self._client.snapshot.restore(
                repository=self._cfg.repository,
                snapshot=snapshot,
                indices=",".join(wanted),
                rename_pattern="(.+)",
                rename_replacement=f"{prefix}$1",
                include_global_state=False,
                include_aliases=False,
                wait_for_completion=True,
            )

        await guarded(run, self._retry, self._sleep)
        restored = [f"{prefix}{name}" for name in wanted]
        _log.info("snapshot_restored", snapshot=snapshot, indices=restored)
        return restored

    async def counts(self, indices: Sequence[str]) -> dict[str, int]:
        """Document counts, to compare a restored index with what was expected."""
        out: dict[str, int] = {}
        for name in indices:

            async def run(name: str = name) -> int:
                return int((await self._meta.count(index=name))["count"])

            out[name] = await guarded(run, self._retry, self._sleep)
        return out


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(prog="snapshot", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="verify the repository")
    sub.add_parser("create", help="snapshot the chunk and state indices")
    sub.add_parser("list", help="show the snapshots")
    sub.add_parser("prune", help="delete snapshots beyond keep_last")
    restore = sub.add_parser("restore", help="restore under new names (nothing is overwritten)")
    restore.add_argument("--snapshot", required=True)
    restore.add_argument("--index", action="append", help="only this index (repeatable)")
    return parser


async def run_command(args: argparse.Namespace, manager: SnapshotManager) -> str:
    """Run one command and return what to print."""
    nl = chr(10)
    match args.command:
        case "check":
            await manager.check()
            return "repository ok"
        case "create":
            info = await manager.create()
            return f"{info.name}: {info.state}, {len(info.indices)} indices"
        case "list":
            return nl.join(
                f"{s.name}  {s.state}  {','.join(s.indices)}" for s in await manager.snapshots()
            )
        case "prune":
            deleted = await manager.prune()
            return (
                f"deleted {len(deleted)}: {', '.join(deleted)}" if deleted else "nothing to delete"
            )
        case "restore":
            restored = await manager.restore(args.snapshot, args.index)
            counts = await manager.counts(restored)
            lines = [f"{name}: {count} documents" for name, count in counts.items()]
            lines.append(
                "Check the counts, then move the alias (runbook 7). Nothing was overwritten."
            )
            return nl.join(lines)
    raise AssertionError(args.command)


async def _main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    client = create_es_client(settings.elasticsearch)
    try:
        print(await run_command(args, SnapshotManager(client, settings)))
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
