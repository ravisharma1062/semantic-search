# Runbook 7: recover from a snapshot

The chunk index can be rebuilt from the source, but a rebuild of hundreds of millions of chunks
takes weeks. Daily snapshots of the chunk index and the state index make recovery take hours.
Proposed targets (to be agreed with the platform team): RPO 24 hours, RTO a few hours.

The snapshot repository (object storage) is registered by the platform team. The service only takes
snapshots and restores them. The existing document index has its own backup and is not part of this.

## Before anything happens: check the setup

```bash
python -m app.jobs.snapshot check      # the repository exists and every node can write to it
python -m app.jobs.snapshot list
```

Run `create` daily (a CronJob with `batch.command: [python, -m, app.jobs.snapshot, create]`), then
`prune` (keeps the newest `snapshot.keep_last`, 14 by default). A snapshot that is not `SUCCESS`
makes the command fail, so the CronJob failure is the alert. Test a restore into a spare name at
least once a quarter: a backup that was never restored is a hope.

## Recover

1. **Stop the damage.** If the index is corrupt or empty and users get bad results, switch
   semantic search off (runbook 1): the Java app uses keyword search meanwhile.
2. **Pick the snapshot:** `python -m app.jobs.snapshot list` (newest first, with the indices in each).
3. **Restore under new names** (nothing live is overwritten):

```bash
python -m app.jobs.snapshot restore --snapshot semsearch-20260101t020000z
# or one index only:  --index doc_chunks_v1_bgem3
```

   Indices come back as `restored_<name>` without an alias, and the command prints their document
   counts. Compare them with what you expect.
4. **Use it.** Chunk index: switch the alias to the restored index (runbook 6). State index: point
   `APP_ELASTICSEARCH__STATE_INDEX` at the restored one and roll the workers.
5. **Catch up.** Everything written after the snapshot is missing. Replay the live topic if Kafka
   still has it, then run `python -m app.jobs.cli reconcile` (per wave): it republishes documents whose
   state is missing or different, and applies ACL changes and deletes. Deletes and permission changes
   have priority (live topic).
6. **Check:** the evaluation smoke set, the dashboard, `backfill status`.
7. Switch semantic search on again.

## Who

L2 with the search platform team (repository, cluster). Tell the business owner about the freshness
gap between the snapshot and now.
