# Runbook 3: replay the dead-letter topic

## When

`DlqGrowth` fired, or `backfill status` shows FAILED documents. A message ends in the DLQ after its
retries, or at once if the error cannot be fixed by retrying (for example a document that is too
large). The DLQ holds the original event (only an `ITEM_ID` and metadata), never document text.

## 1. Find the cause

- Logs: `event_sent_to_dlq` lines have `item_id`, `reason` and the error type. The state record of the
  document (`last_error`) has the same.
- Group by reason without changing anything:

```bash
python -m app.jobs.dlq_replay --dry-run
```

It prints how many messages there are and why they failed (`retries exhausted`,
`non-retryable error`, `invalid event`). A dry run uses its own consumer group, so it can be repeated.

- Fix the cause first: a model server that was down, Elasticsearch rejections, a wrong source field
  name (Java team), a document that needs a limit changed. Replaying before the fix only fills the
  DLQ again.

## 2. Replay

```bash
python -m app.jobs.dlq_replay --max 1000      # try a small batch first
python -m app.jobs.dlq_replay                 # the rest
```

Each valid event is published again to the live topic with its original key and value (header
`x-replayed: 1`). Offsets are committed after the publish, so a crash can send a few events twice,
never lose one, and processing is idempotent. Messages that are not valid events are counted as
`invalid_skipped` and not replayed (they would only fail again). Nothing is deleted from the DLQ.

The group `<consumer_group>-dlq-replay` remembers where the replay stopped. To read everything again
use a new group: `--group dlq-replay-2`.

## 3. Check

- `semsearch_ingestion_events_total{result="indexed"}` rises, `semsearch_ingestion_dlq_total` does not.
- `python -m app.jobs.cli backfill status`: FAILED goes down.
- Documents that stay FAILED after a replay: `python -m app.jobs.cli reconcile` republishes FAILED,
  PENDING and out-of-date documents, and removes chunks of deleted ones.

## Who

L2. The Java team if the source documents or field names are the problem.
