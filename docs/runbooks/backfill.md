# Runbook: backfill, pause, resume, reconcile

Commands run as `python -m app.jobs.cli ...` (in a pod, or as the Helm `batch.command`).
Waves are defined in `backfill.waves` (config or `APP_BACKFILL__WAVES`).

## Start a wave

```bash
python -m app.jobs.cli backfill start --wave 1
```

The job scans the wave, registers each document as PENDING in the state index, and publishes
`UPSERT` events to the backfill topic. The worker processes them in its own consumer group, with
fewer documents in parallel than live events (`ingestion.backfill_max_in_flight`).

## Watch it

```bash
python -m app.jobs.cli backfill status
```

Shows counts per wave and status (PENDING, INDEXED, SKIPPED, FAILED, DELETED) and the jobs. Also watch:
Kafka lag of the backfill group, GPU queue length of the embedding server, Elasticsearch rejected requests.

## Slow down or pause

- Slower: lower `APP_BACKFILL__RATE_PER_SECOND` and start the job again (it continues from its cursor).
- Pause: `python -m app.jobs.cli backfill pause --job-id backfill-wave1`. The job stops after the page it
  is working on and keeps its cursor. Stopping the process (SIGTERM) does the same.
- The worker side: events that are already in Kafka are still processed. To stop them too, set
  `APP_INGESTION__BACKFILL_CONSUMER_ENABLED=false` (the backfill topic is not read, live updates continue), or
  scale the worker down. See [runbook 2](02-backfill-control.md).

## Resume

```bash
python -m app.jobs.cli backfill resume --job-id backfill-wave1
```

`--restart` on `start` scans the wave from the beginning. Documents that are indexed already are skipped
cheaply (`skip_up_to_date`).

## Reconcile

```bash
python -m app.jobs.cli reconcile --wave 1 --max-items 100000
```

Finds documents whose state is missing, FAILED, PENDING, of another version, model or chunker, or whose
permissions differ, and records whose document is gone. It republishes events: UPSERT to the backfill
topic, ACL_CHANGE and DELETE to the live topic (higher priority). Run it nightly for one wave or a sample.
It prints the counts. It is safe to run again.

## Failed documents

A document that fails all retries ends in the DLQ and has state FAILED with the error type in
`last_error`. Fix the cause, then `reconcile` republishes FAILED documents, or replay the DLQ
(`python -m app.jobs.dlq_replay`, [runbook 3](03-dlq-replay.md)).

## Through the admin API

`POST /v1/admin/reindex {"wave": 1}` queues a job (status `REQUESTED`) and `GET /v1/admin/reindex/{jobId}`
shows it. A job process starts the queued jobs: `python -m app.jobs.cli backfill run-requested`.
