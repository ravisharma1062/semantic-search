# Runbook 2: pause, resume and throttle backfill

The commands and the wave model are in [backfill.md](backfill.md). This page is the decision guide.

## When

Alerts `ConsumerLagGrowing`, `BulkItemFailures`, `ElasticsearchRejections`, `GpuMemoryHigh`, or search
latency up while a wave runs (backfill must never slow down live search or live indexing).

## Steps, from the lightest to the hardest

1. **Throttle.** Lower `backfill.rate_per_second` (`APP_BACKFILL__RATE_PER_SECOND`) and start the job
   again. It continues from its saved cursor.
2. **Pause the job.** `python -m app.jobs.cli backfill pause --job-id backfill-wave1`. The job stops
   after the page it is on and keeps its cursor. Events already in Kafka are still processed.
3. **Stop the consumers of the backfill topic.** Set `ingestion.backfill_consumer_enabled=false`
   (`APP_INGESTION__BACKFILL_CONSUMER_ENABLED=false`) and roll the workers. Backfill events stay in
   Kafka, live updates continue. Switch it back on to continue.
4. **Scale the workers down** only if live indexing must also stop (`kubectl scale`, or KEDA
   `minReplicaCount`). Kafka keeps the events.

Elasticsearch side: during a wave the refresh interval is longer and replicas are fewer, and they are
restored after it (see the backfill notes and `docs/performance.md`).

## Resume

Reverse the steps: consumers on, `backfill resume --job-id ...`, rate back up in steps. Watch lag,
bulk failures and search p95 for 15 minutes after each step.

## Start a wave through the API

`POST /v1/admin/reindex {"wave": 1}` queues the job (status `REQUESTED`). A job process starts it:
`python -m app.jobs.cli backfill run-requested` (a Kubernetes Job with `batch.command` set to it).
`GET /v1/admin/reindex/backfill-wave1` shows the progress. The API never runs a scan itself.

## Who

L2. Involve the search platform team if Elasticsearch shows rejections or disk watermarks.
