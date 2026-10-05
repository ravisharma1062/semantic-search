# Runbooks

For the support desk (L1), the on-call engineer (L2) and the platform and search teams (L3). The
numbers match HLD section 18. Each runbook says when to use it, what to do, how to check that it
worked, and who to call.

| # | Runbook | Use it when |
| --- | --- | --- |
| 1 | [Turn semantic search or answers on and off](01-feature-flags.md) | Wrong results, an incident, a release |
| 2 | [Pause, resume and throttle backfill](02-backfill-control.md) | Elasticsearch or the model servers are under pressure |
| 3 | [Replay the dead-letter topic](03-dlq-replay.md) | `DlqGrowth` fired, or documents are FAILED |
| 4 | [Model server outage or GPU loss](04-model-server-outage.md) | Fallback or error alerts, slow embeddings, rerank or answers |
| 5 | [Roll out and roll back a model version](05-model-rollout.md) | A new embedding, reranker or LLM version |
| 6 | [Switch the Elasticsearch alias](06-switch-alias.md) | A new chunk index is ready, or the new one is bad |
| 7 | [Recover from a snapshot](07-snapshot-recovery.md) | Index damaged or lost |
| 8 | [Suspected access-control problem](08-access-incident.md) | Someone may have seen what they may not open |
| 9 | [Rotate secrets and certificates](09-rotate-secrets.md) | Planned rotation or a leak |
| - | [Release and rollback](deploy.md) | Deploying a version |
| - | [Backfill details](backfill.md) | Waves, reconcile, the commands |

Where to look first: the Grafana dashboard `Semantic Search Service`
(`deploy/observability/grafana-dashboard.json`) and the alerts in
`deploy/observability/prometheus-rules.yaml`. Every request has a request ID (`X-Request-Id`, also in
the error body). Logs hold IDs only: user ID, item ID, chunk IDs, never text.

Commands that start with `python -m app...` run inside a pod of the image (`kubectl exec` into an
API pod, or a Helm batch job), with the settings of the environment.
