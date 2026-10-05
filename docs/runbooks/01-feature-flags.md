# Runbook 1: turn semantic search or answers on and off

## Effect

| Flag | Off means |
| --- | --- |
| `feature_flags.semantic_search` | `/v1/search` answers `503 UPSTREAM_UNAVAILABLE`. The Java app uses its own keyword search. |
| `feature_flags.rag` | `/v1/answer` and `/v1/answer/stream` answer `503 UPSTREAM_UNAVAILABLE`. Search is not affected. |
| `reranker.enabled` | Searches that do not ask for `rerank` skip it. A request with `rerank: true` still uses it. |

Switching off is the fastest safe action in an incident: nothing is lost, the Java app falls back by
itself, and indexing continues.

## Switch off

```bash
helm upgrade semantic-search deploy/helm/semantic-search -n semantic-search --reuse-values \
  --set-string config.APP_FEATURE_FLAGS__RAG=false
# for search:   --set-string config.APP_FEATURE_FLAGS__SEMANTIC_SEARCH=false
```

The ConfigMap changes, the API pods roll one after another (the Helm chart puts a config checksum on
the pods) and the PodDisruptionBudget keeps capacity. It takes about as long as a rolling restart.

## Check

- `semsearch_http_requests_total{status="503"}` rises for the route, then the Java app's fallback rate
  rises. That is expected.
- A test call returns `503` with `UPSTREAM_UNAVAILABLE`.
- Indexing is unaffected: workers keep running (`semsearch_ingestion_events_total`).

## Switch on

Set the flag back. For a first release use the canary (see `deploy.md`). Release `rag` separately and
after search is stable (HLD section 20).

## Who

L2 on-call decides. Tell the Java team (they see the fallback) and the business owner.
