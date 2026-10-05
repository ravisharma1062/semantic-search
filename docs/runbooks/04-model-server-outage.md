# Runbook 4: model server outage or GPU loss

## What the service does by itself

| Dependency down | Search | Answers | Indexing |
| --- | --- | --- | --- |
| Embedding server | Falls back to keyword search inside the service (`mode_used` is `bm25`). Vector-only requests fail with 503 | Same as search, the answer is built from keyword hits | Workers retry with backoff, then the retry topic. Events wait in Kafka |
| Reranker | Skipped. The RRF order is returned, `mode_used` has no `+rerank`. A circuit breaker stops waiting on a dead server | Same | Not used |
| LLM | Not used | `503 UPSTREAM_UNAVAILABLE`, so the Java app uses keyword search. A breaker makes this fast | Not used |
| GPU node loss | Slower embeddings, rerank, answers, then the above | Same | Slower, then retries |

Nothing is returned silently: `mode_used` says what ran, and failures are 503 or 504.

## Detect

Alerts `FallbackRateHigh`, `DependencyBreakerOpen` (label `name`: `reranker` or `llm`),
`ErrorRateHigh`, `RagFirstTokenSlow`, `GpuMemoryHigh`. On the dashboard: "Dependency calls by
outcome" and "Circuit breakers open".

## Act

1. Find which server: `semsearch_upstream_calls_total{dependency=...,outcome!="ok"}`.
2. Check the pods and nodes of the model server (`kubectl get pods,nodes -n <model namespace>`), the
   GPU metrics (DCGM exporter) and the server's own logs.
3. Restart or reschedule the pods. If a GPU node is lost, the platform team replaces it. Several
   replicas per model are required (HLD section 16): check that more than one is running.
4. Reduce the load while it recovers:
   - pause backfill (runbook 2): it competes for the same GPUs,
   - switch off answers (runbook 1) if the LLM is the problem and keyword fallback is acceptable,
   - set `reranker.enabled=false` if the reranker is the problem and you want to stop even the failed
     attempts. Breakers already do this after a few failures.
5. If OpenAI is the LLM provider (`llm.provider=openai`, approved data only): switch the provider
   setting to the in-house LLM, or leave answers off.

## Check recovery

The breaker closes by itself after a trial call succeeds (`semsearch_circuit_breaker_open` goes to 0).
`mode_used` shows `hybrid+rerank` again. Kafka lag falls. Run the evaluation smoke set
(`python -m eval.runner ...`) if a server was replaced, to catch a wrong model version.

## Who

L2 on-call. The ML engineer for model or GPU questions. The platform team for nodes.
