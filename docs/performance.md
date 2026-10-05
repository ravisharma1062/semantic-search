# Performance: load tests and Elasticsearch tuning (T5.2)

**Status: the tools and the method are ready. No load number exists yet.** The numbers need the real
Elasticsearch cluster, the real models on GPUs and a chunk index of realistic size. None of those
were available when this was written, so the tables below are empty on purpose. Do not copy a number
into a decision from this page until a run has filled it.

## Targets (HLD sections 3 and 18)

| What | Target |
| --- | --- |
| Search (`/v1/search`, with rerank) | p95 under 3 s, p99 under the Java timeout |
| Answer, first streamed token | p95 under 3 s |
| Errors without a fallback | under 1% |
| Fallback rate | under 5% (above this the `FallbackRateHigh` alert fires) |
| Index size | tests at 10%, 50% and 100% of the target (100 million documents) |

The target traffic (requests per second at peak) is an input from the business. It is not in the
HLD. Ask for it before the first run and write it into the table.

## The tool

`python -m loadtest.run` sends requests at a **fixed rate** (open loop). A closed loop slows down when
the service slows down, and hides the delay it caused. If the client cannot keep up
(`--max-in-flight`), the extra requests are counted as `dropped`, and a run with dropped requests
never passes: the client was the limit, not the service.

```bash
uv run python -m loadtest.run --api https://<test-env> --token <token> \
  --questions eval/smoke_set.jsonl --endpoint search --rate 50 --duration 300 \
  --label 10pct --p95-max 3.0 --error-rate-max 0.01 --fallback-rate-max 0.05
```

- `--endpoint search | answer | stream`. `stream` also measures the time to the first token
  (`--first-token-p95-max 3.0`).
- Questions: the evaluation set (`question`, `as_user`, `groups`) or a text file. Use synthetic or
  approved test questions only (CLAUDE.md rule 12). The users in the file must exist in the test
  environment with realistic rights: a user with no rights gets an empty fast answer and flatters the
  result.
- The report is written to `loadtest/results/<label>-<endpoint>-<rate>rps.json` (not committed).
  Copy the runs you want to keep into the table below.
- The exit code is 1 if a limit was broken.

Percentiles use the nearest-rank method, so a reported value is always one that was measured.
Latency of failed requests is included: a slow error is still a slow answer.

## Test plan

Run each step at the three index sizes. At every size, first warm up (5 minutes at 20% of the rate,
not counted), then measure.

| Step | Rates | Duration | Look at |
| --- | --- | --- | --- |
| Baseline | 1, 5 requests/s | 5 min | single-request latency per stage (`semsearch_search_stage_duration_seconds`) |
| Ramp | 25%, 50%, 100%, 150% of the target | 10 min each | where p95 crosses 3 s (the knee) |
| Soak | 100% of the target | 2 h | memory growth, GC, connection leaks, cache hit rate |
| Spike | 0 to 200% in 10 s, back | 15 min | rate limiting, breaker behaviour, recovery time |
| Failure | 100% while stopping a model server, then an Elasticsearch node | 15 min | fallback rate, error rate, time to recover |
| With backfill | 100% while a backfill wave runs | 30 min | does indexing slow the search? (the HLD keeps the chunk index on its own nodes) |

Record with every run: the Git commit, the settings that matter (`search.*`, `reranker.*`,
`embedding.*`, replica counts, the number of nodes), the chunk index size, and whether the file system
cache was warm.

## Results

| Index size | Endpoint | Rate | p50 | p95 | p99 | Errors | Fallbacks | Date, commit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10% | search | not measured | | | | | | |
| 50% | search | not measured | | | | | | |
| 100% | search | not measured | | | | | | |
| 100% | answer (first token) | not measured | | | | | | |

## Where the time goes

The service records the time of each stage (`semsearch_search_stage_duration_seconds` with
`stage` = `embed`, `retrieve`, `rerank`, `total`) and of every dependency call
(`semsearch_upstream_call_duration_seconds`). The Grafana dashboard
(`deploy/observability/grafana-dashboard.json`) shows both. Read them in this order when p95 is
too high:

1. `embed`: the query embedding is cached in Redis. A low hit rate (`semsearch_cache_lookups_total`)
   or a slow embedding server shows here. The service waits at most `embedding.timeout_s` and then
   falls back to keyword search.
2. `retrieve`: Elasticsearch. This is normally the largest part at 100 million documents.
3. `rerank`: grows with `search.rerank_top_n`. The reranker is skipped if it is slow
   (`reranker.timeout_s`), and then the search answers with the RRF order.

## Elasticsearch tuning notes

These are the levers that the HLD names. Change one at a time, rerun the same test, and keep the
result.

| Lever | Setting or place | Effect and trade-off |
| --- | --- | --- |
| Candidates per leg | `search.candidates` (the HLD starts at 100) | More candidates raise recall and cost latency |
| kNN `num_candidates` | `search.num_candidates_factor` (2 to 5 times `k`) | Better kNN recall, slower queries |
| Rerank window | `search.rerank_top_n` (the HLD starts at 50) | Reranker cost and latency grow with it |
| RRF merge | `search.rrf_mode`: `python` or `retriever` | `python` runs the two legs in parallel and works on any license. `retriever` needs a license with the rrf retriever and then shows no snippets from Elasticsearch |
| Vector memory | node RAM, quantization of `dense_vector` (`int8` or binary) | Fast kNN needs the vector data in the file system cache. Quantization cuts memory about 4 times for a small recall loss: measure the loss on the evaluation set before using it |
| Shard size | index template (`store.shards`), target 10 to 50 GB per shard | Too many small shards add overhead per query. Confirm by a test |
| Replicas | `store.replicas` | One replica for availability. More replicas add read capacity |
| Segments | force merge after a backfill wave | Fewer segments make kNN faster. Do it off-peak |
| Refresh interval | `store.refresh_interval` | Longer during backfill, back to normal after (see the backfill runbook) |
| Hardware | dedicated nodes or a tier for the chunk index | Backfill indexing must not slow the existing keyword search |
| Search budget | `search.timeout_ms` | The upper limit for one search. After it the Java app uses its own search |

Also check, from the service side: the Elasticsearch client connection pool, the HTTP keep-alive to
the model servers, and the number of API replicas against CPU use. Rate limits
(`api.rate_limit_*`) protect the service in a spike: tune them with the Failure and Spike steps.

## Capacity estimate

The sizing method is in HLD section 11 (documents, pages per document, chunks per page, bytes per
vector). The average page count is **not known**: measure it on a random sample of `ITEM_ID`s before
ordering hardware. Redo the estimate with the measured value and with the quantization that the
evaluation set allows.

## Done when (T5.2)

- [ ] The three index sizes are loaded and measured, and the table above is filled.
- [ ] p95 is under 3 s at the target traffic at all three sizes, or the gap and the plan are written
      down.
- [ ] The tuning changes that were tried, and what they did, are written down under "Results".
- [ ] The soak run shows no growth in memory or errors.
