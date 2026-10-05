# 0005: Embedding client (task T1.5)

| Topic | Decision |
| --- | --- |
| In-house provider | TEI-style `POST {endpoint}/embed` with `{"inputs", "normalize": true, "truncate": false}`. Contract checked in a unit test against the local fake model server (in process) |
| Batching | `embedding.batch_size` (32) texts per call. Batches of one call run side by side. If one fails, the others are cancelled and our own typed error is raised, not an exception group |
| Concurrency | `embedding.max_concurrency` (4) calls at the same time per embedder. `AdaptiveLimiter` halves the limit on every 429 or 503 and gives one back after 8 successes (HLD section 4: "lower parallelism") |
| Timeouts | `embedding.timeout_s` (0.5 s, one query) and `embedding.document_timeout_s` (10 s, one batch of chunks). HLD interface I7 only names the query timeout |
| Retries | `core/retry.py`: timeouts, connection errors, 429, 5xx. `400`, `401`, `413`, `422` are not retried |
| Errors | `core/http.py` maps to typed errors. The response body is never copied into an error or log. 429 and 503 raise `UpstreamOverloadedError` (a subclass of `UpstreamUnavailableError`) |
| Vectors | Validated: count, size (`embedding.dims`), finite numbers, not zero. Normalized to unit length when the server did not do it (cosine similarity in the index) |
| Model name | `embedding.model@embedding.model_version`, for example `bge-m3@1`. It goes into the cache key and into `embedding_model` of each chunk |
| OpenAI | Optional and off by default. Needs `embedding.provider: openai` and `embedding.allow_external: true` and an API key. Plain HTTP (no SDK), egress proxy through `embedding.proxy`. Only for data classes approved by security |
| Cache | Redis, query embeddings only. Key = model name and version, dimensions and a SHA-256 of the query (never the text). Value = float32 bytes. TTL `embedding.cache_ttl_s` |
| Redis outage | Every Redis call has a timeout (`redis.timeout_ms`, 50 ms). Any error is a miss. After `redis.breaker_failures` failures in a row Redis is skipped for `redis.breaker_cooldown_s`, so a dead Redis does not slow each request. Tested against a real and a dead Redis |
| Scope of T2.4 | The cache is built here. T2.4 only adds what is left: the access-scope rule for cached answers |
| Embedding server | Helm chart `deploy/helm/embedding-server` and `docs/embedding-server.md`. Model on a read-only volume, `HF_HUB_OFFLINE=1`, GPU limit, startup probe for warm-up. Image and volume are placeholders |
