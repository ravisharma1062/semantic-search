# 0009: Hybrid search and the search API (phase 2: T2.1, T2.2, T2.4, T2.5, T2.6)

## Decisions

| Topic | Decision |
| --- | --- |
| Access filter | `retrieval/acl.py`: `AclFilter.from_identity`. A chunk is visible when the user ID is in `acl_users` or a group of the user is in `acl_groups`. No user means no access (`ForbiddenError`) |
| Query builder | `retrieval/query.py` is the only place that builds search requests. Every request carries the filter in the text query, **inside the kNN part**, and in both legs of the rrf retriever. `QueryBuilder.verify` walks every finished request and refuses it if any query part lacks the filter, or if it uses `match_all` or `query_string`. The builder verifies its own output and the searcher sends nothing unverified (fail closed) |
| Inventory | `tests/unit/test_query_inventory.py` lists every place that sends a search, count or by-query request. A new place fails the test until it is added on purpose. `api` and `rag` may never talk to Elasticsearch directly. This is the T2.1 "find every query" check, and the allow list is the rule 1 exception list of decisions 0006 and 0007 |
| Merge | `search.rrf_mode`: `python` (default, two searches in parallel merged with RRF, works with every license) or `retriever` (one request with the rrf retriever). If the cluster refuses the retriever, the searcher logs it once, remembers, and uses the python merge |
| Real finding | Elasticsearch 8.15 refuses `highlight` together with the rrf retriever (`[rank] cannot be used with [highlighter]`). So the retriever request has no highlight, and snippets and highlight terms for hits without a server-side highlight (retriever, vector-only) are made here from the query words |
| Fallbacks | Visible in `mode_used` (rule 10): query embedding fails or is slow: `bm25`. One leg fails: the other (`bm25` or `knn`). Both fail, the budget (`search.timeout_ms`, 3 s) is over, or the feature flag is off: an error (`UPSTREAM_UNAVAILABLE`, `TIMEOUT`) and the Java app uses its own search. `vector` mode cannot fall back |
| No retries on the request path | `api.request_retry` is 1 attempt. The 3 s budget has no room for backoff |
| Response | `doc_id`, `chunk_id`, `score`, `pages` (first to last page), `snippet` (plain text), `highlights`, never the chunk text. `request_id`, `mode_used`, `took_ms`. `group_by_document` keeps the best chunk per document |
| Authentication | `Authorization: Bearer` with one token per calling service (`api.service_tokens`, from the secret store as JSON). Constant-time comparison. No token configured: everything is refused (fail closed). `api.auth_disabled` exists for development and is refused in prod. **Decision default: bearer tokens plus mTLS at the ingress, no JWT library** |
| Identity | `X-User-Id` and `X-User-Groups` are accepted only from services in `api.identity_services`. Others get 403. IDs are restricted to a safe character set (no wildcards), groups are limited in number. Unknown fields in the body are refused, so identity cannot be sent there |
| Rate limits | Fixed one-minute window per user and per service, in the memory of the pod (`api.rate_limit_*`). Approximate across pods (limit times pods), lost on restart, which is acceptable: it holds no business data. 429 with `Retry-After` |
| Contract | `openapi/openapi.yaml`, written by `python -m app.api.openapi_export`. CI fails if the file is not the current contract. Adds the security schemes and the identity headers that FastAPI cannot see. Bad input is 400 in our error format, never 422. **This is a proposal for the Java team: changes need approval and must be backward compatible** |
| T2.4 cache | The query embedding cache was built in T1.5 (model version in the key, outage safe). `retrieval/cache.py` adds `ScopedCache` for anything that depends on rights (answers): the key contains `AclFilter.scope_key()` (hash of user and sorted groups), so a result is never served across scopes. Hashes only in keys. A `CircuitBreaker` (`core/breaker.py`) keeps a dead Redis from slowing requests |
| T2.5 evaluation | `eval/`: dataset with a dev and a test slice (the test slice needs an explicit allowance), recall@10, recall@50, MRR, nDCG@10 with page-aware matching, a runner against the HTTP API or in process, reports saved with the configuration. A synthetic smoke set and corpus run in CI as an integration test with a quality gate (recall@10 at least 0.9). The full set is the business's, to be provided |
| T2.6 isolation | `tests/unit/test_access_isolation.py` and `tests/integration/test_search_es.py` (marker `access`, CI job "Access isolation"): 5 to 6 users with different rights, same queries, every mode, results differ as expected, and no snippet, highlight, ID or page of a forbidden document appears. Tricks tried: user ID equal to a group name, similar user names, filters, huge `top_k`, grouping, header injection through body and query string. On real Elasticsearch, including the rrf retriever on a trial license and a kNN that must filter before choosing neighbours (30 closer forbidden documents must not push out the 3 allowed ones) |

## Not done in this phase

- The Java integration (T2.3) is the Java team's task.
- Quality numbers on real documents: the evaluation set comes from the business.
- Rerank (phase 3), RAG (phase 4), metrics and tracing (phase 5) are separate commits of this branch.
