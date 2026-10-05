# 0010: Reranking and retrieval tuning

Status: accepted (defaults); numbers pending a real evaluation set.

## Reranking

- The reranker is a `Reranker` provider. The in-house one calls a TEI-style `POST {endpoint}/rerank`
  with `{query, texts, truncate}` and expects `[{index, score}]`. Bad answers are rejected.
- The search fetches `max(top_k, search.rerank_top_n)` candidates, reranks the top `rerank_top_n`, and
  returns `top_k`. The reranker's score replaces the retrieval score.
- Reranking is on per request (`rerank`) or by setting (`reranker.enabled`, default off until measured).
- A timeout, a circuit breaker and a concurrency limit protect the search. If the reranker fails or is
  slow, the RRF order is returned and `mode_used` does not contain `+rerank` (rule 10).
- The reranker only sees passages that already passed the access filter (rule 1; covered by a test).
- Passages and queries are never logged (rule 2).

## Experiments

`eval/experiments.py` runs the matrix of HLD section 10 on the same questions: BM25, kNN, hybrid,
hybrid + rerank, for each chunk variant. `format_comparison` prints metrics with the change against the
baseline. Pick the winner with `best_by` (ties go to the simpler run).

## Caveat

The only dataset in the repository is a small synthetic smoke set. Its numbers prove that the runner
works, not which setting is best. The default stays: hybrid, reranking off. Before switching it on,
run the matrix on the business evaluation set (task owner: business) and record the table here.
