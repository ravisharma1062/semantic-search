# Runbook 6: switch the Elasticsearch alias

Searches read from the alias `search.index_alias` (`doc_chunks_current`). A chunk index version is a
real index with a name like `doc_chunks_v2_bgem3`. Moving the alias is atomic: there is no moment
without an index. The existing document index is never touched (rule 9).

## Look

```bash
python -m app.jobs.index_admin show-alias
```

## Switch forward (a new index is ready)

Before: the new index has the expected document count, the evaluation set meets its gates, the
workers have been writing to it (`APP_STORE__WRITE_INDEX`).

```bash
python -m app.jobs.index_admin switch-alias --index doc_chunks_v2_bgem3
```

The command refuses an index that does not exist, is not one of ours, or is empty
(`--allow-empty` overrides the last one). It prints the indices the alias left.

After: `show-alias`, run the evaluation smoke set, watch the dashboard for 30 minutes (latency,
fallback rate, error rate). Point the workers' write index back at the alias.

## Switch back (the new index is bad)

```bash
python -m app.jobs.index_admin switch-alias --index doc_chunks_v1_bgem3
```

This is instant and is why the old version is kept for at least 14 days.

## Remove an old version

```bash
python -m app.jobs.index_admin delete-index --index doc_chunks_v1_bgem3
```

Refused if the alias points at it or if it is younger than `store.min_index_age_days`
(`--force` ignores the age, never the alias).

## Who

L2 with the search platform team. Tell the Java team before and after a switch.
