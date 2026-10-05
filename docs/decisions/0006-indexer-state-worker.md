# 0006: Indexer, index templates, state store and worker loop (task T1.6)

| Topic | Decision |
| --- | --- |
| Templates | Composable templates for `{prefix}_v*` (chunk index, HLD section 5) and for the state index (HLD section 17). `dynamic: strict`. Vector size from `embedding.dims`. Quantization `store.vector_index_type` (default `int8_hnsw`, also `hnsw` and `bbq_hnsw`). Fields added to the HLD list: `content_hash`, `is_table` (chunks), `meta_hash`, `reason` (state) |
| Index names | `doc_chunks_v1_bgem3`: prefix, version, model. Reads use the alias `search.index_alias`. The worker writes to `store.write_index` (default: the alias). During a re-index it is the new version, so users keep searching the old one |
| Index tools | `python -m app.jobs.index_admin install-templates / create-state-index / create-index / show-alias / switch-alias / delete-index`. They refuse any index that is not `{prefix}_v*` (rule 9: the existing document index is never touched). `switch-alias` is one atomic step and refuses an empty or missing index. `delete-index` refuses the live index, and an index younger than `store.min_index_age_days` (14) unless `--force` |
| Bulk writes | `_id = chunk_id`, so a repeat overwrites. Batches of `store.bulk_batch_size`. Only failed items are sent again (429 and 5xx); a permanent item error (for example a mapping error) stops the write. Chunk IDs and error types are logged, never text |
| Refresh before delete or update | **Delete and update by query only see refreshed documents.** A document written a few seconds ago would keep its old chunks (ghosts) or its old permissions. The worker passes `refresh_first` when the document was indexed within twice the refresh interval (`-1` means always). An integration test shows the bug without it |
| Conflicts | Version conflicts in delete or update by query are not skipped (`conflicts=proceed` would leave old permissions behind). They raise a retryable error |
| Stale chunks | Deleted only after all new chunks are written and acknowledged. A failure in a later window leaves all old chunks in place |
| Windows | `ingestion.window_size` (256) chunks are embedded and written together, so a 3,000 page document never holds all vectors in memory |
| Query rule (rule 1) | `delete_by_query` and `update_by_query` are maintenance of our own index. They return no documents, so they are not user search queries. They exist only in `ingestion/indexer.py`. T2.1 must keep a test that lists every place that builds an Elasticsearch query |
| State store | Optimistic concurrency (`if_seq_no`, `op_type=create`), restarted after a conflict, at most 5 times. A write never moves `doc_version` backwards. `attempts` = failed attempts since the last success |
| Unchanged | Same text hash, embedding model name and version, chunker version, status INDEXED: nothing is embedded. If only permissions or filter fields changed (`meta_hash`), `refresh_metadata` updates `acl_*`, `doc_type`, `tags`, `created_at` |
| Stale events | Compared with `doc_version` of the document (or of the event): an older UPSERT or DELETE is ignored |
| Missing document | `SourceNotReadyError` goes through the retry topic. After the retry limit it is treated as DELETE (HLD "retry later, or treat as DELETE"). The handler gets `HandlerContext` (retry count, limit) from the consumer loop for this |
| Failures | The worker records FAILED (error type only) and re-raises, so the consumer retries. A broken state store does not hide the real error |
| Worker process | `ingestion/runtime.py`. Two consumer loops: live and retry topics (group `kafka.consumer_group`), and backfill (group `kafka.backfill_consumer_group`, `ingestion.backfill_max_in_flight` documents). If one loop dies, the process exits and Kubernetes restarts it |
| Liveness | A heartbeat file (`ingestion.heartbeat_file`) touched from the event loop. The Helm liveness probe checks that it is younger than 60 s |
