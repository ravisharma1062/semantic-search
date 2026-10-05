# 0007: Backfill producer, wave control and reconciliation (task T1.7)

| Topic | Decision |
| --- | --- |
| Scan order | Sorted by one unique field of the source index, `backfill.scan_sort_field` (default `item_id`), with `search_after`. The cursor is that field's last value. A scan can stop for any time and continue. **To confirm with the Java team:** ITEM_ID must be in the source index as a `keyword` field. If it is only the document `_id`, sorting is not possible (`_id` fielddata is off in Elasticsearch 8) and a keyword copy of ITEM_ID is needed. A point-in-time scan was not used: its cursor dies with the point in time, so a pause of a few hours would restart the scan |
| Documents without the sort field | Not reached by the scan (they sort last without a value). They arrive through live events. The scan logs it |
| Waves | `backfill.waves`: a number, optional `doc_types`, and a created-from and created-to date. Structured, no raw query. Waves may overlap: a document that is indexed already is skipped cheaply |
| Events | Schema v1, `source: backfill`, `priority: backfill`, `wave: N` (optional field, additive), key = `ITEM_ID`. No text, no permissions |
| Speed | Token bucket, `backfill.rate_per_second` (200). One page is `backfill.scan_size` (500) documents. The Kafka batch of a page is published with one wait for the acknowledgements |
| Wave records | Before publishing a page, every document gets a state record: a new PENDING record, or the wave number added to the existing record (status untouched). The worker keeps the wave number when it updates the record. So the report counts by status and wave |
| Skipping | With `backfill.skip_up_to_date`, documents that are INDEXED with the current embedding model and chunker version are not published again, but still join the wave |
| Checkpoint | After every page the cursor and counters are saved in the jobs index (`backfill.job_index`). A crash between publishing and saving publishes one page twice, which is harmless (idempotent processing). Tested with real Kafka and Elasticsearch |
| Pause and resume | `pause --job-id` sets `desired: PAUSED`. The job checks between pages, saves the cursor, sets PAUSED and exits. SIGTERM does the same. `resume` and `start` with the same job ID continue from the cursor. `--restart` scans from the start. A pause request is cleared when the job starts again |
| Failure | The job is FAILED with the error type, and the process fails. Kubernetes starts it again (Job `backoffLimit`) and it continues from the cursor |
| Progress report | `backfill status`: counts per wave and status from the state index (a terms aggregation), plus the jobs. Records outside any wave (live events) are shown as wave `-` |
| Reconciliation | `reconcile [--wave N] [--max-items N] [--no-orphans]`. Source to state: no record, FAILED, PENDING, DELETED-but-present, different `doc_version`, other model or chunker: UPSERT to the backfill topic. Different permissions or filter fields (`meta_hash`): ACL_CHANGE to the live topic. State to source: a record whose document is gone: DELETE to the live topic. A full scan of 100 million documents is long, so the job works per wave or on a sample. It is safe to run again |
| Not in reconciliation | Text changes without a version change: the source has no text hash we can read cheaply. Changes arrive as UPSERT events. A wave scan with `--restart` and `skip_up_to_date: false` repairs a whole wave |
| Helm | `batch.command` selects the job (see `values.yaml`). `APP_MODE=batch` without a command only idles |

## Exception to rule 1 (needs sign-off)

CLAUDE.md rule 1: every Elasticsearch **search** query must be built by `retrieval.QueryBuilder` and
carry the access filter. The backfill cannot follow it: a scan of the existing index has no user and
must see every document, and the HLD asks for exactly this scan (section 4). These requests exist and
are listed here so the T2.1 review ("find every place that builds an Elasticsearch query") can check
them:

| Where | What | Why it is safe |
| --- | --- | --- |
| `jobs/scan.py` | Sorted scan of the existing document index, and `_mget` for existence | Batch jobs only, no user path. Returns IDs, and with `include_meta` only permission and filter fields, never OCR text. Output goes to Kafka (IDs) or to a comparison |
| `ingestion/state_admin.py` | Aggregation and ordered scan of our state index | State records hold hashes and counters, not text |
| `jobs/job_store.py` | List jobs in the jobs index | Job counters only |
| `ingestion/indexer.py` | `delete_by_query` and `update_by_query` on the chunk index, by `doc_id` | Maintenance of our own index. Returns no documents (decision 0006) |

None of these is reachable from the API. No API route may call them.

## Open items

- **Re-index and state.** The state index is per `ITEM_ID`, not per index version. During a re-index into a new chunk index version, the state records describe the new index. Use a separate state index for the re-index (`APP_ELASTICSEARCH__STATE_INDEX`) so the live index and its records stay consistent until the alias moves. Live events during the re-index must also reach the new index. This needs a decision from the architect before the first model change (HLD risk "Embedding model change later").
- **Source field name.** `scan_sort_field`, see above.
