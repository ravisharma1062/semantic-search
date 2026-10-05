# 0008: Tests for the indexing pipeline (task T1.8)

## What is tested, and where

| Level | File | What it proves |
| --- | --- | --- |
| Unit, whole path with fakes | `tests/unit/test_pipeline_faults.py` | Event, read, chunk, embed, index, state, commit, with the real consumer loop, worker, chunker and normalizer. Kafka, source, index and state are in memory |
| Unit, model based | `tests/unit/test_pipeline_model.py` | Any history of edits, permission changes, empties, deletes, re-creations, duplicates and late events leaves the index exactly equal to a fresh index of the current document (150 random histories per run). A built-in check shows that a broken worker (never deletes stale chunks) is caught |
| Integration, real worker | `tests/integration/test_pipeline_e2e.py` | `run_worker` against real Kafka and Elasticsearch, with the fake model server over real HTTP: live events with update, permission change and delete, a backfill wave through its own topic and group, poison messages, graceful stop and restart with no reprocessing, a document that never appears |
| Integration, parts | `test_kafka_consumer.py`, `test_es_source_reader.py`, `test_indexing_es.py`, `test_backfill_es_kafka.py`, `test_embedding_cache_redis.py` | Each real component alone (T1.2 to T1.7) |

## Faults covered

| Fault | Test |
| --- | --- |
| Elasticsearch bulk error, short and halfway through a document | `test_a_bulk_error_is_retried...`, `test_a_bulk_failure_halfway...` |
| Permanent index error | `test_a_permanent_index_error_goes_to_the_dlq...` |
| Embedding timeout: quick retry, retry topic, never ending (DLQ) | `test_embedding_timeouts_recover...`, `test_a_long_embedding_outage...`, `test_an_outage_that_never_ends...` |
| Worker crash and restart, also after part of the chunks were written | `test_a_crash_in_the_middle...`, `test_a_crash_after_part...`, e2e restart test |
| Duplicate events, also long after the first | `test_many_duplicates...`, `test_a_duplicate_delivered_long_after...` |
| Out-of-order events | `test_a_late_delete_of_an_old_version...`, `test_an_old_upsert_after_a_newer_delete...`, the model test |
| Delete after update, and re-creation after delete | `test_a_delete_right_after_an_update...`, `test_a_document_deleted_and_created_again...` |
| Poison messages | pipeline and e2e tests |
| Busy mixed traffic with a flaky embedder | `test_busy_mixed_traffic_ends_consistent` |

## Bugs the tests found (fixed in this task)

1. **A late DELETE of an old version beat a newer UPSERT** when both waited in one batch (the coalescing
   ordered by event time and arrival only), and also when no state record existed yet. Now coalescing orders by
   `doc_version` when all events have one, and the worker ignores a DELETE when the source has a document with a
   higher version.
2. (Found earlier in T1.6 by the integration tests: delete and update by query miss chunks that are not refreshed yet.
   The e2e test keeps this covered through the real worker.)

## Coverage

`ingestion` and `store` are measured with unit and integration tests together (`[tool.coverage]` in
`pyproject.toml`). The threshold is **85%** (`fail_under`), the default I chose because the team threshold was not
given. CI runs `pytest --cov`, then `pytest -m integration --cov --cov-append`, then `coverage report`.
The report fails the build under the threshold. Change `fail_under` when the team names its number.

## Not covered

- The real TEI server: the fake model server only follows the same API (checked by a contract test).
- Load and throughput of the worker (task T5.2) and Kafka rebalancing under load beyond the two-consumer test of T1.2.
- Two workers (live and backfill) writing the same document at the same moment: covered by the state store
  concurrency test, not by a full pipeline test.
