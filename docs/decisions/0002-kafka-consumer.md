# 0002: Kafka consumer framework (task T1.2)

| Topic | Decision |
| --- | --- |
| Commit rule | Per partition, up to the lowest finished message. A fast message never commits past a slow one (`ingestion/offsets.py`) |
| Handed-on messages | A message is finished once the handler succeeded, or the retry-topic or DLQ copy was acknowledged. If that publish fails, the offset stays uncommitted and the message is read again |
| Coalescing | Events of one `item_id` that wait together are handled once. Order is by `occurred_at`, then arrival. A last DELETE wins. After it, UPSERT wins over ACL_CHANGE, because the UPSERT path refreshes permissions too |
| Priority | DELETE, then ACL_CHANGE, then UPSERT |
| Retry | 3 quick attempts in process (`consumer.quick_retries`), then `doc-index-retry` with `x-not-before` and `x-retry-count` headers, then `doc-index-dlq` after `consumer.max_retries`. The delay is a pause of the retry partition until the head message is due |
| Bad messages | Invalid JSON, wrong schema, empty value, or a key that is not the `item_id` go to the DLQ at once. The DLQ keeps the original value. The reason never contains message content |
| Error text | Headers carry the error type. Only our own typed errors add their generic message, because other messages can hold document text |
| Rebalance | Eager protocol. On revoke the loop finishes the work of those partitions (up to `consumer.rebalance_timeout_s`), cancels what is left, and commits inside the callback |
| Shutdown | Finishes the messages being handled (up to `consumer.shutdown_timeout_s`), commits, leaves the group |
| Clocks | Wall time (injectable) for `x-not-before`, monotonic time for the commit interval |
| Event schema | Adds the optional `wave` field to schema v1 (additive, for backfill, task T1.7). Times without a zone are read as UTC |
| Worker mode | Still an idle stub. T1.6 wires the handler |
| Retry helper | `core/retry.py`. tenacity awaits only coroutine functions, so the helper wraps the operation |

New settings: `kafka.backfill_consumer_group`, `kafka.request_timeout_s`, `kafka.producer_timeout_s`,
section `consumer`, section `retry`.
