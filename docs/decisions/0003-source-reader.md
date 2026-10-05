# 0003: Source reader and OCR normalizer (task T1.3)

| Topic | Decision |
| --- | --- |
| Reading | `get` and `get_many` use `get` and `_mget` by `_id`. `ITEM_ID` is assumed to be the document `_id`. No search query is used |
| Field names | All in settings, section `source` (pages, page number, page text, document text, type, tags, dates, owner, ACL, version, language). **Still to confirm with the Java team.** Only these fields are fetched (`_source_includes`) |
| Pages | An array of objects with page number and text. Without pages, the document-level text field is used and page citations fall back to the document |
| Untrusted data | Wrong types in the index are skipped, not raised. Blank ACL entries are dropped. Page numbers fall back to the position |
| Huge documents | Cut at `source.max_pages` or `source.max_chars`, and `SourceDocument.truncated` is set (logged with the ID only) |
| No text | The reader still returns the document. `has_text` is false and the worker marks it `SKIPPED` (T1.6) |
| Timeouts, retries | Per-call timeout from `elasticsearch.source_read_timeout_s`. Reads retry on timeout, connection loss, 429 and 5xx through `core.retry` |
| Errors | Mapped to typed errors in `store/client.py`. Other 4xx are `NonRetryableError`. Messages are generic |
| Missing index | A 404 with `found: false` is a missing document. Any other 404 is a setup error, not "document missing". `_mget` reports a missing index as a 200 with per-ID errors, so those are classified by type: setup errors are permanent, others (a failed shard) are retried |
| Client version | `elasticsearch` is pinned to `>=8.15,<9`. A 9.x client cannot talk to an 8.x server |
| Normalizer | `ingestion/normalizer.py`. Fixes ligatures, soft hyphens, control characters, hyphenation, broken lines, repeated headers, footers and page numbers. Table-like lines keep their line breaks and column spacing. Page numbers and page count never change |
| Header detection | The first and last lines of a page (at most a third of the page each) that repeat on at least 40% of the pages, and at least 2 pages, with a minimum of 4 pages. All thresholds are in settings, section `normalizer` |
| Source-file escapes | Use `\N{NAME}` escapes for special characters in source code, never literal invisible characters |
