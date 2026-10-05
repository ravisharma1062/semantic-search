# Runbook 8: suspected access-control problem

Use it when someone reports that a user saw a document, snippet or answer they may not open, or when
an isolation test fails. Treat it as a security incident until security says otherwise.

## 1. Stop it

Switch off what may be leaking, now, then investigate (runbook 1):

```bash
helm upgrade semantic-search deploy/helm/semantic-search -n semantic-search --reuse-values \
  --set-string config.APP_FEATURE_FLAGS__SEMANTIC_SEARCH=false \
  --set-string config.APP_FEATURE_FLAGS__RAG=false
```

The Java app uses keyword search, which has its own access control. Tell the security team and the
service owner now.

## 2. Collect (do not edit anything yet)

- The request IDs from the report. Every response carries `X-Request-Id`.
- Logs for those IDs: `http_request`, `search_done` (user, `mode_used`, **chunk IDs**) and
  `answer_done` (user, cited chunk IDs, reason). They hold IDs only. The question and the answer text
  are not logged, by design.
- Audit lines (`admin_action`, `audit=true`): any admin call that could have changed things.
- The state records and chunks of the documents involved (`doc_id`, `acl_users`, `acl_groups`,
  `indexed_at`).
- The deployed version (`helm history`) and any recent config change.

## 3. Find out which of three things happened

1. **The filter was missing or wrong.** Run `uv run pytest -q -m access` (unit and real Elasticsearch)
   against the deployed commit. These tests build every query through `QueryBuilder`, check that the
   access filter is in every leg, and compare many users on a real cluster. A failure here is a code
   bug: roll back (`deploy.md`) and keep the feature off.
2. **The index had old permissions.** The chunk's `acl_*` fields differ from the source. Causes: a
   lost permission event, a worker that was down, an old snapshot restore. Fix: `python -m app.jobs.cli
   reconcile` republishes `ACL_CHANGE` for documents whose permission hash differs. Check a sample
   against the source system.
3. **The user really had access in the source.** Then it is not a service problem: tell the Java team.

Caches: today the service caches only query embeddings in Redis (the key holds the model and a hash of
the text, the value is a vector, no result and no user). Search results and answers are not cached,
so a stale permission cannot come from a cache. If a result cache is added later, it must use
`retrieval.ScopedCache` (keyed by the access scope, never by text alone) and be flushed after a fix.

## 4. Fix, verify, release

Fix the cause. Run the access tests and the evaluation smoke set. Release through the pipeline with
a canary. Only then switch the flags on again, with the security team's agreement. Write the
timeline, the cause and the fix down.

## Who

Security team leads. L2 on-call collects and acts. L3: the search platform team for the index, the
Java team for permissions in the source system.
