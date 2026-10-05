# Runbook 5: roll out and roll back a model version

Rule: no model, prompt or chunker change goes live without an evaluation run on the fixed set
(HLD section 10). Each run stores its configuration with its scores, so runs can be compared.

## Reranker or LLM (no re-index needed)

1. Deploy the new model server next to the old one (the model files come from the internal artifact
   repository, never from the internet). Shadow traffic first if you can.
2. Run the evaluation against it: `python -m eval.runner ... --rerank on` for the reranker,
   `python -m eval.rag_eval ... --judge` for the LLM. Compare with the last run
   (`eval/experiments.py` and `compare_prompts` print the comparison tables). Keep the report.
3. Canary: point a canary release at the new endpoint or model name (`APP_RERANKER__ENDPOINT`,
   `APP_RERANKER__MODEL`, `APP_LLM__ENDPOINT`, `APP_LLM__MODEL`, `APP_LLM__MODEL_VERSION`) through
   the deployment pipeline (`deploy.md`) and watch the canary check.
4. Roll out. The model name and version go into every answer (`model`) and into Langfuse metadata,
   so a change is visible afterwards.

**Roll back:** put the old endpoint or model name back and redeploy (or `helm rollback`). Nothing in
the index depends on the reranker or the LLM.

## Prompt version

A new prompt is a new file `prompts/answer.v2.txt`, never an edit of `v1`. Compare with
`eval.rag_eval.compare_prompts`, then set `rag.prompt_version` (`APP_RAG__PROMPT_VERSION=v2`).
Rollback: set it back to `v1`.

## Embedding model or chunker (needs a re-index)

Vectors from different models are not comparable, so a change builds a new chunk index version in
the background while users keep searching the old one.

1. Create the new version: `python -m app.jobs.index_admin create-index --version v2 --model <name>`.
2. Point the workers at it: `APP_STORE__WRITE_INDEX=<new index>` (and the new model and chunker
   version settings). Use a separate state index for the re-index (`APP_ELASTICSEARCH__STATE_INDEX`),
   see decision 0007: how live updates reach both indices during the switch needs the architect's
   decision before the first model change.
3. Re-index: `POST /v1/admin/reindex` per wave or `backfill start --wave N --restart`.
4. Evaluate on the new index (`eval.runner`, test slice only for the final check). Compare the
   counts of old and new index.
5. Switch the alias: runbook 6. Keep the old index at least 14 days (`store.min_index_age_days`).

**Roll back:** switch the alias back (runbook 6), then restore the old model and chunker settings.

## Who

ML engineer with L2. The architect for step 2 of the embedding change.
