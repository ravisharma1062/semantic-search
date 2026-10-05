# Embedding server: deployment recipe

The service calls an embedding server over HTTP (`embedding.endpoint`). The default is
Hugging Face Text Embeddings Inference (TEI) with bge-m3, on our own GPUs. Any server with the
same API can be used.

## API the service expects

`POST {endpoint}/embed` with `{"inputs": ["text", ...], "normalize": true, "truncate": false}`
answers with a JSON list of vectors, one per input, in order (1024 numbers for bge-m3). Status
`429` or `503` means "overloaded": the service retries with backoff and lowers its parallelism.
`400`, `413` and `422` are not retried.

## Deploy

1. **Model files.** Copy the bge-m3 folder (config, weights, `tokenizer.json`) from the internal
   artifact repository to a read-only volume. Check the checksum, and make sure the files passed the
   security scan. Pods never download models.
2. **Image.** Use the approved TEI image from the internal registry (GPU build for your GPU
   architecture), pinned to one version.
3. **Values.** Copy `deploy/helm/embedding-server/values.yaml`, fill the placeholders (image,
   volume claim, GPU node selector and tolerations), and install:

   ```bash
   helm upgrade --install embedding deploy/helm/embedding-server -f my-values.yaml -n model-serving
   ```

4. **Point the service at it:** `APP_EMBEDDING__ENDPOINT=http://embedding-embedding-server.model-serving`
   (the Service name is `<release>-embedding-server`).
5. **Tokenizer for the chunker.** Mount the same `tokenizer.json` into the service pods and set
   `APP_CHUNKING__TOKENIZER_FILE`. The chunker must count tokens with the model's own tokenizer.

## Sizing and tuning

- At least 2 replicas, 1 GPU each. Scale on queue length and GPU use.
- `maxBatchTokens`, `maxClientBatchSize` and `maxConcurrentRequests` are starting values. Set them
  from the GPU benchmark (work package 0.4).
- The service sends batches of `embedding.batch_size` (32) chunks, at most `embedding.max_concurrency`
  (4) batches per worker at the same time. Total pressure on the server is workers x 4 batches.
- NetworkPolicy, GPU alerts (DCGM exporter) and autoscaling come with tasks T5.1 and T5.5.

## When it goes wrong

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| Many 429 in the worker logs | Too many workers or too much backfill | Lower worker count or `embedding.max_concurrency`, pause backfill |
| Timeouts on query embedding (0.5 s) | GPU saturated by backfill | Pause backfill, add replicas |
| "Embedding dimension does not match" | Wrong model or `embedding.dims` | Check the model folder and the setting. A new model means a new index version |
| Pods not ready for minutes | Model loading and warm-up | Normal at start. Check the startup probe limits |
