# 0004: Chunker (task T1.4)

| Topic | Decision |
| --- | --- |
| Order of splitting | Headings and paragraphs first, then size. Text is split into sentence units, so overlap is whole sentences |
| Sizes | `chunking.target_tokens` 400, `max_tokens` 512, `overlap_tokens` 60, `min_tokens` 50. Settings are validated: overlap < target <= max, min <= target |
| Headings | A short line (80 characters), without sentence punctuation, that is ALL CAPS or a numbered section ("1.2 Title"). A heading starts a new chunk only when the current chunk has at least `min_tokens`, so small sections are merged forward. `section_title` is the title active at the start of the chunk. Lines ending with ":" are not headings |
| Overlap | The last sentences of the previous chunk, up to `overlap_tokens`. None across a heading or a table |
| Tables | Consecutive table-like lines (same detection as the normalizer) form one table, also across a page break. A table is its own chunk(s). It is cut between rows only, and the header row is repeated on the following chunks. A single row over the maximum is cut by tokens as a last resort |
| Short pieces | **Deviation from HLD section 4 ("skip very short chunks").** A short remainder is merged into the previous text chunk when it fits. It is never dropped, so no text is lost. Only chunks without a letter or digit are skipped |
| OCR confidence | Not flagged: the source index has no confidence field (to confirm with the Java team) |
| IDs | `chunk_id = f"{item_id}:{chunk_no}:{content_hash}"`. The hash is the first 16 hex characters of SHA-256 of the content. Numbers are consecutive from 0 |
| Pages | `page_start` and `page_end` cover the pages of all sentences in the chunk, including overlap. Document-level text gives `None` |
| Tokens | `TokenCounter` protocol. `HfTokenCounter` reads a local `tokenizer.json` (no download at runtime). `WhitespaceTokenCounter` is for development and tests. `chunking.tokenizer: hf` needs `chunking.tokenizer_file` or the service does not start |
| Safety net | Every chunk is recounted. Anything over `max_tokens` is cut at token boundaries |
| Memory | `Chunker.iter_chunks` is a generator, so chunks are produced one at a time. The 3,000 page test stays under 20 MB of extra memory. The source document itself is already in memory (the reader loads all pages) |
| CPU | `split_async` runs the chunking in a worker thread |
| Chunk cap | `chunking.max_chunks_per_document` (20,000) stops a runaway document. It is logged with the item ID |
| `embedding_text` | The section title in front of the content, unless the content already starts with it |

Prod needs `APP_CHUNKING__TOKENIZER_FILE` (path of the bge-m3 `tokenizer.json` from the internal artifact repository).
