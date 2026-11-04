# ADR 0009: Meilisearch hybrid semantic retrieval (replacing VectorMatcher)

## Status

Proposed. Not implemented; no vector code changes accompany this record.

## Context

Context search blends a local `VectorMatcher` (`src/vector_match.rs`) into
ranking, enabled by default (`RAG_VECTOR_MATCH_ENABLED=true`). Review finding
M2 identified four problems:

- **No semantics.** Embeddings are 512-dimension signed FNV feature hashes of
  unigrams, bigrams, 5-character prefixes, and character trigrams. They catch
  inflection ("deployment pipelines" vs "deploy pipeline") but not synonyms or
  paraphrase, so "vector" evidence is a second lexical signal.
- **Serialization.** One process-wide `Mutex` guards the turbovec index; every
  search embeds and scores candidates under it.
- **No reclamation.** Entries for deleted or superseded documents stay in the
  index until restart.
- **Per-process state.** The index is rebuilt lazily per process and is
  invisible to Meilisearch ranking, which (after H4) is the primary relevance
  signal. The local blend can now only nudge Meilisearch scores by at most
  `0.05`, so most of its cost buys little.

Production runs Meilisearch 1.56.1, which supports index `embedders` and the
`hybrid` search parameter.

## Decision (proposed)

Move semantic retrieval into Meilisearch and retire `VectorMatcher` after an
evaluation gate.

### Embedder source options

| Option | Pros | Cons |
| --- | --- | --- |
| `openAi` embedder (`text-embedding-3-small`, 1536 dims) | Strong English quality; Meilisearch manages batching and retries. | Fragment text leaves the host; per-token cost; needs a Meilisearch-side API key separate from `RAG_OPENAI_API_KEY`. |
| `rest` embedder to a self-hosted model (for example `bge-small-en-v1.5` behind a local HTTP server) | Data stays on the host; no per-token cost. | Another service to deploy, monitor, and version. |
| `huggingFace` embedder (model run inside Meilisearch) | No extra service. | CPU-heavy indexing inside Meilisearch; slower on the small gfit host. |
| `userProvided` (Nowledge computes vectors and writes `_vectors`) | Full control; could reuse an existing provider client. | Nowledge must embed on every write and on every query, which puts an LLM-style call on the search path. |

Recommendation: start with `rest` against a small self-hosted English model.
The data is small (well under 1k fragments), so indexing cost is negligible,
and content never leaves the host. Use `openAi` as the fallback if running a
model service is not acceptable. Either way the embedder `documentTemplate`
should be `{{doc.title}}\n{{doc.body}}`, with `documentTemplateMaxBytes`
bounded so large fragments do not exceed model limits.

Secrets: an embedder API key is part of Meilisearch settings. It must come
from a new secret-manager variable, be added to the configured-secret
redaction inventory, and never be echoed by `/healthz`, settings diffs, or
error text. Settings comparisons must not print the embedder object.

### Settings migration

- Add `embedders.<name>` to the context-index branch of `settings_for` (both
  `rag_company_context` and every `rag_context__*` personal index), behind a
  new `RAG_MEILI_EMBEDDER` configuration value that defaults to off.
- Extend `managed_settings_equal` to compare the embedder *shape* (source,
  model, dimensions, template) and ignore the redacted key Meilisearch
  returns, so restarts do not enqueue redundant settings tasks.
- Applying an embedder makes Meilisearch embed every existing document. With
  the current data size this is minutes at most, but the settings task must
  be awaited (or readiness kept unready) before hybrid queries are enabled,
  because documents without vectors only match lexically.
- Rollback: clear the embedder setting. Lexical search is unaffected. No
  document rewrite is needed in either direction.

### Hybrid query shape

```json
{
  "q": "<meili_query_terms(question)>",
  "filter": "<unchanged context_filter(...)>",
  "limit": 10,
  "showRankingScore": true,
  "hybrid": { "embedder": "context", "semanticRatio": 0.5 }
}
```

Meilisearch embeds `q` for the semantic half. Embedding the full question
would be better than embedding the ten-term reduction, but the lexical half
then truncates to ten words again. The evaluation below compares the full
question against the reduced `q` and picks one. `semanticRatio` is a
configuration knob (`RAG_MEILI_SEMANTIC_RATIO`, default `0.5`). The merge
from H4 is unchanged: `_rankingScore` stays the primary score.

### Isolation

Hybrid search keeps the exact `filter` built today: tenant, status, retrieval
role, privacy, `index_kind`, owner, and `index_uid`, so semantic results
cannot widen scope. Searches still target only `rag_company_context` and the
caller's own HMAC-derived personal index, and the store still validates
every returned node (`validate_repository_context_node`). Embeddings are
stored per index, so there is no cross-user vector store. A `rest` or
`openAi` embedder sees fragment text from every tenant, so the trust
boundary of that embedder service must match Meilisearch's.

### Rollout and evaluation plan

1. Ship the embedder setting disabled by default; add it to staging indexes
   only.
2. Build an evaluation set from the existing `rag_eval_cases`
   (expected context and source URIs) plus about 30 hand-written English
   paraphrase and synonym queries over the current corpus.
3. Compare recall@5, MRR, and latency p95 for three arms: lexical-only (H4
   ranking, `RAG_VECTOR_MATCH_ENABLED=false`), lexical with local
   `VectorMatcher` (current default), and Meilisearch hybrid at
   `semanticRatio` 0.3, 0.5, and 0.7.
4. Adopt hybrid if it beats both other arms on recall@5 without a p95
   regression above 50 ms. Otherwise keep lexical-only and still default
   `RAG_VECTOR_MATCH_ENABLED=false` if local vectors do not beat it.
5. Enable on gfit, watch `/healthz`, Meilisearch task failures, and search
   latency for one week before removing code.

### Removal of VectorMatcher

After the gate passes:

1. Default `RAG_VECTOR_MATCH_ENABLED=false` for one release and log a
   deprecation warning when it is set to true.
2. Remove `src/vector_match.rs`, the `turbovec` dependency (which also
   removes the OpenBLAS build requirement on Linux CI and hosts), the
   `RAG_VECTOR_MATCH_*` variables, and the vector maintenance in `store.rs`.
3. Keep `score_breakdown` additive: drop `vector` and `document_vector`, keep
   `lexical`, `meili`, `combined`, and add `semantic` if Meilisearch exposes
   a semantic hit marker.
4. The in-memory backend falls back to the lexical scorer only. It is for
   development and tests, where semantic ranking is not required.

## Consequences

- One semantic system, ranked by the same engine that applies filters.
- No process-wide lock or unreclaimed vector entries on the search path.
- A new runtime dependency (embedder service or provider) and secret, plus a
  one-time re-embedding when settings change.
- Simpler builds once `turbovec` and OpenBLAS are removed.
