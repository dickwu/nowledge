# Nowledge

ContextFS-style State/History RAG service built from the v0.6 user-event-index
specs.

## What Is Implemented

- Rust + axum API service.
- `/v1/state/*` update-style current state APIs.
- `/v1/history/*` append-only history APIs.
- HMAC-derived per-user event indexes:
  `rag_events__t_{tenant_hash}__u_{user_hash}`.
- HMAC-derived per-user personal context indexes:
  `rag_context__t_{tenant_hash}__u_{user_hash}`.
- Shared company context index semantics via `rag_company_context`.
- ContextFS URI nodes with L0 `.abstract`, L1 `.overview`, and L2 detail/chunk
  layers.
- Staged retrieval and trace records.
- Company document preflight, revisions, and activation.
- Structured dataset snapshots, idempotent row ingestion, and deterministic
  numeric summary calculation.
- Hybrid document matching backed by [turbovec](https://github.com/RyanCodrai/turbovec)
  (Google's TurboQuant quantized vector index): saved documents and fragments
  are embedded into a local vector index, and retrieval blends lexical scores
  with fragment-level and document-level vector similarity.
- Obsidian-style bidirectional knowledge links with backlink/outbound search.
- Independent analysis API that can use a separate LLM provider/model to create
  links and insight records from ingested context.
- Insight, session, RAG answer/debug, LLM status, and debug route surfaces.
- Meilisearch bootstrap hooks with in-memory persistence for local development
  and tests.

## Run

```sh
cargo run
```

The default URL is `http://127.0.0.1:14242`.

Useful environment variables:

```sh
RAG_HOST=127.0.0.1
RAG_PORT=14242
RAG_TENANT_ID=default
RAG_INDEX_HASH_SECRET=change-me
RAG_STORE_BACKEND=memory
RAG_BEARER_TOKEN=optional-user-token
RAG_ADMIN_TOKEN=optional-admin-token
RAG_AUTH_USERS=owner-user-id:user-token:user
RAG_RUN_MODE=development
RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true
RAG_MEILI_URL=http://127.0.0.1:7700
RAG_MEILI_API_KEY=optional-meili-key
RAG_MEILI_WAIT_FOR_TASKS=false
RAG_PARSER_PROVIDER=builtin
RAG_MINERU_API_URL=http://127.0.0.1:8000
RAG_MINERU_BACKEND=hybrid-auto-engine
RAG_MINERU_RETURN_MD=true
RAG_MINERU_RETURN_CONTENT_LIST=true
RAG_MINERU_RETURN_MIDDLE_JSON=true
RAG_MINERU_RETURN_IMAGES=true
RAG_VECTOR_MATCH_ENABLED=true
RAG_VECTOR_MATCH_WEIGHT=4.0
RAG_VECTOR_MATCH_DOC_WEIGHT=2.0
RAG_VECTOR_MATCH_MIN_SCORE=0.25
RAG_INGEST_MAX_CONCURRENT_TASKS=2
RAG_INGEST_TASK_RETENTION_SECONDS=86400
RAG_INGEST_CLEANUP_INTERVAL_SECONDS=300
RAG_INGEST_WORKER_ENABLED=true
RAG_LLM_PROVIDER=none
RAG_LLM_MODEL=none
RAG_LLM_REASONING_EFFORT=optional-low-medium-high-xhigh
RAG_ANALYSIS_LLM_PROVIDER=none
RAG_ANALYSIS_LLM_MODEL=gpt-5.3-codex-spark
RAG_ANALYSIS_LLM_REASONING_EFFORT=optional-low-medium-high-xhigh
RAG_OPENAI_API_KEY=optional-openai-key
RAG_CODEX_AUTH_PATH=optional-explicit-codex-auth-json
RAG_HEALTH_LLM_ENABLED=true
RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS=30
RAG_HEALTH_LLM_PROBE_TTL_SECONDS=60
RAG_HEALTH_LLM_MAX_STALE_SECONDS=120
RAG_HEALTH_LLM_TIMEOUT_MS=10000
RAG_HEALTH_REQUIRE_LLM=true
```

Use `RAG_STORE_BACKEND=meili` with `RAG_MEILI_URL` to mirror core writes to
Meilisearch and search per-user event indexes through Meilisearch. Production
mode requires configured auth unless `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true` is
set explicitly.

Health endpoints split process liveness from operational readiness:

- `GET /livez` returns only `{"status":"ok"}` and does not query Meilisearch or
  LLM providers.
- `GET /healthz` checks Meilisearch plus the configured LLM provider/model,
  including auth validity, quota/rate-limit state, stale probe state, and a
  compact usage summary. If `RAG_HEALTH_REQUIRE_LLM=true`, an unconfigured or
  exhausted LLM makes the service unhealthy.
- The `llm.rate_limits` block carries the freshest live provider budget
  snapshot. For `codex_auth` it is parsed from the ChatGPT Codex `x-codex-*`
  response headers on every health probe and real completion: `primary` (5h)
  and `secondary` (weekly) windows with `used_percent` / `remaining_percent` /
  reset times, `plan_type`, credits, and model-scoped `additional_limits`.
  `llm.rate_limit_state` becomes `near_limit` at ≥90% window usage and
  `limited` at 100%, so dashboards can warn before hard 429s.
- LLM-backed responses (`/v1/rag/answer`, `/v1/analysis/insights`,
  `/v1/llm/title`, `/v1/llm/test`) include real provider token counts in
  their `usage` blocks (`input_tokens`, `cached_input_tokens`,
  `output_tokens`, `reasoning_output_tokens`, `total_tokens`) whenever the
  upstream reports them, so consumers no longer need char-based estimates.
- `GET /readyz` uses the same readiness decision as `/healthz`.
- `GET /v1/usage` returns owner-scoped provider snapshots for ordinary users and
  global provider snapshots for admins.

Document parser ingestion is an additive layer in front of the existing RAG
backend. Use `RAG_PARSER_PROVIDER=builtin` for plain text fallback or
`RAG_PARSER_PROVIDER=mineru` to call a remote `mineru-api` service. Ingestion
APIs are `POST /v1/ingest/tasks`, `GET /v1/ingest/tasks/{task_id}`,
`GET /v1/ingest/tasks/{task_id}/result`, `POST /v1/ingest/uploads`,
`POST /v1/ingest/uploads:sync`, and `POST /v1/ingest/files:sync`.
`POST /v1/ingest/tasks` and `/v1/ingest/uploads` return queued task metadata
immediately; background workers perform parsing, fragmenting, and indexing.
Finished (`completed`/`failed`) task records and their stored results are
pruned after `RAG_INGEST_TASK_RETENTION_SECONDS` (default 86400; set 0 to
keep them forever), swept every `RAG_INGEST_CLEANUP_INTERVAL_SECONDS` —
covering both the in-memory maps and the mirrored Meilisearch documents.
Ingested fragments and source documents are unaffected; only the task
bookkeeping expires.
Multipart uploads send binary file bytes to MinerU when `parser_provider=mineru`;
the builtin parser accepts UTF-8 text uploads.
Parsed blocks become retrieval fragments; source documents and parse artifacts
are stored for traceback/read flows but are not searched by default.
`POST /v1/context/search` supports `compact`, `standard`, and `full` return
profiles plus optional include payloads for traceback, links, neighboring
fragments, source summaries, artifact refs, score breakdowns, and raw stage
debug. Default retrieval still returns only active fragments; standard/full
responses add source groups and location/block provenance for highlighting.
`POST /v1/rag/answer` citations preserve the same provenance, including source
document URI, page index, bounding box, block type, section path, artifact refs,
fragment offsets, and checksums when available.

Document matching is hybrid: full documents and their fragments are embedded
into a [turbovec](https://github.com/RyanCodrai/turbovec) quantized vector
index when they are saved, and context search blends the lexical substring
score with fragment-level vector similarity plus document-level vector
evidence from the fragment's source document. Inflected or reordered queries
("deployment pipelines" vs "deploy pipeline") match without an exact
substring, and document-level evidence boosts fragments that already match
on their own. Raw source document bodies stay out of default retrieval:
document evidence never admits a fragment by itself. Vector search is always restricted to the caller's
isolation-filtered candidates, embeddings are deterministic hashed lexical
features (no external embedding service), and `include: ["score_breakdown"]`
exposes the `lexical` / `vector` / `document_vector` / `combined` components
per hit. Disable with `RAG_VECTOR_MATCH_ENABLED=false`; tune the blend with
`RAG_VECTOR_MATCH_WEIGHT`, `RAG_VECTOR_MATCH_DOC_WEIGHT`, and
`RAG_VECTOR_MATCH_MIN_SCORE`. Building on Linux requires OpenBLAS
(`libopenblas-dev` on Debian/Ubuntu); macOS uses the built-in Accelerate
framework.

Link and analysis surfaces:

- `POST /v1/links` creates or updates a directed knowledge link between two
  ContextFS URIs. Link search treats the stored edge as bidirectional for
  navigation.
- `POST /v1/links/search` searches links by text, relation, URI, outbound edge,
  or backlink.
- `POST /v1/analysis/insights` searches ingested context, includes existing
  links as evidence, and can materialize new links plus insight records. It uses
  `RAG_ANALYSIS_LLM_PROVIDER` / `RAG_ANALYSIS_LLM_MODEL`, so analysis can run on
  a different model from `/v1/rag/answer`. `RAG_LLM_REASONING_EFFORT` is passed
  to configured Responses API calls, and
  `RAG_ANALYSIS_LLM_REASONING_EFFORT` can override it for analysis jobs. When
  `history_event_id` is supplied, the caller must also provide or be bound to
  `owner_user_id`; analysis evidence is then constrained to that owner's same
  history event index.

## Verify

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo check
cargo test
```

Optional Meilisearch integration tests run when `RAG_TEST_MEILI_URL` is set.
If the server requires a key, set `RAG_TEST_MEILI_API_KEY` too:

```sh
RAG_TEST_MEILI_URL=http://127.0.0.1:7700 \
RAG_TEST_MEILI_API_KEY=$MEILI_MASTER_KEY \
cargo test --test meili_integration
```

Optional live MinerU integration tests run when `RAG_TEST_MINERU_API_URL` is set:

```sh
RAG_TEST_MINERU_API_URL=http://127.0.0.1:8000 cargo test --test mineru_integration
```

The regression tests cover the v0.6 hard constraints around per-user event
isolation, owner mismatch rejection, state upsert shape, company-doc preflight,
structured row idempotency, and token redaction.
