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
RAG_INDEX_HASH_SECRET=
RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET=false
RAG_STORE_BACKEND=memory
RAG_BEARER_TOKEN=optional-user-token
RAG_BEARER_TOKEN_SCOPE=owner
RAG_BEARER_TOKEN_OWNER_USER_ID=owner-user-id
RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER=false
RAG_ALLOW_LEGACY_SHARED_WRITER=false
RAG_ADMIN_TOKEN=optional-admin-token
RAG_AUTH_USERS=owner-user-id:user-token:user,*:service-token:user,writer-owner:writer-token:user|company_writer
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
RAG_MAX_JSON_BYTES=2097152
RAG_MAX_UPLOAD_BYTES=52428800
RAG_MAX_MULTIPART_FIELDS=32
RAG_UPLOAD_ALLOWED_MIME_TYPES=text/plain,text/markdown,application/octet-stream,application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/vnd.openxmlformats-officedocument.presentationml.presentation,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,image/png,image/jpeg,image/webp,image/gif,image/tiff
RAG_MAX_BULK_EVENTS=500
RAG_MAX_BULK_ROWS=5000
RAG_MAX_SEARCH_LIMIT=100
RAG_MAX_TAGS_PER_ITEM=64
RAG_MAX_TAG_BYTES=128
RAG_REQUEST_TIMEOUT_MS=30000
RAG_SYNC_INGEST_TIMEOUT_MS=120000
RAG_MAX_IN_FLIGHT_REQUESTS=256
RAG_RATE_LIMIT_REQUESTS_PER_MINUTE=600
RAG_CORS_ALLOWED_ORIGINS=*
RAG_ALLOW_WILDCARD_CORS=false
RAG_VECTOR_MATCH_ENABLED=true
RAG_VECTOR_MATCH_WEIGHT=4.0
RAG_VECTOR_MATCH_DOC_WEIGHT=2.0
RAG_VECTOR_MATCH_MIN_SCORE=0.25
RAG_INGEST_MAX_CONCURRENT_TASKS=2
RAG_INGEST_QUEUE_CAPACITY=16
RAG_INGEST_TASK_RETENTION_SECONDS=86400
RAG_INGEST_CLEANUP_INTERVAL_SECONDS=300
RAG_INGEST_WORKER_ENABLED=true
RAG_SHUTDOWN_TIMEOUT_MS=30000
RAG_LLM_PROVIDER=none
RAG_LLM_MODEL=none
RAG_LLM_REASONING_EFFORT=optional-low-medium-high-xhigh
RAG_ANALYSIS_LLM_PROVIDER=none
RAG_ANALYSIS_LLM_MODEL=gpt-5.3-codex-spark
RAG_ANALYSIS_LLM_REASONING_EFFORT=optional-low-medium-high-xhigh
RAG_OPENAI_API_KEY=optional-openai-key
RAG_CODEX_AUTH_PATH=optional-explicit-codex-auth-json
RAG_REDACTION_PREVIOUS_SECRETS=optional-revoked-token,optional-prior-token
RAG_HEALTH_LLM_ENABLED=true
RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS=30
RAG_HEALTH_LLM_PROBE_TTL_SECONDS=60
RAG_HEALTH_LLM_MAX_STALE_SECONDS=120
RAG_HEALTH_LLM_TIMEOUT_MS=10000
RAG_HEALTH_REQUIRE_LLM=true
```

Production requires an independently generated `RAG_INDEX_HASH_SECRET` of at
least 32 bytes with at least 12 distinct byte values. The public development
default, the previously documented `change-me`, and literal documentation
placeholders are rejected; do not reuse an authentication credential. Generate
a new value once with `openssl rand -base64 48`, store it in the service's
secret manager, and keep it stable. This key protects owner/index derivation
plus audit and error fingerprints.

Changing this key changes every per-user event and personal-context index UID.
Before upgrading an existing Meilisearch deployment, classify its current key
without printing it. If the deployment already uses a weak key, preserve that
exact value and temporarily set
`RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET=true`; rotating it in place would make
the existing indexes unreachable. New deployments must never enable this flag.
Remove it only after the `index_hash_secret_v1` migration/reindex has completed
and been verified. The compatibility path expires on 2026-10-01 / v0.13.0.

`RAG_RUN_MODE` accepts only `development`, `test`, or `production`; unknown
values are startup errors and never enable unauthenticated access by default.

HTTP and ingest boundaries are typed startup configuration. Numeric limits
must be positive, malformed values fail startup, and the synchronous ingest
timeout must be at least the ordinary request timeout. Request, sync-ingest,
and shutdown deadlines are capped at seven days so deadline arithmetic cannot
overflow or panic. JSON requests are
limited by `RAG_MAX_JSON_BYTES`; multipart file data is streamed to temporary
storage and limited independently by `RAG_MAX_UPLOAD_BYTES` and
`RAG_MAX_MULTIPART_FIELDS`. `RAG_UPLOAD_ALLOWED_MIME_TYPES` is a comma-separated
list normalized to exact lowercase MIME types; wildcards, duplicates, and an
empty list fail startup. The default allows plain text, Markdown, generic binary
(`application/octet-stream`), PDF, OOXML Word/PowerPoint/Excel, and PNG, JPEG,
WebP, GIF, and TIFF. Bulk event/row counts, tags, tag byte lengths, and search
limits are rejected before mutation when they exceed their configured
maximums. Limit failures use the normal error envelope and return 413 for body
or upload size, 429 for rate/queue pressure, 503 for global capacity or a
disabled/closing worker, and 504 for route timeouts. Pressure responses include
`Retry-After`, and every response includes a server-generated `X-Request-Id`.

`RAG_CORS_ALLOWED_ORIGINS` is a comma-separated list of exact `http://` or
`https://` origins. Development and test default to `*`; production defaults
to no allowed browser origin. A production wildcard is rejected unless it is
the sole origin and `RAG_ALLOW_WILDCARD_CORS=true` is set explicitly.
`RAG_MAX_IN_FLIGHT_REQUESTS` load-sheds excess work without consuming the
`/livez` capacity or timeout path. `/readyz` remains public but has its own
readiness-probe rate bucket. Rate limiting is keyed by the authenticated
logical tenant/principal, so rotating or adding a second token does not create
a new owner budget. CORS exposes `X-Request-Id` and `Retry-After` to allowed
browser origins. See
[ADR 0003](doc/adr/0003-http-ingest-runtime-boundaries.md) for middleware
ordering, overload semantics, upload staging, and rollout.

Use `RAG_STORE_BACKEND=meili` with `RAG_MEILI_URL` to mirror core writes to
Meilisearch and search per-user event indexes through Meilisearch. Production
mode requires configured auth unless `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true` is
set explicitly.

Authentication data scope is explicit. A named owner in `RAG_AUTH_USERS` creates
an owner-bound principal; the literal owner `*` creates a tenant-service
principal. Feature roles do not widen data scope: an owner-bound
`company_writer` can mutate shared company knowledge but still cannot read a
different owner's private data. `*:token:admin` creates admin scope (the
reserved `admin` value is a scope marker, not a feature role), while new admin
credentials should prefer `RAG_ADMIN_TOKEN`. Legacy named-owner
`owner:token:admin` entries temporarily retain admin scope and emit a startup
warning; migrate them to `*:token:admin` or `RAG_ADMIN_TOKEN` before the
compatibility window ends on 2026-10-01 in v0.13.0.

Every bearer, admin, and `RAG_AUTH_USERS` credential must contain at least eight
characters. Other configured secrets used by the response redactor—including
Meilisearch/OpenAI keys, a readable index-HMAC key, Codex auth-file tokens, and
explicit previous secrets—must contain at least four characters. Empty,
whitespace-padded, duplicate authentication credentials and shorter values are
startup errors; error text never echoes the rejected value.

The legacy `RAG_BEARER_TOKEN` must set `RAG_BEARER_TOKEN_SCOPE=owner` together
with `RAG_BEARER_TOKEN_OWNER_USER_ID`, or set
`RAG_BEARER_TOKEN_SCOPE=tenant_service`. Existing intentional tenant-service
clients may temporarily set `RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER=true`; that
compatibility switch is removed on 2026-10-01 in v0.13.0. Company-document
preflight, revision creation/activation, and dataset-schema upserts require the
`company_writer` role or admin scope by default. Operators may temporarily set
`RAG_ALLOW_LEGACY_SHARED_WRITER=true` to preserve ordinary authenticated access
to those shared writes while clients migrate; it emits a startup warning and
has the same removal deadline. Company-document deletion remains admin-only.
See [ADR 0002](doc/adr/0002-principal-scope-and-diagnostics.md) for rollout and
rollback details.

Tenant-service principals must select `owner_user_id` for private state-fact,
state-search, and insight-search reads; omission returns 403 instead of
implicitly searching whichever private owner happens to exist.

Health endpoints split process liveness from operational readiness:

- `GET /livez` returns process status plus build version/revision and does not
  query Meilisearch or LLM providers.
- `GET /readyz` is public for load balancers. It preserves the operational
  readiness decision and 200/503 status semantics while returning only coarse
  dependency state; it does not expose raw provider payloads, usage/private
  counts, plan data, credits, or credential sources.
- `GET /healthz` requires an admin bearer and checks Meilisearch plus the
  configured parser and LLM provider/model, including auth validity,
  quota/rate-limit state, stale probe state, and a compact usage summary. If
  `RAG_HEALTH_REQUIRE_LLM=true`, an unconfigured or exhausted LLM makes the
  service unhealthy.
- The protected health `llm.rate_limits` block carries the freshest live
  provider budget snapshot. For `codex_auth` it is parsed from the ChatGPT Codex `x-codex-*`
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
  Configured secrets are redacted before provider submission, model-output
  persistence/materialization, and response serialization. Analysis parses the
  provider JSON before redaction, validates proposed link locators against the
  prompt's context/seed/existing-link locator set, and then sanitizes free text.
- `GET /v1/usage` returns only the selected owner's usage counters to owner and
  tenant-service principals. Global counters and service-wide Meilisearch,
  parser, and LLM provider diagnostics are admin-only.
- `GET /v1/llm/status` requires authentication and reports `auth_source` as a
  category such as `codex_file` or `environment`, never as a filesystem path.
- Every JSON response passes through a final configured-secret sanitizer,
  including typed state/history/link responses, sync ingest results, parsed
  blocks, and explicit ContextFS source-document reads. Object keys and values
  are covered. JSON bodies above 16 MiB or malformed JSON bodies fail closed
  instead of bypassing the sanitizer. A background task reads the Codex auth
  file at most once per second on the blocking pool and atomically publishes one
  credential snapshot to both LLM clients and redaction; request and liveness
  paths never read the file. Observed tokens remain in the in-process inventory
  across rotations and transient auth-file errors. Structural locators remove
  complete configured secrets but
  do not apply heuristic fragment/token-shape rewriting, preserving stable
  `ctx://` navigation when an ordinary slug overlaps part of a credential.
- Provider previews are redacted before truncation. New ingest masks configured
  secrets with equal-length placeholders before fragmenting so provenance
  offsets remain stable; retrieval also masks configured-secret pieces at
  fragment boundaries. That second boundary prevents legacy data or a
  post-ingest credential rotation from sending split token halves to a provider
  or returning them in content-bearing JSON fields.

Before restarting after a credential rotation, place any revoked value that
can still occur in persisted documents in the comma-separated
`RAG_REDACTION_PREVIOUS_SECRETS` secret-manager value. Retain it until those
records are reingested or scrubbed. This bridges process restarts without
logging or returning the prior credential; remove entries only after verifying
that persisted data no longer contains them.

Every response carries a server-generated `X-Request-Id`. Generic 500 and 502
error bodies include that safe correlation ID without returning the underlying
cause. Request failures and best-effort background failures record only an
allowlisted cause category plus a keyed fingerprint; dynamic task/source IDs
are omitted or fingerprinted. Raw causes and identifiers are never emitted.

Document parser ingestion is an additive layer in front of the existing RAG
backend. Use `RAG_PARSER_PROVIDER=builtin` for plain text fallback or
`RAG_PARSER_PROVIDER=mineru` to call a remote `mineru-api` service. Ingestion
APIs are `POST /v1/ingest/tasks`, `GET /v1/ingest/tasks/{task_id}`,
`GET /v1/ingest/tasks/{task_id}/result`, `POST /v1/ingest/uploads`,
`POST /v1/ingest/uploads:sync`, and `POST /v1/ingest/files:sync`.
`POST /v1/ingest/tasks` and `/v1/ingest/uploads` return queued task metadata
immediately; background workers perform parsing, fragmenting, and indexing.
Queue capacity defaults to eight times `RAG_INGEST_MAX_CONCURRENT_TASKS` and
can be set explicitly with `RAG_INGEST_QUEUE_CAPACITY`. A full queue is rejected
immediately without creating an orphan task. Disabling the worker rejects new
asynchronous ingest while synchronous ingest remains available. Synchronous
ingest uses a separate immediate load-shed lane capped by
`RAG_INGEST_MAX_CONCURRENT_TASKS`; saturation returns 503 plus `Retry-After`
before buffering a JSON or multipart body. SIGINT/SIGTERM stop new queue
acceptance, allow supervised work to finish within
`RAG_SHUTDOWN_TIMEOUT_MS`, and mark unfinished tasks as failed with
`ingest_interrupted`; startup recovery applies the same terminal state to
persisted nonterminal tasks from a prior process.
Finished (`completed`/`failed`) task records and their stored results are
pruned after `RAG_INGEST_TASK_RETENTION_SECONDS` (default 86400; set 0 to
keep them forever), swept every `RAG_INGEST_CLEANUP_INTERVAL_SECONDS` —
covering both the in-memory maps and the mirrored Meilisearch documents.
Ingested fragments and source documents are unaffected; only the task
bookkeeping expires.
Multipart uploads are staged with a generated private filename, incrementally
hashed, and deleted after parsing or on cancellation/error. Duplicate file
parts, excess metadata fields, an empty file, invalid or disallowed MIME values,
and a supplied checksum that is not exactly 64 hexadecimal SHA-256 characters
or does not match the staged file are rejected. Each file part must carry a
`Content-Type` in `RAG_UPLOAD_ALLOWED_MIME_TYPES`; an optional `content_type`
metadata field must match it. A staged file cannot be combined with `content`,
`content_list`, `content_list_v2`, `middle_json`, or `model_json`.
MinerU receives a streaming file part when `parser_provider=mineru`; the builtin
parser performs one bounded UTF-8 read.
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
