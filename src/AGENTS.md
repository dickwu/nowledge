<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-07-13 -->

# src

## Purpose
All Rust source for the `nowledge` crate. Flat module layout (no nested `mod`
directories) — every file under `src/` is a top-level module declared in
`lib.rs`. The binary entrypoint `main.rs` consumes the library through
`build_router(AppState::new(...))`.

## Key Files
| File | Description |
|------|-------------|
| `main.rs` | Binary entrypoint. Initializes JSON tracing, parses `Config::from_env()`, runs `validate_startup`, hydrates the repository-backed store when `store_backend=meili`, and serves the router over a `tokio::net::TcpListener`. |
| `lib.rs` | Module re-exports. Public surface: `Config`, `build_router`, `AppState`, plus the `auth`, `config`, `error`, `fragmenter`, `llm`, `meili`, `models`, `parser`, `repository`, `request_context`, `resolver`, `routes`, `store`, `util`, `vector_match` modules. |
| `config.rs` | `Config`, `AuthUserConfig`, `AuthUserScope`, and `BearerTokenScope`. Parses `RAG_*` variables and rejects unknown run modes plus malformed, duplicate, empty, short, or ambiguous credentials during `validate_startup()`. New production deployments require a random `RAG_INDEX_HASH_SECRET` with at least 32 bytes and 12 distinct byte values; an existing weak-key deployment may preserve its key only behind the bounded migration flag. Provides `Config::test()`, `analysis_llm_config()`, dynamic Codex-token history, and the configured-secret inventory including explicit restart-spanning previous secrets. |
| `error.rs` | `ApiError` enum (`BadRequest`, `Unauthorized`, `Forbidden`, `NotFound`, `Conflict`, `Upstream`, `Internal`) with status mapping and one `{ "error": { code, message, details } }` envelope. Reusable safe-diagnostic helpers reduce raw causes to an allowlisted category and keyed fingerprint; private API errors then serialize stable generic messages plus a request ID. |
| `auth.rs` | Explicit `PrincipalScope` (`Owner`, `TenantService`, `Admin`) plus `UserGuard`, `CompanyWriterGuard`, and `AdminGuard`. Authentication scans all configured credentials with constant-time comparison; owner scope and feature roles are enforced independently. |
| `request_context.rs` | Server-generated UUIDv7 request context. Overwrites untrusted inbound `x-request-id`, propagates the trusted ID to responses, supplies task-local correlation to audit/error logs, and creates keyed cause fingerprints without emitting raw diagnostics. |
| `resolver.rs` | `EventIndexResolver` — HMAC-SHA256 derivation of `tenant_hash` (12 hex), `owner_user_id_hash` (16 hex), and `idempotency_hash` (24 hex). Produces `EventIndexRouting` with the canonical `rag_events__t_*__u_*` and `rag_context__t_*__u_*` UIDs. |
| `util.rs` | Cross-cutting helpers: `now()`, `new_id()` (uuid v7 with prefix), `hmac_hex`, `sanitize_slug`, `validate_meili_uid`, `ancestor_uris` (ContextFS), `require_string`, `text_score` (lowercase contains scoring), `truncate_chars`, and exact/equal-length/boundary configured-secret sanitizers. |
| `models.rs` (~54 KB) | All wire types and domain structs: `HistoryEvent`, `ContextNode`, `StateItem`, `InsightRecord`, `CompanySource`, `SourceRevision`, `SourceDocument`, `ParseArtifact`, `ParsedBlock`, `IngestTask`/`IngestTaskResult`, `StructuredSnapshot`, `KnowledgeLink`, `HarnessComponent`/`HarnessChange`/`HarnessChangeVerdict`, `RagEvalCase`/`RagEvalRun`/`RagEvalOverview`, plus every request/response DTO. |
| `routes.rs` (~74 KB) | `build_router`, `AppState`, `IngestTaskManager`, and every axum handler. Wires `/livez`, `/healthz`, `/readyz`, `/v1/usage`, `/v1/admin/*`, `/v1/state/*`, `/v1/history/*`, `/v1/context/*`, `/v1/rag/*`, `/v1/links/*`, `/v1/analysis/*`, `/v1/ingest/*`, `/v1/fs/*`, `/v1/sessions/*`, `/v1/llm/*`, `/v1/debug/*`. Adds request context, compression, CORS, trace layers, safe background-failure diagnostics, and a final dynamic configured-secret sanitizer for every JSON response. |
| `store.rs` (~264 KB) | `Store` — `Arc<RwLock<StoreData>>` plus an `EventIndexResolver` and a boxed `KnowledgeRepository`. Authoritative in-memory state for events, context nodes, state items, insights, sources/revisions/documents, parse artifacts, ingest tasks, datasets/snapshots/rows, sessions, traces, links, harness components/changes/verdicts, and eval runs. Provides `hydrate_from_repository` for cold start under the Meili backend. |
| `repository.rs` (~45 KB) | `KnowledgeRepository` trait + concrete `MeiliKnowledgeRepository` (writes through `MeiliAdmin`) and `MemoryKnowledgeRepository` (no-op). `repository_from_config` returns the right impl. Also defines `RepositoryContextSearchQuery`, the fragment-search adapter, and best-effort cleanup logs that use bounded cause diagnostics plus fingerprinted source IDs. |
| `meili.rs` (~20 KB) | `MeiliAdmin` — direct `reqwest` client for the admin surface (index create/delete, settings, primary keys, search). Lists `FIXED_INDEXES` (the 27 shared indexes: `rag_company_context`, `rag_state_items`, `rag_user_event_indexes`, sources, structured datasets, insights, links, sessions, traces, harness, ingest, eval, etc.). `bootstrap()` provisions or resets them. |
| `llm.rs` (~34 KB) | `LlmClient` trait, plus `OpenAi*`, `Codex*`, `Mock`, and `None` implementations. `LlmHealthProbe` caches probe results with a TTL/stale window and surfaces `auth_valid`, `quota_state`, `rate_limit_state`, and `RateLimitSnapshot` to `/healthz` and `/v1/llm/status`. Codex auth resolution reads `RAG_CODEX_AUTH_PATH` / `CODEX_AUTH_PATH`. |
| `parser.rs` (~16 KB) | `DocumentParser` trait, `BuiltinTextParser` (UTF-8 text only), `MineruParserClient` (multipart POST to `RAG_MINERU_API_URL` with `RAG_MINERU_BACKEND`). `parser_from_config` picks the impl. `parser_health_status` exposes parser readiness to `/healthz`. |
| `fragmenter.rs` (~13 KB) | `DocumentFragmenter` (legacy plain-text chunker with `chunk_size_chars=1200`, `overlap_chars=150`, `min_chunk_chars=200` defaults) and `BlockAwareFragmenter` (block-anchored chunking that preserves `page_idx`, `bbox`, `section_path`, `asset_refs` for traceback/highlight). Drives the active retrieval fragments. |
| `vector_match.rs` | Hybrid document matching on top of the `turbovec` quantized vector index. Deterministic signed feature-hash embeddings (word unigrams/bigrams, 5-char prefixes, char trigrams, log-TF, L2-normalized, dim 512), `VectorMatcher` with lazy/warm entry maintenance keyed by scoped `{index_uid}\|{uri}` strings, allowlist-restricted scoring, and `VectorScoreMap` blend policy (`RAG_VECTOR_MATCH_*` knobs). Vector fallback logs use bounded cause diagnostics. Includes a calibration regression test tied to the shipped `min_score` default. |

## Subdirectories
None. All Rust code lives at this level.

## For AI Agents

### Working In This Directory
- Module additions must be declared in `lib.rs` to be reachable from `main.rs`
  and the integration tests. Don't expect filename-based auto-discovery.
- `AppState` is `Clone` and threads `Arc<Config>` plus the `Store` clone; every
  handler takes `State<AppState>`. Mutations live behind `Store`'s `RwLock`.
- Owner-scoped routes accept `owner_user_id` either as a path param or in the
  request body. Use `UserGuard::apply_owner_default` early so `Owner` principals
  receive their bound owner, then use `require_owner_read` / `require_owner_write`
  (or the compatibility alias `require_owner_access`) to reject mismatches with
  403. `TenantService` must supply an explicit owner where the route requires
  one; feature roles such as `company_writer` never widen private-data scope.
- Anything writing into the Meili backend goes through `Store` (which calls
  `KnowledgeRepository`). Do not call `MeiliAdmin` directly from handlers
  except in admin bootstrap and the `/v1/debug/meili/search` shim.
- LLM provider routing splits `llm_provider`/`llm_model` (used by `/v1/rag/*`)
  from `analysis_llm_provider`/`analysis_llm_model` (used by
  `/v1/analysis/insights`). When `history_event_id` is supplied to the analysis
  endpoint, the caller must also be bound to the matching `owner_user_id`.
- Per-unit vs absolute contract pitfall: any cross-system numeric contract
  (e.g. macros stored per-unit vs absolute) must be agreed at this boundary;
  see lessons from GFI-205 if structured-dataset writes are involved.

### Testing Requirements
- Touching `routes.rs`, `store.rs`, `repository.rs`, or `resolver.rs` always
  warrants running `cargo test` — the regression suite covers per-user
  isolation, owner-mismatch rejection, state upsert shape, company-doc
  preflight, structured-row idempotency, and token redaction.
- When changing a public response shape, also update the matching
  `doc/api/{method}_{path}.md` and rerun the integration tests.

### Common Patterns
- All ids are minted with `util::new_id("prefix")` (uuid v7 simple form).
- All idempotency keys are HMAC-hashed via `EventIndexResolver::idempotency_hash`
  before being used as map keys.
- `redact_secrets` is the canonical JSON sanitizer. The router applies it to
  every JSON response after the handler and before compression; keep this layer
  inside `CompressionLayer` so it always receives uncompressed bytes. The
  sanitizer fails closed for malformed JSON and caps buffered JSON at 16 MiB.
- Mask configured secrets with equal character counts before fragmentation, and
  redact plus project a full context fragment before snippet truncation. Actual
  fragment bodies use a four-character reconstruction floor; general response
  and provider text uses an eight-character floor to preserve ordinary words.
  A one-second background task refreshes the Codex auth-file snapshot on the
  blocking pool; LLM clients and response/provider redaction consume that same
  cached snapshot, and request/liveness paths never read the file. This keeps
  legacy fragments and rotated credentials covered without a token-use race.
  Locator fields redact complete configured secrets only: do not apply
  heuristic fragment or credential-shape rewriting to protocol identifiers,
  because it breaks stable `ctx://` dereferencing. Keep revoked values in
  `RAG_REDACTION_PREVIOUS_SECRETS` across restarts until persisted records are
  reingested or scrubbed.
- ContextFS ancestor walking uses `util::ancestor_uris`; never split on `/`
  directly because the `ctx://` prefix must be preserved.

## Dependencies

### Internal
- `crate::auth` depends on `crate::routes::AppState` (extractor wiring).
- `crate::request_context` depends on `crate::config`, `crate::llm`, and
  `crate::util` for request correlation and secret-aware log redaction.
- `crate::routes` depends on every other module in the crate.
- `crate::store` depends on `crate::repository`, `crate::resolver`,
  `crate::fragmenter`, `crate::parser`, `crate::models`, `crate::util`.
- `crate::repository` depends on `crate::meili`, `crate::models`,
  `crate::resolver`, `crate::util`.

### External
See the root `AGENTS.md` for the crate-level dependency list; nothing extra is
pulled in below this layer.

<!-- MANUAL: -->
