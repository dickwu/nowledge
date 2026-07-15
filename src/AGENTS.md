<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-07-15 -->

# src

## Purpose
All Rust source for the `nowledge` crate. The filesystem stays flat (no nested
module directories): top-level modules are declared in `lib.rs`, while the
`models_*.rs` and `store_*.rs` feature companions are declared from their
compatibility façades with `#[path]`. The binary entrypoint `main.rs` consumes
the library through `build_router(AppState::new(...))`.

## Key Files
| File | Description |
|------|-------------|
| `main.rs` | Binary entrypoint. Initializes JSON tracing, parses `Config::from_env()`, runs `validate_startup`, verifies pinned durable Meilisearch identities before managed-index reconciliation, captures the reconciled identities, hydrates the repository-backed store, and serves the router with SIGINT/SIGTERM graceful shutdown through `AppState`. |
| `lib.rs` | Flat module declarations and compatibility re-exports. Public surface remains `Config`, `build_router`, `AppState`, `REGISTERED_ROUTES`, route metadata, and the established public modules. Feature route/service/model/store modules stay private behind those paths. |
| `app.rs` | Owns `AppState`, the narrow extractor-only `AuthState`, ingest task lifecycle, runtime supervision, shutdown, and background cleanup. `routes.rs` re-exports `AppState` and `IngestTaskManager` for compatibility. |
| `config.rs` | `Config`, `AuthUserConfig`, `AuthUserScope`, and `BearerTokenScope`. Parses `RAG_*` variables and rejects unknown run modes/backends/providers; malformed URLs, reasoning effort, vector weights, health bounds, and HTTP/upload/bulk/search/queue/timeout/shutdown limits; unsafe production CORS; plus malformed, duplicate, empty, short, or ambiguous credentials during `validate_startup()`. Production Meilisearch requires distinct runtime/admin keys and, outside explicit first provisioning, pinned durable-index `createdAt` identities. New production deployments require a random `RAG_INDEX_HASH_SECRET` with at least 32 bytes and 12 distinct byte values; an existing weak-key deployment may preserve its key only behind the bounded migration flag. Provides `Config::test()`, `analysis_llm_config()`, dynamic Codex-token history, and the configured-secret inventory including explicit restart-spanning previous secrets. |
| `error.rs` | `ApiError` enum with the stable 400/401/403/404/409/413/429/502/503/504/500 status/code mappings and one `{ "error": { code, message, details } }` envelope. Validation errors identify the field; pressure errors carry `Retry-After`. Reusable safe-diagnostic helpers reduce raw causes to an allowlisted category and keyed fingerprint; private API errors then serialize stable generic messages plus a request ID. |
| `auth.rs` | Explicit `PrincipalScope` (`Owner`, `TenantService`, `Admin`) plus `UserGuard`, `CompanyWriterGuard`, and `AdminGuard`. Extractors depend on the small `app::AuthState`, not router/application state. Authentication scans all configured credentials with constant-time comparison; owner scope and feature roles are enforced independently. |
| `request_context.rs` | Server-generated UUIDv7 request context. Overwrites untrusted inbound `x-request-id`, propagates the trusted ID to responses, supplies task-local correlation to audit/error logs, and creates keyed cause fingerprints without emitting raw diagnostics. |
| `resolver.rs` | `EventIndexResolver` — HMAC-SHA256 derivation of `tenant_hash` (12 hex), `owner_user_id_hash` (16 hex), and `idempotency_hash` (24 hex). Produces `EventIndexRouting` with the canonical `rag_events__t_*__u_*` and `rag_context__t_*__u_*` UIDs. |
| `util.rs` | Cross-cutting helpers: `now()`, `new_id()` (uuid v7 with prefix), `hmac_hex`, `sanitize_slug`, `validate_meili_uid`, `ancestor_uris` (ContextFS), `require_string`, `text_score` (lowercase contains scoring), `truncate_chars`, and exact/equal-length/boundary configured-secret sanitizers. |
| `models.rs` | Compatibility façade that preserves `nowledge::models::*` while re-exporting twelve flat `models_*.rs` feature modules for common/defaults, history, state, company docs, structured data, context/RAG/LLM, insights/links/analysis, ingest, operations, sessions, and harness/eval types. |
| `http_boundary.rs` | HTTP boundary middleware primitives: validated exact-origin CORS, bounded non-multipart bodies, immediate semaphore load shedding, fixed-window trusted-principal rate limiting, route deadlines, and `/livez` bypass. All generated failures use `ApiError`. |
| `runtime.rs` | Internal watch/JoinHandle supervisor used by `AppState` to coordinate ingest dispatch, cleanup, and bounded shutdown without detached worker ownership. |
| `route_registry.rs` | Canonical route policy registry. Generates the Axum registrations and static endpoint manifest together, and enforces at compile time that each handler's first extractor matches its declared `Public`, `User`, `CompanyWriter`, or `Admin` guard. |
| `routes.rs` | Router façade only: assembles all feature handlers, request context, bounded HTTP middleware, compression, configured CORS, tracing, and the final fail-closed configured-secret sanitizer for JSON responses. Registered domain handlers live in flat `route_*.rs` modules. |
| `route_*.rs` | Thin HTTP boundaries grouped by health, ingest, RAG/analysis/LLM, company docs, context, history, state, structured data, sessions, eval, and harness. They parse/authorize/validate and delegate orchestration to sibling services. |
| `*_service.rs` | Axum-free feature orchestration for health, ingest, RAG/streaming/analysis/LLM, company docs, context, history, state, structured data, sessions, eval, and harness. |
| `request_validation.rs` | Axum-free centralized request bounds used across canonical and alias routes so maximum item, tag, history bulk, and search-limit behavior cannot drift. |
| `store.rs` | Core `Store`, `StoreData`, mutation gate, persistence coordination, hydration, and cross-feature retrieval/cache state. Feature APIs are split into nine flat `store_*.rs` modules for accessors, company docs, context, harness/eval, history, ingest, sessions, state/insights, and structured data. |
| `repository.rs` (~45 KB) | `KnowledgeRepository` trait + concrete `MeiliKnowledgeRepository` (writes through `MeiliAdmin`) and `MemoryKnowledgeRepository` (no-op). `repository_from_config` returns the right impl. Also defines `RepositoryContextSearchQuery`, the fragment-search adapter, and best-effort cleanup logs that use bounded cause diagnostics plus fingerprinted source IDs. |
| `meili.rs` (~20 KB) | `MeiliAdmin` — redacting direct `reqwest` client used as a paired least-privilege runtime and managed-index/admin surface. Lists the shared `FIXED_INDEXES`, refuses partial or unapproved empty index sets, verifies pinned durable generations before bootstrap, reconciles settings, and guards operations/audit writes plus readiness against generation drift. |
| `llm.rs` (~34 KB) | `LlmClient` trait, plus `OpenAi*`, `Codex*`, `Mock`, and `None` implementations. `LlmHealthProbe` caches probe results with a TTL/stale window and surfaces `auth_valid`, `quota_state`, `rate_limit_state`, and `RateLimitSnapshot` to `/healthz` and `/v1/llm/status`. Codex auth resolution reads `RAG_CODEX_AUTH_PATH` / `CODEX_AUTH_PATH`. |
| `parser.rs` (~16 KB) | `DocumentParser` trait, reference-counted delete-on-drop staged uploads, `BuiltinTextParser` (one bounded UTF-8 read), and `MineruParserClient` (length-known streaming multipart POST to `RAG_MINERU_API_URL` with `RAG_MINERU_BACKEND`). `parser_from_config` picks the impl. `parser_health_status` exposes parser readiness to `/healthz`. |
| `fragmenter.rs` (~13 KB) | `DocumentFragmenter` (legacy plain-text chunker with `chunk_size_chars=1200`, `overlap_chars=150`, `min_chunk_chars=200` defaults) and `BlockAwareFragmenter` (block-anchored chunking that preserves `page_idx`, `bbox`, `section_path`, `asset_refs` for traceback/highlight). Drives the active retrieval fragments. |
| `vector_match.rs` | Hybrid document matching on top of the `turbovec` quantized vector index. Deterministic signed feature-hash embeddings (word unigrams/bigrams, 5-char prefixes, char trigrams, log-TF, L2-normalized, dim 512), `VectorMatcher` with lazy/warm entry maintenance keyed by scoped `{index_uid}\|{uri}` strings, allowlist-restricted scoring, and `VectorScoreMap` blend policy (`RAG_VECTOR_MATCH_*` knobs). Vector fallback logs use bounded cause diagnostics. Includes a calibration regression test tied to the shipped `min_score` default. |

## Subdirectories
None. All Rust code lives at this level.

## For AI Agents

### Working In This Directory
- Top-level module additions must be declared in `lib.rs`; feature companions
  must be declared explicitly from their façade with `#[path]`. Don't expect
  filename-based auto-discovery.
- `AppState` is `Clone` and threads the shared runtime dependencies. HTTP
  handlers use `State<AppState>`; authentication extractors use the narrower
  `AuthState`. Mutations live behind `Store`'s lock and persistence coordinator.
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
- `crate::auth` depends on `crate::app::AuthState` for extractor wiring and does
  not depend on `routes` or `AppState`.
- `crate::request_context` depends on `crate::config`, `crate::llm`, and
  `crate::util` for request correlation and secret-aware log redaction.
- `crate::routes` depends on the feature `route_*` modules, config, request
  context, and HTTP boundary middleware; route modules call Axum-free services.
- `crate::store` and its feature modules depend on `crate::repository`, `crate::resolver`,
  `crate::fragmenter`, `crate::parser`, `crate::models`, `crate::util`.
- `crate::repository` depends on `crate::meili`, `crate::models`,
  `crate::resolver`, `crate::util`.

### External
See the root `AGENTS.md` for the crate-level dependency list; nothing extra is
pulled in below this layer.

<!-- MANUAL: -->
