<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-05-20 -->

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
| `lib.rs` | Module re-exports. Public surface: `Config`, `build_router`, `AppState`, plus the `auth`, `config`, `error`, `fragmenter`, `llm`, `meili`, `models`, `parser`, `repository`, `resolver`, `routes`, `store`, `util` modules. |
| `config.rs` | `Config` and `AuthUserConfig` structs. Parses all `RAG_*` env vars in `from_env()`, with `validate_startup()` enforcing the Meili-URL invariant and production-auth invariant. Provides `Config::test()` for fixtures and `analysis_llm_config()` to swap to the analysis provider. |
| `error.rs` | `ApiError` enum (`BadRequest`, `Unauthorized`, `Forbidden`, `NotFound`, `Conflict`, `Upstream`, `Internal`) with `IntoResponse` mapping to status codes and a single `{ "error": { code, message, details } }` JSON envelope. |
| `auth.rs` | `Principal`, `UserGuard`, `AdminGuard` axum extractors. Resolves the bearer token against `RAG_AUTH_USERS`, then `RAG_ADMIN_TOKEN`, then `RAG_BEARER_TOKEN`. Enforces `require_owner_access` and applies the owner default when missing. |
| `resolver.rs` | `EventIndexResolver` — HMAC-SHA256 derivation of `tenant_hash` (12 hex), `owner_user_id_hash` (16 hex), and `idempotency_hash` (24 hex). Produces `EventIndexRouting` with the canonical `rag_events__t_*__u_*` and `rag_context__t_*__u_*` UIDs. |
| `util.rs` | Cross-cutting helpers: `now()`, `new_id()` (uuid v7 with prefix), `hmac_hex`, `sanitize_slug`, `validate_meili_uid`, `ancestor_uris` (ContextFS), `require_string`, `text_score` (lowercase contains scoring), `truncate_chars`, `redact_secrets` / `redact_string`. |
| `models.rs` (~54 KB) | All wire types and domain structs: `HistoryEvent`, `ContextNode`, `StateItem`, `InsightRecord`, `CompanySource`, `SourceRevision`, `SourceDocument`, `ParseArtifact`, `ParsedBlock`, `IngestTask`/`IngestTaskResult`, `StructuredSnapshot`, `KnowledgeLink`, `HarnessComponent`/`HarnessChange`/`HarnessChangeVerdict`, `RagEvalCase`/`RagEvalRun`/`RagEvalOverview`, plus every request/response DTO. |
| `routes.rs` (~74 KB) | `build_router`, `AppState`, `IngestTaskManager`, and every axum handler. Wires `/livez`, `/healthz`, `/readyz`, `/v1/usage`, `/v1/admin/*`, `/v1/state/*`, `/v1/history/*`, `/v1/context/*`, `/v1/rag/*`, `/v1/links/*`, `/v1/analysis/*`, `/v1/ingest/*`, `/v1/fs/*`, `/v1/sessions/*`, `/v1/llm/*`, `/v1/debug/*`. Adds compression, CORS, and trace layers. |
| `store.rs` (~264 KB) | `Store` — `Arc<RwLock<StoreData>>` plus an `EventIndexResolver` and a boxed `KnowledgeRepository`. Authoritative in-memory state for events, context nodes, state items, insights, sources/revisions/documents, parse artifacts, ingest tasks, datasets/snapshots/rows, sessions, traces, links, harness components/changes/verdicts, and eval runs. Provides `hydrate_from_repository` for cold start under the Meili backend. |
| `repository.rs` (~45 KB) | `KnowledgeRepository` trait + concrete `MeiliKnowledgeRepository` (writes through `MeiliAdmin`) and `MemoryKnowledgeRepository` (no-op). `repository_from_config` returns the right impl. Also defines `RepositoryContextSearchQuery` and the search adapter the store calls during fragment retrieval. |
| `meili.rs` (~20 KB) | `MeiliAdmin` — direct `reqwest` client for the admin surface (index create/delete, settings, primary keys, search). Lists `FIXED_INDEXES` (the 27 shared indexes: `rag_company_context`, `rag_state_items`, `rag_user_event_indexes`, sources, structured datasets, insights, links, sessions, traces, harness, ingest, eval, etc.). `bootstrap()` provisions or resets them. |
| `llm.rs` (~34 KB) | `LlmClient` trait, plus `OpenAi*`, `Codex*`, `Mock`, and `None` implementations. `LlmHealthProbe` caches probe results with a TTL/stale window and surfaces `auth_valid`, `quota_state`, `rate_limit_state`, and `RateLimitSnapshot` to `/healthz` and `/v1/llm/status`. Codex auth resolution reads `RAG_CODEX_AUTH_PATH` / `CODEX_AUTH_PATH`. |
| `parser.rs` (~16 KB) | `DocumentParser` trait, `BuiltinTextParser` (UTF-8 text only), `MineruParserClient` (multipart POST to `RAG_MINERU_API_URL` with `RAG_MINERU_BACKEND`). `parser_from_config` picks the impl. `parser_health_status` exposes parser readiness to `/healthz`. |
| `fragmenter.rs` (~13 KB) | `DocumentFragmenter` (legacy plain-text chunker with `chunk_size_chars=1200`, `overlap_chars=150`, `min_chunk_chars=200` defaults) and `BlockAwareFragmenter` (block-anchored chunking that preserves `page_idx`, `bbox`, `section_path`, `asset_refs` for traceback/highlight). Drives the active retrieval fragments. |

## Subdirectories
None. All Rust code lives at this level.

## For AI Agents

### Working In This Directory
- Module additions must be declared in `lib.rs` to be reachable from `main.rs`
  and the integration tests. Don't expect filename-based auto-discovery.
- `AppState` is `Clone` and threads `Arc<Config>` plus the `Store` clone; every
  handler takes `State<AppState>`. Mutations live behind `Store`'s `RwLock`.
- Owner-scoped routes accept `owner_user_id` either as a path param or in the
  request body. Use `UserGuard::apply_owner_default` early so the caller's
  configured `owner_user_id` is filled when missing, and `require_owner_access`
  rejects mismatched callers with 403.
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
- `redact_secrets` is the canonical sink for any JSON that may be returned in
  trace/debug payloads — apply it before serializing to a trace record.
- ContextFS ancestor walking uses `util::ancestor_uris`; never split on `/`
  directly because the `ctx://` prefix must be preserved.

## Dependencies

### Internal
- `crate::auth` depends on `crate::routes::AppState` (extractor wiring).
- `crate::routes` depends on every other module in the crate.
- `crate::store` depends on `crate::repository`, `crate::resolver`,
  `crate::fragmenter`, `crate::parser`, `crate::models`, `crate::util`.
- `crate::repository` depends on `crate::meili`, `crate::models`,
  `crate::resolver`, `crate::util`.

### External
See the root `AGENTS.md` for the crate-level dependency list; nothing extra is
pulled in below this layer.

<!-- MANUAL: -->
