<!-- Generated: 2026-05-20 | Updated: 2026-07-15 -->

# nowledge

## Purpose
ContextFS-style State/History RAG service implementing the v0.6 user-event-index
specs. Provides per-user Meilisearch event indexes, per-user personal context
indexes, shared company context, ContextFS URI navigation, document ingestion
(builtin/MinerU), staged retrieval with provenance, and LLM-backed analysis and
RAG answer surfaces. Built as a single Rust + axum binary that runs on
`127.0.0.1:14242` by default and optionally mirrors writes to Meilisearch.

## Key Files
| File | Description |
|------|-------------|
| `Cargo.toml` | Crate manifest. Name `nowledge`, edition 2021, MSRV 1.88. Pins axum 0.8, meilisearch-sdk 0.33, reqwest 0.12, and uuid v7. |
| `Cargo.lock` | Locked dependency graph for reproducible builds. |
| `README.md` | Public-facing run/verify guide. Lists environment variables, the locked verify/package command set, and optional Meili/MinerU integration test gates. |
| `.env.example` | Safe, explicitly local development baseline with secret placeholders only. |
| `SECURITY.md` | Supported-version and private vulnerability reporting policy. |
| `.gitignore` | Rust build, local environment, deployment helper, and agent-state ignores. |
| `deny.toml` | Cargo dependency advisory, license, ban, and source policy. |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `src/` | Rust source and compatibility façades (see `src/AGENTS.md`). |
| `tests/` | Integration tests against the full router (see `tests/AGENTS.md`). |
| `doc/` | Per-endpoint API documentation generated to mirror `routes.rs` (see `doc/AGENTS.md`). |
| `.github/` | GitHub Actions CI and Dependabot configuration (see `.github/AGENTS.md`). |

Ignored and excluded from AGENTS.md generation: `target/` (build output),
`.git/`, local `scripts/`, and the `.omc/` / `.omx/` session-state caches.

## For AI Agents

### Working In This Directory
- The crate is published as the public repo `dickwu/nowledge`. The deployed
  production-style service lives on the `gfit` host at `/server` with Nowledge
  on `127.0.0.1:14242` and Meili on `127.0.0.1:7700`; that boundary intentionally
  excludes local deployment scripts and runtime artifacts.
- Default bind address is `127.0.0.1:14242`. Override with `RAG_HOST` / `RAG_PORT`.
- Storage backend is `memory` by default. Set `RAG_STORE_BACKEND=meili` plus
  `RAG_MEILI_URL` to use Meilisearch. Production also requires distinct
  least-privilege `RAG_MEILI_API_KEY` (runtime document/search) and
  `RAG_MEILI_ADMIN_API_KEY` (managed index/settings) credentials, with pinned
  `createdAt` identities for the durable operations and audit indexes.
- Auth modes: `RAG_BEARER_TOKEN`, `RAG_ADMIN_TOKEN`, or a comma-separated
  `RAG_AUTH_USERS=owner:token:role|role` list. Production mode requires one of
  these unless `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true` is set explicitly.
- All per-user index UIDs are HMAC-derived: `rag_events__t_{tenant_hash}__u_{user_hash}`
  and `rag_context__t_{tenant_hash}__u_{user_hash}`. Never hand-roll these — go
  through `resolver::EventIndexResolver`.
- Per-user isolation is a hard regression-test invariant: owner mismatch must
  return 403, not silently fall through.
- Tokens and bearer-style secrets must be redacted at trace/log boundaries via
  `util::redact_secrets` / `util::redact_string`. The regression suite covers
  token redaction explicitly.

### Testing Requirements
- Default verify gauntlet:
  ```sh
  cargo fmt --check
  cargo clippy --locked --all-targets -- -D warnings
  cargo check --locked --all-targets
  cargo test --locked --test route_manifest
  cargo test --locked
  cargo package --locked
  ```
- Optional Meili integration tests are gated by `RAG_TEST_MEILI_URL`
  (and `RAG_TEST_MEILI_API_KEY` when the server requires a key):
  `cargo test --test meili_integration`.
- Optional MinerU integration tests are gated by `RAG_TEST_MINERU_API_URL`:
  `cargo test --test mineru_integration`.
- CI (`.github/workflows/ci.yml`) runs locked stable quality/package gates,
  checks Rust 1.88 compatibility, audits RustSec advisories, and applies the
  `cargo-deny` dependency policy on every push/PR and on a weekly schedule.

### Common Patterns
- One axum `Router` built in `src/routes.rs::build_router`, shared `AppState`
  composed of `Config`, `Store`, `MeiliAdmin`, provider health/runtime state,
  `IngestTaskManager`, protected metrics, and a narrow audit recorder.
- All errors flow through `error::ApiError` and serialize as a single
  `{ "error": { "code", "message", "details" } }` envelope.
- ContextFS URIs use the `ctx://` scheme; `util::ancestor_uris` walks parent
  segments for fs-style listings.
- Ingest jobs are queued through an mpsc + Semaphore worker pool sized by
  `RAG_INGEST_MAX_CONCURRENT_TASKS` (default 2). Per-task results land in the
  store keyed by `task_id`.

## Dependencies

### External
- `axum` 0.8 — HTTP router and extractors.
- `tokio` 1 — async runtime.
- `tower-http` 0.6 — compression, CORS, tracing middleware.
- `meilisearch-sdk` 0.33 — Meilisearch client (plus direct `reqwest` for admin
  bootstrap calls).
- `reqwest` 0.12 — HTTP client for Meili admin, MinerU, and LLM providers.
- `hmac` / `sha2` — per-user index HMAC derivation.
- `serde` / `serde_json` — wire format.
- `uuid` 1 (v7) — id minting.
- `chrono` — timestamps.
- `turbovec` 0.9 — TurboQuant quantized vector index behind
  `src/vector_match.rs` hybrid document matching. Links OpenBLAS on Linux
  (CI and Linux hosts need `libopenblas-dev`) and Accelerate on macOS.
- `prometheus-client` 0.25 — OpenMetrics encoding and bounded process metrics.
- `thiserror` 2, `anyhow` 1, and `secrecy` 0.10 — supporting.

### Runtime Services
- Meilisearch (optional) — `RAG_MEILI_URL`, fixed indexes listed in
  `src/meili.rs::FIXED_INDEXES`, plus dynamic per-user indexes.
- MinerU parser (optional) — `RAG_MINERU_API_URL` when `RAG_PARSER_PROVIDER=mineru`.
- LLM providers (optional) — `RAG_LLM_PROVIDER` / `RAG_ANALYSIS_LLM_PROVIDER`
  (`none`, `mock`, OpenAI, Codex). Analysis can use a different provider/model
  from the main RAG answer surface.

<!-- MANUAL: Custom project notes can be added below -->
