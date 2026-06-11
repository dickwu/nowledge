<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-05-20 -->

# tests

## Purpose
Integration tests that exercise the full axum router built by
`nowledge::build_router`. Tests construct an `AppState` from a `Config::test()`
fixture (or a Meili/MinerU-backed variant) and drive HTTP requests through
`tower::ServiceExt::oneshot`. The suite enforces the v0.6 hard constraints
around per-user event isolation, owner-mismatch rejection, state upsert shape,
company-doc preflight, structured-row idempotency, and token redaction. It
also covers turbovec hybrid document matching: inflected-query matches with
zero lexical score, vector-path owner isolation, and document-level vector
evidence exposed through `score_breakdown`.

## Key Files
| File | Description |
|------|-------------|
| `api_spec.rs` (~86 KB) | Primary integration suite. Builds the router with `Config::test()` and several auth variants (`authed_app`, `bearer_user_app`, `mock_llm_app`, `analysis_llm_app`, `codex_import_app`, `stale_llm_health_app`, `llm_health_app(provider)`) and exercises every public route group — state, history, context, rag, links, analysis, ingest, sessions, harness, eval, debug, llm, and admin. |
| `meili_integration.rs` (~19 KB) | Meili-backed integration tests. Gated by `RAG_TEST_MEILI_URL` (and `RAG_TEST_MEILI_API_KEY` when the server is keyed). Verifies dynamic per-user index creation and the Meili-search path through the repository backend. |
| `mineru_integration.rs` (~5 KB) | Live MinerU integration test. Gated by `RAG_TEST_MINERU_API_URL`. Performs a multipart PDF upload through `/v1/ingest/uploads:sync`, asserts task `state=completed`, then runs `/v1/context/search` plus `/v1/context/traceback` to confirm fragment provenance survives the round trip. |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Tests run as separate binaries (Cargo `[[test]]` convention). They link
  against the library through `use nowledge::{build_router, AppState, Config};`
  and `use nowledge::config::AuthUserConfig;`.
- Use `Config::test()` as the baseline — it sets `allow_unsafe_unauthenticated=true`,
  `store_backend=memory`, and an in-memory `index_hash_secret`. Override fields
  on the returned `Config` before wrapping it in `Arc::new` and passing to
  `AppState::new`.
- For auth coverage, mirror `authed_app()` in `api_spec.rs`: three users
  (`u1-token`, `u2-token`, `admin-token`) with `u1`/`u2`/`admin` roles. This
  pattern is what proves owner isolation in the regression suite.
- For optional integration tests, follow the existing skip-on-unreachable
  pattern (`eprintln!` and `return`) so the suite still passes in environments
  without Meili or MinerU running locally.
- Meili tests must mint a unique `tenant_id` per run (`format!("test-tenant-{}", uuid::Uuid::now_v7())`)
  to avoid colliding with previous test debris when the same Meili server is reused.

### Testing Requirements
- `cargo test` runs `api_spec.rs` only by default. The two gated integration
  tests are surfaced via `cargo test --test meili_integration` and
  `cargo test --test mineru_integration`.
- Helper scripts under `scripts/` (`gfit_meili_test.sh`) run the Meili
  integration test through an SSH tunnel to the `gfit` host.
- Coverage targets the v0.6 hard constraints — anything that loosens
  per-user isolation, idempotency, or token redaction should fail at least one
  existing test.

### Common Patterns
- `call(app, method, uri, body)` and `call_with_token(app, method, uri, body, Some(token))`
  return `(StatusCode, Value)` pairs for ergonomic assertions.
- `read_response` is the shared helper that maps an `axum::response::Response`
  into `(StatusCode, Value)`; it falls back to `{"raw": "..."}` for non-JSON
  bodies (used by the MinerU multipart smoke).
- The MinerU test ships a tiny inline PDF fixture (`mineru_pdf_fixture`) so it
  doesn't depend on external binary fixtures on disk.

## Dependencies

### Internal
- `nowledge` crate (library re-exports `build_router`, `AppState`, `Config`,
  and `config::AuthUserConfig`).

### External
- `tower` 0.5 (dev) — `ServiceExt::oneshot` for direct router dispatch.
- `axum` (re-exported via the crate) — request/response building.
- `serde_json`, `tokio`, `uuid`, `reqwest` — shared with the library.

<!-- MANUAL: -->
