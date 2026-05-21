# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Deep context — read before editing

Per-directory context lives in `AGENTS.md` files, organized hierarchically from the repo root down (`src/AGENTS.md`, `tests/AGENTS.md`, `doc/AGENTS.md`, `doc/api/AGENTS.md`, `scripts/AGENTS.md`, `.github/AGENTS.md`, `.github/workflows/AGENTS.md`). Each child file declares its parent on line 1 (`<!-- Parent: ../AGENTS.md -->`). Before editing inside a directory, read its `AGENTS.md` for module-level details — module purposes, file responsibilities, and gotchas live there, not here.

@AGENTS.md
@README.md

## Verify gauntlet

The project's pre-merge verification is exactly this sequence — match it, in order, when reporting work done:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo check
cargo test
```

Clippy with `-D warnings` is non-negotiable; CI fails on any warning. `--all-targets` covers tests and examples.

Optional integration tests are gated by environment variables and are skipped (with `eprintln!`) when unset:
- `cargo test --test meili_integration` requires `RAG_TEST_MEILI_URL` (and `RAG_TEST_MEILI_API_KEY` when the server is keyed).
- `cargo test --test mineru_integration` requires `RAG_TEST_MINERU_API_URL`.

## Hard invariants

These break callers if violated — the regression suite covers each:

- **Per-user isolation**: every per-user index UID must come from `resolver::EventIndexResolver` (`rag_events__t_{tenant_hash}__u_{user_hash}` and `rag_context__t_{tenant_hash}__u_{user_hash}`). Owner mismatch returns 403, never a silent fallback.
- **Token redaction**: anything that may land in a trace/log/debug payload must pass through `util::redact_secrets` / `util::redact_string` before serialization.
- **Idempotency keys** are HMAC-hashed via `EventIndexResolver::idempotency_hash` before use as map keys.
- **Per-unit vs absolute numeric contracts** must be agreed at boundaries — don't silently mix the two in structured-dataset writes (lesson from GFI-205 in cross-system contracts).

## Project conventions

- **Module layout is flat under `src/`** — every file is a top-level module declared in `src/lib.rs`. Add a new file → add the `pub mod` line in `lib.rs`, otherwise it won't reach the binary or the tests.
- **Routes ↔ docs are tightly coupled.** Adding, renaming, or changing the shape of an endpoint in `src/routes.rs::build_router` requires synchronized updates to:
  1. The `| Method | Path | Handler | Document |` table in `doc/README.md`.
  2. The `{ method, path, handler, group, file }` entry in `doc/api_manifest.json`.
  3. A matching `doc/api/{method_lowercase}_{path_with_underscores}.md` file using the template in `doc/api/AGENTS.md`.
  The `/add-route` skill walks this checklist.
- **`scripts/` is gitignored** (operator-private gfit deploy + Meili tunnel helpers). Don't add scripts there expecting them to ship; do not auto-commit changes inside `scripts/`.
- **Commit style is conventional commits** (`feat:`, `fix:`, `chore:`, `refactor:`, `docs:`, etc.). The most recent example is `feat: add POST /v1/llm/title for LLM-based document summarization`.

## Service runtime

Default bind is `127.0.0.1:14242`. Storage defaults to in-memory (`RAG_STORE_BACKEND=memory`); set `RAG_STORE_BACKEND=meili` + `RAG_MEILI_URL` to mirror writes to Meilisearch. Production mode (`RAG_RUN_MODE=production`) requires `RAG_BEARER_TOKEN`, `RAG_ADMIN_TOKEN`, or `RAG_AUTH_USERS` — or an explicit `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true`. The full env-var list is in `README.md`.

LLM provider routing splits the main RAG provider (`RAG_LLM_PROVIDER` / `RAG_LLM_MODEL`, consumed by `/v1/rag/*`) from the analysis provider (`RAG_ANALYSIS_LLM_PROVIDER` / `RAG_ANALYSIS_LLM_MODEL`, consumed by `/v1/analysis/insights`). Both default to `none`.

## Working notes

- `routes.rs`, `store.rs`, and `models.rs` are large by design — keep them flat; don't split into nested modules without a concrete reason.
- `MeiliAdmin` should only be called directly from admin bootstrap and the `/v1/debug/meili/search` shim. Everything else flows through `Store` → `KnowledgeRepository`.
- All ids use `util::new_id("prefix")` (uuid v7 simple form) — don't hand-mint identifiers.
- ContextFS ancestor walking goes through `util::ancestor_uris`; don't split on `/` directly because the `ctx://` prefix must be preserved.
