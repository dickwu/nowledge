# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Deep context — read before editing

Per-directory context lives in `AGENTS.md` files, organized hierarchically from the repo root down (`src/AGENTS.md`, `tests/AGENTS.md`, `doc/AGENTS.md`, `doc/api/AGENTS.md`, `scripts/AGENTS.md`, `.github/AGENTS.md`, `.github/workflows/AGENTS.md`). Each child file declares its parent on line 1 (`<!-- Parent: ../AGENTS.md -->`). Before editing inside a directory, read its `AGENTS.md` for module-level details — module purposes, file responsibilities, and gotchas live there, not here.

@AGENTS.md
@README.md

## Verify gauntlet

The pre-merge gauntlet is defined once, in `AGENTS.md` → *Testing Requirements* — that list is the source of truth; run it in that order and report results in that order. Clippy `-D warnings` is non-negotiable. What CI runs (including the Meilisearch integration job) is described in `.github/workflows/AGENTS.md`; don't restate either list here.

## Hard invariants

Isolation and token-redaction invariants are covered in `AGENTS.md`. Additionally — the regression suite covers each:

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
- **Commit style is conventional commits** (`feat:`, `fix:`, `chore:`, `refactor:`, `docs:`, etc.).

## Working notes

- `routes.rs`, `store.rs`, and `models.rs` are large by design — keep them flat; don't split into nested modules without a concrete reason.
- `MeiliAdmin` should only be called directly from admin bootstrap and the `/v1/debug/meili/search` shim. Everything else flows through `Store` → `KnowledgeRepository`.
- All ids use `util::new_id("prefix")` (uuid v7 simple form) — don't hand-mint identifiers.
- ContextFS ancestor walking goes through `util::ancestor_uris`; don't split on `/` directly because the `ctx://` prefix must be preserved.
- `build.rs` injects `NOWLEDGE_GIT_REV`, consumed at compile time via `env!` in the `routes.rs` health surfaces — the crate does not compile without the build script; don't remove or bypass it.
- The reported git rev gets a `-dirty` suffix only when tracked files are modified; untracked files are ignored by `build.rs`. `.omc/`, `.omx/`, `.agents/`, `.env`, and `scripts/` are gitignored, so agent session state does not dirty the build — a `<sha>-dirty` on a deployed `/livez` means modified tracked source and is worth investigating.
- Production deploy, verification, and rollback on `gfit` follow `doc/runbooks/gfit-deploy.md`.
