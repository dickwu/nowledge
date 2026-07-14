<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-05-20 -->

# doc

## Purpose
Human-readable API reference for `nowledge`. The directory contains one
hand-maintained markdown file per HTTP endpoint registered in
`src/routes.rs::build_router`, plus a top-level index and a machine-readable
manifest. Endpoint coverage is exhaustive — the manifest tracks 87 documented
APIs and must stay in sync with the router.

## Key Files
| File | Description |
|------|-------------|
| `README.md` | Endpoint index. Lists every documented route as a `\| Method \| Path \| Handler \| Document \|` row, plus narrative context for retrieval semantics (return profiles, include values, structured filters, traceback rules, citation provenance, and the `ContextNode.node_kind` / `retrieval_role` enumerations). |
| `api_manifest.json` | Machine-readable list of `{ method, path, handler, group, file }` entries. Groups in use: `Health`, `Admin`, `Harness`, `Analysis`, `Context`, `Debug`, `Eval`, `Context FS`, `Company Docs`, `History Alias`, `Insights`, `Structured History`, `History User Indexes`, `History Events`, `Links`, `LLM`, `Ingest`, `RAG`, `Sessions`, `State`, `Structured State`, `Usage`. Useful for tooling that validates handler/path coverage. |
| `adr/0003-http-ingest-runtime-boundaries.md` | Decision record for typed HTTP limits, production CORS, stable pressure/timeout errors, streamed multipart staging, queue admission, recovery, and coordinated shutdown. |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `api/` | Per-endpoint markdown docs, one file per route (see `api/AGENTS.md`). |

## For AI Agents

### Working In This Directory
- The index in `README.md` is the source of truth for human navigation;
  `api_manifest.json` is the source of truth for tooling. Both must list every
  route in `src/routes.rs::build_router`.
- When adding or renaming a route in `routes.rs`:
  1. Update `src/routes.rs` and the handler signature.
  2. Create or rename `doc/api/{method_lowercase}_{path_with_underscores}.md`
     (see `doc/api/AGENTS.md` for the file-naming convention).
  3. Append a row to the table in `doc/README.md`.
  4. Append a `{ method, path, handler, group, file }` entry in
     `doc/api_manifest.json`.
- When a request/response schema changes, update the corresponding endpoint
  document. The integration tests in `tests/api_spec.rs` are the behavior
  contract, but the docs are the contract for downstream consumers.

### Testing Requirements
- No automated linter currently validates `doc/` against `routes.rs`. If you
  touch this layout, manually grep `routes.rs` for `.route("/v1/...", ...)`
  entries and confirm each appears once in both `README.md` and
  `api_manifest.json`.

### Common Patterns
- Endpoint docs are written in a consistent template — see
  `api/AGENTS.md` for the exact section ordering.
- Retrieval-related endpoints (`/v1/context/*`, `/v1/rag/*`, `/v1/fs/*`) link
  back to the retrieval narrative in `README.md` rather than repeating it.

## Dependencies

### Internal
- `src/routes.rs` — source of truth for endpoints. Every documented entry must
  match the router.
- `src/models.rs` — schema references (e.g. `ContextSearchRequest`,
  `ContextSearchResponse`, `HealthResponse`).

### External
None. The directory is plain markdown plus a JSON manifest.

<!-- MANUAL: -->
