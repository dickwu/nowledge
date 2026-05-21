<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-05-20 -->

# api

## Purpose
One markdown document per HTTP endpoint exposed by `src/routes.rs::build_router`.
Each document captures the request shape, response shape, access rules, error
behavior, and a Mermaid flowchart of the internal call graph so callers can
review behavior without reading Rust source. The directory currently holds 68
endpoint files plus no index of its own (the index lives in `../README.md`).

## Key Files
Examples (one of every documented HTTP verb):

| File | Description |
|------|-------------|
| `get_healthz.md` | Operational health probe documentation. Lists Meilisearch, LLM, store-backend, and usage payload fields. |
| `get_livez.md` | Process-liveness probe. Returns `{"status":"ok"}` and does not query Meilisearch or LLM. |
| `get_readyz.md` | Readiness probe — same readiness decision as `/healthz` but documented separately for ergonomics. |
| `post_v1_context_search.md` | Search documentation. Defines the structured filter grammar (`source_id`, `revision_id`, `block_type`, `page_idx*`, `section_path_contains`, `artifact_kind`), the `compact`/`standard`/`full` return profiles, and the supported `include` values (`traceback`, `links`, `neighbor_fragments`, `source_summary`, `artifact_refs`, `score_breakdown`, `raw_stage_debug`). |
| `put_v1_state_profile_facts_fact_key.md` | Upsert state fact. Documents `PUT` semantics and the path parameter `fact_key`. |
| `patch_v1_state_insights_insight_id.md` | Partial update of an insight record. |
| `post_v1_ingest_uploads.md` / `post_v1_ingest_uploads_sync.md` | Async vs synchronous multipart upload flows for parser ingestion. |

The full set is enumerated in `../README.md` and `../api_manifest.json`.

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- **File naming**: `{method_lowercase}_{path}.md`. Path segments are joined with
  `_` and braces are dropped. Examples:
  - `GET /v1/fs/read` → `get_v1_fs_read.md`
  - `POST /v1/state/structured/datasets/{dataset_key}/apply-snapshot` →
    `post_v1_state_structured_datasets_dataset_key_apply_snapshot.md`
  - Action verbs use `_` as the separator: `POST /v1/admin/history/user-event-indexes:reconcile`
    → `post_v1_admin_history_user_event_indexes_reconcile.md`.
- **Template (in order)**: Title (`# METHOD /path`), `## Summary`, `## Handler`
  (Rust handler name, route registration line, authentication note),
  `## Path Parameters`, `## Query Parameters`, `## JSON Body Parameters`
  (with a `Schema:` reference to the type in `src/models.rs`),
  `## Response` (with `Schema:` reference + a `| Field | Type | Description |` table),
  `## Errors and Access Rules`, `## Internal Logic Call Graph`
  (a Mermaid `flowchart TD` showing dispatch → handler → store/repository →
  status code).
- Cross-link related retrieval semantics into `../README.md` rather than
  duplicating long narrative sections in every endpoint doc.
- Keep the handler/route names accurate — they are the join key tooling uses
  against `api_manifest.json` to detect drift from `src/routes.rs`.

### Testing Requirements
- No automated test. Manual cross-check against the router when adding a route:
  ```sh
  grep -nE '\.route\("' src/routes.rs | wc -l   # should equal the number of doc files
  ls doc/api/ | wc -l
  ```

### Common Patterns
- Authentication notes use one of `None`, `UserGuard`, `UserGuard; owner default may apply`,
  `AdminGuard`, or `AdminGuard required`.
- Errors section is uniform: malformed JSON → 400, owner mismatch → 403, store/
  Meili/LLM failures → shared `ApiError` envelope. Endpoint-specific failure
  modes are appended below.

## Dependencies

### Internal
- `src/routes.rs` — handler/route ground truth.
- `src/models.rs` — referenced schema types in each doc's `Schema:` lines.
- `../README.md` — owns the cross-cutting retrieval narrative.
- `../api_manifest.json` — must list every file in this directory.

### External
- Mermaid renderers (GitHub, IDEs, doc generators) consume the
  `## Internal Logic Call Graph` blocks.

<!-- MANUAL: -->
