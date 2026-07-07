---
name: add-route
description: Add (or rename, or change the shape of) an HTTP route in the nowledge axum service following the project's documented multi-file workflow. Use when the user says "add a route", "add an endpoint", "expose a new API", "wire up POST/GET/PUT/PATCH/DELETE /v1/...", or when changing an existing route's path/method/schema.
---

# add-route

A new route in `nowledge` touches five files in a fixed order. Follow this checklist exactly — missing a step is the most common drift between the running service and the API docs.

## Inputs to gather first

Before writing anything:
1. HTTP method (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`).
2. Path (e.g. `/v1/state/profile/facts/{fact_key}`).
3. Rust handler name (snake_case, matches `src/routes.rs::build_router` registration).
4. Group label for the manifest — pick from the existing `group` values in `doc/api_manifest.json` (e.g. `Health`, `Harness`, `Eval`, `Company Docs`); introduce a new group only for a genuinely new API surface.
5. Authentication mode: `None` | `UserGuard` | `UserGuard; owner default may apply` | `AdminGuard`.
6. Request and response schema names from `src/models.rs` (or new types you'll add there).

## Step 1 — code

Edit in this order:

1. **`src/models.rs`** — add or update the request/response structs and any DTOs. Derive `Debug, Clone, Serialize, Deserialize` and skip empty `Option<...>` with `#[serde(skip_serializing_if = "Option::is_none")]`. If the route is owner-scoped, the request should carry an `Option<String> owner_user_id`.
2. **`src/store.rs`** — add the domain logic. Keep mutations behind `StoreData` and the `RwLock`. Use `util::new_id("prefix")` for new IDs and `EventIndexResolver::idempotency_hash` for any idempotency key.
3. **`src/repository.rs`** — only if the route needs to persist through the Meili backend. Add the trait method on `KnowledgeRepository`, implement on both `MeiliKnowledgeRepository` and `MemoryKnowledgeRepository`. Memory impl is usually a no-op.
4. **`src/routes.rs`** — register the handler in `build_router` and write the handler function. Use `State<AppState>`, the appropriate guard, and return `Result<Json<Response>, ApiError>`. For owner-scoped routes, call `guard.apply_owner_default(&mut req.owner_user_id)?` early.

## Step 2 — tests

Add a regression test in `tests/api_spec.rs` (or a new dedicated test file if the surface is large). Use the existing helpers:

- `app()` for unauthenticated runs.
- `authed_app()` for owner-isolation tests (users `u1`, `u2`, `admin`).
- `call(app, method, uri, body)` and `call_with_token(app, method, uri, body, Some(token))` for dispatch.

At minimum cover the happy path. For owner-scoped routes, **always** add an owner-mismatch test (request from `u1` for `u2`'s data → 403). The owner-isolation invariant is load-bearing.

## Step 3 — docs (do not skip)

Three coordinated updates:

1. **Create `doc/api/{method_lowercase}_{path_with_underscores}.md`** using the template from `doc/api/AGENTS.md`. Path braces are dropped; segments joined with `_`. Examples:
   - `GET /v1/fs/read` → `get_v1_fs_read.md`
   - `POST /v1/admin/history/user-event-indexes:reconcile` → `post_v1_admin_history_user_event_indexes_reconcile.md`

   Section order: Title (`# METHOD /path`), `## Summary`, `## Handler`, `## Path Parameters`, `## Query Parameters`, `## JSON Body Parameters` (with `Schema:` line referencing the type in `src/models.rs`), `## Response`, `## Errors and Access Rules`, `## Internal Logic Call Graph` (a Mermaid `flowchart TD`).

2. **Add a row to the index table in `doc/README.md`**, alphabetized within its method group:
   ```markdown
   | `METHOD` | `/v1/path` | `handler_name` | [api/<file>.md](api/<file>.md) |
   ```

3. **Append an entry to `doc/api_manifest.json`**:
   ```json
   {
     "method": "METHOD",
     "path": "/v1/path",
     "handler": "handler_name",
     "group": "GroupLabel",
     "file": "api/<file>.md"
   }
   ```

## Step 4 — verify

Run the `/verify` gauntlet. If you bumped a public response shape, also rerun the relevant integration block by hand to confirm the docs match runtime behavior.

## Cross-check

After everything is in place:
```sh
grep -nE '\.route\("' src/routes.rs | wc -l
ls doc/api/ | wc -l
jq 'length' doc/api_manifest.json
```

The doc count and manifest count must match the number of registered routes. When the count changes, update the "Total documented APIs" line in `doc/README.md` to match.
