# POST /v1/debug/meili/search

## Summary
Run a raw debug search against a Meilisearch index and redact configured secrets from the response.

This admin-only helper can inspect managed indexes directly, including `rag_source_documents`. It bypasses normal default retrieval behavior and should not be treated as user-facing RAG search semantics.

## Handler
- Rust handler: `debug_meili_search`
- Route registration: `src/routes.rs::build_router`
- Authentication: AdminGuard

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
Schema: `DebugMeiliSearchRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| index_uid | string | required | Target Meilisearch index UID, such as `rag_company_context`, an owner personal context index, `rag_links`, or `rag_source_documents`. |
| query | string | optional, default empty string | Raw search query passed to the debug search helper. |

## Response
Schema: `JsonValue`

| Field | Type | Description |
| --- | --- | --- |
| ... | object or array | Endpoint-specific JSON returned by the store or debug helper. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Admin authentication is required because raw index search can expose source document content.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.
- index_uid body field is required.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["AdminGuard authenticates caller"]
  n1["require_string validates index_uid"]
  n2["Store.debug_meili_search_async executes raw search"]
  n3["redact_for_state removes configured secrets"]
  n4["Return raw redacted result"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
```
