# GET /v1/fs/overview

## Summary
Read layer 1 overview content for a context URI.

## Handler
- Rust handler: `fs_overview`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard; owner default may apply

## Path Parameters
None.

## Query Parameters
| Name | Type | Requirement | Description |
| --- | --- | --- | --- |
| uri | string | optional except read/abstract/overview | Context URI to list from or read. |
| depth | integer | optional | Tree traversal depth for /v1/fs/tree. |
| owner_user_id | string | optional | Owner scope. Owner-bound auth can supply a default. |

## JSON Body Parameters
No JSON body.

## Response
Schema: `JsonValue`

| Field | Type | Description |
| --- | --- | --- |
| ... | object or array | Endpoint-specific JSON returned by the store or debug helper. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.
- uri query parameter is required.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["UserGuard authenticates caller"]
  n1["apply_owner_default fills owner query when possible"]
  n2["require_string validates uri"]
  n3["Store.fs_layer_async reads layer 1"]
  n4["Return ContextNode JSON"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
```
