# GET /v1/history/structured/snapshots/{snapshot_id}/rows

## Summary
List rows attached to a structured snapshot.

## Handler
- Rust handler: `list_rows`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard; snapshot owner enforced

## Path Parameters
| Name | Type | Description |
| --- | --- | --- |
| snapshot_id | string | Structured snapshot identifier. |

## Query Parameters
None.

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

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["UserGuard authenticates caller"]
  n1["Store.snapshot_owner_async resolves owner"]
  n2["require_owner_access enforces owner"]
  n3["Store.list_rows_async reads rows"]
  n4["Return store-defined rows JSON"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
```
