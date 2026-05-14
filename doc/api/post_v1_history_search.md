# POST /v1/history/search

## Summary
Owner-aware alias for searching history events without owner in the path.

## Handler
- Rust handler: `search_events_alias`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard; body owner or owner-bound auth required

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
Schema: `HistorySearchRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| query | string | optional | Full-text query used for event search. |
| event_types | string[] | optional, default [] | Restrict results to these event types. |
| entity_type | string | optional | Restrict results to one entity type. |
| entity_id | string | optional | Restrict results to one entity id. |
| owner_user_id | string | optional or path-derived | Owner scope for alias endpoints; path-scoped routes override it. |
| from | RFC3339 datetime | optional | Lower occurred_at bound. |
| to | RFC3339 datetime | optional | Upper occurred_at bound. |
| limit | integer | optional, default 10 | Maximum number of events returned. |

## Response
Schema: `HistorySearchResponse`

| Field | Type | Description |
| --- | --- | --- |
| hits | HistoryEvent[] | Matching history events. |
| routing | EventIndexRouting | Owner index routing searched. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["UserGuard authenticates caller"]
  n1["apply_owner_default fills body owner_user_id when possible"]
  n2["Store.search_events_async resolves owner routing"]
  n3["Return search response"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
```
