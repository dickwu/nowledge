# GET /healthz

## Summary
Operational health probe for Meilisearch, LLM health, store backend, and compact usage.

## Handler
- Rust handler: `healthz`
- Route registration: `src/routes.rs::build_router`
- Authentication: None

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
No JSON body.

## Response
Schema: `HealthResponse`

| Field | Type | Description |
| --- | --- | --- |
| status | string | ok, degraded, or unhealthy. |
| ready | boolean | True when Meilisearch and required LLM checks allow traffic. |
| store_backend | string | Active store backend name. |
| meilisearch | object | Meilisearch health payload. |
| llm | object | LLM health payload with provider, model, auth, quota, and stale status. |
| usage | object | Compact usage summary. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["Router dispatches GET /healthz"]
  n1["healthz calls operational_health"]
  n2["Store builds compact usage summary"]
  n3["MeiliAdmin checks health"]
  n4["LlmHealthProbe checks configured provider"]
  n5["HTTP status is 200 when ready, 503 when unhealthy"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
  n4 --> n5
```
