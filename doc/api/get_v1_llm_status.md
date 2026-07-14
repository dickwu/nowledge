# GET /v1/llm/status

## Summary
Return authenticated, sanitized status for the configured LLM client.

## Handler
- Rust handler: `llm_status`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
No JSON body.

## Response
Schema: `LlmStatusResponse`

| Field | Type | Description |
| --- | --- | --- |
| provider | string | Configured provider. |
| model | string | Configured model. |
| auth_source | string | Non-sensitive credential-source category such as `codex_file`, `environment`, `mock`, or `none`; never a path or secret. |
| healthy | boolean | Client health result. |

## Errors and Access Rules
- Missing or invalid bearer authentication returns 401.
- Any authenticated owner, tenant-service, company-writer, or admin principal may read the sanitized status.
- Provider failures use the shared ApiError JSON envelope without exposing raw upstream response bodies or credential paths.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["Router dispatches GET /v1/llm/status"]
  n1["UserGuard authenticates caller"]
  n2["Build LLM client from effective config"]
  n3["client.status checks provider/model/auth source"]
  n4["Map auth source to a non-sensitive category"]
  n5["Return LlmStatusResponse"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
  n4 --> n5
```
