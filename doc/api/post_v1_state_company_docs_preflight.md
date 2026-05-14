# POST /v1/state/company-docs/preflight

## Summary
Evaluate a company document candidate for duplicate/similar source handling before creating a revision.

## Handler
- Rust handler: `preflight_doc`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
Schema: `CompanyDocPreflightRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| title | string | optional | Candidate document title. |
| source_uri | string | optional | Original document URI. |
| content_type | string | optional | MIME type or logical content type. |
| text_preview | string | optional | Preview text used for similarity checks. |
| checksum | string | optional | Content checksum used to detect duplicates. |
| tags | string[] | optional, default [] | Document tags. |
| scope | string | optional | Document visibility or business scope. |
| similarity_threshold | number | optional, default 0.82 | Threshold used to flag similar existing sources. |

## Response
Schema: `CompanyDocPreflightResponse`

| Field | Type | Description |
| --- | --- | --- |
| decision_id | string | Preflight decision id. |
| recommended_action | string | Recommended ingest/create action. |
| confidence | number | Decision confidence. |
| matched_sources | object[] | Potentially matching sources. |
| reasons | string[] | Decision reasons. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["UserGuard authenticates caller"]
  n1["Store.preflight_company_doc compares candidate metadata and preview text"]
  n2["Return decision, reasons, and matched sources"]
  n0 --> n1
  n1 --> n2
```
