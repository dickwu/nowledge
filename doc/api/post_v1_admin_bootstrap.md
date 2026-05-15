# POST /v1/admin/bootstrap

## Summary
Bootstrap or reset managed Meilisearch indexes and settings.

Managed indexes include `rag_source_documents`, which stores full raw source documents outside default context/RAG retrieval.

## Handler
- Rust handler: `bootstrap`
- Route registration: `src/routes.rs::build_router`
- Authentication: AdminGuard

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
Schema: `BootstrapRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| reset | boolean | optional, default false | When true, asks the Meilisearch bootstrapper to reset managed indexes before applying settings. |

## Response
Schema: `BootstrapResponse`

| Field | Type | Description |
| --- | --- | --- |
| indexes | array | Managed index bootstrap results. |
| tasks | array | Meilisearch task identifiers or task details. |
| dry_run | boolean | Whether bootstrap ran without mutating indexes. |

### Managed RAG Index Notes
- Context indexes include filterable attributes for `node_kind`, `retrieval_role`, `retrieval_enabled`, `parent_uri`, `source_document_uri`, and `fragment_index`.
- `rag_source_documents` stores full source document content with `retrieval_enabled=false` by default.
- `rag_links` keeps `part_of` links from fragments to source documents.

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["AdminGuard authenticates caller"]
  n1["Read reset flag from JSON body"]
  n2["MeiliAdmin.bootstrap applies context, source document, and link index setup"]
  n3["Return index and task summary"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
```
