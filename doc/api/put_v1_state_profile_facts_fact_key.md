# PUT /v1/state/profile/facts/{fact_key}

## Summary
Create or merge a profile/state fact by natural key. When a document payload is supplied, store the full document separately and index searchable fragments.

## Handler
- Rust handler: `upsert_state_fact`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard; owner default may apply

## Path Parameters
| Name | Type | Description |
| --- | --- | --- |
| fact_key | string | Natural key for a profile/state fact. |

## Query Parameters
None.

## JSON Body Parameters
Schema: `UpsertStateFactRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| owner_user_id | string | optional, auth default may apply | Owner for the state fact. Required after auth defaults are applied. |
| state_type | string | required | State category such as profile, preference, or memory. |
| title | string | optional | Display title for the fact. |
| statement | string | required | Canonical natural-language statement. Keep this as a summary when `document` is supplied. |
| value | JSON value | optional, default null | Structured value for the fact. |
| confidence | number | optional, default 0.7 | Confidence score. |
| salience | number | optional, default 0.5 | Salience score. |
| valid_from | RFC3339 datetime | optional | Start of validity interval. |
| valid_to | RFC3339 datetime | optional | End of validity interval. |
| source_refs | SourceRef[] | optional, default [] | Evidence references. |
| document | object | optional | Full document payload to store as a non-retrieval `SourceDocument` and split into fragments. |
| fragment_policy | object | optional | Fragmenting policy applied when `document` is provided. |
| merge_policy | string | optional, default merge | How to merge with existing facts with the same key. |
| idempotency_key | string | optional | Client deduplication key. |

### Document Fields
| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| content | string | required when document is provided | Full document content. This is stored on the source document, not copied into the state fact statement. |
| content_type | string | optional | Source content type, such as markdown or plain text. |
| source_uri | string | optional | External source URI for traceability. |
| fragment_policy | object | optional | Per-document fragmenting policy; overrides the top-level policy for this document. |

### FragmentPolicy Fields
| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| chunk_size_chars | integer | optional, default 1200 | Target fragment size in characters. |
| overlap_chars | integer | optional, default 150 | Character overlap between adjacent fragments. |
| min_chunk_chars | integer | optional, default 200 | Minimum fragment size before small chunks are merged where possible. |

## Response
Schema: `StateItemResponse`

| Field | Type | Description |
| --- | --- | --- |
| item | StateItem | Current state fact. |
| history_event_id | string | History event emitted for the mutation. |
| context_uri | string | Context URI for the fact. |
| decision | string | Store merge/upsert decision. |

### Document Ingestion Notes
- `StateItem.statement` remains the current-state summary and should not contain the entire document.
- The full document is stored as a source document with `retrieval_enabled=false`.
- Generated fragments are written to the owner's personal context index with `retrieval_enabled=true` and `retrieval_role=fragment`.
- System `part_of` links connect each fragment to the source document.
- Response `item.source_refs` includes a `source_document` reference with `source_document_uri` and fragment metadata when a document is ingested.

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Tenant-service principals must provide `owner_user_id`; the service never
  infers a write target from existing tenant data.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["UserGuard authenticates caller"]
  n1["Path fact_key selects natural key"]
  n2["apply_owner_default fills owner_user_id when possible"]
  n3["Store.upsert_state_fact_async writes state and history event"]
  n4["If document exists, store SourceDocument, fragments, and part_of links"]
  n5["Return StateItemResponse"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
  n4 --> n5
```
