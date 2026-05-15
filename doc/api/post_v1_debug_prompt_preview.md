# POST /v1/debug/prompt/preview

## Summary
Build a fragment-grounded RAG answer and return the redacted prompt preview, trace id, and citations.

## Handler
- Rust handler: `prompt_preview`
- Route registration: `src/routes.rs::build_router`
- Authentication: AdminGuard

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
Schema: `RagAnswerRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| question | string | required | Question to answer. |
| mode | string | optional, default auto | Retrieval mode selector. |
| session_id | string | optional | Session to associate with the answer. |
| owner_user_id | string | optional, auth default may apply | Owner scope. |
| debug | boolean | optional, default false | Request debug data from retrieval. |

## Response
Schema: `JsonValue`

| Field | Type | Description |
| --- | --- | --- |
| prompt | string | Redacted grounded prompt preview. |
| trace_id | string | Retrieval trace id. |
| citations | Citation[] | Fragment citations used to build the prompt. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Prompt preview uses the same default fragment-only RAG retrieval as /v1/rag/answer.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["AdminGuard authenticates caller"]
  n1["answer_rag_with_llm builds answer and fragment citations"]
  n2["build_prompt constructs prompt"]
  n3["redact_for_state removes configured secrets"]
  n4["Return prompt, trace_id, and citations"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
```
