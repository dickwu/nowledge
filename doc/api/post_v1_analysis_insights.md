# POST /v1/analysis/insights

## Summary
Analyze context or a selected history event to propose and optionally persist links and insights. When history_event_id is supplied, only the selected owner event index is used.

## Handler
- Rust handler: `analyze_insights`
- Route registration: `src/routes.rs::build_router`
- Authentication: UserGuard; owner write scope required; `debug=true` requires admin

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
Schema: `AnalysisInsightRequest`

| Field | Type | Requirement | Description |
| --- | --- | --- | --- |
| owner_user_id | string | optional normally; required when history_event_id is supplied | Owner scope for search, link creation, and insight upsert. |
| history_event_id | string | optional | When supplied, analysis is constrained to the selected event and other events from the same owner event index. |
| query | string | required by handler | Analysis question or topic. |
| seed_uris | string[] | optional, default [] | Extra seed context URIs used when not running in history_event_id mode. |
| context_limit | integer | optional, default 8 | Maximum context hits used in prompt construction. |
| link_limit | integer | optional, default 10 | Maximum existing links considered. |
| create_links | boolean | optional, default true | Persist proposed link candidates. |
| upsert_insights | boolean | optional, default true | Persist proposed insight candidates. |
| debug | boolean | optional, default false | Include prompt and debug-stage data where available; admin-only. |

## Response
Schema: `AnalysisInsightResponse`

| Field | Type | Description |
| --- | --- | --- |
| analysis_id | string | New analysis id. |
| query | string | Analysis query. |
| history_event_id | string? | Selected history event id when same-index mode was used. |
| event_index_uid | string? | Owner event index UID used for same-index history analysis. |
| context_hits | ContextHit[] | Context fragments or history hits used as evidence. |
| existing_links | KnowledgeLink[] | Existing links included in the analysis prompt. |
| link_candidates | LinkCandidate[] | Proposed links from deterministic or LLM analysis. |
| insight_candidates | InsightCandidate[] | Proposed insights from deterministic or LLM analysis. |
| created_links | KnowledgeLink[] | Links persisted when create_links is true. |
| insights | InsightRecord[] | Insights persisted when upsert_insights is true. |
| usage | object | Provider/model/backend metadata; includes history_scope same_index for history_event_id mode. Admin debug may include a configured-secret-redacted provider preview. |
| prompt | string? | Configured-secret-redacted prompt included only when debug is true. |

## Errors and Access Rules
- Malformed JSON or missing required runtime fields returns 400.
- Owner-scoped endpoints return 403 when the authenticated principal cannot access the requested owner.
- Authenticated non-admin principals receive 403 when `debug=true` because the response can contain a grounded prompt.
- Configured secrets are redacted from the complete response. Provider previews
  are redacted before they are truncated.
- Store, Meilisearch, or LLM failures are returned through the shared ApiError JSON envelope.
- history_event_id analysis requires owner_user_id after auth defaults are applied.
- Non-history context evidence uses the same default fragment-only context search as /v1/context/search.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["UserGuard authenticates caller"]
  n1["apply_owner_default fills owner_user_id when possible"]
  n2["require_owner_for_write enforces owner scope"]
  n3["Require admin scope when debug=true"]
  n4["require_string validates query"]
  n5["If history_event_id exists, history_analysis_scope loads selected event and same-owner same-index hits"]
  n6["Otherwise Store.search_context_async collects fragment evidence and Store.search_links collects links"]
  n7["build_analysis_prompt constructs grounded prompt"]
  n8["analysis_llm_config selects analysis-specific provider/model"]
  n9["deterministic or LLM draft yields link and insight candidates"]
  n10["Optional Store.upsert_link_async persists links"]
  n11["Optional Store.upsert_insight_async persists insights"]
  n12["Redact configured secrets and return AnalysisInsightResponse; prompt only for admin debug"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
  n4 --> n5
  n5 --> n6
  n6 --> n7
  n7 --> n8
  n8 --> n9
  n9 --> n10
  n10 --> n11
  n11 --> n12
```

## Internal Logic Notes
- history_event_id mode resolves the selected event through Store.get_event_async(owner_user_id, history_event_id), searches only that owner routing, keeps hits whose event_index_uid matches the selected event, and reports usage.history_scope.mode = same_index.
