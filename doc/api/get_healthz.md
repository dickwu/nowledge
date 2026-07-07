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
| version | string | Crate version baked in at compile time. |
| git_rev | string | Short git revision of the build, `-dirty` suffix when built from a modified tree, `unknown` outside a git checkout. |
| store_backend | string | Active store backend name. |
| meilisearch | object | Meilisearch health payload. |
| llm | object | LLM health payload with provider, model, auth, quota, rate-limit, and stale status. |
| usage | object | Compact usage summary. |

### `llm.rate_limits` Fields
The freshest live snapshot for the configured provider. Health probes and
real completions (RAG answer, analysis, title) both refresh it; `captured_at`
says when it was last observed. For `codex_auth` the windows come from the
`x-codex-*` response headers of the ChatGPT Codex backend.

| Field | Type | Description |
| --- | --- | --- |
| captured_at | string? | RFC3339 time the snapshot was observed on a live upstream response. |
| plan_type | string? | Codex subscription plan (`x-codex-plan-type`). |
| active_limit | string? | Limit bucket currently governing (`x-codex-active-limit`). |
| primary | object? | Short (5h) window: `used_percent`, `remaining_percent`, `window_minutes`, `resets_in_seconds`, `resets_at`. |
| secondary | object? | Long (weekly) window, same fields as `primary`. |
| credits | object? | `has_credits`, `unlimited`, `balance` from the Codex credits headers. |
| additional_limits | array? | Model-scoped buckets (`name`, `limit_name`, `primary`, `secondary`). |
| remaining_requests / remaining_tokens / reset_requests / reset_tokens | string? | OpenAI API-style `x-ratelimit-*` values when the provider is `openai_api_key`. |

`llm.rate_limit_state` is `ok`, `near_limit` (any window ≥ 90% used),
`limited` (any window ≥ 100% used or upstream 429), or `unknown`.
`remaining_percent` is the "left available usage" for dashboards.

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
