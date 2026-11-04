# GET /readyz

## Summary
Public coarse readiness probe for load balancers and deployment checks.

## Handler
- Rust handler: `readyz`
- Route registration: `src/routes.rs::build_router`
- Authentication: None

## Path Parameters
None.

## Query Parameters
None.

## JSON Body Parameters
No JSON body.

## Response
Schema: JSON readiness summary

| Field | Type | Description |
| --- | --- | --- |
| status | string | ok, degraded, or unhealthy. |
| ready | boolean | True when mandatory hydration completed and Meilisearch, parser, and required LLM checks allow traffic. |
| version | string | Crate version baked in at compile time. |
| git_rev | string | Revision asserted through `NOWLEDGE_GIT_REVISION` against a clean Git `HEAD` when metadata is available; otherwise the short checkout revision, with `-dirty` for modified tracked files or `unknown` outside a checkout. |
| dependencies | object | Coarse state for Meilisearch, hydration, parser, primary `llm`, and `analysis_llm` dependencies. Hydration is `pending`, `complete`, `incomplete`, or `not_required`. Each LLM profile is `ok`, `degraded`, `unhealthy`, or `not_probed`. |
| llm_checked_at | object | When the cached `llm` and `analysis_llm` verdicts were observed (RFC 3339), or `null` while `not_probed` or with probing disabled. Readiness keeps a verdict whatever its age; this shows how old it is. |

The readiness decision still checks configured mandatory dependencies, but the
public response intentionally omits store-backend details, raw dependency
payloads, provider/model names, credential sources, usage and private object
counts, plan data, rate-limit budgets, and credits.

This probe never calls an LLM provider. Its LLM verdicts are the last ones in
the health-probe cache, kept whatever their age (see `llm_checked_at`) rather
than expiring. The server probes each LLM profile once in the background at
startup; after that only authenticated `/healthz` re-probes a profile whose
verdict is older than `RAG_HEALTH_LLM_PROBE_TTL_SECONDS`, and periodic
background probing happens only when `RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS`
opts in (`0`, the default, disables it; otherwise it must be at least 300).
A profile with no completed probe yet is `not_probed`: it keeps the service
unready when `RAG_HEALTH_REQUIRE_LLM=true`, and otherwise leaves it ready with
`status=degraded`. With `RAG_HEALTH_LLM_ENABLED=false` the disabled-probing
verdict applies without any probe.

## Errors and Access Rules
- Public; no bearer token is required.
- Returns 200 when ready and 503 when hydration is incomplete/pending or any mandatory dependency makes the service unready.
- Dependency failures are represented only by the coarse status and ready fields; use authenticated `/healthz` for details.
- The public readiness-probe rate bucket can return 429
  `too_many_requests`; global in-flight pressure can return 503
  `service_unavailable`. Both boundary responses use the shared error envelope
  and include `Retry-After`, unlike an ordinary dependency-health 503.
- `RAG_REQUEST_TIMEOUT_MS` can return 504 `timeout`. Every response includes
  `X-Request-Id`.

## Internal Logic Call Graph
```mermaid
flowchart TD
  n0["Router dispatches GET /readyz"]
  n1["Compute mandatory hydration, Meilisearch, and parser readiness plus cached primary and analysis LLM verdicts (no provider call)"]
  n2["Discard detailed provider and usage payloads"]
  n3["Return coarse status, ready, and dependency-state fields"]
  n4["HTTP status is 200 when ready, 503 when unhealthy"]
  n0 --> n1
  n1 --> n2
  n2 --> n3
  n3 --> n4
```
