# ADR 0007: Bounded operational metrics

## Status

Accepted.

## Decision

Nowledge exposes OpenMetrics 1.0 at the administrator-only
`GET /v1/admin/metrics` endpoint. The registry contains build identity, HTTP
traffic and latency, response-body-aware in-flight work, ingest admission, and
fixed-state counts for ingest tasks and mutation-journal operations.

The metrics path reads only process state and the in-memory tenant snapshot. A
scrape does not trigger Meilisearch, parser, or LLM probes, so monitoring cannot
amplify an upstream outage or consume provider quota. Detailed readiness stays
on `/healthz`; public coarse readiness stays on `/readyz`.

## Cardinality and confidentiality contract

HTTP methods are mapped to a fixed allowlist plus `OTHER`. Statuses are reduced
to status classes. Route labels come from Axum's registered `MatchedPath` or the
literal `unmatched`; the raw request URI is never used. Task and operation
labels use fixed enum vocabularies.

Tenant IDs, owner IDs, request IDs, source/document/task/operation identifiers,
HMAC identifiers, query strings, bodies, prompts, model/provider material, and
credentials are forbidden as metric labels or values. New metrics must preserve
that contract and add a regression test before introducing another label.

## Timing semantics

HTTP duration and completion counters finalize when the response body reaches
EOF, returns a body error, or is dropped by a cancelled client. This keeps SSE
and other streaming responses in flight for their actual lifetime rather than
only until response headers are produced.

## Build identity

The package version is always present. Release builds may inject a 7-64
character hexadecimal immutable Git revision through `NOWLEDGE_GIT_REVISION`;
missing or malformed values expose `unknown` rather than executing Git during
compilation or publishing arbitrary build-environment content.
