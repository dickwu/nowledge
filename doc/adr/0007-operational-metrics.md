# ADR 0007: Bounded operational metrics

## Status

Accepted.

## Decision

Nowledge exposes OpenMetrics 1.0 at the administrator-only
`GET /v1/admin/metrics` endpoint. The registry contains build identity, HTTP
traffic and latency, response-body-aware in-flight work, ingest admission, and
fixed-state counts for ingest tasks and mutation-journal operations. It also
contains request/response byte counters; ingest, Meilisearch, RAG, and LLM
timings; provider-reported tokens and actual retries; bounded LLM timeout and
rate-limit state; cache/read-through/hydration counts; and best-effort audit
drop counters.

The metrics path reads only process state and the in-memory tenant snapshot. A
scrape does not trigger Meilisearch, parser, or LLM probes, so monitoring cannot
amplify an upstream outage or consume provider quota. Detailed readiness stays
on `/healthz`; public coarse readiness stays on `/readyz`.

## Cardinality and confidentiality contract

HTTP methods are mapped to a fixed allowlist plus `OTHER`. Statuses are reduced
to status classes. Route labels come from Axum's registered `MatchedPath` or the
literal `unmatched`; the raw request URI is never used. Task and operation
labels use fixed enum vocabularies. Ingest/RAG stages, Meilisearch operation
classes, primary/analysis LLM profiles, provider classes, token kinds,
rate-limit states, cache resources, hydration domains, outcomes, and failure
classes are likewise mapped to closed vocabularies with an `other` fallback.

Tenant IDs, owner IDs, request IDs, source/document/task/operation identifiers,
HMAC identifiers, query strings, bodies, prompts, model/provider material, and
credentials are forbidden as metric labels or values. New metrics must preserve
that contract and add a regression test before introducing another label.

## Timing semantics

HTTP duration and completion counters finalize when the response body reaches
EOF, returns a body error, or is dropped by a cancelled client. This keeps SSE
and other streaming responses in flight for their actual lifetime rather than
only until response headers are produced. Response-byte counters advance from
body frames and therefore represent bytes emitted before that same terminal
event. Streaming LLM latency and token metrics finalize only on a validated
terminal provider event; dropping or aborting the stream records failure.

## Build identity

The package version is always present. Release builds may inject a 7-64
character hexadecimal Git revision through `NOWLEDGE_GIT_REVISION`; when Git
metadata is available, the build treats it as an assertion against a clean
tracked `HEAD` rather than an arbitrary label. A malformed assertion fails the
build; without an assertion, the build uses the short Git revision (marked
`-dirty` for tracked changes) or `unknown` when Git metadata is unavailable.
The build script emits the resulting canonical value as `NOWLEDGE_GIT_REV`,
which is the single compile-time source used by health and metrics build info.
