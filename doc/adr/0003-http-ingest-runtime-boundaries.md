# ADR 0003: Bounded HTTP and ingest runtime

- Status: Accepted
- Date: 2026-07-13

## Context

The API accepted permissive browser origins, relied on extractor defaults for
body limits, and had no shared concurrency, rate, or request-time boundary.
Multipart files were materialized as a complete byte vector before parsing.
Asynchronous ingest used a bounded channel but waited indefinitely for space,
created its durable task record before confirming queue capacity, accepted work
when workers were disabled, and detached worker/background tasks from process
shutdown. A restart could therefore leave a persisted task permanently in a
nonterminal state.

ADR 0001 assigns these API-01/API-02/ING-01/ING-02 controls to PR 3 while
requiring stable routes, owner isolation, and the existing API error envelope.

## Decision

### HTTP boundary

`Config` exposes positive, startup-validated limits for JSON and multipart
bodies, bulk events/rows, tags, search results, ordinary and synchronous-ingest
timeouts, in-flight requests, per-minute principal requests, ingest queue
capacity, and shutdown grace. Malformed numeric environment values are startup
errors rather than silent fallback. The default queue capacity is eight times
the configured ingest worker concurrency. Upload MIME policy is normalized to
a nonempty, duplicate-free list of exact lowercase types; wildcards are
rejected. Its
default includes text, Markdown, `application/octet-stream`, PDF, OOXML office
documents, and common raster image formats.

Browser origins are an explicit list of exact HTTP(S) origins. Development and
test default to wildcard access; production defaults to none. Production
wildcard access requires a separate opt-in and the wildcard must be the sole
entry.

The router applies one generated request ID, then tracing and CORS, and keeps
response redaction ahead of compression. Boundary failures stay inside the
redaction/response contract. The API buffers only bounded non-multipart request
bodies, uses immediate semaphore load shedding, applies a fixed-window rate
limit to a trusted logical tenant/principal key, and bounds route execution.
`/livez` bypasses capacity and timeout enforcement so saturation cannot hide a
live process. `/readyz` is load-shed and uses a separate public-probe rate key.
Raw credentials are neither rate keys nor diagnostics.

New public failures retain `{"error":{"code","message","details"}}`:

| Condition | Status/code | Additional contract |
| --- | --- | --- |
| Body or upload too large | 413 `payload_too_large` | Stop reading at the configured ceiling |
| Principal or ingest queue pressure | 429 `too_many_requests` | `Retry-After` |
| Global capacity, disabled worker, or closing runtime | 503 `service_unavailable` | `Retry-After` |
| Route execution deadline | 504 `timeout` | Task terminalized when one was created |
| Bulk/tag/search bound | 400 `validation_error` | `details.field` identifies the rejected input |

Bulk and tag validators run before store mutation. Both canonical and alias
history routes share the same checks. Every externally supplied search or
analysis limit is capped by `RAG_MAX_SEARCH_LIMIT`.
Request, synchronous-ingest, and shutdown deadlines are positive, bounded to
seven days, and rejected at startup when malformed or out of range.

### Multipart staging

Multipart parsing accepts at most one file part and a bounded number/size of
metadata fields. File chunks are written with create-new semantics to a
generated file in the process temporary directory; Unix files are mode 0600.
The server counts bytes and computes SHA-256 as chunks arrive, stops immediately
above the upload limit, rejects empty files, validates an optional caller
checksum, and sanitizes the client filename. A file part must supply a valid
`Content-Type` that is present in `RAG_UPLOAD_ALLOWED_MIME_TYPES`; an optional
metadata `content_type` must match it. File bytes cannot be combined with
alternate `content` or supplied parser-output fields.

The staged file has shared ownership with deletion on final drop. Partial files
are removed on parse errors, cancellation, or limit failure. MinerU receives a
length-known streaming file part. The builtin parser performs one bounded UTF-8
read. No public request DTO accepts a server-local staging path.

### Queue and lifecycle

Asynchronous handlers reserve channel capacity before persisting a task.
Reservation failure returns immediately and cannot create a phantom task. A
committed reservation owns both the request and any staged file until a worker
claims it. Workers are supervised and concurrency-bound without dequeuing more
jobs than can run. Disabled workers reject asynchronous routes; synchronous
routes remain available. Synchronous ingest uses a separate semaphore, also
capped by `RAG_INGEST_MAX_CONCURRENT_TASKS`, and load-sheds immediately with
503 plus `Retry-After` before body buffering when that lane is saturated. This
keeps concurrent upload/JSON memory and temporary-disk amplification bounded
without consuming asynchronous worker slots.

SIGINT/SIGTERM starts graceful shutdown: stop new queue acceptance, close the
HTTP server, drain supervised tasks within `RAG_SHUTDOWN_TIMEOUT_MS`, abort any
remaining tasks, and mark every unfinished ingest record `failed` with
`error=ingest_interrupted`. Startup hydration applies that same terminal state
to persisted nonterminal tasks because the original request/upload payload is
not durable and cannot be replayed safely.

## Compatibility and rollout

Routes and successful response shapes do not change. Clients must handle the
new 413, 429, 503, and 504 responses and honor `Retry-After`. Operators must set
an explicit production origin list (or intentionally leave it empty), size
limits for their workload, and queue/concurrency values that fit parser and
Meilisearch capacity. The upload limit excludes bounded multipart metadata.

Before rollout:

1. Validate the complete environment with `validate_startup`.
2. Exercise an exact-limit and an over-limit JSON/upload request.
3. Saturate the ingest queue and global request semaphore; confirm prompt
   envelope responses while `/livez` remains 200.
4. Send SIGTERM during an ingest and confirm the task completes within grace or
   becomes `ingest_interrupted` after restart.
5. Confirm an allowed browser origin receives CORS headers and an unconfigured
   origin does not.
6. Send a default-allowed `application/octet-stream` upload and reject a valid
   but unconfigured MIME type before task creation.

## Rollback

No storage migration is required. Restore the last green artifact and its
configuration, preserving authentication and index-HMAC secrets. Persisted
tasks already marked `ingest_interrupted` remain terminal; do not move them back
to queued because their payload may no longer exist. A rollback also restores
the former weaker pressure behavior, so reduce ingress/load before switching.

## Consequences

Overload becomes explicit and retryable instead of consuming unbounded memory
or waiting indefinitely. Uploads use disk proportional to active staged files
and require a writable temporary directory with enough capacity. The in-process
fixed-window limiter is intentionally per replica; a future distributed limiter
can replace it without changing public responses or the trusted-principal-key
boundary.
