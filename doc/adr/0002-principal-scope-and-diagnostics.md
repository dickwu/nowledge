# ADR 0002: Explicit principal scope and protected diagnostics

- Status: Accepted
- Date: 2026-07-13
- Compatibility removal: 2026-10-01 / v0.13.0

## Context

The previous authentication model represented data scope as an optional owner
identifier. An ownerless ordinary bearer therefore behaved like a tenant-wide
service credential, even though that capability was neither explicit nor
separate from feature roles. The same model let every authenticated principal
mutate tenant-shared company documents and dataset schemas. Public health and
LLM-status responses also exposed detailed provider state, private usage counts,
credit data, and configured credential paths.

ADR 0001 requires PR 2 to make these capabilities explicit without renaming
public routes or changing the shared API error envelope.

## Decision

### Principal scope

Runtime principals use exactly one data scope:

- `Owner { owner_user_id }` can access only that owner's private data.
- `TenantService` can select explicit owners on routes whose policy permits
  tenant-service access. It never receives implicit global usage or admin
  access. Private state-fact reads, state searches, and insight searches return
  403 when `owner_user_id` is omitted.
- `Admin` can access every owner and admin-only operation in the tenant.

Feature roles remain orthogonal to scope. In particular, `company_writer` grants
the shared-write operations listed below but does not grant cross-owner access.
An owner-bound company writer therefore remains confined to that owner for
private history, state, context, session, and insight routes.

`RAG_AUTH_USERS=owner:token:role|role` maps a named owner to `Owner` scope. The
literal owner `*` maps explicitly to `TenantService`; it is no longer interpreted
as an ownerless ordinary user. The reserved form `*:token:admin` maps to `Admin`
scope; here `admin` is a scope marker, not a feature role. New admin credentials
should prefer `RAG_ADMIN_TOKEN`.

For one compatibility window, legacy named-owner `owner:token:admin` entries
also retain `Admin` scope and emit a token-free startup warning. Operators must
migrate them to `*:token:admin` or `RAG_ADMIN_TOKEN` by 2026-10-01 / v0.13.0;
after that window a named owner combined with `admin` is rejected as ambiguous.

### Legacy bearer configuration

`RAG_BEARER_TOKEN` is accepted only when its data scope is explicit:

- `RAG_BEARER_TOKEN_SCOPE=owner` requires
  `RAG_BEARER_TOKEN_OWNER_USER_ID=<owner>`.
- `RAG_BEARER_TOKEN_SCOPE=tenant_service` creates a `TenantService` principal.

An unscoped legacy bearer is rejected at production startup by default. For one
compatibility window, operators may set
`RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER=true`; this preserves the former
tenant-service behavior and emits a startup warning without logging the token.
The compatibility switch will be removed on 2026-10-01 in v0.13.0.

Malformed entries, empty tokens, invalid scope combinations, and duplicate
credential tokens are startup errors rather than silently ignored entries.
Bearer-token matching uses a constant-time comparison. Authentication tokens
must contain at least eight characters. Other configured redaction secrets must
contain at least four characters so the sanitizer cannot be amplified by a
one-character configuration value; validation errors never echo the value.

### Shared company and dataset mutations

| Operation | Default required permission |
| --- | --- |
| List/read company documents and revisions | Any authenticated principal |
| Preflight a company document | `company_writer` or admin |
| Create a company-document revision | `company_writer` or admin |
| Activate a company-document revision | `company_writer` or admin |
| Upsert a structured dataset schema | `company_writer` or admin |
| Delete a company document | Admin only |

Tenant-service scope alone does not grant shared-write permission. Shared-write
operations require `company_writer` or admin by default. For the same bounded
compatibility window, `RAG_ALLOW_LEGACY_SHARED_WRITER=true` preserves ordinary
authenticated access to the non-delete shared writes and emits a token-free
startup warning. It never grants company-document deletion or cross-owner data
access and is removed on 2026-10-01 / v0.13.0.

The request middleware creates the `X-Request-Id` returned to the caller, and
the same value correlates shared-write audit events. Guards audit authorization
denials; handlers audit store success or failure. Events include keyed tenant,
principal-owner, and logical-resource identifiers plus principal scope, action,
reason code/fingerprint, and outcome. Caller-provided activation reasons are
represented only by the allowlisted `caller_supplied` code and a keyed
fingerprint. Raw identifiers, reasons, tokens, and document bodies are never
logged. New production deployments must supply an independently generated
`RAG_INDEX_HASH_SECRET` with at least 32 bytes and 12 distinct byte values; the
public development default, the previously documented `change-me`, and literal
documentation placeholders are rejected so these fingerprints cannot be
reversed with a known key. Because the key also derives physical per-user index
UIDs, an existing weak-key deployment must preserve its exact current value and
explicitly enable `RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET=true` until the
`index_hash_secret_v1` migration/reindex has completed and been verified.

### Diagnostics and debug output

- `GET /livez` remains public and process-only.
- `GET /readyz` remains public and returns only coarse readiness information.
  It preserves the existing 200/503 readiness status semantics but omits raw
  provider payloads, store details, usage/private counts, plans, and credits.
- `GET /healthz` becomes admin-only and retains detailed operational diagnostics.
- `GET /v1/llm/status` requires an authenticated principal. Its
  `auth_source` field is retained for response-shape compatibility but contains
  a category such as `codex_file`, `environment`, `mock`, or `none`, never a
  filesystem path or secret value.
- `POST /v1/rag/debug`, stored debug traces, and `POST /v1/llm/test` become
  admin-only. Every JSON response, not only diagnostics, passes a final dynamic
  configured-secret sanitizer before compression. This covers object keys,
  typed state/history/link records, parsed blocks, ingest results, and explicit
  source-document reads. Malformed or larger-than-16-MiB JSON responses fail
  closed. Protocol locators remove complete configured secrets while preserving
  incidental substrings so redaction cannot silently break `ctx://` navigation.
- `POST /v1/analysis/insights` remains owner-scoped for ordinary analysis, but
  `debug=true` is admin-only because the response can contain a grounded prompt.
  Debug prompts and provider previews are redacted before preview truncation.
- New ingest applies an equal-character-count mask before fragmentation so
  provenance offsets remain stable. Retrieval and history-analysis snippets are
  then redacted from their full source text before the 240-character boundary
  is applied. Query and response boundaries conservatively mask recognizable
  configured-secret pieces at content-field edges, covering legacy fragments,
  adjacent parsed blocks, and credentials rotated after ingest. A one-second
  background task reads Codex credentials on the blocking pool and atomically
  publishes the same snapshot to LLM clients and response/provider redaction;
  request and liveness paths never touch the auth file. Analysis parses provider
  JSON without rewriting locators, validates proposed link locators against the
  context/seed/existing-link set, and sanitizes free text before persistence.
  Every observed Codex token remains in shared process-lifetime history across
  config clones, rotations, and transient auth-file failures. A restart cannot
  rediscover a revoked value, so operators must supply values still present in
  persisted records through `RAG_REDACTION_PREVIOUS_SECRETS` until reingestion
  or scrubbing is verified.
- `GET /v1/usage` returns only the selected owner's counters to `Owner` and
  `TenantService` principals. Global counters and service-wide provider
  diagnostics are admin-only.

API responses use the stable public messages `internal server error` and
`upstream service unavailable` for 500 and 502 failures. Their error details
include the safe request correlation ID, and the response carries the matching
`X-Request-Id`; raw causes are never returned. Server-side cause logs redact
raw causes down to an allowlisted category plus a keyed fingerprint, so paths,
provider bodies, prompts, documents, and credentials are not emitted.
Best-effort background failures use the same bounded diagnostics and omit or
fingerprint dynamic source/task identifiers. Status codes, error codes, and the
`{"error":{"code","message","details"}}` envelope remain unchanged.

## Compatibility and rollout

Before deploying this change:

1. Classify the deployed `RAG_INDEX_HASH_SECRET` without printing it. For a new
   deployment, generate a strong key. For an existing weak-key Meilisearch
   deployment, preserve the exact key and temporarily set
   `RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET=true`; do not rotate it before
   migrating or reindexing its per-user indexes.
2. Provision explicit owner/service credentials and `RAG_ADMIN_TOKEN`, then set
   `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=false` (or remove an existing `true`
   override) before replacing the running artifact.
   Ensure every authentication credential has at least eight characters and
   every other configured redaction secret has at least four.
3. Smoke a protected route without credentials for 401, an owner credential
   against another owner's resource for 403, and admin-authenticated `/healthz`
   for its expected 200/503 dependency status. Keep public `/readyz` in the
   deployment check.
4. Inventory every `RAG_BEARER_TOKEN` consumer.
5. Bind owner clients with `RAG_BEARER_TOKEN_SCOPE=owner` and
   `RAG_BEARER_TOKEN_OWNER_USER_ID`.
6. Mark intentional tenant-wide clients with
   `RAG_BEARER_TOKEN_SCOPE=tenant_service`.
7. Migrate named-owner `admin` entries to `*:token:admin` or, preferably,
   `RAG_ADMIN_TOKEN`.
8. Add `company_writer` only to credentials that need company preflight,
   revision, activation, or dataset-schema writes. Use an admin credential for
   company-document deletion.
9. Keep load balancers on public `/readyz`. Configure administrative monitoring
   of `/healthz` with an admin bearer token and update consumers for the reduced
   `/readyz` payload.
10. Update `/v1/usage` consumers so owner/service callers use owner counters only,
   and move service-wide provider diagnostics to an admin credential. Update
   `/v1/llm/status` clients to authenticate and treat `auth_source` as an
   enum-like category rather than a path.
11. When rotating a credential that may occur in persisted content, add the
    revoked value to `RAG_REDACTION_PREVIOUS_SECRETS` before restart and retain
    it until the affected records have been reingested or scrubbed.

Operators who cannot update an intentional legacy tenant-service consumer in the
same rollout may temporarily enable
`RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER=true`. New deployments must not use the
compatibility switch.

Operators who need additional time to migrate ordinary shared-write callers may
temporarily set `RAG_ALLOW_LEGACY_SHARED_WRITER=true`. Both compatibility
switches, the weak-index-key compatibility switch, and legacy named-owner
`admin` mapping expire on 2026-10-01 / v0.13.0. The weak-key switch must not be
removed until `index_hash_secret_v1` migration verification succeeds.

## Rollback

Code rollback does not require data migration only while the exact
`RAG_INDEX_HASH_SECRET` remains unchanged. Keep explicit credentials provisioned
and unsafe unauthenticated mode disabled while restoring the last green artifact
and its compatible configuration. If an index-key migration has begun, follow
its generated rollback plan; never restore an old artifact against a different
key and assume its per-user indexes remain reachable. If legacy unscoped bearer
clients must be restored before artifact rollback, enable only
`RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER`; if ordinary shared writers must be
restored, additionally enable `RAG_ALLOW_LEGACY_SHARED_WRITER`. Confirm
`/readyz`, then repeat the unauthenticated 401, cross-owner 403, company-writer,
and admin `/healthz` smokes. Remove each temporary switch as soon as its callers
have explicit scope and role configuration.

## Consequences

The health and readiness route paths remain stable, but `/healthz` authentication
and the reduced `/readyz` body are intentional operational contract changes.
`AuthUserConfig` also gains explicit scope in the Rust API, so source consumers
that construct it directly must migrate. These changes trade implicit ambient
authority for an explicit, testable policy matrix.
