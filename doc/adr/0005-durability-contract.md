# ADR 0005: Tenant Durability and Startup Hydration

## Status

Accepted for the PR5 persistence boundary.

## Context

Nowledge exposes state, history, structured data, sessions, traces, ingest jobs,
and evaluation records as durable API objects. Before this decision, some of
those objects were written only to process memory, some were persisted but not
reloaded, and several startup scans silently stopped at 1,000 or 2,000 rows.
Startup hydration also published each domain as it loaded, so a later failure
could leave a process serving a mixed pre- and post-restart view.

## Decision

Every runtime domain has one durability class and one restart strategy:

| Domain | Class | Restart strategy |
| --- | --- | --- |
| User event-index registry | durable canonical | hydrate the fixed registry; dynamic event and context indexes remain read-through |
| User events | durable canonical | query the deterministic per-user event index |
| Personal context | derived durable | query the deterministic per-user context index; rebuildable from canonical writes |
| Company context | derived durable | hydrate all tenant rows with a bounded paginated scan |
| State items | durable canonical | persist and hydrate all tenant rows |
| Insights | durable canonical | persist and hydrate all tenant rows |
| Links | durable canonical | persist and hydrate all tenant rows |
| Company sources and revisions | durable canonical | hydrate all tenant rows |
| Source documents | durable canonical | canonical read-through by tenant, owner, and URI |
| Parse artifacts | derived durable | hydrate every tenant artifact row independently of ingest-task retention |
| Parsed blocks | ephemeral | opportunistically rebuild from retained ingest results; durable source documents and context fragments remain the serving projection after result retention |
| Dataset schemas | durable canonical | persist and hydrate all tenant rows |
| Structured snapshots | durable canonical | hydrate snapshot metadata |
| Structured rows | durable canonical | load all rows lazily for the selected snapshot before mutation or analysis |
| Structured summaries | derived durable | persist and hydrate; they may be regenerated from snapshots and rows |
| Sessions | durable canonical | persist the complete record after create, message, and commit; hydrate all tenant rows |
| Traces | durable canonical | persist and hydrate so owner authorization works immediately after restart |
| Harness and evaluation records | durable canonical | hydrate all tenant rows, including run children |
| Ingest tasks and results | durable canonical | hydrate all tenant rows and terminalize interrupted work idempotently |
| Preflight decisions, vector embeddings, queue permits, and provider-health samples | ephemeral | intentionally process-local and safe to recreate |

The memory backend has no external hydration obligation and reports
`not_required`. A configured Meilisearch backend begins in `pending`, becomes
`complete` only after all mandatory startup domains have been collected and
published atomically, and becomes `incomplete` on any missing index, scan,
decode, or recovery-persistence failure.

Non-destructive startup provisions the managed fixed-index set only when none
of those indexes exists. If the set is partial, startup fails closed instead of
recreating missing indexes as empty stores. When the complete set exists,
startup performs settings-only reconciliation, rechecks existence at that
boundary, and reads the current managed settings before submitting an update.
An identical configuration produces no mutation task. Registry-owned dynamic
event and personal-context indexes follow the same recovery rule: a missing
registered index is treated as durable data loss, never as permission to create
an empty replacement. The explicit reset path is the only startup operation
allowed to delete and recreate managed indexes.

Hydration first builds a tenant-scoped staging snapshot without mutating the
live store. Registry-owned dynamic event and personal-context indexes have
their current settings reapplied before read-through is enabled. Interrupted
ingest tasks are transformed into the stable failed state and persisted before
publication. Startup confirms every accepted reconciliation and recovery task,
regardless of the ordinary write-wait configuration. Only after all mandatory
work succeeds is the staged snapshot committed under one store write lock. A
failed attempt therefore publishes none of its staged domains.

Process caches for source documents and parsed blocks use tenant, optional
owner, and URI together as their identity. Parse-artifact caches likewise use
tenant, optional owner, and artifact identity. A public ContextFS or artifact
URI is not a globally unique cache key because the same URI may validly exist
in multiple owner indexes.

Repository-backed ContextFS reads and searches require the row's tenant,
owner, privacy, index UID, index kind, and active status to match the requested
company or personal scope. Meilisearch filters enforce that identity first and
the Store validates returned rows again before caching or returning them;
malformed or misrouted search hits are quarantined.

Every domain report includes its durability class, load strategy, mandatory
flag, status, expected count, loaded count, skipped count, quarantined count,
and recovered count. `/readyz` requires hydration readiness in addition to
dependency health; `/healthz` exposes the redacted detailed hydration report.

All fixed-index scans use the tenant-required filtered document-fetch API,
stable `id:asc` ordering, a validated page size, and a configurable hard
ceiling. Missing indexes, unstable totals or offsets, premature empty pages,
duplicate physical IDs, and ceiling exhaustion are explicit errors. The
unfiltered migration reader remains separate because `tenant_scope_v1` must
inventory legacy rows that do not yet carry a tenant.

## Consequences

- A process never reports ready with a partially reconstructed mandatory
  tenant state.
- Existing durable APIs survive restart without making startup load every
  high-volume per-user or structured-row record.
- Large tenant inventories are either loaded completely or rejected with a
  visible error; they are never silently truncated.
- Increasing the scan ceiling is an operator decision, not an implicit change
  in API behavior.
- Write atomicity and read-your-writes semantics remain PR6 responsibilities;
  this ADR defines what must exist after a successful durable write and restart.
