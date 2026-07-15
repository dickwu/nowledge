# ADR 0006: Mutation journal and read-your-writes consistency

- Status: Accepted
- Migration identifier: `operations_v1`
- Applies to: durable API mutations and Meilisearch-backed reads

## Context

Nowledge keeps an in-process serving cache while Meilisearch is the durable
backend in production. Previously, many write paths changed the shared cache
before the corresponding Meilisearch task had even been accepted. A failed
primary write could therefore remain visible until restart, while a successful
task could remain absent from immediate searches. Composite mutations also
span several indexes, so a failure after the primary write left no durable,
operator-visible description of the work that remained.

Meilisearch does not provide a transaction across indexes. The service must
therefore make the primary commit boundary explicit, preserve enough immutable
intent to retry derived work, and define when a caller can expect a write to be
visible.

## Decision

Every request-time durable mutation, and every recovery mutation performed
after the journal becomes available, is first evaluated against an isolated
copy of the store. Validation, generated records, and all intended persistence
resources form an immutable, tenant-scoped operation plan. The plan is written
to the `rag_operations` fixed index in `pending` state and that journal task is
confirmed before any domain resource is submitted.

Each plan has exactly one primary step and zero or more side-effect steps. The
primary aggregate is submitted before its scoped cache delta is published. A
primary submission failure therefore leaves the live cache unchanged. Cache
publication applies only the accepted step against the current cache; it never
replaces the whole store with an old snapshot or exposes an unaccepted
projection. Once the backend has durably accepted the primary, derived
context, history, source-document, link, trace, and other projection steps may
be submitted using the stable resources embedded in the plan. Failure in one
of those steps produces a discoverable `partially_failed` operation; it does
not erase an already committed primary or publish the failed projection.
Completing a domain task is not by itself enough to advance the live operation:
the journal checkpoint that records the completed step must also be confirmed.
If that checkpoint fails, the cache retains the last confirmed operation state
and does not publish the newly completed resource projection.

Step state distinguishes backend acceptance from indexing completion:

- `pending` means no durable resource task has been accepted;
- `submitted` retains the accepted Meilisearch task UIDs;
- `completed` means a synchronous write completed or all retained tasks were
  confirmed;
- `failed` retains only a redacted error category and keyed fingerprint and may
  be retried.

An operation may have status `completed` while its `indexing_state` is still
`pending`: all intended writes have been accepted, but asynchronous indexing
has not yet been confirmed. Operation plans never store raw credentials or raw
idempotency keys. Actor owner IDs and idempotency keys are HMAC-derived. Before
every initial apply and replay, dynamic event/context UIDs, registry IDs,
tenant hashes, and owner hashes are recomputed through `EventIndexResolver`;
company context is restricted to the exact fixed index.

Idempotent plans also retain the original typed application response; plans
without an idempotency key avoid that redundant response copy. Owner-scoped
idempotent retries derive the same operation ID before evaluating a new staged
mutation, reconcile only unfinished steps, and then replay that response.
Event responses set `duplicate=true` while preserving the original
event, materialization job, routing, and operation IDs. A committed event
primary with a failed projection returns those stable IDs plus explicit
`partially_failed` persistence metadata; response types that do not expose
that metadata continue to return a safe error while the operation remains
discoverable and retryable.
Every journaled operation-level idempotency key binds to a keyed canonical
fingerprint of the tenant, operation kind, target, and typed request. Reusing a
key for another target or payload returns 409 instead of replaying an unrelated
response. History bulk writes accept only the batch-level key and reject nested
`events[].idempotency_key`; ingest rejects idempotency keys explicitly because
that workflow does not yet implement replay. ID-less rows in a keyed structured
row batch receive distinct stable IDs derived from the snapshot, batch key, and
row ordinal. State type is immutable for an existing fact, and distinct raw
state identities that normalize to the same physical context path are rejected
before staging any write.

The configured consistency policy is typed:

- `eventual` publishes after primary acceptance but gives no immediate-search
  guarantee; production rejects this mode;
- `read_your_writes` publishes after primary acceptance and merges scoped
  in-process event/context writes with repository search results using the same
  tenant, owner, status, filter, scoring, de-duplication, and limit rules;
- `wait_for_index` confirms every resource task before publishing and is the
  deterministic default for tests. Request-time and reconciliation publication
  are buffered until every planned step confirms, so a failed derived step
  cannot expose a partial cache projection under this mode. Destructive
  company-source deletion is the deliberate exception: each confirmed delete
  step is published immediately so already-removed data cannot remain locally
  visible or be recreated while the remaining delete plan is reconcilable.

A task UID proves backend submission, not successful indexing. Under
`wait_for_index`, a definitive Meilisearch `failed` or `canceled` primary task
returns an error and the journal retains the UID and failed state; the API does
not return newly generated object IDs as a successful write. Under an
acceptance-based mode, an idempotent response may already have been returned
while that task was pending. Later retries therefore replay that historical
response with explicit failed persistence metadata instead of silently changing
the earlier response contract.

Bulk event requests validate the full bounded batch and its single-owner scope
before building a plan. The primary event batch is submitted as one repository
operation, so the API never silently commits an unknown prefix.

Company-source deletion captures the complete source URI closure, including
source documents, fragments, parse artifacts, ingest records, and related
links. Before the immutable plan is built, read-through-only company source
documents are loaded by tenant and source ID; the link step deletes by both
known link IDs and every captured source/target URI. It rejects deletion while
an older operation can still write any member of that closure, and a second
DELETE cannot start while an earlier delete is nonterminal. A partial deletion
likewise blocks source recreation, activation, company ingest, and new
related-link writes until reconciliation, preventing an older plan from
resurrecting deleted data.

Registered dynamic indexes are identity-checked and confirmed present before
the journal is replayed, so recovery cannot mask a missing registered index by
recreating it empty. Only nonterminal or indexing-pending operations are
scanned and reconciled oldest-first during startup; retained completed history
does not make startup work grow without bound. Startup fails readiness when
the journal cannot be read or a required replay cannot be confirmed.
This registered-index readiness bootstrap is the sole mutation-journal
exception: before the server accepts traffic, it may reconcile managed
settings on an already registered, confirmed-present dynamic index and refresh
that registry row. It neither creates a missing registered index nor publishes
domain data to the serving cache, and it runs before operation replay so replay
can validate every dynamic target against current managed settings. A missing
index or failed settings task fails readiness. The exception is intentionally
limited to this pre-traffic identity/settings prerequisite; request-time
settings reapplication uses the ordinary journal path. Startup repair of
interrupted ingest tasks, embedded results, and recovered parse artifacts is
also written as a System-attributed operation before any repaired row is
submitted, and readiness waits for every repair step to complete.
Administrators can inspect
summary-first operation records with `POST /v1/admin/operations/search` and
retry selected operations with `POST /v1/admin/operations:reconcile`. Replay
uses the immutable plan and stable document identities; running reconciliation
again is safe. Full plans are returned only when explicitly requested by an
administrator.

Explicit dynamic-index settings reapplication is journaled even when the
registry row already matches and therefore produces no ordinary cache delta.
Task metadata preserves repository submission order and separately identifies
the primary step's task UIDs so legacy response fields cannot be populated by
an unrelated side-effect task.

## Migration and rollout

`rag_operations` is tenant-safe from its first release. Adding it to the fixed
managed-index set makes an existing deployment's old index set intentionally
incomplete, so an upgraded binary will fail closed until `operations_v1` has
created and verified the new index.

Back up Meilisearch before rollout. Build the maintenance binary from the exact
application revision and provide `RAG_MEILI_URL` plus `RAG_MEILI_API_KEY` when
required:

```sh
cargo run --bin operations_v1 -- plan --out /secure/operations-v1-plan.json
cargo run --bin operations_v1 -- apply \
  --plan /secure/operations-v1-plan.json --dry-run
cargo run --bin operations_v1 -- apply \
  --plan /secure/operations-v1-plan.json
cargo run --bin operations_v1 -- verify \
  --plan /secure/operations-v1-plan.json
```

Stop writers before the plan/apply/verify sequence and audit the existing
indexes for persisted legacy idempotency hashes. This migration cannot safely
invent canonical request fingerprints or response snapshots for keys accepted
before `rag_operations` existed. Any such keys require an application-specific
backfill or must be retired at cutover; clients must not replay pre-journal
keys against the upgraded service.

The migration is non-destructive and manages only `rag_operations`. It creates
the index when absent or reconciles its managed settings when present, requires
the exact primary key `id`, waits for all returned tasks, rejects tampered
plans, and is idempotent. It refuses
to recreate an index that existed during planning but disappeared before
apply, because doing so could hide data loss.
The `verify` command exits nonzero when any readiness check fails; rollout
automation must not infer success merely because it received a JSON report.

The previous application revision ignores the additional index, so application
rollback does not require deleting it. Preserve the index and the migration
artifact; a later retry or forward deployment can reuse the verified state.

## Consequences

- A rejected primary write cannot leak into the process cache.
- A committed primary can remain visible while derived work is explicitly
  partial and retryable.
- Immediate search visibility no longer depends on Meilisearch task timing in
  the production default mode.
- Every durable write adds journal traffic and serializes mutation planning in
  one process; this is the cost of a stable recovery boundary without adding a
  new canonical database.
- This journal implementation has a single-writer contract per tenant. Deploy
  one Nowledge writer process for a tenant until a durable lease/CAS protocol
  is introduced; process-local mutation serialization is not a multi-replica
  consensus mechanism.
- Journal retention and compaction are operational follow-up work; records are
  retained so recovery evidence is not silently discarded. Completed records
  are excluded from automatic startup reconciliation.
