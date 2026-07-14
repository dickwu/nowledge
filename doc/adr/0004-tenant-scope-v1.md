# ADR 0004: Tenant-scope-v1 fixed-index migration

- Status: Accepted
- Migration identifier: `tenant_scope_v1`
- Applies to: every index in `meili::FIXED_INDEXES`

## Context

Fixed Meilisearch indexes historically used public logical IDs as the primary
document ID. Two tenants could therefore collide when they used the same ID or
URI, and several legacy document shapes did not carry a trustworthy tenant.
The new persistence contract keeps `id` as the Meilisearch primary key, stores
the public identifier in `logical_id`, derives the primary key from tenant,
index kind, and a storage identity, and requires a non-empty `tenant_id`. The
storage identity is normally the public logical ID; structured rows require an
additional snapshot scope.

A migration must not guess ownership, page through an index while adding rows
to that same index, or remove the legacy copy before the compatibility release
has been proven. It must also be safe to resume after either a Meilisearch task
or the maintenance process fails.

## Decision

The `tenant_scope_v1` maintenance binary has four modes: `plan`,
`rollback-plan`, `apply`, and `verify`.

`plan` is the only mode that discovers legacy rows. It scans each fixed index
without mutating it, using the Meilisearch documents API with offset/limit
pagination. It rejects a scan whose reported total changes. The resulting plan
contains the complete immutable transformed-document manifest, deterministic
per-index/per-tenant counts and checksums, representative checksums, and a
quarantine report. `apply` consumes that manifest; it never paginates a
changing source index.

Tenant ownership must be explicit in an operator-reviewed mapping artifact:

```json
{
  "migration": "tenant_scope_v1",
  "documents": [
    {
      "index_uid": "rag_sources",
      "legacy_id": "source-1",
      "tenant_id": "tenant-a"
    }
  ]
}
```

There are no default-tenant, prefix, URI, owner, or content heuristics. Missing
or conflicting assignments are quarantined by index, legacy ID, source
checksum, and reason. Multiple tenants assigned to one row are ambiguous and
are also quarantined. A non-empty `tenant_id` already present in a legacy row
must agree with the explicit assignment. An already migrated document is
recognized only when its tenant, logical ID, and derived `ts1_` primary key are
consistent.

Each transformed document is produced by the tenant-scope document helpers:

- `id` is the deterministic tenant/index/storage-identity primary key;
- `logical_id` is the former public `id`;
- `tenant_id` is the reviewed mapping value;
- all other document fields are preserved.

There are four inventory-specific identity rules. Legacy
`rag_company_context` rows used a context hash as their primary key, so their
public logical identity is `uri`, while the mapping still addresses the old
hash in `legacy_id`. The physical `rag_harness_components` index stores both
components and revisions; its persistence kinds are
`rag_harness_components:component` and
`rag_harness_components:revision`, selected only from a validated `doc_kind`.
This prevents a component and revision with the same logical ID from deriving
the same tenant-safe primary key. Missing/unknown kinds are quarantined.
`rag_parse_artifacts` scopes the former public artifact ID by
`owner_user_id`: non-empty owner strings represent personal artifacts and a
missing or JSON `null` owner represents the company scope. The length-prefixed
identity from `owner_scoped_storage_identity` keeps equal artifact IDs for two
owners and the company as three distinct tenant-safe targets. Blank owner
strings and non-string/non-null owner values are quarantined rather than
silently treated as company data.
`rag_structured_rows` keeps the row's former public `id` in `logical_id`, but
derives its primary key from a length-prefixed `(snapshot_id, logical_id)`
storage identity through `scoped_storage_identity` and
`tenant_document_with_storage_identity`. Thus the same row ID in two snapshots
remains two distinct persisted rows without exposing the composite identity as
the public logical ID. The specialized `tenant_structured_row_document` helper
also retains the complete arbitrary row object under its internal payload
shadow, so a business field named `logical_id` is restored unchanged on reads
instead of colliding with the persistence metadata field. A missing,
non-string, or empty `snapshot_id` is quarantined. Planning, source preflight,
checkpoint replay, rollback inventory, and verification all retain this storage
identity so those rows cannot collapse in counts or checksums. Migrated-row and
plan validation require a valid payload shadow and an exact snapshot-scoped
`ts1_` identity.

Before `apply`, an operator must generate a rollback artifact for the exact
plan checksum and pass back its exact acknowledgement string. The rollback
artifact inventories only new tenant-safe IDs. It never names a legacy row for
deletion. `apply` writes at most the plan's bounded batch size, waits for each
Meilisearch task to succeed, and then atomically advances a local checkpoint.
If writing succeeds but checkpoint persistence fails, the same batch may be
replayed; deterministic primary keys make replacement idempotent. A completed
checkpoint makes a full rerun a no-op.

The binary deliberately has no rollback-apply mode. Rollback first returns
application traffic to legacy-compatible code and verifies legacy checksums.
Only then may an operator execute the reviewed actions against tenant-safe IDs.
For a target created by this plan, the action deletes that new target. For a
correction of a tenant-safe target that already existed, the artifact embeds
the checksum and full pre-apply target document and the action restores it.
Legacy-row cleanup is a later, separately reviewed migration.

## Operator procedure

Build the maintenance binary from the exact application revision and provide
`RAG_MEILI_URL` plus `RAG_MEILI_API_KEY` when the server requires one. Store
artifacts on encrypted operator storage; the plan contains full document
copies and is written with mode `0600` on Unix.

The final cutover migration requires a quiesced write boundary. Stop and drain
every application process that can write these indexes **before** generating
the final plan. Keep those legacy writers stopped through rollback-plan review,
dry-run, apply, verify, installation of the tenant-safe application revision,
its startup settings reconciliation and hydration, and readiness verification.
Do not reuse a plan captured while writers were active: a legacy write after
apply could otherwise be hidden by the older tenant-safe copy that dual-read
correctly prefers. Read-only exploratory plans may be generated earlier, but
they are not cutover artifacts.

```sh
cargo run --bin tenant_scope_v1 -- plan \
  --mapping /secure/tenant-mapping.json \
  --out /secure/tenant-scope-v1-plan.json \
  --batch-size 250
```

Review all counts, representative checksums, unused mappings, and quarantined
rows. Resolve every quarantine by correcting the mapping and creating a fresh
plan. A plan with quarantine may be applied for investigation, but `verify`
will not report it ready for cutover.

```sh
cargo run --bin tenant_scope_v1 -- rollback-plan \
  --plan /secure/tenant-scope-v1-plan.json \
  --out /secure/tenant-scope-v1-rollback.json
```

Record the `acknowledgement` from the structured JSON output. Exercise the
complete preflight without remote writes or checkpoint updates:

```sh
cargo run --bin tenant_scope_v1 -- apply \
  --plan /secure/tenant-scope-v1-plan.json \
  --rollback-plan /secure/tenant-scope-v1-rollback.json \
  --ack 'ack:tenant_scope_v1:<plan-checksum>:<action-checksum>' \
  --checkpoint /secure/tenant-scope-v1-checkpoint.json \
  --dry-run
```

Run the same command without `--dry-run` to apply. Preserve the plan,
rollback, mapping, and checkpoint together. Re-running that command is the
supported restart path.

```sh
cargo run --bin tenant_scope_v1 -- verify \
  --plan /secure/tenant-scope-v1-plan.json
```

Cutover is permitted only when `ready_to_cutover` is `true`: every planned new
document exists with its expected checksum, every legacy document still exists
with its source checksum, and quarantine is empty. Verification output includes
per-index and per-tenant expected/observed counts and aggregate checksums.
Start only the tenant-safe application revision, wait for startup reconciliation
and hydration to finish, and prove its readiness before allowing writers or
traffic to resume.

## Rollback procedure

1. Stop migration writers and retain the checkpoint.
2. Route application traffic to the compatibility revision that dual-reads
   legacy and tenant-safe IDs.
3. Run `verify` and confirm `legacy_rows_preserved` remains `true`.
4. Review the rollback artifact checksum and action list against the plan.
5. If cleanup is required, execute each listed action individually: delete only
   targets marked `delete_migrated_copy_after_traffic_rollback`, and restore the
   embedded document for actions marked
   `restore_previous_migrated_copy_after_traffic_rollback`. Do not use a broad
   filter and do not delete any legacy ID.
6. Re-run application isolation tests before resuming traffic.

## Consequences

- Plan artifacts can be large because they intentionally freeze the entire
  write manifest. This trades local encrypted storage for deterministic,
  restartable mutation behavior.
- No row without explicit ownership reaches a tenant index identity.
- Legacy and tenant-safe copies coexist for the compatibility release, so
  capacity planning must account for temporary duplication.
- A changed legacy source invalidates apply preflight and requires a new plan.
- Structured JSON is emitted for every success and failure, allowing operator
  automation to archive evidence without parsing human log lines.
