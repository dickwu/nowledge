# Production upgrade, migration, and rollback runbook

Use this procedure for an existing Meilisearch-backed Nowledge deployment.
Run every maintenance binary from the exact application revision being
installed. Keep migration artifacts and backups private (`0700` directory,
`0600` files), checksum them, and copy them off the application host.

## 1. Release and backup gate

1. Require the target commit's GitHub quality, MSRV, RustSec, and dependency
   policy checks to be green. Re-run the locked local verification gauntlet.
2. Build in an isolated, clean checkout with
   `NOWLEDGE_GIT_REVISION=<full-target-sha>` and retain the previous binary.
   The build script treats that value as an assertion against Git `HEAD`; it
   refuses a mismatched revision or modified tracked files.
   Also retain a tested tenant-scope compatibility binary from `068e67b` or a
   later revision that dual-reads legacy and tenant-safe document identities.
   A binary older than `34cc3d1` is not a valid rollback target after the
   tenant-scope migration has written new identities.
3. Record the current binary checksum, source revision, environment-file
   checksum, and systemd unit definitions without printing secret values.
   Separately verify off-host secret-manager recovery for the exact
   `RAG_INDEX_HASH_SECRET`, tenant identity, Meilisearch master key, and service
   authentication material. A checksum of the environment file is evidence,
   not a recoverable secret backup.
4. A preliminary Meilisearch dump may be captured for diagnostics, but it is
   not the cutover backup: writes accepted after its dump task are not covered.
   The required rollback dump is created only after writers are stopped below.
5. Before running any migration, use the Meilisearch master key to create and
   capture three distinct custom keys in root-only files. The key creation
   response exposes each key value only once. Scope every key to `rag_*`, which
   covers the fixed indexes plus both derived dynamic families
   `rag_events__t_*__u_*` and `rag_context__t_*__u_*`:

   - runtime: `search`, `documents.add`, `documents.get`, `documents.delete`,
     `indexes.get`, `settings.get`, and `tasks.get`;
   - temporary maintenance: the runtime actions plus `indexes.create`,
     `indexes.delete`, and `settings.update`; use this as
     `RAG_MEILI_ADMIN_API_KEY` for all three migration binaries and only for
     deleting the disposable canary indexes and their registry row afterward;
   - rollback compatibility: the same actions as temporary maintenance, kept
     outside the active service environment for the retained dual-read binary.
     Because `068e67b` accepts only one Meilisearch credential, its rollback
     environment must set this broad compatibility key as `RAG_MEILI_API_KEY`;
     never substitute the target release's narrow runtime key.

   Custom-key actions and index patterns are immutable, so later narrowing is
   a create-switch-verify-revoke rotation, not an in-place edit. Preserve the
   old service key until both canaries pass. Before any revocation, compare it
   against master-authenticated key metadata without printing key values and
   classify it as a confirmed custom-key UID, a default key, or the master key.
   Delete only a confirmed custom-key UID. Default/master-key replacement is a
   separate master-key rotation procedure, never an inferred `DELETE /keys`.
   Do not expose the master key to the application or maintenance binaries. See
   the official
   [key creation reference](https://www.meilisearch.com/docs/reference/api/keys/create-api-key)
   and [rotation guidance](https://www.meilisearch.com/docs/capabilities/security/how_to/manage_api_keys).
6. Stop and drain every Nowledge writer. Keep Meilisearch running for the
   maintenance binaries. Do not reuse a tenant-scope plan captured while a
   legacy writer was active.
7. After the writer drain is complete, create the final cutover dump with
   `POST /dumps`, wait for its task to reach `succeeded`, checksum the resulting
   `.dump`, and copy it off-host. Do not start any migration until this exact
   post-drain dump and its off-host checksum are verified. Meilisearch documents
   the asynchronous dump and restore procedure in its
   [official backup guide](https://www.meilisearch.com/docs/resources/self_hosting/data_backup/dumps).

Run every migration command below with a minimal maintenance environment:
`RAG_RUN_MODE=production`, the exact `RAG_MEILI_URL`, and the temporary key as
`RAG_MEILI_ADMIN_API_KEY`. Leave `RAG_MEILI_API_KEY` unset or set it to a
different narrow runtime key so production credential separation remains
enforced. Do not source the old development service environment wholesale, and
do not expose unrelated application, authentication, Codex, or LLM secrets to
the maintenance binaries.

## 2. Create the new fixed indexes first

The final `tenant_scope_v1` binary scans every entry in `FIXED_INDEXES`.
Therefore the empty operation-journal and audit indexes must exist before the
tenant-scope plan is generated.

```sh
cargo run --locked --bin operations_v1 -- plan \
  --out /secure/operations-v1-plan.json
cargo run --locked --bin operations_v1 -- apply \
  --plan /secure/operations-v1-plan.json --dry-run
cargo run --locked --bin operations_v1 -- apply \
  --plan /secure/operations-v1-plan.json
cargo run --locked --bin operations_v1 -- plan \
  --out /secure/operations-v1-verify-plan.json
cargo run --locked --bin operations_v1 -- verify \
  --plan /secure/operations-v1-verify-plan.json

cargo run --locked --bin audit_records_v1 -- plan \
  --out /secure/audit-records-v1-plan.json
cargo run --locked --bin audit_records_v1 -- apply \
  --plan /secure/audit-records-v1-plan.json --dry-run
cargo run --locked --bin audit_records_v1 -- apply \
  --plan /secure/audit-records-v1-plan.json
cargo run --locked --bin audit_records_v1 -- plan \
  --out /secure/audit-records-v1-verify-plan.json
cargo run --locked --bin audit_records_v1 -- verify \
  --plan /secure/audit-records-v1-verify-plan.json
```

Stop immediately if either verification exits nonzero. These migrations are
non-destructive, but a missing-index plan is deliberately single-generation:
once that UID exists, the same plan cannot be applied again. If index creation
was accepted and the command then crashed or lost its response, stop and inspect
Meilisearch task history for the exact UID, time, and result. Preserve that
evidence, then generate and review a fresh plan and branch on its
`observed_state`: `already_present` may proceed directly to verify;
`settings_drift` requires dry-run/apply of that generation-bound plan and then
verify; `missing` is eligible for a new full sequence only after task history
confirms creation failed; `primary_key_mismatch` or an unexplained generation is
an incident/restore stop. Never blindly retry the original missing-index
artifact or adopt an unexplained index.
An older application ignores the added empty indexes, so code rollback does not
delete them. Preserve both the pre-apply plan and the fresh post-apply
verification plan: the former authorizes strict creation, while the latter
binds the exact immutable `createdAt` generation.

## 3. Migrate fixed-index document identities

Create an operator-reviewed mapping for every legacy document. There is no
implicit default tenant. Generate the final plan only after the two indexes
above exist and all legacy writers are stopped.

```sh
cargo run --locked --bin tenant_scope_v1 -- plan \
  --mapping /secure/tenant-mapping.json \
  --out /secure/tenant-scope-v1-plan.json \
  --batch-size 250
cargo run --locked --bin tenant_scope_v1 -- rollback-plan \
  --plan /secure/tenant-scope-v1-plan.json \
  --out /secure/tenant-scope-v1-rollback.json
cargo run --locked --bin tenant_scope_v1 -- apply \
  --plan /secure/tenant-scope-v1-plan.json \
  --rollback-plan /secure/tenant-scope-v1-rollback.json \
  --ack '<exact acknowledgement from rollback-plan>' \
  --checkpoint /secure/tenant-scope-v1-checkpoint.json \
  --dry-run
cargo run --locked --bin tenant_scope_v1 -- apply \
  --plan /secure/tenant-scope-v1-plan.json \
  --rollback-plan /secure/tenant-scope-v1-rollback.json \
  --ack '<exact acknowledgement from rollback-plan>' \
  --checkpoint /secure/tenant-scope-v1-checkpoint.json
cargo run --locked --bin tenant_scope_v1 -- verify \
  --plan /secure/tenant-scope-v1-plan.json
```

Review the plan before apply. Quarantine and unused-mapping counts must be
understood, and cutover requires `ready_to_cutover=true`. Preserve the mapping,
plan, rollback plan, acknowledgement, checkpoint, and verification output as
one release evidence set. The detailed data rollback rules remain in
[ADR 0004](../adr/0004-tenant-scope-v1.md).

## 4. Configuration and canary gate

1. Preserve the exact `RAG_INDEX_HASH_SECRET` and tenant identity used by the
   existing data. Never rotate the index key in place. Use the runtime key
   created before migration and rotate the temporary maintenance key to a new,
   narrower service admin key:

   - `RAG_MEILI_API_KEY` is the runtime document/search key. Grant only the
     document, search, task-read, index-read, and settings-read actions needed
     by the service; explicitly omit index create and delete.
   - `RAG_MEILI_ADMIN_API_KEY` is the service-time managed-index key. Grant
     `indexes.create`, `indexes.get`, `settings.get`, `settings.update`, and
     `tasks.get`, scoped to `rag_*`. It has no search or document permission.

   Keep the temporary maintenance key through target-canary cleanup and the
   first live check. Keep the rollback compatibility key and its root-only compatible
   environment through the rollback retention window. Never revoke the old
   service key until the compatibility canary below has proved the replacement
   rollback credential works.

   Never give both variables the same key. Keep both files/environment values
   private and do not print them in release evidence.
2. Set `RAG_RUN_MODE=production`, `RAG_STORE_BACKEND=meili`, and the exact
   `RAG_MEILI_URL`; configure explicit authentication and set
   `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=false`. Do not set the ephemeral-storage
   acknowledgement for this deployment. Set
   `RAG_MEILI_ALLOW_INITIAL_PROVISION=false`; an upgrade must never accept an
   all-missing managed index set as a new installation.
3. After both fixed-index migrations verify, read the exact `createdAt` values
   from `GET /indexes/rag_operations` and
   `GET /indexes/rag_audit_records`. Set them as
   `RAG_MEILI_OPERATIONS_INDEX_CREATED_AT` and
   `RAG_MEILI_AUDIT_INDEX_CREATED_AT`. Preserve these integrity pins with the
   release evidence and deployment configuration; do not rebaseline them just
   because startup reports a mismatch.
4. Replace deprecated `RAG_MEILI_WAIT_FOR_TASKS` with one explicit
   `RAG_WRITE_CONSISTENCY`; production must not use `eventual`.
5. Start the retained `068e67b`-or-later dual-read
   compatibility binary on another loopback port with its root-only rollback
   environment. Set the broad rollback compatibility key as that legacy
   binary's `RAG_MEILI_API_KEY`; do not give it the target release's narrow
   runtime key or split-key environment. Require readiness, fixed index
   hydration, and an authenticated owner read against the migrated data. Stop
   it before continuing. This proves the post-migration rollback binary, data
   shape, and server-side credential are viable together.
6. Start the release binary as an isolated canary on a different loopback port
   against the migrated data. Do not replace the service binary yet.
7. Require the target canary to pass all of these checks:

   - `/livez` reports the exact target commit;
   - `/readyz` returns 200;
   - unauthenticated protected routes return 401;
   - authenticated `/healthz` reports `store_backend=meili` plus healthy
     Meilisearch, parser, hydration, and every required primary/analysis LLM
     dependency;
   - authenticated `/v1/admin/metrics` returns OpenMetrics ending in `# EOF`;
   - a disposable owner credential can create both dynamic indexes, append a
     uniquely tagged event through the runtime key, and find it through owner
     search; capture the returned event/context index UIDs and exact registry
     document ID, then use the temporary maintenance key to delete those two
     disposable indexes and that registry document, waiting for every task,
     verifying all three resources are absent, and preserving audit evidence;
   - `nowledge_audit_background_drops_total` has not increased;
   - service logs contain no panic, migration, authentication, or secret leak.

For a genuinely new production installation only, an operator may set
`RAG_MEILI_ALLOW_INITIAL_PROVISION=true` for one observed first start. The gate
must first prove through the Meilisearch index-list API that the entire instance
contains zero indexes, including no orphaned dynamic event/context families;
absence of only the fixed indexes is not sufficient. Verify all managed indexes
and settings, capture and configure both durable-index
`createdAt` pins, stop the process, remove the flag (or set it false),
narrow/rotate the admin key, and start again. Never use that gate to
recover an unexpectedly empty cluster; restore the verified dump instead.

## 5. Install and verify

Stop the canary, atomically replace the service binary, restart the service,
and repeat the complete non-destructive canary checks on the live port. Also
re-run the three migration verification commands and confirm systemd has not
entered a restart loop. Classify the superseded credential as described in
section 1. If it is a confirmed custom key, revoke it and then rerun readiness,
an authenticated runtime write/search, and fresh dynamic-index creation. Next
revoke the temporary maintenance custom key and repeat those same probes; use
the still-retained rollback compatibility key to clean up the post-revocation
disposable indexes and registry row. If either probe fails, restore the revoked
capability through a master-authenticated credential rotation, repair the active
environment to the intended retained runtime/service-admin keys, or roll back
before continuing. Do not
delete a default/master credential here; schedule its separate master-key
rotation and repeat the full canary procedure for that change. Keep the tested dual-read
compatibility binary, its still-valid rollback credential and environment,
the old binary (for pre-migration forensics only), dump, and evidence set until
the release has passed its retention window.

Audit records have indefinite retention in this revision. Alert on any audit
background drop, free space below 25 percent, or a 90-day growth projection
that no longer fits. The deployment owner also owns legal holds and archival.
Before an approved cleanup, stop writers, create and verify an off-host dump,
and constrain deletion by both `tenant_id` and `occurred_at`; never delete the
entire audit index as a retention shortcut.

## 6. Disaster restore from the cutover dump

Use this only for lost or damaged Meilisearch data, not as a routine code
rollback.

1. Stop and drain every writer. Preserve the damaged data directory and its
   logs read-only for incident analysis; never import over the only copy.
2. Verify the off-host dump checksum, release evidence, and the Meilisearch
   version that created it. Restore into an isolated empty data directory with
   a compatible Meilisearch version using the documented startup-time dump
   import. Do not point production traffic at the import process.
3. Restore the exact `RAG_INDEX_HASH_SECRET`, tenant identity, and Meilisearch
   master-key material from the secret manager. Dump import does not make the
   generated custom-key secret values recoverable. If the prior master material
   cannot be restored, create and distribute replacement runtime, maintenance,
   and rollback credentials before any canary; never guess or reuse leaked
   values.
4. On the isolated restore, inventory fixed and dynamic indexes and compare
   document counts/checksums with the retained evidence. Re-run each migration
   verification using a freshly generated and reviewed generation-bound plan.
   If the restored state predates a migration, repeat its full plan/dry-run/apply/
   fresh-plan/verify sequence instead of editing the dump.
5. Read the restored durable indexes' `createdAt` identities. Treat changing the
   two deployment pins as an explicit operator-reviewed rebaseline tied to the
   restore evidence, never an automatic response to a mismatch.
6. Run the compatibility and target canaries from section 4 against the isolated
   restore. Promote the restored data directory only after both pass, then
   repeat the live checks from section 5 before revoking any recovery credential.

See Meilisearch's official
[dump import documentation](https://www.meilisearch.com/docs/resources/self_hosting/data_backup/dumps)
for version and startup semantics.

## 7. Code rollback

1. Stop the new application. Before tenant migration, the previous binary may
   be restored. After tenant migration, restore only the retained dual-read
   compatibility binary (`068e67b` or later), its compatible environment, and
   its retained server-side Meilisearch credential; never restart an older
   raw-ID-only binary against migrated copies.
2. Leave `rag_operations`, `rag_audit_records`, legacy documents, and the new
   tenant-safe copies in place. The migrations deliberately preserve legacy
   rows; the compatibility binary ignores the added audit/journal indexes and
   dual-reads the legacy and tenant-safe document identities.
3. Verify legacy checksums using the retained tenant-scope plan before
   restarting the retained dual-read compatibility binary.
4. Do not bulk-delete migrated documents. If a data rollback is truly needed,
   review and execute only the exact actions in the retained tenant-scope
   rollback artifact as described by ADR 0004.
5. Preserve logs and failed-release artifacts for incident review, then fix
   forward and repeat this runbook from a new exact commit.
