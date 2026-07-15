# ADR 0008: Durable shared-mutation audit records

## Status

Accepted.

## Context

The five shared-knowledge mutation targets previously emitted a tracing event
after their Store call returned. That provided useful diagnostics but did not
prove that an attempt was retained before an authorized mutation ran, and a
process or logging failure could erase the only audit evidence. Authorization
denials used the same process-local tracing path.

Audit data is security-sensitive in a second way: recording a raw source ID,
owner ID, activation explanation, request body, route path, query, prompt,
token, or provider response would create a new disclosure surface. The audit
contract therefore needs a deliberately closed schema rather than a generic
JSON or message field.

## Decision

Add the tenant-scoped fixed Meilisearch index `rag_audit_records`, with primary
key `id`, and a typed `AuditRecord` model. The model permits only:

- an `audit_` identifier backed by UUIDv7 and UUIDv7 request correlation;
- the configured trusted tenant identifier;
- a bounded principal-scope enum and optional HMAC owner identity;
- a bounded five-value action enum and HMAC resource identity;
- a bounded reason code plus HMAC reason fingerprint;
- a bounded outcome and optional bounded API error kind;
- an optional bounded operation identifier; and
- occurrence and update timestamps.

There are no free-form identifier, reason, body, query, prompt, path, token, or
provider-response fields. Repository validation rejects malformed UUIDs,
non-HMAC identity fields, incompatible outcome/error combinations, owner-scope
mismatches, invalid timestamps, and unbounded operation identifiers.

`AuthState` receives only a cloneable `AuditRecorder` capability. It does not
receive Store, repository, or AppState coupling. The recorder owns a narrow
sink implemented by Store; Store validates and confirms the repository write
before publishing the accepted record to its in-process cache. Memory and
Meilisearch repositories implement the same typed upsert boundary. The
Meilisearch document uses the existing tenant-safe physical identity wrapper.

For an authorized mutation:

1. Construct and persist an `attempted` record.
2. If persistence or task confirmation fails, return 503 and do not invoke the
   mutation.
3. Invoke the mutation only after the attempt is accepted.
4. Supervise a bounded background update of the same record ID to `success` or
   `failure`, without replacing a completed mutation result with a request
   timeout.
5. If finalization fails, preserve the mutation's original result, retain the
   accepted attempt, and emit only bounded action/outcome metadata plus HMAC
   identities and a redacted cause fingerprint.

For a mapped authentication or authorization denial, admit one `denied`
record to a supervised, bounded best-effort background write. Capacity,
shutdown, or persistence failure is diagnosed in the same bounded form but
never delays or replaces the original 401/403 response. Bounded admission
prevents an unavailable audit backend from turning denial traffic into
unbounded queued work. The process independently admits at most five anonymous
authentication denials and ten authenticated authorization denials per minute
(or lower limits derived from the configured request rate), and exports
bounded `rate_limit`, `capacity`, `shutdown`, and `persistence` drop reasons.
Anonymous traffic therefore cannot consume the authenticated denial budget.

The runtime Meilisearch document key and index-administration key are distinct
in production. The document key must not have `indexes.create` or
`indexes.delete`, so a document add cannot silently recreate a missing durable
index. Startup captures `{uid, primaryKey, createdAt}` for `rag_operations` and
`rag_audit_records`; every durable write checks the same generation and exact
managed settings before the add and again after its task succeeds. A missing,
replaced, or drifted index fails closed, and readiness remains unhealthy.

## Retention and capacity policy

This revision deliberately retains durable audit evidence indefinitely. There
is no automatic age-based deletion, because a process-local cleanup policy
could destroy evidence under legal hold or during an incident. The
`occurred_at` and `updated_at` fields are filterable and sortable so a future
approved retention migration can operate by tenant and age without changing
the schema.

The deployment owner is responsible for capacity and legal-hold decisions.
Monitor Meilisearch free space and audit-index growth, alert on every increase
of `nowledge_audit_background_drops_total`, and keep enough capacity for at
least 90 days at the observed growth rate. Alert before either free space falls
below 25 percent or the 90-day projection no longer fits. Before any manual
deletion, obtain the retention/legal approval, stop writers, create and verify
an off-host dump, and constrain the deletion by both `tenant_id` and
`occurred_at`. Never delete the whole index as a retention shortcut.

No public or admin audit-query route is added. Operational access to the index
is a deployment concern outside this API revision.

## Migration and rollout

Adding `rag_audit_records` to the managed fixed-index set is intentionally
fail-closed. A new binary sees a pre-upgrade Meilisearch deployment as partial
and refuses automatic recreation. Build `audit_records_v1` from the exact
application revision, back up Meilisearch, drain writers, and run:

```sh
cargo run --bin audit_records_v1 -- plan \
  --out /secure/audit-records-v1-plan.json
cargo run --bin audit_records_v1 -- apply \
  --plan /secure/audit-records-v1-plan.json --dry-run
cargo run --bin audit_records_v1 -- apply \
  --plan /secure/audit-records-v1-plan.json
cargo run --bin audit_records_v1 -- plan \
  --out /secure/audit-records-v1-verify-plan.json
cargo run --bin audit_records_v1 -- verify \
  --plan /secure/audit-records-v1-verify-plan.json
```

Deployment must stop if verify exits nonzero. The migration manages only the
audit index. It is non-destructive, waits for every returned
Meilisearch task, reconciles settings drift, rejects an incompatible primary
key or a tampered/destructive plan, and refuses to recreate an index that was
present at plan time but disappeared before apply. Plan, dry-run, and verify
perform no mutations.

An absent-index plan is deliberately not reusable after its UID appears. If
creation was accepted but the command outcome became ambiguous, operators must
stop, preserve and inspect Meilisearch task history for the exact UID, time, and
result, then generate and review a fresh plan. `already_present` proceeds to
verify; `settings_drift` requires dry-run/apply and verify with that
generation-bound plan; `missing` starts a new full sequence only after task
history confirms the create failed. `primary_key_mismatch` or an unexplained
generation is an incident/restore stop. Operators must not retry the original
absent-index artifact or adopt an unexplained generation.

Rollback leaves the audit index and plan artifact in place. The previous
application revision ignores the additional index, and deleting it would
discard evidence needed for a forward retry.

## Consequences

- Authorized shared writes have durable attempted evidence before execution.
- Audit unavailability fails authorized writes closed, adding repository
  latency and availability to those five mutation paths.
- Finalization failure can leave an `attempted` record even when the mutation
  completed; bounded diagnostics identify that uncertainty without exposing
  request data.
- Authorization responses remain stable during audit outages.
- Audit evidence has indefinite retention in this revision; operators own
  capacity alerts, legal holds, archival, and any explicitly approved
  tenant-and-age-scoped deletion.
- There is no in-product audit retrieval surface yet; operators must use
  protected Meilisearch access and tenant filters when investigation requires
  it.
