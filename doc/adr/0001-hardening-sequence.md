# ADR 0001: Sequence hardening before architectural decomposition

- Status: Accepted
- Date: 2026-07-13
- Audit baseline: `main` at `d332c4bf22be9a7fe7588e9d054771fdfd0cce9f`
- Package: `nowledge` 0.11.0

## Context

Nowledge already has typed configuration, structured API errors, tracing, LLM
and repository abstractions, staged retrieval, provenance, and broad router-level
tests. The immediate production risks are at the boundaries: authorization,
tenant isolation, persistence and hydration, request and queue bounds,
read-after-write behavior, and external-provider resilience.

A broad rewrite of `routes.rs`, `store.rs`, or the storage architecture before
these behaviors are characterized would make regressions harder to detect and
rollback. Hardening will therefore proceed as small, independently green pull
requests. Behavioral characterization precedes security and correctness changes;
feature-by-feature modularization follows them.

## Baseline record

The audit source revision is `main` at `d332c4b`. The original checkout
contained pre-existing user-owned dirty state; it was treated as out of scope
and was not modified for this work. PR 1 uses an isolated worktree on branch
`codex/hardening-pr1-guardrails`, which was clean at the baseline revision.

The local audit toolchain is the Homebrew 1.96.1 build dated 2026-06-26:

- `rustc 1.96.1 (31fca3adb 2026-06-26)`
- `cargo 1.96.1 (356927216 2026-06-26)`
- Host: `aarch64-apple-darwin`

This is the 1.96.1 nightly-era baseline supplied for the hardening work; command
output identifies it as a Homebrew build rather than using a `-nightly` suffix.

### Verification results

| Command | Result | Baseline evidence |
|---|---|---|
| `cargo fmt --check` | Pass | Clean formatter exit. |
| `cargo clippy --all-targets -- -D warnings` | Pass | All targets linted; no warnings or diagnostics. |
| `cargo check` | Pass | Default targets type-check. |
| `cargo check --all-targets` | Pass | Library, binary, and test targets type-check. |
| `cargo test` | Pass | 28 library tests and 40 API tests pass; binary and doc targets contain no tests. |
| `cargo package --allow-dirty --no-verify` | Pass with warnings | 131 files packaged, 1.1 MiB (213.8 KiB compressed). |

The default test command executes 68 non-gated tests: 28 unit tests in
`src/lib.rs` and 40 tests in `tests/api_spec.rs`. `src/main.rs` and doc tests
contain zero tests.

Cargo also discovers six Meilisearch and one MinerU integration test. With
`RAG_TEST_MEILI_URL` and `RAG_TEST_MINERU_API_URL` unset, those seven test bodies
return early and self-skip; Cargo therefore reports them as passing rather than
ignored. Live integration behavior is not proven by the default suite and must
not be represented as such.

Packaging emits two accepted baseline warnings:

1. The manifest has no `documentation`, `homepage`, or `repository` metadata.
2. `spin v0.9.8` in `Cargo.lock` is yanked from crates.io.

These warnings do not fail PR 1, but they are recorded debt. A pull request must
not introduce additional packaging warnings, and the dependency warning must be
resolved or governed by explicit policy in the final hygiene phase.

### Audit discrepancies established locally

- GitHub Actions follows the rolling Rust `stable` channel, which advanced to
  Rust/Clippy 1.97 after the 1.96.1 local baseline was recorded. The newer
  `useless_borrows_in_formatting` lint rejected three redundant references in
  one existing `format!` call. PR 1 removes only those references; formatting
  macros borrow their arguments internally, so the emitted event text and
  runtime behavior are unchanged. Toolchain pinning and minimum-supported-Rust
  policy remain explicit PR 10 decisions rather than an incidental CI repair.
- Axum 0.8 already imposes an implicit body limit near 2 MiB. Oversized JSON is
  rejected with a framework 413 outside Nowledge's error envelope; oversized
  multipart input is converted to a generic 400 envelope. The missing control is
  an explicit, typed, route-appropriate limit and stable rejection contract, not
  the complete absence of any ceiling.
- Router documentation is synchronized at the baseline: 87 exact
  method/path/handler triples match 87 manifest entries and 87 endpoint files.
  Counting `.route(...)` calls is not a valid drift check because six HTTP
  methods are chained onto shared path registrations.
- Default `cargo test` discovers the optional integration binaries but does not
  prove live Meilisearch or MinerU behavior when their gate variables are unset.
- Meilisearch-backed writes still default to asynchronous task acceptance in
  environment configuration even though deterministic test configuration waits
  for tasks.
- The source-revision hydration query asks Meilisearch for 2,000 rows, but the
  live index's default `maxTotalHits` still truncates the result at 1,000. The
  requested limit therefore does not provide a distinct 2,000-row ceiling;
  pagination and index-setting policy must be addressed together in PR 5.
- Source-document restart behavior is mixed. Explicit-owner personal documents
  can use repository read-through, but company documents serialize without an
  `owner_user_id` field and the current `owner_user_id IS NULL` query does not
  match that missing field, so an authorized read returns 404 after restart.

## Decision

Adopt the following hardening order and do not combine the phases into a single
branch:

1. **Characterize and guard:** record the baseline and add named tests for
   current authorization, disclosure, request-bound, queue, persistence,
   consistency, pagination, reset, error-envelope, and route-manifest behavior.
2. **Authorize explicitly:** replace implicit owner scope, protect tenant-wide
   mutations and detailed diagnostics, and preserve legacy behavior only through
   an explicit compatibility path.
3. **Bound resource use:** add request/upload limits, safe CORS, timeouts,
   concurrency/load shedding, queue backpressure, request IDs, and coordinated
   shutdown.
4. **Make fixed indexes tenant-safe:** tenant-key every persisted fixed-index
   entity and operation, introduce collision-safe IDs and filters, and provide a
   dry-run, dual-read, verifiable migration.
5. **Complete durability:** define durability by domain, remove silent scan
   ceilings, hydrate durable data completely, and recover interrupted work.
6. **Make mutation consistency explicit:** persist primary state before cache
   application, journal composite operations, provide idempotent reconciliation,
   guarantee the configured read-your-writes behavior, and wait for Meilisearch
   tasks where required.
7. **Harden providers:** reuse bounded clients, define timeout/retry budgets,
   separate instructions from untrusted evidence, and validate and authorize all
   model-produced persistent data.
8. **Implement honest streaming:** make `/v1/rag/stream` incremental and
   cancellable while retaining `/v1/rag/answer` compatibility.
9. **Decompose by feature:** only after behavior is locked, extract thin HTTP,
   service, domain, storage, and provider boundaries one feature at a time.
10. **Finish operational controls:** add protected metrics, redacted audit
    records, tested/generated API contracts, dependency policy, and repository
    hygiene.

Meilisearch remains the storage/search implementation during this sequence. A
new canonical database is explicitly out of scope; the system-of-record decision
requires a later ADR after the present behavior and failure modes are measured.

## Baseline invariants already enforced

Every pull request must preserve these verified baseline properties unless a
reviewed migration and compatibility plan explicitly changes the contract:

1. Named owner principals are rejected with 403 when they request another
   owner's private data.
2. Per-user index UIDs remain HMAC-derived; raw tenant and user identifiers are
   not exposed in index names.
3. Retrieval remains fragment-first; source-document bodies do not become
   default hits solely through document-vector evidence.
4. Traceback retains source URI, page, bounding box, block type, section path,
   offsets, checksum, assets, and artifact references when present.
5. Existing public route paths, serialized response shapes, and the
   `{ "error": { "code", "message", "details" } }` envelope remain stable
   unless an additive compatibility path is documented.
6. Existing idempotency keys remain deterministic and retry-safe.
7. Memory mode and deterministic mock providers remain available for local
   development and tests; memory mode is not represented as durable production
   storage.
8. Production startup rejects an unauthenticated configuration unless the unsafe
   override is explicit, and `/livez` remains process-only.
9. Tokens and bearer-style secrets remain redacted at ordinary trace and log
   boundaries. Document bodies and grounded prompts appear only in explicitly
   authorized payloads, including `/v1/fs/read` and the currently characterized
   authenticated debug/prompt-preview surfaces; those debug disclosures are not
   treated as public health or error output.
10. No new canonical storage system or broad service-layer rewrite is introduced
    in the hardening sequence.

## Target invariants and characterized exceptions

PR 1 does not claim that the audited target properties already hold. It locks
their current exceptions so later pull requests must invert them deliberately:

| Target invariant | Characterized baseline exception | Planned correction |
|---|---|---|
| Every ordinary principal is explicitly owner-, tenant-service-, or admin-scoped. | The legacy ownerless bearer can cross owner IDs (SEC-01). | PR 2 |
| Shared company and dataset mutations require an explicit writer/admin permission. | Ordinary authenticated owners currently pass these guards (SEC-02). | PR 2 |
| Public diagnostics and client errors reveal no auth paths or internal/upstream details, and authenticated debug surfaces follow an explicit prompt/body disclosure policy. | LLM status exposes the configured auth path, internal errors can echo source details, and authenticated RAG/analysis debug or prompt-preview responses currently include grounded prompts (INFO-01, ERR-01). | PR 2, then provider-specific hardening in PR 7 |
| Every fixed-index operation and document identity is tenant-isolated. | Several fixed-index models, IDs, scans, and deletes are tenantless or incompletely filtered (TEN-01, TEN-02). | PR 4 |
| Every API-presented durable domain survives restart or is explicitly classified otherwise. | Startup hydration covers only company context/sources/revisions, eval/harness, and ingest metadata; other domains rely on read-through or disappear. Source-document read-through works for explicit owners but misses company documents whose omitted owner field does not match `IS NULL`. Canonical parse-artifact and parsed-block maps disappear even though both values remain available inside startup-hydrated ingest-result records (PERS-01). | PR 5 |
| Readiness fails when mandatory hydration is incomplete. | Hydration has no complete-domain report or readiness gate (PERS-01). | PR 5 |
| Persistence failure never leaves an unacknowledged live mutation. | Some memory mutations precede failed remote writes (PERS-02). | PR 6 |
| Accepted writes meet their documented read-after-write consistency level. | Asynchronous Meili acceptance can make an immediate read miss (PERS-03). | PR 6 |
| Bootstrap reset waits for destructive tasks before recreation/settings. | Current reset phases can overlap (PERS-04). | PR 6 |

## Required decision records

This ADR fixes sequence and gates, not every downstream design. Before the
relevant implementation is merged, focused ADRs must record:

- principal scope, shared-write roles, and legacy bearer compatibility (PR 2);
- HTTP/queue limit defaults and overload semantics (PR 3);
- tenant-safe fixed-index identity, migration, dual-read, verification, and
  destructive rollback rules (PR 4);
- the durability matrix, hydration authority, and pagination contract (PR 5);
- mutation-journal authority, partial-operation reconciliation, and consistency
  modes (PR 6);
- provider retry budgets, prompt trust boundaries, and structured-output
  authorization (PR 7);
- SSE event and compatibility contracts (PR 8);
- feature-module dependency boundaries and temporary re-exports (PR 9);
- metrics cardinality, audit retention/redaction, API contract generation, and
  dependency/advisory policy (PR 10).

An evaluation of a transactional system of record plus Meilisearch as a search
index is deferred to a separate post-hardening ADR.

## Pull-request gates

Every pull request in the sequence must:

1. name the finding IDs and behavior it addresses;
2. add or update the smallest focused tests before changing characterized
   behavior where practical;
3. remain independently reviewable and avoid unrelated refactors;
4. preserve the invariants above and document compatibility or migration impact;
5. pass, in order:

   ```sh
   cargo fmt --check
   cargo clippy --all-targets -- -D warnings
   cargo check --all-targets
   cargo test
   cargo package --allow-dirty --no-verify
   ```

6. run `cargo check` as the repository's additional default type-level sanity
   check;
7. update docs only to match behavior actually implemented;
8. report changed files, command results, known gaps, remaining risk, and exact
   rollback steps.

Additional phase gates are mandatory:

| PR | Focused merge gate |
|---|---|
| 1 | No intended production behavior changes; every discovered gap is represented by a clearly named, non-ignored characterization or guardrail test. |
| 2 | Owner/tenant/admin policy branches have router tests; compatibility mode is explicit; public diagnostics and client errors reveal no sensitive details. |
| 3 | 413, overload, disabled-worker, timeout, CORS, request-ID, cancellation, and shutdown behavior is proven under pressure. |
| 4 | Run live Meilisearch tests; prove same logical IDs and URIs are isolated across two tenants; migration is dry-run capable, restartable, idempotent, and verified before cleanup. |
| 5 | Run live Meilisearch restart tests; every durable domain survives restart; 1,001/2,001-record scans do not truncate; readiness reports incomplete hydration. |
| 6 | Run live Meilisearch fault and immediate-read tests; failed/partial operations remain observable and retry-safe; reset waits for task completion. |
| 7 | Run mock-provider timeout, 429, 5xx, auth, quota, malformed-output, and adversarial-evidence tests; run live MinerU tests when parser behavior changes. |
| 8 | Verify first-delta timing, SSE wire format, usage/citation ordering, upstream cancellation, and JSON-route compatibility. |
| 9 | Run focused and full suites after each feature extraction; domain modules do not import Axum and handlers contain no domain orchestration. |
| 10 | CI enforces quality, packaging, API drift, and dependency/advisory policy; metrics and audit records obey redaction and cardinality rules. |

Before each merge, run a production-configuration startup smoke test proving
unsafe defaults are rejected. Optional integration suites count as evidence only
when their gate variables point to reachable services; self-skipping test bodies
do not satisfy a live-integration gate.

## Rollback policy

PR 1 changes tests and documentation plus one behavior-neutral formatting call
needed by the rolling stable CI toolchain. Its runtime rollback is a normal
revert; it carries no data migration or production configuration effect.

For every later PR:

- build and retain the last green artifact before deployment;
- keep compatibility flags and old read paths until new behavior and data are
  verified in production-like conditions;
- make migrations plan/apply/verify operations, with dry-run as the default;
- never delete or rewrite legacy tenant data as part of the initial apply step;
- require migration operations and reconciliation to be idempotent and safe to
  resume after interruption;
- separate code rollback from data rollback, and document both before apply;
- stop rollout and restore the last green artifact on any cross-owner or
  cross-tenant access, secret disclosure, unreported live-only mutation,
  incomplete hydration reported healthy, or public contract regression;
- after rollback, restore compatible configuration, leave legacy data intact,
  run readiness plus owner/tenant isolation smoke tests, and reconcile any
  journaled partial operations before accepting writes.

No phase may depend on an irreversible migration from a later phase. If a pull
request cannot be reverted without data loss or loss of access, it is not ready
to merge.

## Consequences

This sequence deliberately delays large-scale cleanup and new storage choices.
It adds characterization work and temporary compatibility paths, but yields
small reviewable changes, explicit failure semantics, verifiable migration
points, and a reliable rollback boundary after every pull request.
