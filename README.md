# Nowledge

ContextFS-style State/History RAG service built from the v0.6 user-event-index
specs.

## What Is Implemented

- Rust + axum API service.
- `/v1/state/*` update-style current state APIs.
- `/v1/history/*` append-only history APIs.
- HMAC-derived per-user event indexes:
  `rag_events__t_{tenant_hash}__u_{user_hash}`.
- HMAC-derived per-user personal context indexes:
  `rag_context__t_{tenant_hash}__u_{user_hash}`.
- Shared company context index semantics via `rag_company_context`.
- ContextFS URI nodes with L0 `.abstract`, L1 `.overview`, and L2 detail/chunk
  layers.
- Staged retrieval and trace records.
- Company document preflight, revisions, and activation.
- Structured dataset snapshots, idempotent row ingestion, and deterministic
  numeric summary calculation.
- Insight, session, RAG answer/debug, LLM status, and debug route surfaces.
- Meilisearch bootstrap hooks with in-memory persistence for local development
  and tests.

## Run

```sh
cargo run
```

The default URL is `http://127.0.0.1:14242`.

Useful environment variables:

```sh
RAG_HOST=127.0.0.1
RAG_PORT=14242
RAG_TENANT_ID=default
RAG_INDEX_HASH_SECRET=change-me
RAG_STORE_BACKEND=memory
RAG_BEARER_TOKEN=optional-user-token
RAG_ADMIN_TOKEN=optional-admin-token
RAG_AUTH_USERS=owner-user-id:user-token:user
RAG_RUN_MODE=development
RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true
RAG_MEILI_URL=http://127.0.0.1:7700
RAG_MEILI_API_KEY=optional-meili-key
RAG_MEILI_WAIT_FOR_TASKS=false
RAG_LLM_PROVIDER=none
RAG_LLM_MODEL=none
RAG_OPENAI_API_KEY=optional-openai-key
RAG_CODEX_AUTH_PATH=optional-explicit-codex-auth-json
RAG_ALLOW_CODEX_AUTH_IMPORT=false
RAG_HEALTH_LLM_ENABLED=true
RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS=30
RAG_HEALTH_LLM_PROBE_TTL_SECONDS=60
RAG_HEALTH_LLM_MAX_STALE_SECONDS=120
RAG_HEALTH_LLM_TIMEOUT_MS=10000
RAG_HEALTH_REQUIRE_LLM=true
```

Use `RAG_STORE_BACKEND=meili` with `RAG_MEILI_URL` to mirror core writes to
Meilisearch and search per-user event indexes through Meilisearch. Production
mode requires configured auth unless `RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true` is
set explicitly.

Health endpoints split process liveness from operational readiness:

- `GET /livez` returns only `{"status":"ok"}` and does not query Meilisearch or
  LLM providers.
- `GET /healthz` checks Meilisearch plus the configured LLM provider/model,
  including auth validity, quota/rate-limit state, stale probe state, and a
  compact usage summary. If `RAG_HEALTH_REQUIRE_LLM=true`, an unconfigured or
  exhausted LLM makes the service unhealthy.
- `GET /readyz` uses the same readiness decision as `/healthz`.
- `GET /v1/usage` returns owner-scoped provider snapshots for ordinary users and
  global provider snapshots for admins.

## Verify

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo check
cargo test
```

Optional Meilisearch integration tests run when `RAG_TEST_MEILI_URL` is set:

```sh
RAG_TEST_MEILI_URL=http://127.0.0.1:7700 cargo test --test meili_integration
```

The regression tests cover the v0.6 hard constraints around per-user event
isolation, owner mismatch rejection, state upsert shape, company-doc preflight,
structured row idempotency, and token redaction.
