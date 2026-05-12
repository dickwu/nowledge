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
RAG_BEARER_TOKEN=optional-user-token
RAG_ADMIN_TOKEN=optional-admin-token
RAG_MEILI_URL=http://127.0.0.1:7700
RAG_MEILI_API_KEY=optional-meili-key
RAG_LLM_PROVIDER=none
RAG_LLM_MODEL=none
RAG_ALLOW_CODEX_AUTH_IMPORT=false
```

## Verify

```sh
cargo fmt --check
cargo check
cargo test
```

The regression tests cover the v0.6 hard constraints around per-user event
isolation, owner mismatch rejection, state upsert shape, company-doc preflight,
structured row idempotency, and token redaction.
