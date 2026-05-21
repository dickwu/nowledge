---
name: meili-test
description: Run the gated Meilisearch integration test suite against the gfit host's Meilisearch through an SSH+socat tunnel. Use when asked to "run meili tests", "test against gfit meili", "run the meili integration suite", or when changes touch the Meili-backed repository path (src/meili.rs, src/repository.rs's MeiliKnowledgeRepository, or store hydration).
disable-model-invocation: true
---

# meili-test

Runs the optional `meili_integration` test suite against a remote Meilisearch on the `gfit` host. The flow opens a persistent local tunnel (via `tmux`+`socat`+`ssh`), fetches the master key from the remote `.env`, and runs `cargo test --test meili_integration`.

This skill is `disable-model-invocation: true` because it has external side effects (opens an SSH connection, creates a `tmux` session). Only the user should trigger it.

## Preconditions

- `ssh gfit` resolves (or override with `GFIT_HOST`).
- `tmux` and `socat` are installed locally. Defaults look in `/opt/homebrew/bin/`; the scripts fall back to `command -v`.
- The remote `.env` file under `/home/peilin/nowledge/.env`, `/home/peilin/meilisearch/.env`, or `/server/meilisearch/.env` contains `RAG_MEILI_API_KEY`, `MEILI_MASTER_KEY`, or `MEILI_API_KEY`.

## One-shot run

The helper at `scripts/gfit_meili_test.sh` does everything (open tunnel → fetch key → run tests). Forward additional `cargo test` args after the script name:

```sh
bash scripts/gfit_meili_test.sh
bash scripts/gfit_meili_test.sh meili_backend_creates_dynamic_user_indexes_and_searches_events -- --nocapture
```

Note: `scripts/` is gitignored. The helpers are present locally on the operator's machine but not in CI. If the script is missing, fall through to the manual flow below.

## Manual flow (when helpers are missing)

1. Open the tunnel — keeps running in a `tmux` session named `gfit-socat-meili`:
   ```sh
   bash scripts/gfit_meili_tunnel.sh
   ```
   Confirms by curling `http://127.0.0.1:7700/health`.

2. Fetch the master key from the remote env (this is what `scripts/gfit_meili_env.sh` automates if available):
   ```sh
   key=$(ssh -o BatchMode=yes gfit "awk -F= '/^(RAG_MEILI_API_KEY|MEILI_MASTER_KEY|MEILI_API_KEY)=/ {print \$2; exit}' /home/peilin/nowledge/.env /home/peilin/meilisearch/.env /server/meilisearch/.env 2>/dev/null")
   ```

3. Run the tests with the integration-test env vars set:
   ```sh
   RAG_TEST_MEILI_URL=http://127.0.0.1:7700 \
   RAG_TEST_MEILI_API_KEY="$key" \
   cargo test --test meili_integration
   ```

## What the suite covers

The integration tests in `tests/meili_integration.rs` exercise the dynamic per-user index creation path through `MeiliKnowledgeRepository`, end-to-end through the public router. They mint a unique `tenant_id` per run (`test-tenant-<uuid_v7>`) to avoid colliding with previous test debris when the same Meili server is reused.

If the tunnel is up but the suite is skipping with `skipping Meilisearch integration test`, either `RAG_TEST_MEILI_URL` is missing or the server requires a key and `RAG_TEST_MEILI_API_KEY` is empty.

## Cleanup

The tmux session persists between runs intentionally. To stop:

```sh
tmux kill-session -t gfit-socat-meili
```
