---
name: verify
description: Run the project's standard pre-merge verification gauntlet for the nowledge crate (cargo fmt --check, cargo clippy --all-targets -- -D warnings, cargo check, cargo test) and report results. Use after substantive code changes, before claiming work is done, when the user says "verify", "check this", or "run the gauntlet", or when about to open a PR.
---

# verify

Run the exact verification sequence used by CI. CI (`.github/workflows/ci.yml`) runs the same steps in the same order, so a green local run mirrors a green pipeline.

## Mandatory sequence

Run these in order. Stop and surface failures immediately — do not continue to later steps with a failing earlier step.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo check
cargo test
```

Notes:
- `--all-targets` is required for clippy so test and example code are linted.
- `-D warnings` is non-negotiable — any warning is a hard fail.
- `cargo check` after clippy looks redundant but matches CI; keep it.
- `cargo test` runs the default integration suite (`tests/api_spec.rs`). The optional `meili_integration` and `mineru_integration` tests are skipped silently unless `RAG_TEST_MEILI_URL` / `RAG_TEST_MINERU_API_URL` are set.

## Optional integration runs

Only run these when the user asks or when a change clearly affects the Meili or MinerU code paths:

```sh
RAG_TEST_MEILI_URL=http://127.0.0.1:7700 \
RAG_TEST_MEILI_API_KEY="$MEILI_MASTER_KEY" \
cargo test --test meili_integration

RAG_TEST_MINERU_API_URL=http://127.0.0.1:8000 \
cargo test --test mineru_integration
```

The `scripts/gfit_meili_test.sh` helper (gitignored) wraps the Meili variant against the `gfit` host's Meili through an SSH tunnel — see the `/meili-test` skill.

## Reporting

When everything passes, report exactly which steps ran and their status. If something fails, surface the first failing step's output (or its compiler/clippy diagnostics) and stop — do not "fix forward" silently. Treat `cargo fmt --check` failures as a signal to run `cargo fmt` and re-verify, not as code to edit.
