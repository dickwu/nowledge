<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-05-20 -->

# workflows

## Purpose
GitHub Actions workflow definitions. A single Rust CI workflow runs on every
pull request and on every push to `main` or `master`.

## Key Files
| File | Description |
|------|-------------|
| `ci.yml` | Rust CI pipeline. Single `rust` job on `ubuntu-latest` using `dtolnay/rust-toolchain@stable`. Steps in order: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, `cargo package --allow-dirty --no-verify`. Triggers: `pull_request` and `push` to `main` or `master`. |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- The CI pipeline is intentionally minimal (lint → typecheck-by-build → test →
  package). Avoid adding optional Meili/MinerU jobs here unless the workflow
  also provisions those services in the same job — the Rust suite skips Meili
  and MinerU tests when the env vars are unset, so they're a no-op in CI as it
  stands.
- Keep `cargo clippy --all-targets -- -D warnings` non-negotiable — `clippy`
  failures are treated as build failures by the project.
- Don't bypass hooks or signing with `--no-verify` / `--no-gpg-sign` outside
  the existing `cargo package --allow-dirty --no-verify` line, which is
  specifically packaging the crate for publish dry-run.
- Pin to `dtolnay/rust-toolchain@stable` rather than nightly. Edition is 2021
  per `Cargo.toml`.

### Testing Requirements
- A push to a feature branch with the workflow modified will execute the full
  pipeline. The package step prevents accidental publish-breaking changes from
  reaching `main`.

### Common Patterns
- One job, ordered steps, fail-fast. No matrix builds. No caching layer yet —
  add `Swatinem/rust-cache@v2` if CI time becomes a constraint.

## Dependencies

### External
- `actions/checkout@v4`
- `dtolnay/rust-toolchain@stable`
- Ubuntu runner toolchain (cargo, rustc).

<!-- MANUAL: -->
