<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-07-15 -->

# workflows

## Purpose
GitHub Actions workflow definitions. The Rust CI workflow runs independent
quality, MSRV, advisory, and dependency-policy jobs on every pull request and
on every push to `main` or `master`.

## Key Files
| File | Description |
|------|-------------|
| `ci.yml` | Rust CI pipeline. The stable job runs locked fmt, clippy, all-target check, route-manifest, full-test, and package gates; separate jobs check Rust 1.88, RustSec advisories, and `cargo-deny`. Triggers: `pull_request` and `push` to `main` or `master`. |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Avoid adding optional Meili/MinerU jobs unless the workflow also provisions
  those services in the same job. The Rust suite skips those integration tests
  when their gate variables are unset.
- Keep `cargo clippy --locked --all-targets -- -D warnings` non-negotiable — `clippy`
  failures are treated as build failures by the project.
- Keep the package dry-run locked and fully verified; do not add
  `--allow-dirty` or `--no-verify` in CI.
- Keep both stable and the declared Rust 1.88 MSRV. Edition is 2021 per
  `Cargo.toml`.

### Testing Requirements
- Parse the YAML locally and run the represented cargo commands. A GitHub push
  is still required to validate runner permissions and action integration.

### Common Patterns
- Independent jobs expose stable quality, MSRV, advisory, and policy failures
  without a matrix. No caching layer is currently configured.

## Dependencies

### External
- `actions/checkout@v7`
- `dtolnay/rust-toolchain@stable` and `dtolnay/rust-toolchain@1.88.0`
- `rustsec/audit-check@v2.0.0`
- `EmbarkStudios/cargo-deny-action@v2`
- Ubuntu runner toolchain and `libopenblas-dev`.

<!-- MANUAL: -->
