<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-07-15 -->

# workflows

## Purpose
GitHub Actions workflow definitions. The Rust CI workflow runs independent
quality, Meilisearch integration, MSRV, advisory, and dependency-policy jobs on
every pull request and on every push to `main` or `master`, plus a weekly
scheduled security refresh.

## Key Files
| File | Description |
|------|-------------|
| `ci.yml` | Rust CI pipeline. The stable `quality` job runs the root `AGENTS.md` gauntlet: locked fmt, clippy, all-target check, route-manifest, full-test, and package gates. The `meili` job starts a `getmeili/meilisearch` service container (tag pinned to the production version, throwaway master key), waits for `/health`, and runs `cargo test --locked --test meili_integration --test insight_history` with `RAG_TEST_MEILI_URL`, `RAG_TEST_MEILI_API_KEY`, and `RAG_TEST_REQUIRE_MEILI=1`, so an unavailable server fails instead of skipping. Separate jobs check Rust 1.88, RustSec advisories, and `cargo-deny`. Rust jobs cache with `Swatinem/rust-cache@v2`. Triggers: `pull_request`, `push` to `main` or `master`, and a weekly schedule. |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Only add live-service jobs that provision the service in the same job. The
  Rust suite skips Meili/MinerU integration tests when their gate variables are
  unset, so a job without the service would pass vacuously. Any job that
  provisions Meilisearch must set `RAG_TEST_REQUIRE_MEILI=1`.
- When production Meilisearch is upgraded, bump the `getmeili/meilisearch`
  image tag in the `meili` job in the same change (see
  `doc/runbooks/gfit-deploy.md`).
- The `meili` job does not set `RAG_TEST_MEILI_MIGRATION_ISOLATED`; the
  destructive tenant-scope migration test stays opt-in for a dedicated empty
  server.
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
- Independent jobs expose stable quality, Meilisearch integration, MSRV,
  advisory, and policy failures without a matrix. The Rust jobs use
  `Swatinem/rust-cache@v2` (keyed per job and toolchain).

## Dependencies

### External
- `actions/checkout@v7`
- `dtolnay/rust-toolchain@stable` and `dtolnay/rust-toolchain@1.88.0`
- `Swatinem/rust-cache@v2`
- `getmeili/meilisearch` Docker image (service container)
- `rustsec/audit-check@v2.0.0`
- `EmbarkStudios/cargo-deny-action@v2`
- Ubuntu runner toolchain and `libopenblas-dev`.

<!-- MANUAL: -->
