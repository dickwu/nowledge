<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-05-20 -->

# .github

## Purpose
GitHub-specific configuration. Currently scoped to CI workflows; no issue
templates, code owners, or Dependabot config are checked in.

## Key Files
None at this level — see the `workflows/` subdirectory.

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `workflows/` | GitHub Actions workflow definitions (see `workflows/AGENTS.md`). |

## For AI Agents

### Working In This Directory
- Anything that lives under `.github/` is consumed by GitHub directly. Avoid
  putting build-system artifacts or environment-specific files here — they
  belong under `scripts/` or `.omc/`.
- If issue/PR templates are added later, place them at `.github/ISSUE_TEMPLATE/`
  and `.github/PULL_REQUEST_TEMPLATE.md` per GitHub conventions and update this
  document.
- For deployment automation, the project's standing preference is Cloudflare
  Workers Builds over GitHub Actions when a Worker is involved (per project
  memory) — Workers Builds avoids needing a `CLOUDFLARE_API_TOKEN` secret. The
  Rust API server is not a Worker, so GitHub Actions is appropriate for the
  current `ci.yml`.

### Testing Requirements
- Workflow changes can be smoke-tested by pushing to a branch and watching the
  Actions tab. No local runner is wired up.

### Common Patterns
- Single-job, lint+test+package pipeline per `workflows/ci.yml`. Keep new
  workflows similarly minimal unless there is a concrete need.

## Dependencies

### External
- GitHub Actions runners (Ubuntu).
- `dtolnay/rust-toolchain@stable` — toolchain pin for Rust jobs.

<!-- MANUAL: -->
