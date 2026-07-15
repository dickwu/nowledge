<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-05-20 | Updated: 2026-07-15 -->

# .github

## Purpose
GitHub-specific configuration for CI and automated dependency updates. No issue
templates or code owners are checked in.

## Key Files
| File | Description |
|------|-------------|
| `dependabot.yml` | Weekly grouped Cargo and GitHub Actions dependency updates. |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `workflows/` | GitHub Actions workflow definitions (see `workflows/AGENTS.md`). |

## For AI Agents

### Working In This Directory
- Anything that lives under `.github/` is consumed by GitHub directly. Keep
  build artifacts and local runtime state out of this directory.
- If issue/PR templates are added later, place them at `.github/ISSUE_TEMPLATE/`
  and `.github/PULL_REQUEST_TEMPLATE.md` per GitHub conventions and update this
  document.
- For deployment automation, the project's standing preference is Cloudflare
  Workers Builds over GitHub Actions when a Worker is involved (per project
  memory) — Workers Builds avoids needing a `CLOUDFLARE_API_TOKEN` secret. The
  Rust API server is not a Worker, so GitHub Actions is appropriate for the
  current `ci.yml`.

### Testing Requirements
- Parse workflow and Dependabot YAML locally, run the commands represented by
  the workflow, and confirm third-party action inputs against their upstream
  action definitions. GitHub-hosted execution remains the final integration
  check.

### Common Patterns
- Separate stable quality, MSRV, RustSec, and dependency-policy jobs so each
  failing contract is visible independently.

## Dependencies

### External
- GitHub Actions runners (Ubuntu).
- GitHub Dependabot.
- Official/upstream Rust and dependency-policy actions listed in
  `workflows/AGENTS.md`.

<!-- MANUAL: -->
