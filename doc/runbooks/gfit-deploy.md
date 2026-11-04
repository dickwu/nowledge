# gfit deploy, verification, and rollback runbook

Routine deploy procedure for the production-style Nowledge service on the
`gfit` host. Use it for ordinary code updates. When a release also carries a
fixed-index or tenant-scope migration, follow the
[production upgrade runbook](production-upgrade.md) instead; its backup,
migration, and rollback gates take precedence over this document.

The local, gitignored `scripts/deploy_gfit.sh` predates this layout (it ships a
tarball to `~/nowledge` and runs the binary in tmux). It does not match
production; do not use it until it is brought in line with this runbook.

## Production layout

| Item | Value |
|------|-------|
| Host | `gfit` |
| Source checkout | `/server/nowledge` (a Git checkout of `dickwu/nowledge`) |
| Binary | `/server/nowledge/target/release/nowledge`, built on the host |
| Service unit | `nowledge.service` (systemd) |
| Configuration | `/server/nowledge/.env` (untracked; never commit or print it) |
| Listen address | `127.0.0.1:14242` |
| Run mode | `RAG_RUN_MODE=production`, `RAG_STORE_BACKEND=meili`, `RAG_WRITE_CONSISTENCY=wait_for_index` |
| Meilisearch unit | `nowledge-meilisearch.service` (systemd), version 1.56.1 |
| Meilisearch address | `127.0.0.1:7700` |
| Meilisearch binaries | `/server/meilisearch/bin` |
| Meilisearch data | `/server/meilisearch/data` |
| Old releases | `/server/releases` (currently only stale July builds) |

`.env` is ignored by Git and the build script ignores untracked files, so a
checkout holding `.env` still builds as a clean revision.

## 1. Preflight

Run on `gfit` from `/server/nowledge`:

```sh
cd /server/nowledge
git status --short --branch      # must print no modified tracked files
git rev-parse HEAD               # record the currently deployed commit
git log -1 --oneline
cargo --version && rustc --version   # must be >= 1.88 (crate MSRV)
systemctl status nowledge nowledge-meilisearch --no-pager
curl -fsS http://127.0.0.1:14242/livez   # git_rev should equal the HEAD above
```

Stop if the checkout has modified tracked files: find out who changed what on
the server before deploying over it. Also confirm the target commit's GitHub CI
(quality, Meilisearch integration, MSRV, RustSec, dependency policy) is green.

`RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS` only opts into periodic background LLM
health probing: it must be unset, `0`, or at least `300`. An environment that
still sets the former default `30` fails startup validation, so check the
unit's environment before restarting.

## 2. Keep the current binary for rollback

Copy the running binary before building, because `cargo build` overwrites
`target/release/nowledge` in place:

```sh
PREV_REV=$(git rev-parse --short=12 HEAD)
install -d -m 0700 /server/releases
cp -p target/release/nowledge "/server/releases/nowledge-$PREV_REV"
sha256sum "/server/releases/nowledge-$PREV_REV"
```

Prune `/server/releases` deliberately (keep at least the last two known-good
builds); do not delete the only copy of the binary that is currently running.

## 3. Update the source and build

```sh
git fetch --prune origin
git pull --ff-only origin main      # or: git checkout --detach <tag-or-sha>
NOWLEDGE_GIT_REVISION=$(git rev-parse HEAD) cargo build --release --locked
```

`--ff-only` refuses to merge or rewrite local history on the server. Setting
`NOWLEDGE_GIT_REVISION` makes `build.rs` assert that the build matches a clean
`HEAD`; it fails the build instead of attesting modified source. Building does
not affect the running process; the restart below switches to the new binary.

## 4. Restart and verify

```sh
sudo systemctl restart nowledge
systemctl status nowledge --no-pager
curl -fsS http://127.0.0.1:14242/livez     # status ok, git_rev = new HEAD
curl -fsS http://127.0.0.1:14242/readyz    # 200 once tenant hydration finishes
journalctl -u nowledge --since "10 min ago" --no-pager
```

`/readyz` stays 503 until every mandatory tenant domain has hydrated from
Meilisearch and, with `RAG_HEALTH_REQUIRE_LLM=true`, until the one-time startup
LLM health probe has completed (`dependencies.llm` reads `not_probed` until
then); poll it for a short while instead of treating the first 503 as a
failure. Readiness keeps that LLM verdict afterwards (`llm_checked_at` shows
when it was observed); only `/healthz` re-probes unless the interval above opts
into periodic probing. Then check the admin-only detailed health without
putting the token on the command line or in shell history, for example with a
root-only header file:

```sh
curl -fsS -H @/path/to/root-only-admin-auth-header http://127.0.0.1:14242/healthz
```

Confirm `store_backend` is `meili`, Meilisearch is healthy, and the parser and
LLM entries match expectations. In the journal, look for startup validation
errors, Meilisearch identity/generation drift, and hydration failures.

## 5. Rollback

Use rollback when the new build fails verification. It only restores the
binary; it does not undo data written by the new revision. If the release ran
a migration, use the rollback section of the
[production upgrade runbook](production-upgrade.md) instead.

```sh
sudo systemctl stop nowledge
cp -p "/server/releases/nowledge-$PREV_REV" target/release/nowledge
git checkout --detach "$PREV_REV"   # keep source and binary on the same revision
sudo systemctl start nowledge
curl -fsS http://127.0.0.1:14242/livez     # git_rev = PREV_REV
curl -fsS http://127.0.0.1:14242/readyz
journalctl -u nowledge --since "5 min ago" --no-pager
```

Afterwards, return the checkout to a branch (`git switch main`) before the next
deploy, and record what failed.

## 6. Meilisearch backup and upgrade

Nowledge and Meilisearch are separate units; a routine Nowledge deploy does not
restart Meilisearch. For a Meilisearch version upgrade (the most recent was
1.43.1 to 1.56.1), follow the same pattern:

1. Read the upstream release notes and upgrade guide for every version being
   crossed, and keep CI's `getmeili/meilisearch` image tag in
   `.github/workflows/ci.yml` equal to the production version.
2. Stop Nowledge (`systemctl stop nowledge`) so no writes arrive during the
   backup.
3. Create a dump with the master key (`POST /dumps`) and wait until its task
   succeeds (`GET /tasks/<uid>`). Copy the dump file off the host.
4. Stop `nowledge-meilisearch`, then copy the data directory with permissions
   intact, e.g. `cp -a /server/meilisearch/data /server/meilisearch/data.bak-<old-version>-<date>`.
5. Keep the old Meilisearch binary in `/server/meilisearch/bin` alongside the
   new one; switch the unit to the new binary, start it, and confirm
   `GET /version` and `GET /health`.
6. Start Nowledge and run the verification in section 4.

To roll back Meilisearch, stop both units, restore the copied data directory
(or import the dump) with the old binary, and start them again.

Open item: verify whether Meilisearch scheduled snapshots are configured
(`--schedule-snapshot` / `MEILI_SCHEDULE_SNAPSHOT` in the
`nowledge-meilisearch` unit or its environment) and that recent snapshot files
actually exist. Until confirmed, the manual dump plus data-directory copy are
the only known backups.

## Example `nowledge.service`

Example only. Reconcile it with the real unit (`systemctl cat nowledge`) before
changing anything on the server; the production unit is authoritative.

```ini
[Unit]
Description=Nowledge RAG service
After=network-online.target nowledge-meilisearch.service
Wants=network-online.target
Requires=nowledge-meilisearch.service

[Service]
Type=simple
# User=<service account used by the real unit>
WorkingDirectory=/server/nowledge
EnvironmentFile=/server/nowledge/.env
ExecStart=/server/nowledge/target/release/nowledge
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
# Longer than RAG_SHUTDOWN_TIMEOUT_MS (30 s default) so graceful shutdown can
# mark unfinished ingest tasks before systemd escalates to SIGKILL.
TimeoutStopSec=45
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

systemd's `EnvironmentFile=` parser is not a shell: it does not expand
variables or command substitutions. Keep `.env` to plain `KEY=value` lines so
`set -a; . ./.env` and systemd read the same values.
