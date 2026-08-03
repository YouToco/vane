# Vane deployment control plane

This private repository is the only GitHub repository allowed to schedule the
production deployment runner. The `vane` and `vane-web` source repositories
contain no production workflow, runner label, or production credential.

## Three trust domains

1. `vane-test` runs ordinary source-repository pull-request and main CI. It has
   no production access and its result is not trusted as a deployment gate.
2. `vane-build` independently checks and builds the exact 40-character source
   SHAs selected by this repository. It has two read-only source deploy keys,
   but no production secrets.
3. `vane-deploy` owns durable deployed-SHA and certificate state and is the only
   runner that receives production credentials. Its dedicated
   `vane-deploy-runner` Unix user has no local `sudo` or Docker-group access.

`deploy.yml` is a strict `plan → build → deploy` DAG:

- `plan` runs on `vane-deploy`. It uses native `git ls-remote` with the two
  read-only deploy keys to resolve each source `main`, then compares those exact
  SHAs with the VM's durable state. It neither checks out nor executes source.
- `build` runs on `vane-build` only when a component changed. Backend Gate is
  `go mod download`, `go vet ./...`, and the complete uncached race test suite
  against PostgreSQL 18. It deliberately matches source CI test ordering:
  Store integration tests share one database, so random start-order changes
  produce timing-only migration-lock and refill-clock failures rather than an
  independent isolation signal. The Go package timeout is 25 minutes because
  the migration-heavy store package can exceed 15 minutes under race,
  coverage, and a forced uncached run. Frontend Gate is `npm ci`, tests,
  typecheck, and build. After each Gate, the source is materialized again at
  the exact SHA with a short-lived read key; release builds start from that new
  clean tree. PostgreSQL uses a GitHub-assigned ephemeral host port so stale or
  concurrent runner workloads cannot collide on a fixed host port before the
  Gate starts.
- `deploy` runs on `vane-deploy`. It restores only release handoffs cached under
  an exact key containing the current run ID, component, and source SHA. No
  prefix fallback is allowed and a miss fails closed. Omitting the attempt is
  intentional: a failed deploy job can reuse the already gated immutable
  handoff when only failed jobs are retried. This keeps the
  build/deploy trust boundary while avoiding a production dependency on the
  account's separately billed GitHub Artifact quota. GitHub documents exact
  run-ID caches and cross-job save/restore as supported short-lived
  handoff patterns. The deploy job never restores source trees and runs no
  source build, npm install, or Go build/test command.

Each component release handoff is a tarball plus SHA256 sidecar and JSON
manifest. The
manifest binds source SHA, archive SHA256/size, and the complete file allowlist
with per-file SHA256, size, and mode. Before any production secret is exposed,
the deploy job rejects extra inputs, duplicate JSON keys, traversal, symlinks,
hardlinks, devices, FIFOs, non-regular files, unexpected paths/modes, oversized
content, checksum mismatches, and dirty/mismatched Go VCS build metadata. Files
are copied out member-by-member; `extractall` is not used.

Backend, Aliyun frontend, and Cloudflare frontend steps receive separate secret
sets. The no-secret frontend finalizer requires exact-SHA success receipts from
both channel steps. Backend state advances atomically only after upload,
graceful drain proof, restart, readiness/well-known checks, and the 24-hour
production Gate. Frontend state advances only in that finalizer after both OSS +
Aliyun CDN and Cloudflare Pages succeed. If backend succeeds and frontend fails,
backend state remains truthfully advanced; the next poll rebuilds and retries
only the frontend.

The primary server runtime currently has an explicit temporary release fence.
Until the complete Store recovery/reconciliation and tenant-RLS graph passes a
real PostgreSQL gate, deployment preserves the proven `User=root` plus
`EnvironmentFile=/opt/vane/.env` contract only as a rollback-compatible input.
The new explicit control-repository unit,
`deploy/vane-legacy-compat.service`, runs as `User=vane`/`Group=vane` so its
Unix-socket peer UID continues to match the research gateway contract. Its
dedicated `/opt/vane/env/server-owner-compat.env` is root:vane mode 0640 and
contains the owner-compatible primary DSN. An active legacy release remains
available through split bootstrap and is drained only after preflight. A known
inactive or failed split b9 release is snapshotted together with both its
restricted `server.env` and the separately trusted owner `.env`, then converged
to the audited legacy contract. Any failed recutover restores the complete
previous binary, unit, and environment and deliberately leaves that previously
inactive release disabled. Missing, changed, or non-owner `.env` data fails
closed.
The cutover environment keeps the snapshotted, root-owned and non-group/other-
writable owner `VANE_DB_URL` unchanged and
imports only an exact allowlist of research-runtime, capability-key, gateway-
socket, and V3/push-recovery canary settings from the gated restricted
`server.env`. Migration and gateway DSNs, `POSTGRES_PASSWORD`, and gateway
process credentials are stripped; values are compared without being logged.
The restricted `server.env` itself is normalized from the legacy vane:vane
mode 0600 state to root:vane mode 0640 before success and is verified
non-writable by `vane`, so the primary process cannot alter the allowlist used
by a later deployment. Rollback restores the exact earlier ownership/content
contract when recutover fails.
The gateway continues to allow only the real `vane` UID via `SO_PEERCRED`, while
its database and LLM credential files remain gateway-user mode 0400 and are
explicitly verified unreadable by the primary `vane` process.
The reviewed split primary unit is installed only as
`/opt/vane/vane.service.deferred` and is never made
live by this release path. This does not undo the independently isolated
one-shot `vane-migrate` process or the split research gateway service and
credentials. Removing the fence requires the Store RLS graph gate, not merely
a new server binary.

The source server must also print the exact side-effect-free release receipt
`vane.server-release-contract/v1 primary_store=owner_compat_v1
research_store=restricted_v1` for `-print-release-contract`. The no-production-
secret build step executes that probe and binds the exact result into the
strict backend manifest; artifact validation rejects a missing or altered
receipt. The remote deploy repeats the probe with an empty environment before
bootstrap. This binds source intent, the transferred binary, and the audited
live unit without granting the build process any production credentials.

The backend artifact also carries the owner-only rollout utility
`vane-research-prepare`. Deployment verifies its exact source revision and
installs it beside the other operational binaries, but never starts it or adds
it to the long-lived server unit. An operator must run it explicitly as the
dedicated `vane-migrate` system user with `CREDENTIALS_DIRECTORY` pointing at
the split, mode-0400 migration-owner credential. The owner database URL is
therefore never copied into the server environment, and merely deploying a new
release cannot prepare or roll back a V3 task.

Native V3 task-definition edit recovery uses a fourth, independent database
login. After `vane-migrate` has created the restricted NOLOGIN role, deployment
runs the resumable `native_v3_edit_recovery_runtime_v1` credential upgrade. A
root-only mode-0600 pending password is created before `ALTER ROLE`; the same
password is reused if either the database response or the later atomic file
rename is lost. The resulting PostgreSQL URL lives only in
`/etc/vane/credentials/native_v3_edit_recovery_db_url` as `vane:vane` mode 0400.
It is never placed in an environment variable, process argument, or log. The
Both the incoming split `vane.service` and the control repository's live
owner-compatible unit load that exact file through systemd `LoadCredential`;
either missing consumer fails before the old worker is drained. Direct Gate
probes run with an empty environment containing only `PATH` and the non-secret
`CREDENTIALS_DIRECTORY=/etc/vane/credentials` locator.
Both a host with the existing `runtime_bootstrap_v1.complete` marker and a
first-time split-runtime bootstrap follow this same versioned upgrade path.

The Aliyun line first requires a closed Vite manifest dependency graph and
content-addressed names for every runtime JavaScript/CSS object referenced by
HTML or the Vite manifest. It then publishes every non-HTML object without
deleting older objects and requires OSS `stat` to report the exact local
`Content-Length` for every critical referenced asset. Secondary HTML and
owner-preview metadata follow; the canonical `index.html` object is overwritten
only after those checks pass and its own OSS `Content-Length` is checked after
the write. CDN invalidation uses URL-encoded `File` refreshes for HTML entries
and stable non-hashed public objects such as web manifests and root icons,
instead of a directory-wide refresh. These invariants keep the previous entry's
content-hashed assets available during cutover and make an exact-SHA retry
idempotent.

The fixed owner-preview path remains a documented residual risk in this
mitigation. When a build includes it, deployment publishes it before the root
entry and verifies `Cache-Control: no-store`. When a build omits it, deployment
does not delete or overwrite an older fixed-path preview, so stale preview
content may remain reachable. This risk is **not closed** here; closing it
requires the follow-up `releases/<sha>` preview migration plus explicit
version-aware garbage collection.

Control-plane CI runs on every `main` push and every five minutes. The
production workflow listens only to a completed Control-plane CI run and
requires a successful same-repository `push` or `schedule` run on `main` whose
head SHA is still the current default-branch SHA. PR, feature-branch, failed,
stale-SHA, and cross-repository completions cannot enter `plan`. There is no
`workflow_dispatch` or `repository_dispatch` production entry. The certificate
workflow remains schedule-only. A failed production run is retried with
GitHub's run-rerun operation using **Re-run all jobs** so `plan`, the exact-SHA
Gates, release handoffs, and deployment remain bound to one workflow run.
GitHub
Environment approval is intentionally not used as a security boundary on this
Free private repository.

## Runner provisioning

All runner registrations are repository-scoped and implement three roles:

- control-plane pull-request CI: `[self-hosted, Linux, ARM64, vane-test]`; this
  runner lives in `vane-test`, so PR-controlled checks never enter a trusted VM.
- control-plane exact-`main` push and schedule CI:
  `[self-hosted, Linux, vane-build]`; only trusted default-branch workflow code
  reaches this runner, avoiding a `vane-test` outage becoming a
  production-control-plane single point of failure. The label deliberately
  permits both X64 and ARM64 Linux hosts.
- build VM: `[self-hosted, Linux, vane-build]`; Docker is available for the
  PostgreSQL service, and the pinned setup actions provide Go 1.26 and Node 22.
- primary deploy VM: `[self-hosted, Linux, vps-primary, vane-deploy]`. `plan`,
  `deploy`, and certificate renewal are deliberately pinned to this one
  durable-state owner; a broad-label fallback must not split a single DAG or
  create a second certificate/deployed-SHA authority. Install Git, Python 3,
  OpenSSH, OpenSSL, `flock`, GNU `date`, `curl`, `sha256sum`, and `strings`.
  Do not install Docker access or `sudo` for the runner user. The primary is
  the isolated VPS runner so a Mac login, sleep, or Colima outage cannot block
  production. A standby runner requires an explicit state migration and unique
  label handoff before it can become primary; the broad `vane-deploy` label
  alone never authorizes ownership.

The 2026-07-30 handoff first cancelled the queued Mac-owned DAG, proved the
live backend revision from its embedded Go build information, proved the
frontend revision from the last successful dual-line deployment plus a
same-main plan, and only then migrated both durable SHA files to the VPS
runner. The `vps-primary` label was assigned after that state write. This is
the required failover order: quiesce old DAG, prove live state, atomically
migrate state, move the unique label, then trigger a fresh plan.

The optional Windows WSL2 build runner uses the same `vane-build` trust role
without production secrets. Its systemd service must use a dedicated `HOME`
under the runner directory, a Linux-only `PATH`, and make Windows mounts such
as `/mnt/c` and `/mnt/d` inaccessible. Install `build-essential` so Go race
tests retain CGO support. This keeps trusted exact-`main` builds from inheriting
workstation credentials while providing an X64 fallback when the ARM64 build
runner is unavailable.

Wrangler is materialized per frontend deployment below that run's private
`RUNNER_TEMP` directory. `actions/setup-node` pins Node `v22.23.1`, while
[`tools/wrangler/package-lock.json`](tools/wrangler/package-lock.json) pins the
complete Wrangler `4.115.0` dependency tree with registry integrity hashes.
Its Miniflare dependency is resolved to the patched, same-major Undici
`7.29.0` through an exact npm override because Wrangler still pins the
vulnerable `7.28.0` release.
Lifecycle scripts are disabled; the exact version and Pages command are checked
before provider credentials enter any step. The deploy VM therefore needs no
out-of-band Node/Wrangler installation and a replacement runner receives the
same verified tool environment automatically.

Aliyun CLI and ossutil are installed per run below `RUNNER_TEMP` from exact
official release archives. The installer accepts only Linux x86_64/amd64 and
ARM64/aarch64, maps each to a separately pinned SHA256, and fails closed for
every other architecture. This lets the isolated x86_64 primary deploy runner
and ARM64 standby use the same audited workflow without executing a
wrong-architecture binary.
No `latest` lookup, `curl | sh`, container action, or local `sudo` is used.

The VPS SSH principal is separate from the runner Unix user and must retain the
existing remote permissions for `/opt/vane`, systemd, Docker, and journal
inspection. Native OpenSSH accepts the VPS only when the runtime Ed25519 host
key matches the repository's trusted SHA256 fingerprint.

## Required repository secrets

| Secret | Used by | Purpose |
| --- | --- | --- |
| `VANE_READ_KEY` | plan/build checkout steps | Read-only deploy key for `YouToco/vane` |
| `VANE_WEB_READ_KEY` | plan/build checkout steps | Read-only deploy key for `YouToco/vane-web` |
| `VPS_HOST`, `VPS_PORT`, `VPS_USER` | backend deploy step | Production VPS endpoint |
| `VPS_SSH_KEY` | backend deploy step | Production VPS private key |
| `VPS_SSH_HOST_ED25519_FINGERPRINT` | backend deploy step | Out-of-band trusted `SHA256:...` |
| `ALIYUN_ACCESS_KEY_ID`, `ALIYUN_ACCESS_KEY_SECRET` | frontend/certificate steps | OSS, CDN, and AliDNS |
| `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID` | frontend deploy step | Cloudflare Pages |
| `ACME_ACCOUNT_EMAIL` | certificate workflow | Let's Encrypt account email |

The source keys must be distinct and have **Allow write access** disabled. Key
files are created and deleted within the checkout/lookup step; source-controlled
commands run only in later steps after the files and secret environments are
gone.

## Durable state, locking, and recovery

The default persistent state directory is
`~/.local/state/vane-deploy` (or `$XDG_STATE_HOME/vane-deploy`). It contains:

- `deployed-vane.sha` and `deployed-vane-web.sha`;
- `releases/<frontend-source-sha>/frontend-aliyun.json`, a deterministic
  successful-publication receipt containing the source SHA, complete file
  SHA256/size list, and canonical entry SHA256;
- the VM-wide `control-plane.lock`, shared by backend, frontend, and certificate
  production mutations;
- dedicated acme.sh account, key, certificate, issuance-attempt, and verified
  fingerprint state.

SHA writes use `mktemp` + `mv` on the same filesystem. If a remote deployment
succeeds but the runner dies before the state write, the next run safely repeats
that component. To intentionally redeploy, remove only the exact component SHA
file while no workflow is running.

`cert-renew.yml` is independent but shares workflow concurrency and the VM lock.
acme.sh is fetched only at commit
`3661fd86b6304115e42f43910e6dd452ab9866d6`, installed with `--no-cron
--no-profile`, and confined to its dedicated state directory. A valid local
certificate with at least 60 days remaining is reused for upload/verification,
so retries after CDN failure do not issue again. Issuance need is persisted
before contacting Let's Encrypt and throttled to one attempt per 24 hours.
Success requires the CDN edge leaf's SHA256 fingerprint to exactly match the
newly uploaded local leaf and both to have at least 60 days remaining.

On the first successful control-plane run, absent SHA state intentionally causes
both components to pass their independent Gates and deploy once.
