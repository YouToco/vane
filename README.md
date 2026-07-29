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
  `go mod download`, `go vet ./...`, and the complete uncached shuffled race
  test suite against PostgreSQL 18. Frontend Gate is `npm ci`, tests, typecheck,
  and build. After each Gate, the source is materialized again at the exact SHA
  with a short-lived read key; release builds start from that new clean tree.
- `deploy` runs on `vane-deploy`. It downloads only artifacts named with the
  current run ID, attempt, component, and source SHA. It never downloads source
  trees and runs no source build, npm install, or Go build/test command.

Each component artifact is a tarball plus SHA256 sidecar and JSON manifest. The
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
Gates, artifacts, and deployment all share the new run attempt. GitHub
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
- deploy VM: `[self-hosted, Linux, vane-deploy]`; both X64 and ARM64 Linux hosts
  are supported. Install Git, Python 3, OpenSSH, OpenSSL, `flock`, GNU `date`,
  `curl`, `sha256sum`, and `strings`. Do not install Docker access or `sudo` for
  the runner user. The production VPS fallback uses the dedicated
  `vane-deploy-runner` system user with systemd hardening, no supplementary
  groups, and durable state under its private home.

The optional Windows WSL2 build runner uses the same `vane-build` trust role
without production secrets. Its systemd service must use a dedicated `HOME`
under the runner directory, a Linux-only `PATH`, and make Windows mounts such
as `/mnt/c` and `/mnt/d` inaccessible. Install `build-essential` so Go race
tests retain CGO support. This keeps trusted exact-`main` builds from inheriting
workstation credentials while providing an X64 fallback when the ARM64 build
runner is unavailable.

Wrangler and its Node runtime are provisioned out-of-band on the deploy VM.
[`tools/wrangler/package-lock.json`](tools/wrangler/package-lock.json) pins the
complete dependency tree with registry integrity hashes. From an audited
checkout, an administrator runs `scripts/provision-wrangler.sh` in a root shell.
The script downloads the fixed official Node `v22.23.1` Linux ARM64 archive,
checks SHA256
`0294e8b915ab75f92c7513d2fcb830ae06e10684e6c603e99a87dbf8835389c1`,
and uses that exact Node/npm with the committed lock. npm runs as
`vane-deploy-runner` with dependency lifecycle scripts disabled. After Wrangler
version and Pages-command verification, Node and the `4.115.0` dependency tree
become root-owned/read-only. `/opt/vane-deploy-tools/wrangler` is a root-owned
wrapper that explicitly executes the pinned Node and Wrangler entrypoint; it
does not depend on `PATH`. Workflows verify the exact Wrangler version and never
run npm on the deployment VM.

Aliyun CLI is installed per run below `RUNNER_TEMP` from the exact `3.4.10`
ARM64 release archive after checking the official SHA256
`349f3d31af9cc85aa2b444899e7d805f6409f5a53d667ce74d00dafbc17f9ae5`.
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
