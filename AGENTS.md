# Vane monorepo collaboration contract

This repository is the only active Vane source repository. Product code and
production control-plane code share a revision, but they do not share trust.

## Directory authorities

- `server/`: the only application Go module and the only home for shipped Go binaries.
- `web/`: the only browser application and Node lockfile.
- `contracts/`: machine-readable HTTP, Temporal, and release contracts.
- `infra/`: declarative desired state only; no deployment state machine or credentials.
- `tests/`: cross-component black-box tests only.
- `tools/`: development/build checks and release scripts with no embedded credentials.
- `docs/`: human explanation; it is never a second executable authority.

Do not create root `scripts/`, `deploy/`, `common/`, `utils/`, `pkg/`, or
speculative `packages/` directories. New modules, lockfiles, executable entry
points, and release binaries must be added to the machine contracts and policy
checks in the same change.

## Commands and release boundary

Use the root Makefile for checks. Production deployment is the GitHub Actions
`Deploy` workflow (`.github/workflows/deploy.yml`): it builds both components
from an exact revision with the pinned toolchain, ships the payload to the VPS
over SSH using a repository-secret deploy key, and runs
`tools/release/remote-atomic-release.sh` (CAS check, online migrate, atomic
symlink switch, service restart, `/readyz` plus live-binary verification,
automatic rollback). It then publishes the verified web `dist/` to Cloudflare
Pages and to OSS plus Ali CDN. Web files never pass through or reside on the
VPS.

Feature code and tests have no production credentials; they exist only as
GitHub Actions repository secrets. Every `uses:` action reference must be a
pinned 40-character SHA with a release-tag comment. The workflow is the only
path that mutates production, and the root-owned VPS remains the boundary
that must withstand a compromised runner.

## Risk gates

- B: relevant tests and one self-review.
- A: targeted race/integration or replay tests and one independent validator.
- S: full race/real dependency gates, a direct invariant proof, and two independent validators.

Unknown impact is full impact. Skipped/disabled tests require an allowlist entry
with owner, reason, and expiry. The default allowlist is empty.

## Durable compatibility

Directory and Go module moves do not justify Temporal `GetVersion`. Workflow,
activity, task-queue, converter identities, command history, HTTP contracts,
and migration bytes must remain stable and be proven by contracts/replay.
Migrations are forward-only; never claim a binary rollback is safe unless the
previous binary was proven compatible with the current schema.
