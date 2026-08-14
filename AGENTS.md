# Vane monorepo collaboration contract

This repository is the only active Vane source repository. Product code and
production control-plane code share a revision, but they do not share trust.

## Directory authorities

- `server/`: the only application Go module and the only home for shipped Go binaries.
- `web/`: the only browser application and Node lockfile.
- `contracts/`: machine-readable HTTP, Temporal, and release contracts.
- `infra/`: declarative desired state only; no deployment state machine or credentials.
- `ops/`: release, recovery, certificate, and audit state machines.
- `tests/`: cross-component black-box tests only.
- `tools/`: development/build checks with no production permission.
- `docs/`: human explanation; it is never a second executable authority.

Do not create root `scripts/`, `deploy/`, `common/`, `utils/`, `pkg/`, or
speculative `packages/` directories. New modules, lockfiles, executable entry
points, and release binaries must be added to the machine contracts and policy
checks in the same change.

## Commands and release boundary

Use the root Makefile for checks and `./ops/bin/vane` for operations. GitHub
Actions are intentionally absent. A production release must name an exact
40-character `origin/main` SHA and pass the signed manifest chain:

`plan -> gate -> artifact -> deploy -> verify -> finalize`

Feature code and tests have no production credentials. Build and Gate run
directly on the release Mac without a VM or build container. Server mutation,
the global lock, CAS state, and durable evidence belong to the root-owned VPS
broker installed outside this checkout. Web publication is the one exception:
the local release command uses local Aliyun credentials to upload the already
verified `dist/` directly to OSS and refresh CDN entry paths; Web files never
pass through or reside on the VPS.

Controller changes are tested in their introducing release but cannot activate
themselves. The installed controller revision may advance only after the
current release is finalized, for use by a later release.

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
