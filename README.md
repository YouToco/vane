# Vane deployment control plane

This private repository is the only GitHub repository allowed to schedule the
production deployment runner. The `vane` and `vane-web` source repositories run
tests on their own runner, but contain no production workflow, runner label, or
production credential.

`deploy.yml` polls both source repositories' `main` branches every five minutes
(and supports manual dispatch). It checks out each repository with a separate,
read-only deploy key, compares the actual checked-out commit with persistent
runner-VM state, and skips an unchanged component. A state file advances only
after that component's complete deployment succeeds:

- backend: four Linux binaries, infra sync, graceful drain proof, restart,
  readiness/well-known checks, and the 24-hour production Gate;
- frontend: one production build deployed to OSS + Aliyun CDN and Cloudflare
  Pages.

`cert-renew.yml` retains the monthly Let's Encrypt DNS-01 renewal and Aliyun CDN
upload. Both workflows use the same repository concurrency group and the same
VM-wide `flock`, so backend, frontend, and certificate production mutations
cannot overlap.

## Trust boundary

- The sole runner labels are `[self-hosted, Linux, ARM64, vane-deploy]`.
- The runner service account is `vane-deploy-runner`. It must not be in the
  Docker group and has no local `sudo` access.
- Workflows are triggered only by `schedule` and `workflow_dispatch`; neither
  source pushes nor pull requests can enqueue this runner.
- This repository intentionally does not use a GitHub Environment as a security
  gate. Production credentials are repository secrets here only.
- Every GitHub Action is pinned to a full commit SHA. Checkout credential
  persistence is disabled.
- VPS access uses native OpenSSH. `ssh-keyscan` output is accepted only after
  its Ed25519 SHA256 fingerprint equals the pre-provisioned secret.
- Aliyun CLI is pinned to `3.4.8` and its official ARM64 archive SHA256. It is
  installed below `RUNNER_TEMP`, without `sudo` or a `latest` lookup.
- Wrangler is installed below `RUNNER_TEMP` at exactly `4.111.0`.
- acme.sh is fetched at commit
  `3661fd86b6304115e42f43910e6dd452ab9866d6`, installed with `--no-cron
  --no-profile`, and confined to the dedicated deploy state directory.

The default persistent state directory is
`~/.local/state/vane-deploy` (or `$XDG_STATE_HOME/vane-deploy` when set).
It contains deployed SHA files, the global lock, and acme.sh account/certificate
state. It must be backed up and readable only by `vane-deploy-runner`.

## Required repository secrets

| Secret | Purpose |
| --- | --- |
| `VANE_READ_KEY` | Private half of a read-only deploy key registered on `YouToco/vane` |
| `VANE_WEB_READ_KEY` | Private half of a read-only deploy key registered on `YouToco/vane-web` |
| `VPS_HOST`, `VPS_PORT`, `VPS_USER` | Production VPS SSH endpoint |
| `VPS_SSH_KEY` | Production VPS private key |
| `VPS_SSH_HOST_ED25519_FINGERPRINT` | Trusted value such as `SHA256:...`, obtained out-of-band |
| `ALIYUN_ACCESS_KEY_ID`, `ALIYUN_ACCESS_KEY_SECRET` | OSS, CDN, and AliDNS access |
| `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID` | Cloudflare Pages deployment |
| `ACME_ACCOUNT_EMAIL` | Let's Encrypt account email |

The source deploy keys must be distinct and must have **Allow write access**
disabled. The VPS host fingerprint must be copied from a trusted VPS console or
an already-verified connection, not learned from the first workflow run.

## State and recovery

Successful SHA files are `deployed-vane.sha` and `deployed-vane-web.sha`.
Writing is atomic (`mktemp` + `mv`). If a deployment succeeds remotely but the
runner dies before the state write, the next poll safely repeats that component.
To intentionally redeploy a commit, remove only its exact SHA file while no
workflow is running; the next dispatch will reconcile it.
