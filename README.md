# Vane

Vane is a monorepo for the Go/Temporal service and the React browser
application. Deployment runs as a GitHub Actions workflow.

## Layout

| Path | Authority |
| --- | --- |
| `server/` | Go service, migrations, and all shipped binaries |
| `web/` | React/Vite browser application |
| `contracts/` | Machine-readable cross-component contracts |
| `infra/` | Declarative development and production desired state |
| `tests/` | Cross-component contract, E2E, and replay tests |
| `tools/` | Pinned toolchain installers, generators, and policy checks |
| `docs/` | Architecture, decisions, development, and runbooks |

Component tests stay with their component. The root Makefile is deliberately a
thin dispatcher:

```bash
make test-server
make test-web
make test-contract
```

Production deployment is the `deploy` job of the `CI` workflow
(`.github/workflows/ci.yml`). It runs only after the `test` and `build` jobs
pass on the same revision (the payload is built once and reused), requires
approval on the `production` GitHub environment, and is restricted to `main`.
It ships the payload to the VPS over SSH using a repository-secret deploy key
and runs `tools/release/remote-atomic-release.sh` (CAS check, online migrate,
atomic symlink switch, service restart, `/readyz` plus live-binary
verification, automatic rollback). It then publishes the verified web `dist/`
to Cloudflare Pages and to OSS plus Ali CDN. Web files never pass through or
reside on the VPS.

The single VPS runs every Vane server release as native Go binaries supervised
by systemd under `/opt/vane/releases/<sha>/`; the `current` symlink is the
application switch. There is no server container image build, registry push,
or deployment pull. Compose is reserved for the independently pinned
PostgreSQL, Temporal, Temporal UI, and Caddy middleware, and a server-only
release must not recreate or restart them.

AliDNS remains authoritative for the browser hostname: the default/domestic
line targets Ali CDN backed by OSS, while the overseas line targets Cloudflare
Pages. Both providers must prove the same exact release before one combined
publication can finalize. On Aliyun, content-hashed assets are uploaded first,
`index.html` and the release marker are committed last, old hashed assets are
kept, and mutable paths are refreshed. Cloudflare deployment retention is a
separate fail-closed operation and never deletes the active deployment, its
designated rollback deployment, the Pages project, the custom domain, or DNS.
