# Vane

Vane is a monorepo for the Go/Temporal service, React browser application, and
the release control plane used to publish both components.

## Layout

| Path | Authority |
| --- | --- |
| `server/` | Go service, migrations, and all shipped binaries |
| `web/` | React/Vite browser application |
| `contracts/` | Machine-readable cross-component contracts |
| `infra/` | Declarative development and production desired state |
| `ops/` | Release, rollback, certificate, and audit state machines |
| `tests/` | Cross-component contract, E2E, replay, and release tests |
| `tools/` | Pinned toolchain installers, generators, and policy checks |
| `docs/` | Architecture, decisions, development, and runbooks |

Component tests stay with their component. The root Makefile is deliberately a
thin dispatcher:

```bash
make quick
make test-server
make test-web
make test-contract
make test-release
make full
```

Production release is intentionally not a Makefile recipe assembled from loose
workspace paths. It starts from an exact remote-main revision:

```bash
./ops/bin/vane doctor
./ops/bin/vane release --sha <40-character-origin-main-sha>
# Only after a finalized Server / failed Web split:
./ops/bin/vane resume-web --sha <sha> --release-root /path/to/release-<sha>
```

The local command tests and builds both components directly on the Mac. It
submits only the native Server bundle to the externally installed root-owned
broker; after Server ready/Gate/UAT succeeds, it publishes the same verified
Web `dist/` first to Cloudflare Pages and then to OSS plus Ali CDN. Web files
never pass through or reside on the VPS.

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
