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
```

The local command can submit immutable evidence, but only the externally
installed root-owned broker can acquire the production lock, read credentials,
mutate providers, or finalize durable state.
