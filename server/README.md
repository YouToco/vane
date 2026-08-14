# Vane server

`server/` is the single Go application module (`github.com/YouToco/vane/server`).
All production Go binaries live under `cmd/`; the release inventory is
machine-readable at `../contracts/release/server-binaries.json`.

## Domain map

- Agent: `agent`, `agentcontext`, `agentledger`, `acquisitiontool`, `toolsearch`.
- Task: `task`, `taskstate`, `taskhealth`, `definitioneditwire`, `scheduler`.
- Content and research: `fetcher`, `dedup`, `researchtools`, `researchgateway`,
  `tikhubcatalog`, `tikhubinvoke`.
- Insight: `observation`, `eventqualifier`, `scorer`, `selector`, `cardgen`,
  `executivebrief`, `periodicbrief`.
- Delivery and feedback: `feishu`, `pusher`, `pusheffect`, `pushrecovery`,
  `feedback`.
- Runtime: `workflow`, `workflowruntime`, `runcontext`, `runoutcome`,
  `runtimeconfig`, `runtimepolicy`, `config`.
- Persistence: `store`; its embedded `store/migrations` directory is the only
  application schema authority.
- Transports: `api` and `a2a`.

The migration preserves the existing domain-first package structure. Do not
introduce generic `common`, `helpers`, or `pkg` packages; add code to the domain
that owns its invariant.

Production ships these commands as native Linux/amd64 binaries and runs them
under systemd. Do not add a server Dockerfile or application image: Compose is
owned by `infra/` and is limited to PostgreSQL, Temporal, Temporal UI, and
Caddy middleware.

Run independently of any optional root workspace:

```bash
GOWORK=off go list ./...
GOWORK=off go test -race ./...
make build
```
