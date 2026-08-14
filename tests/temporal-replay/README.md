# Development Temporal replay gate

The root-owned read-only broker exports one recent, representative JSON history
that still exists for every entry in
`../../contracts/temporal/production-history-replay.json` into a private
temporary directory. Histories may contain customer payloads and are never
stored in Git or general build caches.

The exact candidate source runs:

```bash
cd server
VANE_TEMPORAL_HISTORY_DIR=/absolute/broker/export \
  GOWORK=off go test -tags productionreplay ./cmd/server \
  -run '^TestProductionHistoryReplay$' -count=1
```

Missing history, a non-regular file, a duplicate workflow, or replay
non-determinism fails the release. A pure directory/module-path move does not
add `GetVersion`; it must pass this gate with unchanged registration names.

This single-VPS deployment is currently a development environment. Its
Temporal namespace retains history for 24 hours and has no archive, so the
external replay contract contains only workflow types for which authentic
server history still exists. Registration names, task queue, and converter are
frozen separately in `../../contracts/temporal/production-registration.json`.
Real-Temporal integration tests cover the retention-clock and periodic paths.
The retired `PushPipelineWorkflow` generations remain as deterministic Go
fixtures under `../../server/workflow`; an expired server history is neither
fabricated nor treated as broker evidence.
