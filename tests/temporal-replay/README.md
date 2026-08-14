# Production Temporal replay gate

The root-owned read-only broker exports one recent, representative JSON history
for every entry in
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
