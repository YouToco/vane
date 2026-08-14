# Infrastructure

- `development/`: local-only desired state.
- `production/compose`: the production dependency topology.
- `production/caddy`: the canonical edge configuration.
- `production/systemd`: canonical application units and socket.
- `production/temporal`: Temporal dynamic configuration.
- `production/env`: variable names and examples only; never secret values.

Container images are pinned by tag and digest. A digest update must include a
compatibility test and the observed platform/version evidence.
