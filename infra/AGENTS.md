# Infrastructure authority

`infra/` contains declarative desired state only: Compose, Caddy, systemd,
Temporal configuration, and environment schemas/examples. It must not contain
shell/Python deployment logic, cloud mutation calls, credential values, or
durable release state.

Application SQL migrations belong only to `server/store/migrations`. Runtime
configuration here is canonical; `ops/` packages these exact files and must not
keep template copies.
