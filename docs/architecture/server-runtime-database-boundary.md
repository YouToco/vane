# Vane server database runtime boundary

Migration 098 installs owner-only provision/deprovision functions but does not
create the cluster-global `vane_server_runtime` role. After the complete schema
succeeds, `cmd/migrate` explicitly provisions the `NOLOGIN NOINHERIT` shell.
Ordinary `store.Migrate` callers and scratch databases remain schema-only.
Deployment may then turn that exact role into a login, but the long-lived
server must use `NewServerRuntime`, or
`NewServerRuntimeWithResearchRuntimeCapability` when the separate paid-research
runtime is enabled. The schema-owner URL remains an offline migration
credential, and neither constructor accepts a provider-gateway URL.

The constructor verifies the login has the exact capability membership set and
cannot enter either V3 research/provider role. It then probes the inert login,
`vane_app`, and every other role the server can explicitly enter: none may have
owner/superuser/BYPASSRLS authority, create in `public`, mutate protected data,
read the verifier secret, or execute provider-gateway functions. Each accepted
connection then enters `vane_app` as its default role; existing Store
transactions can use only their explicitly allowlisted `SET LOCAL ROLE`
capabilities.

The combined constructor keeps the research connection on the existing
`vane_research_runtime` probe and the existing per-run capability keyring. It
does not merge paid research authority into the primary pool and does not
create a gateway pool.

## Creating another database in the same PostgreSQL cluster

PostgreSQL roles and memberships are cluster-global. Several migrations before
098 deliberately reject unrelated principals that can enter their capability
roles. Therefore a second database must not be migrated from zero while a live
`vane_server_runtime` from the first database retains those memberships.

Use a fresh PostgreSQL cluster, or perform this fail-closed deprovision first:

1. Stop every Vane server using the cluster and prove no
   `vane_server_runtime` session remains.
2. As the offline owner, run `ALTER ROLE vane_server_runtime NOLOGIN`.
3. Still connected directly as that database's migration owner, call:

```sql
SELECT public.deprovision_vane_server_runtime_v1();
```

The function revokes only migration 098's exact membership set and requires
`DROP ROLE` to succeed. A dependency error is evidence of direct grants,
unexpected membership, or object ownership drift; investigate it rather than
using `DROP OWNED` or granting the runtime schema-owner authority. Migrate the
second database through 098. Only after every schema migration succeeds, call
`ProvisionServerRuntime` (the production `cmd/migrate` does this), then enable
the recreated shell as the runtime login.

## Current compatibility status

The 098 gate proves connection startup, `Ping`, a normal `vane_app`
`UpsertUserByOpenID`, an explicit `SET LOCAL ROLE vane_intelligence_reader`,
and construction of the separate restricted research pool with its capability
keyring and no gateway pool. It is a minimal Store smoke, not yet a claim that
every legacy Store method is compatible with the non-owner runtime. Full Store
behavior, race, and service wiring remain separate release gates.
