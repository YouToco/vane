# Workspace memory runtime production UAT

Status: operator-only, explicit one-shot.

`vane-migrate workspace-memory-uat` is a release-bound production acceptance
command. It is not part of the migration default path, the HTTP API, Agent
tools, Temporal workers, or any channel adapter.

The command requires all of the following:

- an exact clean embedded source revision matching `--expected-revision`;
- the exact confirmation value `vane.workspace-memory-runtime-uat/v1`;
- a canonical non-zero operation UUID;
- distinct migration-owner and `vane_server_runtime` database authorities.

The operator wrapper at `ops/audit/workspace-memory-runtime-uat.py` additionally
pins `/opt/vane/current` to the expected release, verifies root-owned immutable
credential and binary paths, and runs the command as a hardened transient
`vane-migrate` systemd unit with a four-minute start deadline.

Each run serializes on a database advisory lock, removes residue from an
interrupted earlier run, and creates synthetic identities whose external IDs
use the reserved `vane-runtime-uat:` prefix. Product memory operations then use
an independently authenticated `NewServerRuntime` pool:

1. write and recall one personal record;
2. write one team record as member A;
3. recall that record as member B;
4. prove the personal record is absent from the team corpus;
5. prove the team record is absent from the personal corpus.

The migration-owner connection is used only for fixture setup and tenant purge.
Both workspaces and both users must be absent before a success receipt is
emitted. The receipt contains booleans and content digests, never memory text,
database URLs, credentials, or user data. A failed run emits no success receipt;
the deferred cleanup uses a separate bounded context, and the next run also
recovers stale reserved-prefix fixtures.

This command is an acceptance probe, not a general administrative endpoint.
It must only be invoked by the trusted release controller after the ordinary
migration, startup, readiness, and rollback gates have succeeded.
