# Workspace memory runtime production UAT

Status: operator-only, explicit one-shot.

`vane-migrate workspace-memory-uat` is a release-bound production acceptance
command. It is not part of the migration default path, the HTTP API, Agent
tools, Temporal workers, or any channel adapter.

The command requires all of the following:

- an exact clean embedded source revision matching `--expected-revision`;
- the exact confirmation value `vane.workspace-memory-runtime-uat/v1`;
- a canonical non-zero retryable correlation UUID;
- distinct migration-owner and `vane_server_runtime` database authorities.

The operator additionally pins `/opt/vane/current` to the expected release,
verifies the established `vane-migrate:vane-migrate:0400` migration credential
and root-owned immutable binary/config paths, and runs the command as a
hardened transient `vane-migrate` systemd unit with a six-minute start
deadline. The operator reads only `VANE_DB_URL` from the root-owned server
environment into a short-lived dedicated `runtime_db_url` systemd credential;
the UAT process does not receive the server environment's provider keys or
application secrets.

Each run serializes on a database advisory lock, removes residue from an
interrupted earlier run, and creates synthetic identities whose external IDs
use the reserved `vane-runtime-uat:` prefix. Product memory operations then use
an independently authenticated `NewServerRuntime` pool:

1. write and recall one personal record;
2. write one team record as member A;
3. recall that record as member B;
4. prove creator A's personal record is absent from the team corpus for both A
   and B;
5. prove member B cannot read creator A's personal workspace;
6. prove the team record is absent from creator A's personal corpus.

The migration-owner connection is used only for fixture setup and tenant purge.
Both workspaces and both users must be absent before a success receipt is
emitted. The receipt contains booleans and content digests, never memory text,
database URLs, credentials, or user data. A failed run emits no success receipt;
the deferred cleanup uses a separate bounded context, and the next run also
recovers stale reserved-prefix fixtures.

This command is an acceptance probe, not a general administrative endpoint.
The trusted forced-command production handler invokes it automatically after
the ordinary migration, startup, readiness, and authenticated UAT gates have
succeeded, then binds its evidence into the signed verify manifest. The
controller follows the existing N→N+1 rule: the release that first carries this
handler only stages it; the following product release is the first one that can
run the UAT under that already-finalized controller. A correlation UUID is safe
to retry because the probe purges its complete synthetic fixture; it is not a
durable one-time business-operation receipt.
