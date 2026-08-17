# Remote MCP runtime contract (migration 153 dark slice)

Migration 153 and `server/mcpclient` establish a non-production execution
slice for remote MCP. They do not activate a server call site, grant
`vane_server_runtime` the capability-ledger coordinator role, or enable secret
use. Product/UI claims must therefore continue to describe MCP execution as
unavailable.

## Historical knowledge consulted

The required pre-design `llm-wiki` search on 2026-08-16 found no Vane-specific
MCP decision or MCP incident. The applicable compiled pages were:

- `playbooks/agent-governance`: remote annotations are never ToolPolicy;
  execute in `validate → authorize → budget → confirm → execute → sanitize →
  record` order.
- `playbooks/skill-key-management`: secret values must not enter logs,
  arguments, repository content, or child-process command lines.
- `invariants/truth-source-over-cache`: read the real remote `tools/list`
  schema rather than trusting an installation-time self-report.
- `invariants/unsafe-by-default`: unsupported capabilities remain explicitly
  disabled.
- `invariants/verified-not-done`: a validated connection is not an activated
  runtime.
- `postmortems/2026-08-14--vane-oversea-dns-misroute`: control-plane success
  cannot be promoted into unrelated data-plane authority.

## Immutable local authority

`mcp_runtime_bindings` binds one immutable MCP capability version to:

- the exact capability-version payload digest, endpoint, protocol,
  authentication kind and connection-schema digest;
- the distinct manually approved frozen tool-catalog digest;
- the approving user and approval time;
- for authenticated connections, the opaque `vault:*` label plus exact vault
  row, scope, tenant/user coordinates, provider, purpose, generation and keyed
  fingerprint.

The insert trigger re-proves all connection coordinates, the capability
version, human authority and vault row in the same transaction. Personal
connections may bind only the owner's user credential; workspace connections
may bind only a tenant credential.
OAuth is rejected. Rows are immutable outside explicit tenant purge. FORCE RLS
requires the exact current user and a live membership before any workspace row
is visible. Catalog assertions freeze every column, type, default, nullability,
CHECK, FK, PK and unique constraint as well as policies, grants and functions.

The migration is dark: only the migration/control-plane owner may create a
binding. The ledger coordinator has read-only visibility, and
`vane_server_runtime` is neither granted table access nor made a coordinator
member.

## Dark execution sequence

`ApprovedConnectionV1` contains an exact migration-153 row projection. Its
digest includes manual approval, capability version, endpoint, protocol,
connection schema and approved catalog. The coordinator performs all
network-free validation first, then passes that projection through both the
durable ledger's Prepare and Acquire boundaries. The returned permit binds both
the invocation and row-projection digests, so even a same-catalog endpoint swap
returns before DNS. Only an exact permit can reach:

1. fresh public-address DNS admission and a pinned TLS connection;
2. MCP `initialize` without sampling, roots or elicitation capabilities;
3. invocation-scoped `Mcp-Session-Id` binding;
4. `notifications/initialized`;
5. real paginated `tools/list`;
6. canonical local read-only allowlist freeze and exact digest comparison;
7. one approved `tools/call`;
8. bounded result wrapping as `trust=external, tainted=true`;
9. durable ledger settlement.

Every HTTP round trip creates a new transport, resolves DNS again and pins the
connection to the admitted public addresses. Redirected POSTs pass through the
same validator. Loopback, private, link-local, documentation, multicast and
cloud-metadata ranges fail closed. Reverse JSON-RPC requests are rejected.

If the tool-call request is sent but its response is lost, the coordinator
does not fabricate a definite failure. It leaves the lease executing so
migration 152 can converge it to `unknown_effect` with append-only evidence.

## Gates intentionally still open

- No production construction or call site exists.
- Credential decryption/header injection is disabled even when a migration-153
  binding exists; credentialful ledger Prepare retains migration 152's hard
  rejection.
- OAuth 2.1, prompts, resources, sampling, roots and elicitation are disabled.
- Schema drift is rejected before call but is not yet wired to append a
  `schema_drifted` lifecycle event or pause the capability.
- The migration owner binding operation is not exposed through an authorized
  Web capability-center flow.
- Real public MCP conformance, timeout/budget, redirect, DNS-rebinding and
  credential-rotation canary evidence is still required before activation.

These gates must land with a later activation migration and production UAT;
this slice must not be relabeled as an active MCP runtime.
