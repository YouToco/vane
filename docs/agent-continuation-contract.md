# Agent durable continuation contract

## 7.10-A scope

This batch covers feedback facts only. It does not re-enter the model, invoke a
tool, send or patch a provider message, or reconstruct a historical business
result. It makes the existing fixed `[卡片回调]` session fact recoverable.

## Producer boundary

`Store.InsertFeedback` owns one repeatable-read transaction:

1. acquire the shared producer/downgrade transaction-level admission lock,
   before taking any table lock, then resolve the delivery's exact tenant;
2. enter `vane_app` with tenant RLS;
3. append the feedback (and problem triage when applicable);
4. select and freeze the latest session whose status is `active` and remains
   inside the configured Agent inactivity TTL, including its exact
   tenant/user/session scope and fixed code-generated message; or append a
   terminal `suppressed/no_active_session` checkpoint;
5. commit the business fact and outbox together.

The scanner never asks which session is active. A session created after step 4
cannot receive the earlier fact. An old `misjudged` unique-key replay returns
the existing feedback and does not manufacture a new outbox fact.

## Consumer boundary

The dispatcher performs an immediate startup pass, bounded tenant-keyset pages,
a two-second periodic pass, four-way bounded concurrency, and context-owned
drain. It reads only `agent_session_fact_outbox`; it has no provider or Agent
model dependency.

Each pending fact uses a DB-clock lease and monotonically increasing fence.
`ProjectAgentSessionFact` locks the exact outbox row, then calls the existing
route-aware session append with stable identity `feedback-click:<feedback_id>`.
The helper runs inside a savepoint: any post-write validation failure rolls
back every helper write before the same fenced outbox row may be marked
`blocked`. The ledger snapshot, retained JSONB replica, and `completed`
checkpoint otherwise commit in one transaction.

- A lost commit response is replay-only: it must find and validate the complete
  existing batch for the same identity and bytes. Missing or damaged evidence
  fails closed and is never reconstructed from a `completed` checkpoint.
- A transient database error rolls back and releases the lease with bounded
  backoff; it is not marked corrupt.
- Invalid durable payload, incomplete ledger, exact-scope loss, or
  legacy/ledger drift is fail-closed as `blocked`; no fallback session is used.
- `suppressed` and `blocked` are terminal and are not scanned.

## Database authority

Feedback production retains only specified-column `INSERT` on the outbox under
`vane_app`. Projection uses the separate cluster-wide
`vane_agent_session_fact_projector` identity:

- `NOLOGIN`, `NOINHERIT`, `NOBYPASSRLS`, no role creation/database/superuser
  attributes;
- only the migration owner may enter it;
- exact session projection read/message update, append-only event read/insert,
  projection-authority read, and fenced outbox read/checkpoint columns;
- no payload update, delete, truncate, or provider capability.

Before every Acquire, Project, or Release, the Store re-proves the projector's
exact attributes, role configuration and membership graph, schema/table/column/
sequence ACLs, lack of public `CREATE`, and lack of access to any public
security-definer function. Any drift fails closed before entering the role.

The migration Down path first takes the same transaction-level admission lock
as every producer, then follows the projector/purge order—outbox before session
root—and refuses to remove any non-regenerable fact history. Tenant purge uses
the same table lock and delete order.
