# Agent durable continuation contract

> **退役边界（2026-07-29）：** 本文的通用会话事实投影仍有效；专用于
> `enable_source/remove_source` 的 action proposal、confirmation、continuation 和
> dispatcher 已由 `task-playbook-fetch-target-cutover.md` 删除，不得重新接回。

## 7.10-A scope

This batch covers ordinary attitude and reason feedback facts only. It does not
re-enter the model, invoke a tool, send or patch a provider message, or
reconstruct a historical business result. It makes their existing fixed
`[卡片回调]` session fact recoverable.

`question` and `deep_dive` remain outside migration 056. A question is already
normal Agent input and must not be projected back as a second feedback turn.
Deep-dive generation has its own result-delivery lifecycle; after that result
is successfully sent it temporarily retains the legacy best-effort
`SessionNotifier` callback. Therefore 056 must not be read as durable
continuation for every feedback action.

## Producer boundary

`Store.InsertFeedbackWithSessionCutoff` owns one repeatable-read transaction:

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

## 7.10-B1a dark exact-action substrate

Migration 058 adds a separate, default-off continuation lane only for a
confirmed `enable_source` action. This checkpoint is deliberately dark: no
Agent, callback, server, startup scanner, model, provider, or Temporal
production call site invokes it. Ordinary newly-created confirmation cards
remain historical execution-version 0 actions and keep the existing
claim-and-execute behavior.

The only admission primitive is an explicit exact-action operator activation.
It locks one still-pending v0 root, verifies its tenant/user/session,
`enable_source` name, strict `{source_id}` arguments, DB-clock expiry, and
absence of prior continuation history, then atomically:

1. freezes canonical arguments and digests, the complete registered tool
   definition/policy versions, the Postgres adapter version, and both possible
   fixed code-generated terminal session facts;
2. creates the exact continuation row and generation-1 durable authority;
3. promotes only that root to execution version 2.

The control operator and effect continuator are unrelated cluster-wide roles.
Only the operator can promote/demote and append authority; only the
continuator can confirm, lease, update `sources`, append the exact B3 session
batch, and checkpoint terminal state. Every control/acquire/project/release
transaction re-proves exact role attributes, membership graph, search path,
schema/table/column/sequence ACLs, and lack of public security-definer access
before entering either role.

A pristine canary may be rolled back once. Rollback appends generation-2
legacy authority, terminalizes the continuation as `rolled_back`, and demotes
the untouched root to v0 in one transaction. The immutable row/history stays
for audit; reactivation is forbidden. Confirmation, lease acquisition, or any
effect attempt permanently closes rollback.

Projection never calls the generic Agent tool executor or `EnableSource`, and
never discovers a current session. It uses the frozen source ID and original
session. The owned-source update, selected fixed terminal fact, route-aware B3
append, and `completed` checkpoint share one transaction/savepoint. A damaged
or conflicting B3 batch rolls the source update back before the continuation
is terminally blocked. A lost commit response re-enters replay-only mode and
must prove the existing complete batch byte-for-byte; it cannot reconstruct a
missing batch from a completed flag.

Tenant purge follows root → continuation → session lock order and deletes
authority → continuation → pending root. Migration Down uses the same root
order and refuses both any continuation history and any orphan execution
version 2 root.

The remaining B1b work is intentionally not claimed by this dark checkpoint:
confirmation routing before legacy Claim, startup/periodic bounded dispatcher,
graceful drain, and exact-action runtimeadmin CLI activation/status/rollback.

## 7.10-B1b live exact-action cutover

B1b routes every card decision through the v2 controller before any legacy
protocol. Only an exact `ErrNotRouted` may fall through; lookup, database and
integrity ambiguity preserve the card and fail closed. A bounded dispatcher
runs one startup pass before external ingress, then periodic tenant-keyset
passes with fenced leases and graceful drain. The runtimeadmin surface remains
an exact-action canary control plane; it is not a bulk switch.

## 7.10-B2 atomic durable proposal

Migration 059 changes only newly-issued `enable_source` cards. The Agent sends
an internally generated action ID, exact session, strict arguments, visible
summary and expiry to one proposal controller. Store derives the tenant from
that exact session, acquires the tenant admission root, and share-locks the
live session/membership/active-tenant relation. A second membership of the
same user can never choose the proposal tenant. Under the shared action
admission lock, the independent `vane_agent_action_proposer` role commits:

1. an execution-version 2 pending root with canonical `{source_id}` arguments;
2. the complete frozen continuation payload used by B1;
3. generation-1 durable authority with fixed producer evidence.

All three rows commit or roll back together. `CreatePendingAction → Activate`
is forbidden for normal B2 cards. An exact retry after a lost commit response
must verify the root, visible summary, expiry, every frozen payload byte and
the sole authority event. The initial continuation control fields are also
exact: no lease, attempt, terminal or timestamp drift is adoptable. Partial
evidence is an integrity failure, never a retryable database ambiguity, and
the controller retries transient database ambiguity with the same action ID
inside one bounded convergence window; it may not repair or recreate evidence.

Confirmation peeks the immutable tenant/session identity, acquires the same
tenant admission root before locking the action, and revalidates the live
membership before accepting. Membership revocation therefore linearizes
against both proposal and confirmation; cancellation remains available because
it cannot create an effect.

The proposer has only specified-column SELECT/INSERT on those three tables and
sequence USAGE for the authority event. It has no UPDATE/DELETE/TRUNCATE,
session projection, source effect, generic tool execution, provider, Temporal,
operator or continuator capability. Existing v0 cards and every write tool
other than `enable_source` retain their historical protocol.
