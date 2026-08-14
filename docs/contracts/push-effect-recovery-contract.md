# Push Durable Effect / Recovery Contract

> Status: frozen implementation contract for the delivery effect recovery train.
> This contract is security and consistency critical. A change to its identity,
> state transitions, provider idempotency, or recovery authority requires a new
> version and the S-level verification gates in `AGENTS.md`.

Implementation status (2026-07-25): PR-D wires PR-C's exact-task
`pushrecovery.Coordinator.Attempt` behind the independent, default-empty
`pipeline.push_effect_recovery_canary_schedule_id`. It adds an outbound-only
Feishu readiness barrier, immutable DB-clock task/tenant/effect keyset
discovery, a bounded startup pass before every ingress, periodic recovery,
fair cursor rotation, finite concurrency/time budgets, low-cardinality process
metrics/logs, and context-owned drain. It still has no unrestricted operator or
legacy-batch adoption.

## 1. Truth boundaries

Three durable histories remain deliberately separate:

1. `agent_events` is the append-only semantic history of an Agent session. It
   records turns, messages, tools, and confirmations. It is not a work queue and
   never authorizes a Runtime or delivery effect.
2. Temporal history is the orchestration history. PostgreSQL must not duplicate
   every deterministic Workflow transition.
3. A Push effect is the business truth for one external `Message.Create`.
   Its checkpoint owns the exact target, immutable card bytes, delivery set,
   provider idempotency key, result ambiguity, and recovery lease.

`push_batches` and `deliveries` remain query projections. Their terminal fields
must converge from proven Push effect state; a `pending` delivery alone is never
proof that a message was not sent.

## 2. Effect identity and immutable input

One effect represents one aggregate-card chunk and is uniquely identified by:

```text
(TenantID, UserID, TaskID, RunSnapshotID, RunID, StepID, ChunkIndex)
```

The immutable effect input also binds:

- batch ID and the exact ordered delivery ID set;
- provider and application identity;
- recipient type and exact recipient ID;
- exact card bytes and SHA-256 digest;
- a stable provider UUID derived from the durable effect identity;
- schema version and immutable run-snapshot authority.

All IDs are explicit. No Store or recovery method may infer tenant ownership
from current membership, a mutable task row, or the effect row alone.

Create is first-writer-wins. A response-lost retry may return the existing row
only when every immutable field and digest is exactly equal. Any drift is a
conflict and no external call is authorized.

## 3. State machine

```text
prepared
  -> sending
      -> sent
      -> definite_failed -> prepared (bounded backoff)
      -> ambiguous -> sent | prepared | blocked
prepared | sending | definite_failed | ambiguous -> blocked
```

- `prepared`: immutable input exists and no external attempt is in flight.
- `sending`: one fenced owner is authorized to make the exact provider call.
- `definite_failed`: the provider call is proven not to have created a message;
  retry remains subject to policy, backoff, live authority, and UUID window.
- `ambiguous`: the provider may have created the message. Direct Create is
  forbidden until an authoritative resolver proves the outcome.
- `sent`: provider message identity is durable. This state is terminal.
- `blocked`: automatic progress is unsafe or impossible. This state is terminal
  until an explicit restricted operator records a new, audited decision.

No generic recovery path may rewrite terminal evidence or move backward from
`sent`.

## 4. Lease, fence, and authority

- Lease timestamps and retry scheduling use PostgreSQL `clock_timestamp()`.
- Every claim increments a monotonic fence. A stale fence cannot checkpoint a
  provider result, release a lease, or change ambiguity.
- Takeover waits for both lease expiry and the configured grace window so an
  in-flight provider RPC is not prematurely duplicated.
- Before a new external send, the coordinator revalidates the exact tenant,
  user, task, immutable run snapshot, batch, and current effect authorization.
- After an external send has definitely happened, the receipt path deliberately
  does not require mutable membership or task status. Revocation must stop new
  sends, not erase the durable receipt of an already-created message.
- Coordinator, receipt, and operator duties use separate restricted PostgreSQL
  roles with tenant RLS and column-level grants.

## 5. Provider UUID and the ambiguous window

Feishu `Message.Create` supports a caller-provided UUID. Requests with the same
UUID can succeed at most once during the provider's one-hour deduplication
window. The UUID is created once from durable effect identity and reused by
every attempt; it must never be generated per retry.

The UUID prevents duplicate success inside its provider window but does not by
itself supply a durable local receipt. A timeout, response loss, empty message
ID, process exit after the remote call, or local receipt uncertainty enters
`ambiguous`.

During the one-hour provider window, an ambiguous effect may replay the exact
same target, card bytes, and UUID as a reconciliation attempt. This is not a
new send authorization: changing any input or UUID is forbidden, and the effect
remains ambiguous until a durable message ID is obtained. After the window,
Create is forbidden.

An ambiguous effect may otherwise progress only through a provider-specific
resolver:

1. exact matching message found: adopt its message ID and record `sent`;
2. no authoritative answer when the UUID window expires: record `blocked`.

Matching only time, title, recipient, or a text excerpt is not authoritative.
Feishu exposes no get-by-UUID API, and a message-list miss does not prove that a
send did not happen.

For positive Feishu reconciliation, the effect must also freeze the owner P2P
chat ID, the non-secret App identity that owns that chat, and embed its
non-sensitive effect ID into the exact card JSON. An owner chat may be captured
only from a `p2p` event; a group event may identify the owner but cannot supply
the push conversation. Secret rotation for the same App preserves the binding,
while switching Apps invalidates the old chat until positive evidence from the
new App replaces it.

The durable Create boundary atomically validates the frozen expected App
against the exact selected client generation and returns that actual App
identity with every observation. A reconfiguration racing an in-flight request
therefore cannot relabel the result. Transport failures, missing success
receipts, unknown business failures, and decoded JSON 5xx responses are
ambiguous; only explicit no-side-effect HTTP/provider rejections are definite
not-sent.

The resolver may list that exact chat and adopt a message only when one
interactive message from the expected application contains the exact effect ID
and frozen card content. Zero matches remain ambiguous; multiple matches are an
invariant violation and become blocked. The required chat ID must be durably
captured from an inbound message/event or a successful Create receipt rather
than re-derived from mutable owner state.

## 6. Atomic projection settlement

After a proven successful Create, one receipt transaction:

1. validates original effect scope, immutable digest, active fence, and provider
   result;
2. records `effect=sent` and provider message ID;
3. marks every delivery in the frozen chunk `sent` with the same exact card
   bytes and message ID;
4. marks the batch `done` only when every effect belonging to that batch is
   durably `sent`.

The current row-by-row delivery receipt window must not remain the authority for
compiled Push once the effect path is enabled.

## 7. Recovery lifecycle and limits

These lifecycle behaviors run only when the independent exact-task recovery
canary is non-empty and inside the compiled-runtime rollout. Empty is the
complete rollback state and performs no outbound preparation or attempt.

- A bounded startup pass runs before external ingress.
- A DB-clock, exact-task, tenant-sharded and keyset-page-bounded periodic worker
  handles due `prepared`, `definite_failed`, and `ambiguous` effects plus stale
  `sending` effects.
- The worker has a finite global concurrency limit, per-attempt timeout, maximum
  attempts, per-pass discovery/admission deadline, fair capped-pass cursor, and
  context-owned shutdown. Admitted work retains its bounded durable checkpoint
  budget; no new attempt is admitted after the pass deadline.
- Recovery metrics use only low-cardinality labels: state, provider, outcome,
  and failure class. Tenant, user, task, run, effect, recipient, provider UUID,
  and card contents must not be metric labels or log message text.
- Recovery logs carry only bounded trigger/outcome/error-code/counter fields;
  they never carry tenant, user, task, run, effect, recipient, card, message,
  credential, or provider UUID values.

## 8. Legacy pending rows

Rows created before this protocol have no immutable effect input, provider UUID,
attempt fence, or durable ambiguity classification. A new worker must never
automatically adopt or resend them.

An individual legacy batch may be handled only by a restricted operator when
there is exact evidence of its outcome. In particular, a persisted failure from
the local “Feishu channel is disconnected” branch is evidence that the provider
API was not called. The operator records the evidence source and creates a new
versioned effect; it does not delete deliveries or forge `sent`.

For every deterministic `prepared` or `definite_failed` effect, the recovery
coordinator also owns a restricted database-clock terminal checkpoint. If the
remaining provider UUID window can no longer contain one complete send lease,
the exact unclaimed fence is atomically changed to
`blocked/provider_window_expired_no_send`. The transition races the normal
authorized claim under the same batch lock, so exactly one path wins; it never
applies to `sending`, `ambiguous`, or `sent`. This prevents an expired,
definitely-not-sent effect from remaining recoverable forever.

### 8.1 Physical batch 63 one-time repair

The only initial legacy adoption is a physically pinned, one-time
`runtimeadmin legacy-batch63-repair` operation. It has no batch, tenant, user,
task, Run, target, App, card, or UUID selector. Its database role and
SECURITY DEFINER functions are dedicated to physical `push_batches.id=63`;
`PUBLIC`, `vane_app`, the generic coordinator, and the generic effect operator
cannot execute them.

The evidence file is canonical, strict JSON and content-addressed by the Store.
It binds all of the following together:

- batch 63, the exact task, Temporal Workflow/Run, Activity ID 52 and attempt 1;
- service revision
  `5a82b1350aba467189ba36a90105f6de3d4d65e4`;
- the non-retryable `CONFLICT` result with exactly five items;
- the exact `client == nil` code branch before
  `client.Im.Message.Create`;
- the exact journald lines and their SHA-256 digest; and
- the explicit fact that Temporal history is no longer retained. History
  `not found`, an empty provider receipt, or a caller-supplied label alone is
  never proof.

`preview` is read-only. Its only plan-time input is an absolute RFC3339
`-expires-at`, reused as the same exact instant by `apply`; the Store accepts it
only in the database-clock 45-to-60-minute safety window. Preview re-locks and
hashes the
exact failed batch, snapshot,
five ordered pending deliveries, frozen content/source card inputs, and current
non-secret Feishu target generation. The Store, not command-line input,
deterministically rebuilds the aggregate card and all `pusheffect.Prepared`
fields. The output carries only `phase`, the exact plan digest, `enable_by`,
`expires_at`, and remaining seconds; it never emits evidence, raw logs, card
bytes, recipient, App identity, provider UUID, or database error details.

`apply` requires the evidence file, exact preview digest, and
`-confirm-apply`. Under one physical batch/effect lock transaction it recomputes
the complete material and evidence digest, rejects any drift, changes batch 63
from `failed/NULL` to `pending/effect`, creates one `prepared` effect, and
appends the `finalized` audit event. Exact replay may only return the already
finalized identical plan. No provider call is reachable from this operator.

The repair UUID expires no later than one database-clock hour after preview,
and apply requires at least 45 minutes to remain. Fresh claim also requires the
complete send lease to fit inside the frozen provider window. The recovery
canary stays empty through preview and apply; after finalized verification, the
exact task key must be enabled before the emitted absolute `enable_by` deadline.
If that deadline is missed, `abort` with the exact digest and
`-confirm-abort` atomically wins against claim, blocks only an unclaimed
`prepared` effect, restores the batch to `failed`, and appends a `blocked`
audit event. It cannot abort `sending`, failed-attempt, ambiguous, or sent
effects.

`verify` is read-only and reports the durable phase, exact plan digest, and
deadline window without exposing the underlying effect/card/provider fields.
The production Gate is not complete until recovery records `sent`, all five
deliveries share the exact receipt/card, batch 63 is `done`, the Feishu message
is positively retrievable, the exact recovery key has been removed again, and
health/readiness/log probes remain green.

## 9. Release train

1. Deploy schema, codec, Store, roles, and integrity tests with zero production
   call points.
2. Deploy the stable-UUID Feishu adapter and dark effect preparation.
3. Enable live effect settlement for one exact task; recovery remains disabled.
4. Deploy the exact-task recovery authority, then its independently gated
   bounded lifecycle. Keep the recovery key empty until the production canary
   task is explicitly selected and the complete fault matrix is green.
5. Expand only after shadow/projection audits remain exact. Agent ledger main
   read switching is an independent release decision.

## 10. Required fault matrix

At minimum, tests must kill or lose responses:

- before effect insert and after insert;
- before claim, after claim, and after lease takeover;
- before Create;
- after provider application but before HTTP response;
- after provider success but before local receipt commit;
- after receipt commit but before the caller sees success;
- after effect settlement but before batch finalization;
- during multi-chunk partial success;
- after tenant membership, task status, or schedule deletion changes;
- after the provider UUID window expires.

Negative tests must prove that a stale fence, cross-tenant scope, changed payload
digest, changed delivery set, and every `ambiguous` state cannot call Create.
