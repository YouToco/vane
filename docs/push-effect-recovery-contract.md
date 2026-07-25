# Push Durable Effect / Recovery Contract

> Status: frozen implementation contract for the delivery effect recovery train.
> This contract is security and consistency critical. A change to its identity,
> state transitions, provider idempotency, or recovery authority requires a new
> version and the S-level verification gates in `AGENTS.md`.

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

- A bounded startup pass runs before external ingress.
- A tenant-sharded, page-bounded periodic worker handles stale nonterminal
  effects.
- The worker has a finite global concurrency limit, per-attempt timeout, maximum
  attempts, and context-owned shutdown.
- Recovery metrics use only low-cardinality labels: state, provider, outcome,
  and failure class. Tenant, user, task, run, effect, recipient, provider UUID,
  and card contents must not be metric labels or log message text.
- Logs may carry internal IDs as structured attributes where operationally
  required, but never card bytes, recipient identifiers, credentials, or
  provider UUID.

## 8. Legacy pending rows

Rows created before this protocol have no immutable effect input, provider UUID,
attempt fence, or durable ambiguity classification. A new worker must never
automatically adopt or resend them.

An individual legacy batch may be handled only by a restricted operator when
there is exact evidence of its outcome. In particular, a persisted failure from
the local “Feishu channel is disconnected” branch is evidence that the provider
API was not called. The operator records the evidence source and creates a new
versioned effect; it does not delete deliveries or forge `sent`.

## 9. Release train

1. Deploy schema, codec, Store, roles, and integrity tests with zero production
   call points.
2. Deploy the stable-UUID Feishu adapter and dark effect preparation.
3. Enable live effect settlement for one exact task; recovery remains disabled.
4. Enable bounded recovery for the exact task and run the complete fault matrix.
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
