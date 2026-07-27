# Canonical RunOutcome / Brief V1 contract

Status: P1-A dark substrate. Migration 061 and Store methods exist, but no
workflow, API, renderer, scheduler, or startup path calls them.

## Identity

- One `task_run_outcomes` row belongs to one immutable
  `task_run_snapshots.id`; tenant, user, and task are bound by a composite FK.
- One finalized content outcome may own one `brief_snapshots` row.
- A Brief binds one exact compiled `push_batches` row from the same run.
- `InsightV1.ID` is the existing `deliveries.id`. Phase 1 therefore preserves
  the current feedback foreign key and does not dual-write feedback identity.
- `BriefV1.ID` is database-generated. The request digest excludes that ID for
  response-loss replay; the payload digest includes it and every payload field.

## Outcome semantics

`result`, `source_coverage`, and `processing` are independent:

| result | source coverage | processing | honest meaning |
|---|---|---|---|
| `content` | complete/partial | complete/partial | reliable content exists; either partial axis means the issue may be incomplete |
| `quiet` | complete | complete | a complete check found no qualifying change |
| `quiet` | any partial | any partial | no reliable change found, but never claim a complete “no change” |
| `failed` | complete/partial | complete/partial | the run could not form a reliable conclusion |
| `interrupted` | complete/partial | complete/partial | cancellation/termination stopped conclusion formation |

The Store creates a `pending` marker and finalizes it with a single CAS.
Identical finalization retries replay the stored outcome; a different terminal
claim conflicts. A database trigger admits only `pending→finalized` and rejects
every update to an already-finalized row, so immutability does not depend on
callers using the Store. P1-B will own lifecycle wiring and termination recovery.

## Brief payload

The V1 payload freezes:

- exact outcome, run snapshot, batch, tenant, user, and task identity;
- `generated_at`;
- the complete delivery set for that batch;
- explicit contiguous `rank_position`;
- title and the existing channel-neutral Markdown body;
- canonical HTTP(S) source URL, source title, publication time, and discovery
  time.

Before seal, a scope-checked `SECURITY DEFINER` evidence reader binds the
delivery to its durable `content_items` row and to a source present in the
immutable run snapshot at delivery time. The Store compares title, body,
canonical URL, frozen source title, publication time, and discovery time. The
writer has no direct read grant on the global content tables or `deliveries`.
The complete encoded payload, including worst-case JSON escaping, is capped at
the same 32 MiB limit in Go and PostgreSQL.

It deliberately does not parse historical `body_md` or `card_json`, derive
importance tiers from score, invent an executive summary, or add an LLM call.
P1-C may write only when the structured inputs already exist at the
post-delivery/pre-render seam.

## Immutability and race fence

Migration 061 adds `push_batches.brief_state=open|sealed`.

Every delivery insert takes a key-share lock on its batch through a database
trigger. Brief freeze takes an update lock, verifies that its insight IDs equal
the complete durable delivery set, then changes `open→sealed` and inserts the
Brief in the same transaction. Therefore:

- an earlier delivery commits first and must be included;
- a later new delivery is rejected after seal;
- an exact `(batch_id, content_item_id)` response-loss retry may still reach
  the retained unique arbiter and recover the existing row;
- a failed Brief transaction rolls the seal back.

`deliveries(batch_id,tenant_id,user_id)` also has a composite FK to the batch
scope. Migration 061 audits existing rows before adding it, and the insert
trigger binds all three columns. RLS therefore cannot hide a poisoned
cross-tenant or cross-user row and let a filtered subset appear complete.

## Permissions and rollout

`vane_brief_writer` is NOLOGIN, NOINHERIT, NOBYPASSRLS, unrelated to
`vane_app`, and settable only by the migration owner. If the cluster-wide role
already exists, the migration rejects owned objects and removes every direct
ACL in the current database before applying the whitelist. It can:

- read only the run and batch identity columns needed for admission;
- execute the scope-checked delivery evidence reader without direct access to
  global content tables;
- insert and finalize outcomes;
- insert immutable Brief snapshots;
- update only `push_batches.brief_state`.

It cannot delete/truncate, mutate Brief payloads, send a message, call a model,
or enter another runtime role. Tenant and exact-user restrictive RLS policies
apply to both new tables and all three evidence tables.

The Down migration takes access-exclusive locks and refuses to destroy any
outcome or Brief evidence. Production remains default-off until later batches
add versioned lifecycle wiring, exact-task canary, explicit rollback, and the
external ICP rollout Gate.
