# Canonical RunOutcome / Brief V1 contract

Status: P1-B exact-run lifecycle. Migration 062 adds recovery and the workflow
may create/finalize RunOutcome only for the independent compiled-runtime
canary. Brief freeze/read, API, renderer, and every user-visible surface remain
dark.

## Identity

- One `task_run_outcomes` row belongs to one immutable
  `task_run_snapshots.id`; a database admission trigger proves existence and
  binds the exact tenant, user, and task scope. Runtime roles cannot delete
  snapshots, and tenant purge deletes outcomes before snapshots.
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
Workflow and recovery submit `RunOutcomeClaimV1`, which contains neither time
nor digest. Under the row/advisory lock, Store reads database time, seals the
digest, and performs the one-way update. Identical semantic retries replay the
stored outcome even after a lost response; a different terminal claim
conflicts. A database trigger admits only `pending→finalized` and rejects every
update to an already-finalized row, so immutability does not depend on callers
using the Store.

For P1-B Actions, `BeginRunOutcomeV1` is the first command after authorized
`PrepareRun` and precedes Evolve, Fetch, LLM, notice, and push Activities.
Fetch, Score, and CardGen use outcome-aware Activity variants without changing
their business payload. Every normal exit finalizes. Error and cancellation
paths attempt finalization on a disconnected workflow context while preserving
the original Temporal error. Raw provider/driver chains are never persisted.

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
The constrained evidence reader returns every scoped delivery, including rows
whose content/source evidence is missing; Freeze rejects any incomplete row.
Delivery batch/tenant/user scope is immutable after insertion. Source-bearing
fields may be corrected while the batch is open, but become immutable at seal;
receipt fields such as status, card, message ID, and sent time remain updatable
after the batch is sealed.

## Permissions and rollout

`vane_brief_writer` is NOLOGIN, NOINHERIT, NOBYPASSRLS, unrelated to
`vane_app`, and settable only by the migration owner. If the cluster-wide role
already exists, the migration rejects owned objects or any preexisting ACL in
the current database before applying the whitelist. Any effective cluster-wide
parameter privilege, including one inherited from `PUBLIC`, is also rejected
because it could disable the trigger boundary. The
migration never uses cluster-wide `DROP OWNED`, so privileges on another
database are not mutated. It can:

- execute the exact-scope run identity reader and read only the batch identity
  columns needed for admission;
- execute the scope-checked delivery evidence reader without direct access to
  global content tables;
- insert and finalize outcomes;
- insert immutable Brief snapshots;
- update only `push_batches.brief_state`.

It cannot delete/truncate, mutate Brief payloads, send a message, call a model,
or enter another runtime role. Tenant and exact-user restrictive boundaries
apply to both new tables, the batch evidence table, and the constrained run and
delivery evidence readers.

`vane_run_outcome_recovery` is a separate NOLOGIN, NOINHERIT,
NOBYPASSRLS role. It has no table access or write authority and can execute
only a fixed two-minute-stale, 100-row-maximum `(created_at,id)` keyset reader.
That reader returns the pending marker plus exact Temporal WorkflowID/RunID.
The recovery runner performs one 30-second startup pass, then runs every 30
seconds with four workers and five-second Temporal queries. Running executions
are skipped. Exact terminal executions converge through the normal writer CAS;
a completed execution without a terminal receipt becomes
`failed/outcome_missing_terminal_receipt`. Shutdown stops admission and drains
already-admitted recovery work.

Up and Down first drain schema-aware writers and take producer-compatible
access-exclusive locks. Migration 062 Down refuses while any outcome remains
pending; Migration 061 Down still refuses to destroy any outcome or Brief
evidence.

The independent rollout keys are `run_outcome_enabled`,
`run_outcome_canary_schedule_id`, and `run_outcome_allow_all`. Selection is
valid only inside the compiled-runtime rollout. Turning it off stops new marker
creation but recovery remains active. Initial production rollout is one exact
compiled task; `allow_all` stays false. Brief, API, renderer, model, message,
and sending authority remain unchanged.
