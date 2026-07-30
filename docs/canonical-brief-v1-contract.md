# Canonical RunOutcome / Brief V1 contract

Status: P1-E exact-task dual-channel implementation. Migration 064 adds a
durable pre-render Brief stage; migration 065 adds the least-privilege Brief
reader used by the task feed and feedback rebuild. The workflow may stage only
inside the nested compiled-runtime, RunOutcome, and durable push-effect
canaries. Feishu canonical rendering has a separate exact-task rollback switch
and remains off by default.

## Identity

- One `task_run_outcomes` row belongs to one immutable
  `task_run_snapshots.id`; a database admission trigger proves existence and
  binds the exact tenant, user, and task scope. Runtime roles cannot delete
  snapshots, and tenant purge deletes outcomes before snapshots.
- Source-free Tool V2 snapshots reuse this outcome fact and recovery lifecycle
  through a distinct typed reference boundary; they do not participate in
  canonical Brief staging.
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

Before stage, a scope-checked `SECURITY DEFINER` evidence reader binds the
delivery to its durable `content_items` row and to a source present in the
immutable run snapshot at delivery time. The Store compares title, body,
canonical URL, frozen source title, publication time, and discovery time. The
writer has no direct read grant on the global content tables or `deliveries`.
The complete encoded payload, including worst-case JSON escaping, is capped at
the same 32 MiB limit in Go and PostgreSQL.

It deliberately does not parse historical `body_md` or `card_json`, derive
importance tiers from score, invent an executive summary, or add an LLM call.
P1-C writes only when the structured inputs already exist at the
post-delivery/pre-render seam.

## Immutability and race fence

Migration 061 adds `push_batches.brief_state=open|sealed`; migration 064 adds
`canonical_brief_stages` with `staged→promoted|aborted`.

Every delivery insert takes a key-share lock on its batch through a database
trigger. P1-C stage takes an update lock, verifies that its insight IDs equal
the complete durable delivery set, stores the immutable canonical draft as
exact `BYTEA`, then changes `open→sealed` in the same transaction. Therefore:

- an earlier delivery commits first and must be included;
- a later new delivery is rejected after seal;
- an exact `(batch_id, content_item_id)` response-loss retry may still reach
  the retained unique arbiter and recover the existing row;
- a failed stage transaction rolls the seal back.

The Prepare result carries the exact physical `push_batches.id`; Push never
reconstructs that identity from mutable events. A zero-insight plan seals that
same batch without creating a stage. Its receipt path proves the exact
run/snapshot/batch scope, effect authority, `done` state, and absence of both
deliveries and effects. A lost empty receipt is therefore replayed as
`quiet/partial`, not guessed from a newly planned mutable event set.

P1-C still uses the legacy renderer and existing durable effect sender. P1-E
may replace only the not-yet-created durable card bytes for its exact selected
task. Existing effect rows always retain authority and replay their stored
bytes; disabling P1-E changes only future plans and never rewrites or abandons
an already-prepared provider effect.

After Push, the common RunOutcome claim transaction resolves the stage:

- `content` atomically inserts the final immutable `brief_snapshots` row and
  moves the stage to `promoted`;
- every non-content terminal result moves it to `aborted` without a Brief;
- no-stage P1-B claims are a strict no-op;
- a deferred database trigger rejects commit of a finalized outcome with an
  unresolved stage.

Workflow and recovery both use this same transaction path. Promotion trusts
only the already-validated stage bytes and digest, never live content/source
evidence that could drift during the provider call. Exact response-loss replay
returns the same outcome, Brief ID, payload digest, and resolution time.
If Push succeeded but its response or finalizer failed, the common claim path
normalizes a generic `failed` receipt to `content/partial` only when the exact
stage is already promoted, or to `quiet/partial` only when the exact sealed
empty receipt is already durable. Different evidence remains a conflict.

Prepared P1-C effects are not eligible for recovery send while their stage is
pending or aborted. The recovery coordinator may claim them only after the
exact stage is promoted by a finalized `content` outcome. Batches with no P1-C
stage retain the P1-B recovery contract.

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
- insert immutable pre-render stages and update only their terminal state;
- update only `push_batches.brief_state`;
- read only `push_batches.status`, `idempotency_key`, and
  `delivery_authority` for the exact sealed-empty receipt.

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

Migration 061 Up and Down drain schema-aware writers and take
producer-compatible access-exclusive locks. Migration 063 Down takes the same
exclusive admission fence and refuses while any outcome remains pending.
A Begin admitted after that downgrade must also prove the recovery reader is
still installed before inserting, so neither side of the fence can strand a
pending marker. Migration 061 Down still refuses to destroy any outcome or
Brief evidence.

Migration 064 Up/Down joins the same schema fence and producer-compatible lock
order. Down refuses while any stage exists or an exact pending Outcome still
owns sealed, zero-evidence empty-batch state that depends on the P1-C receipt
functions. A sealed-empty batch becomes safe to downgrade only after its
receipt and terminal Outcome are both durable. Store checks the stage
capability only after taking the shared fence: P1-C fails closed if 064 is
absent, while P1-B finalization remains valid and resolves no stage. Database
triggers admit stage insert only for an exact pending outcome and sealed batch,
promotion only for the exact finalized content outcome and Brief, and abort
only for finalized non-content outcomes. Final Brief payload digests and stage
request digests are validated against their canonical envelopes by Store; the
stage request digest is additionally bound to its exact stored draft bytes by
PostgreSQL SHA-256.

The independent rollout keys are `run_outcome_enabled`,
`run_outcome_canary_schedule_id`, and `run_outcome_allow_all`. Selection is
valid only inside the compiled-runtime rollout. Turning it off stops new marker
creation but recovery remains active. Initial production rollout is one exact
compiled task; `allow_all` stays false.

P1-C adds `canonical_brief_enabled`,
`canonical_brief_canary_schedule_id`, and `canonical_brief_allow_all`.
Current validation permits only an exact-task canary, never allow-all, and
requires that task to be simultaneously selected by compiled runtime,
RunOutcome, fresh durable push effects, and push-effect recovery. Turning the
selection off stops new P1-C Actions; an already-frozen P1-C history can still
complete its stage/effect commands. Brief API, model calls, visible cards,
message count, and sending authority remain unchanged.

P1-E adds `canonical_brief_renderer_canary_schedule_id`. Empty is the exact
rollback state. A non-empty value must equal the canonical Brief writer,
RunOutcome, compiled runtime, fresh push-effect, and push-effect recovery
canaries. It also requires a path-free HTTP(S) `dashboard.origin`. No allow-all
renderer key exists in P1-E.

Both P1-C Temporal changes are versioned. The Prepare-result wire-version
marker is recorded before the Prepare Activity command, so a pre-v2 execution
can resume safely even when its history frontier is exactly the completed old
Prepare result.

## P1-D authenticated read model

`GET /api/schedules/{task_id}/briefs` pages immutable whole Briefs by
`(generated_at,id)` descending. A page boundary never splits the ranked
insights of one Brief. The opaque V1 cursor carries and validates the exact
task ID, timestamp, and Brief ID, so a cursor from one task is rejected on
another task.

The response also returns `latest_check` from the newest finalized RunOutcome.
It is deliberately independent from `items[0]`: a newer quiet, failed, or
interrupted run advances the check projection without erasing or relabeling the
most recent non-empty Brief. Historical legacy deliveries are not guessed into
canonical Briefs.

The Store starts an exact tenant/user/task transaction, takes the shared tenant
purge-admission root, then locks membership and schedule in the established
task-creation order. It sets both RLS identities and enters
`vane_brief_reader`. That NOLOGIN/NOINHERIT/NOBYPASSRLS role can read only the
required columns of `brief_snapshots`, `task_run_outcomes`, and `feedbacks`;
its feedback access has an additional restrictive exact-user policy. It cannot
read deliveries, content items, sources, schedules, memberships, or any
sequence and has no write privilege. Every payload is decoded and its complete
relational envelope, canonical JSON bytes, request digest, payload digest, and
exact scope are revalidated before it reaches HTTP.

Feedback remains a live projection keyed by the frozen delivery/Insight ID and
is not added to the Brief digest. The projection matches the Feishu card state:
latest `interested|not_interested` wins, while `misjudged` and requested
deep-dive are durable booleans; historical feedback rows are not presented as
simultaneous current states. Web renders `body_md` without raw HTML,
external images, credential-bearing URLs, or non-HTTP(S) links. The API and
Web read surface do not create model calls, messages, effects, or sending
authority.

P1-D has an explicit additive deployment order: migration 065 and the backend
route must be healthy before the Web bundle is released; rollback reverses that
order and removes the Web bundle before the route or migration. The P1-D Web
bundle is not compatible with a backend still at migration 064, and the
release gate verifies the authenticated endpoint before publishing Web.

## P1-E Feishu prefix projection

For a newly prepared exact-task effect, Feishu consumes the staged canonical
Brief bytes rather than rebuilding title, body, source, URL, or time from live
content rows. The stage is the immutable byte envelope later promoted to the
single `BriefV1`; Push validates its complete delivery IDs and order before
rendering.

The card contains at most the first three ranked Insights. Prefix planning
checks the 28 KiB provider budget with the worst-case transient feedback form
opened on each possible clicked Insight while every other visible Insight
carries its maximum persistent feedback-state line; this keeps every later
callback rebuild inside the same hard limit. If the card would exceed that
budget, the visible prefix shrinks without reordering or truncating an Insight;
a single oversized Insight fails before provider send. The header reports the
whole Brief count, and the footer says “另有 N 条” with an exact task deep link
to the Web `TaskBriefFeed`. No score or live platform label is injected into
the canonical card.

The one durable provider effect still receipts the complete Brief delivery
set. This is Phase 1's documented compatibility bridge while `InsightV1.ID`
remains the delivery ID and channel delivery is not yet physically split. The
card bytes carry the batch ID, total count, visible prefix count, and Web URL
in server-generated callback metadata. A feedback click reloads the promoted
Brief through `vane_brief_reader`, proves the exact user/delivery/batch scope,
and rebuilds the same frozen ordered prefix with current feedback state. The
reader takes the shared tenant purge-admission root before any delivery/batch
row lock, then repeats the exact scope lookup under lock. The callback Web URL
must exactly equal the canonical task URL reconstructed from the trusted
configured dashboard origin; stored card bytes cannot select another host. If
promotion is not yet visible, metadata differs, or a defensive rebuilt-card
size check fails, the feedback write succeeds but the existing card is left
unchanged; it never falls back to mutable content/source rows.

P1-E adds no Workflow command and no model call. Pre-P1-E histories and every
legacy/ad-hoc task retain the old renderer. The Activity-level selector is safe
across retry because provider bytes are created before any external send and
then replay only from the immutable effect row. If a legacy multi-chunk effect
plan was only partially prepared before the renderer canary changed, its
existing plan identity wins and the retry completes the original legacy
chunks; it never attempts to replace them with one canonical effect.
