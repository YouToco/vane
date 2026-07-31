# Task Manual → Tool Runtime

Status: active product contract  
Owner decision: 2026-07-29

## Product model

The user owns only a task and its manual. The Agent owns tool selection.

```text
task manual
    ↓ Agent selects versioned tools
sealed Tool calls
    ↓ scheduled run
run snapshot
    ↓ authorized tool execution
evidence and delivery
```

There is no Source product entity, source collection, source catalog,
confirmation card, or internal-ID workflow. Monitoring one blogger or many
bloggers is expressed directly in the task manual. The Agent compiles that
manual into one or more internal Tool calls. Those calls are implementation
state of the task, not separately managed user objects.

True ambiguity is resolved with one targeted natural-language question.
Otherwise the Agent executes directly.

Natural-language edits are one user operation even when the request changes
several fields of the same task. The Agent first resolves the task from the
user's remembered name, schedule, topic or purpose, then submits one complete
definition edit. A separate side-effect-free semantic adjudication first routes
the current owner turn to edit, delete, run, create, profile update, one-off
research, or an answer-only request such as advice, a hypothetical, negation,
or cancellation.
Natural first-profile intake, such as directly answering with an industry,
role, and interests, uses the same semantic route; the profile store still
rejects overwriting an existing profile.
Lexical matches never authorize deletion. The edit then requires a second
independent model decision to call the bound write tool. Other explicit actions
expose only their matching side-effect capability; unrelated writes remain
closed. Answer-only decisions and any adjudication error fail closed into a
side-effect-free turn: all writes, deliveries, activation changes, and billable
tools are absent at both declaration and execution boundaries. The isolated edit lane
exposes only `list_schedules` followed by `edit_task_definition`; a requirement
that future runs open official pages does not authorize ad-hoc web research
during the edit. The user is never asked for an internal task ID or to split one
task/manual edit into smaller requests.
The lookup phrase must be a specific contiguous phrase from the authenticated
request, the lookup must resolve exactly one owned task, and the durable edit
must target that exact resolved ID. Zero or multiple matches produce one
readable clarification and expose no write tool.

## Current writer contract

`create_schedule` accepts:

- `spec`: when to run;
- `intent`: the complete task manual;
- `tool_calls`: one or more `{name, arguments}` acquisition Tool calls;
- optional delivery/observation preferences.

The model must never produce:

- a source or target object;
- a fetch plan;
- synthetic internal URLs;
- selectors, provider configuration, credentials, or internal IDs.

Each acquisition Tool owns one definition containing:

- its model-visible name and arguments;
- its model-visible description and external-locator policy;
- strict argument decoding and canonicalization;
- availability and the reason for deliberate unavailability;
- output kind;
- the retained implementation and credential generations used by a run.

No second catalog or specification compiler may restate those facts.
`create_schedule` embeds a generated discriminated union from these
definitions; the creation controller calls the same Tool decoder and
materializer instead of maintaining a second argument switch.

The accepted Tool calls are stored on the approved task head without losing
the Tool name or canonical arguments. Materialized provider URL/config is
derived execution data, not the task's authority.

## External-fact authority

Model weights are not a current truth source for mutable external facts.
The Agent may translate the user's goal into semantic search terms, but it
must not infer a domain, URL, social handle, UID, username or other external
locator from its training data.

An external locator may be sealed only when it appears explicitly in the
authenticated current user request. The creation controller enforces this
against the owner request itself, never against model-produced `intent`.
If no locator was supplied:

- use a live semantic-search Tool call when that Tool does not require a fixed
  locator; or
- ask one targeted question when the selected Tool requires an exact locator.

The scheduled run executes the sealed Tool call against the live network.
Therefore a model-authored semantic query is an instruction to a live Tool,
not a model-authored claim about the current web. A legacy
manual-to-`fetch_plan` LLM translator is forbidden: it duplicates the Tool
contract and can freeze stale model knowledge as execution authority.

## Isolation invariants

Tool arguments, provider configuration, cursor, failure count and next-run
state belong to one exact tenant/user/task/Tool-call identity. They are
stored inside the existing approved-definition/adaptive-state/run-snapshot
aggregates and follow the task's ownership and purge rules. They do not get a
new independently addressable table or lifecycle.

Two users may monitor the same public blogger. Vane may de-duplicate public
content bytes by canonical content identity, but the following never cross
that boundary:

- private query terms, URLs, credentials or Tool arguments;
- Tool-call health and failure state;
- acquisition cursors and retry timing;
- content appearance and evidence attribution.

Deleting a task removes its current Tool calls, state and private configuration.
Historical delivery/evidence retention follows the explicit evidence policy;
it is never achieved by retaining an unscoped global acquisition row.

## Run invariants

Before a paid or external effect:

1. tenant, owner and task authorization are current;
2. the task manual and exact Tool calls are sealed;
3. the run references an immutable snapshot;
4. the exact versioned Tool route exists;
5. quota is authorized beside the effect.

Historical snapshots never consult the current Tool definitions. A current
Tool may change only by creating a new versioned route; it cannot reinterpret
old snapshot bytes.

The wire boundary is explicitly split:

- retained Source execution uses `vane.run-snapshot-ref/v1` and the frozen
  `RunSnapshotRef` reader;
- Source-free Tool execution uses `vane.run-snapshot-ref/v2` and the distinct
  `RunSnapshotRefV2` type.

A V1-only authorization/effect path must never accept the V2 Go type. The V2
payload freezes the Approved version/digest, Adaptive version/digest and its
exact Approved basis, plus one logical Tool contract and selected runtime
capability per invocation. The capability includes implementation revision and
opaque credential generation references, never credential values.

Migration 075 separates database admission as well: ref/v1 retains the old
shadow marker/sidecar fence, while ref/v2 requires an active Approved V2 head
and the exact current Adaptive V2 basis and writes no legacy shadow.

The registered `PrepareToolRunV2` Activity is the dark run-start boundary for
this protocol. It first recovers an exact committed ref by Temporal RunID,
before reading current policy, and then validates the frozen definition,
adaptive basis, logical Tool contracts and selected capability routes. It has
its own Store interface and cannot call retained Source-ID effects. No
Schedule Action or Workflow selects the V2 runtime label until Tool execution
and observation provenance are complete.

Every accepted content item and every delivery must trace to:

```text
tenant/user/task → approved Tool invocation → run snapshot
                 → canonical Tool arguments → observation/evidence
```

Display labels are not acquisition identity.

### Live evidence qualification

The Tool runtime does not treat a successful search request as a publishable
result. After canonical content dedup and before score/card generation, it
applies the sealed observation policy and task manual to the current run's
live Tool candidates.

- The qualifier receives the task manual, current deterministic observation
  window and only the canonical candidate content fetched in this run.
- It has no Tool surface and cannot use model memory as evidence.
- If the manual requires an official original, a matching event must cite the
  official page from the current candidate set first. Media coverage, reposts
  and results without live page text alone cannot qualify an event.
- If the manual requires cross verification, the event must cite at least one
  different current-run candidate after the primary. Its publication time may
  differ, but it must remain inside the deterministic observation window.
- Missing cross evidence is never synthesized from lexical overlap. The
  qualifier must cite the pair it judged to describe the same release, and
  deterministic validation rejects a secondary page whose title does not
  identify that release.
- `no_match`, uncertain model output, malformed citations and unavailable
  evidence all stop before score, card generation and push.
- A qualified Tool-task event is already matched to explicit user intent in
  the task manual. The retained `ScoreToolCandidatesV2` Temporal activity
  therefore assigns a deterministic high score (subject only to sealed
  observation penalties); it does not call the generic profile-interest
  scorer. Frozen strictness and Top-N still apply in selection. This prevents
  an empty or unrelated profile from overruling the task the user explicitly
  asked Vane to monitor, while preserving replay-compatible activity names.
- Card generation follows explicit output fields in the task manual. Its
  generic three-part layout is only a default. The model cannot author URLs:
  official and cross-evidence links are rendered from the admitted canonical
  evidence bundle, and a model-authored URL fails closed.
- The ordered evidence manifest is persisted with each Tool V2 delivery.
  Application and database admission both prove every content/invocation pair
  belongs to the exact frozen run snapshot; the push boundary re-derives
  presentation metadata from canonical content before any external effect.
  A new qualification run freezes whether evidence is required; required
  evidence cannot be omitted or later erased. Pre-qualification Temporal
  histories deserialize the new flag as false and retain replay compatibility.
- Chinese and English manuals use the same semantic output contract. Change
  and impact fields may be written in either approved language; official and
  cross-evidence URLs remain system-owned in both.

This keeps responsibilities small: acquisition Tools observe the live world;
the strong model performs bounded semantic judgment over those observations;
deterministic code enforces ownership, window, citation identity, quota and
the no-push terminal gates.

### Run outcome truth

Every newly authorized Tool V2 run creates one `task_run_outcomes` marker for
its exact immutable snapshot before any provider Tool, paid model or delivery
effect is scheduled. The same durable result vocabulary used by the retained
compiled runtime is authoritative:

- a delivered card finalizes as `content`;
- a fully evaluated run with nothing publishable finalizes as `quiet`;
- a provider, storage or delivery failure finalizes as `failed`;
- cancellation finalizes as `interrupted`.

`source_coverage` means Tool acquisition coverage for Tool V2; it is `complete`
only after every frozen invocation has returned a valid receipt. `processing`
is `partial` when qualification is uncertain or quota prevents a required
stage, even though the user-facing terminal remains a successful silent run.
Normal no-match and deterministic empty gates are `quiet` with complete
coverage and processing. Finalization is an idempotent claim bound to database
time, and the existing stale-marker recovery path applies without a second
Tool-specific status table.

## Compatibility boundary

The deployed v1 snapshot and content provenance schema used positive global
source IDs. Existing content rows and recoverable v1 runs still reference those
IDs. Until their evidence is migrated, the old physical acquisition tables are:

- read/write only for retained v1 execution and recovery;
- forbidden in the Agent/model protocol;
- forbidden as a current control-plane truth source;
- not product objects and not reusable for new task Tool-call compilation;
- forbidden from receiving new private query, URL or configuration data.

This is a temporary compatibility root, not the target architecture.

The words `source` and `sources` may still appear in retained V1 replay,
historical evidence and human-readable evidence lists. They do not imply a
current Source product entity. Deleting those compatibility readers before
their durable histories are drained would destroy replay or evidence; adding
new current writers to them is forbidden.

## Deliberate simplification log

The runtime configuration must expose only controls that change behavior.
The following no-op knobs were removed in 7.10-B2:

- `agent.token_budget_daily`: never enforced and duplicated the durable tenant
  quota mechanism;
- `agent.endpoint_msg_cap` and `agent.exa_msg_cap`: ignored after the single
  per-message Tool fuse became authoritative.

The rolling daily provider caps remain because they protect real billable
effects. Historical profile quota columns remain data compatibility fields,
not runtime controls.

One operational debt remains intentionally separate from this behavior patch:
the exact-task release train is represented by many nested canary settings.
They currently protect independently recoverable historical stages, so simply
deleting them would weaken rollback safety. The target is one operator-facing
release selector that derives the internal stage scopes atomically, plus
quarantine of a failed recovery effect instead of process-fatal startup.

## Removal sequence

The remaining data-plane cutover is intentionally ordered:

1. Seal canonical Tool calls directly in the approved task head.
2. Key Tool-call runtime state inside `task_adaptive_states` by the exact
   invocation digest; do not create a new entity table.
3. Build new run snapshots only from those sealed Tool calls.
4. Add task/run-snapshot/invocation-digest provenance to content observations.
5. Route candidate selection, card reconstruction and evidence rendering
   through that provenance.
6. Stop all current writers from materializing user configuration in the
   legacy global table.
7. Drain or terminally resolve recoverable v1 runs.
8. Migrate retained historical evidence IDs and drop the legacy global target
   and task-target compatibility projections.

Steps 3–8 are one safety-critical schema cutover. The migration must fail
closed when any active head, recoverable run, content appearance or evidence
claim cannot be represented exactly.

## Forbidden regressions

- reintroducing `approved_fetch_plan` or `fetch_requirements` to Agent tools;
- creating a Source entity, Source CRUD, Source screen or Source ID workflow;
- sharing Tool-call arguments, configuration or health across
  tenant/user/task/call boundaries;
- asking the user for task, source or target IDs;
- inferring domains, URLs, handles or account IDs from model weights and
  freezing them as task authority;
- reviving a separate model-to-`fetch_plan` compiler beside acquisition Tool
  definitions;
- reconstructing execution from mutable current Tool definitions;
- deleting provenance rows to make a migration pass;
- silently falling back from a missing retained Tool route;
- treating an empty/corrupt Tool-call set as a successful empty run.
