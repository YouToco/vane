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
- strict argument decoding and canonicalization;
- availability and the reason for deliberate unavailability;
- output kind;
- the retained implementation and credential generations used by a run.

No second catalog or specification compiler may restate those facts.

The accepted Tool calls are stored on the approved task head without losing
the Tool name or canonical arguments. Materialized provider URL/config is
derived execution data, not the task's authority.

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

Every accepted content item and every delivery must trace to:

```text
tenant/user/task → approved Tool invocation → run snapshot
                 → canonical Tool arguments → observation/evidence
```

Display labels are not acquisition identity.

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
- reconstructing execution from mutable current Tool definitions;
- deleting provenance rows to make a migration pass;
- silently falling back from a missing retained Tool route;
- treating an empty/corrupt Tool-call set as a successful empty run.
