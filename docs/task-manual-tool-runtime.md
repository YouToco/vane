# Task Manual → Tool Runtime

Status: active product contract  
Owner decision: 2026-07-29

## Product model

The user owns only a task and its manual. The Agent owns tool selection.

```text
task manual
    ↓ Agent selects versioned tools
task-owned Source instances
    ↓ scheduled run
run snapshot
    ↓ authorized tool execution
evidence and delivery
```

There is no account-wide source collection, source catalog, confirmation card,
or internal-ID workflow. A Source is still a first-class task concept: it is
one task-owned, canonical invocation of a Tool, such as “monitor posts from
blogger X”. The user manages it by editing the task manual in natural language,
not by operating a separate CRUD console. True ambiguity is resolved with one
targeted natural-language question. Otherwise the Agent executes directly.

The terms are deliberately distinct:

- a **Tool** is a versioned capability Vane knows how to execute;
- a **Source** is one tenant/user/task-owned configuration of that Tool;
- a **run snapshot** freezes the exact Source and Tool route used by one run;
- an **appearance** records that content was observed through that Source.

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

The accepted Tool calls are stored on the task Source without losing the Tool
name or canonical arguments. Materialized provider URL/config is derived
execution data, not the task's authority.

## Isolation invariants

Source identity, title, Tool arguments, provider configuration, cursor,
status, failure count and next-run state belong to exactly one
tenant/user/task. They must be protected by the same ownership and purge rules
as the task.

Two users may monitor the same public blogger. Vane may de-duplicate public
content bytes by canonical content identity, but the following never cross
that boundary:

- private query terms, URLs, credentials or Tool arguments;
- source enable/disable and failure state;
- acquisition cursors and retry timing;
- display metadata;
- content appearance and evidence attribution.

Deleting a task removes its current Source instances and private configuration.
Historical delivery/evidence retention follows the explicit evidence policy;
it is never achieved by retaining an unscoped global Source row.

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

Every accepted content item and every delivery must trace to:

```text
tenant/user/task → task Source → run snapshot
                 → Tool invocation → canonical Tool arguments → appearance/evidence
```

Display labels are not acquisition identity.

## Compatibility boundary

The deployed v1 snapshot and content provenance schema used positive global
source IDs. Existing content rows and recoverable v1 runs still reference those
IDs. Until their evidence is migrated, the old physical acquisition tables are:

- read/write only for retained v1 execution and recovery;
- forbidden in the Agent/model protocol;
- forbidden as a current control-plane truth source;
- not product objects and not reusable for new task Source materialization;
- forbidden from receiving new private query, URL or configuration data.

This is a temporary compatibility root, not the target architecture.

## Removal sequence

The remaining data-plane cutover is intentionally ordered:

1. Seal canonical Tool calls directly in task-owned Source rows and the
   approved task head.
2. Give Source rows tenant/user/task ownership, RLS/purge behavior and
   independent health/cursor state.
3. Build new run snapshots only from those sealed Source rows.
4. Add task-Source/run/Tool-invocation provenance to content appearances.
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
- creating account-level Source CRUD or Source screens;
- sharing Source identity, configuration, health or display metadata across
  tenant/user/task boundaries;
- asking the user for task, source or target IDs;
- reconstructing execution from mutable current Tool definitions;
- deleting provenance rows to make a migration pass;
- silently falling back from a missing retained Tool route;
- treating an empty/corrupt Tool-call set as a successful empty run.
