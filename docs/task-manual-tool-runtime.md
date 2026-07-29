# Task Manual → Tool Runtime

Status: active product contract  
Owner decision: 2026-07-29

## Product model

The user owns only a task and its manual. The Agent owns tool selection.

```text
task manual
    ↓ Agent selects versioned tools
sealed tool calls
    ↓ scheduled run
run snapshot
    ↓ authorized tool execution
evidence and delivery
```

There is no user-facing source collection, source catalog, fetch
specification, target projection, confirmation card, or internal-ID workflow.
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
run snapshot → tool invocation → canonical tool arguments → evidence
```

Display labels are not acquisition identity.

## Compatibility boundary

The deployed v1 snapshot and content provenance schema used positive source
IDs. Existing content rows and recoverable v1 runs still reference those IDs.
Until their evidence is migrated, the old physical acquisition tables are:

- read/write only for retained v1 execution and recovery;
- forbidden in the Agent/model protocol;
- forbidden as a current control-plane truth source;
- not product objects and not reusable by new user APIs.

This is a temporary compatibility root, not the target architecture.

## Removal sequence

The remaining data-plane cutover is intentionally ordered:

1. Seal canonical Tool calls directly in the approved task head.
2. Build new run snapshots only from that sealed head.
3. Add run/tool-invocation provenance to content appearances.
4. Route candidate selection and evidence rendering through the new
   provenance.
5. Drain or terminally resolve recoverable v1 runs.
6. Prove no current writer or reader uses the legacy target projection.
7. Drop the task-target projection.
8. Migrate retained historical evidence IDs and drop the global target table.

Steps 3–8 are one safety-critical schema cutover. The migration must fail
closed when any active head, recoverable run, content appearance or evidence
claim cannot be represented exactly.

## Forbidden regressions

- reintroducing `approved_fetch_plan` or `fetch_requirements` to Agent tools;
- creating account-level source CRUD or Source screens;
- asking the user for task, source or target IDs;
- reconstructing execution from mutable current Tool definitions;
- deleting provenance rows to make a migration pass;
- silently falling back from a missing retained Tool route;
- treating an empty/corrupt Tool-call set as a successful empty run.
