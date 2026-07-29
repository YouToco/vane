# Task Tool Invocation Isolation

Status: accepted architecture; runtime cutover in progress  
Owner correction: 2026-07-29

## Product decision

The task and its manual are the only user-facing information objects.
Monitoring one blogger, ten bloggers, a feed, a search or a page is written
directly in that manual.

The Agent compiles the manual into internal versioned Tool calls:

```text
task manual
  → sealed Tool calls
  → frozen run Tool calls
  → scoped observations and evidence
```

A Tool call is runtime implementation state, not a Source entity. Users do not
create, list, edit, enable, disable or delete Sources and never need an
internal Source/target/Tool-call ID. They edit the task in natural language.

## Why the legacy model is unsafe

`fetch_targets` combines three responsibilities:

1. global knowledge of acquisition capabilities;
2. a task's private query, URL, account and provider configuration;
3. mutable health such as cursor, next-fetch time and failure count.

`task_fetch_targets` is only a join to that shared row. It cannot prevent one
task from disabling, postponing or re-enabling another task, and it retains a
user's configuration outside the task lifecycle.

The fix is not to create a better Source entity. The fix is to remove Source
from the current domain and store only:

- immutable Tool definitions in trusted code;
- canonical Tool calls on the approved task head;
- task-scoped runtime state keyed by the exact Tool-call identity;
- frozen run Tool calls and their observation/evidence provenance.

## Internal isolation

Tool definition metadata is global, immutable and contains no user data. One
definition owns its name, argument schema, strict decoder/canonicalizer,
output kind, implementation version and availability.

Each approved Tool call retains:

- tenant, user and task scope;
- Tool name and definition version;
- canonical arguments;
- a stable invocation digest;
- derived provider route/configuration.

Cursor, retry timing, failure count and health are keyed to that exact
tenant/user/task/invocation digest inside the existing task adaptive-state
aggregate. Two users can monitor the same blogger without sharing mutable
state.

A run freezes the exact approved Tool calls and implementation versions it
will execute inside the existing immutable run snapshot. Content observation
and evidence records point to the run snapshot plus invocation digest, not to
a new Tool-call entity or a mutable global row.

The freeze contains two separate layers:

1. a logical binding of Tool name/version to platform, capability, output kind
   and allowed implementation contract;
2. the selected runtime capability, including its implementation revision and
   opaque credential generation references.

Current write validation uses the exact Tool argument decoder and rejects
unknown fields, duplicate keys, explicit invalid nulls and credential-shaped
extras before approval. Frozen readers validate the persisted logical binding
without consulting the mutable current Tool registry, so a retired Tool
remains replayable but cannot be selected for a new definition.

## Cutover sequence

1. **Containment.** Compiled runs stop reading and writing global due/health
   state. Implemented in `36d5011`.
2. **Tool-call authority.** Preserve Tool name and canonical arguments on the
   approved task head. Implemented in `61c0b0f`.
3. **Scoped state.** Store invocation-digest-keyed health/cursor state in the
   existing `task_adaptive_states` payload and freeze calls in the existing run
   snapshot. Do not add Source or Tool-invocation entity tables.
4. **Writer/runtime cutover.** New task creation, edit, run admission, fetch,
   health and candidate selection use only sealed/scoped Tool calls.
5. **Evidence cutover.** Card reconstruction and evidence rendering attribute
   observations to the exact run Tool call.
6. **v1 recovery.** Recoverable v1 runs read frozen legacy bytes and adapt them
   to run Tool calls without writing new global target/content state.
7. **Removal.** After v1 recovery and historical readers drain, delete the
   legacy global target projections and unscoped APIs.

Do not dual-write new private Tool arguments into the legacy global tables.
Do not backfill a historical observation to every task that happened to share
a target; only exact run/delivery/event evidence can establish provenance.

## Release gates

1. One task containing one blogger and one task containing multiple bloggers
   compile directly from their manuals without creating Source objects.
2. Two tenant/user/task scopes using the same Tool arguments cannot read or
   mutate each other's Tool-call state.
3. Approved head, run admission and frozen run calls match exactly by Tool
   version, canonical arguments and invocation digest.
4. Task edits replace the approved call set without mutating an already frozen
   run.
5. Current runtime execution writes zero rows to legacy global target tables.
6. Every new observation/evidence row traces to the same-scope run snapshot
   and invocation digest.
7. Tenant purge removes private Tool-call configuration/state/evidence without
   changing another tenant.
8. Static guards forbid Source/Tool-invocation entity tables, Source CRUD and
   current-runtime use of unscoped legacy target APIs.

7.10-B2 is accepted only when these gates pass. A renamed or hidden Source
entity does not satisfy the decision.
