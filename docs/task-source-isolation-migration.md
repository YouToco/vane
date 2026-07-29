# Task Source Isolation Migration

Status: accepted architecture; runtime cutover in progress  
Owner correction: 2026-07-29

## Decision

Vane is an intelligence system, so monitoring a blogger, feed, search or page
is a real Source. Source was incorrectly conflated with both global capability
metadata and a globally de-duplicated fetch target.

The corrected model is:

```text
task manual
  → versioned Tool definition
  → tenant/user/task-owned Source revision
  → frozen run Source
  → scoped content appearance and evidence
```

The user manages Sources by editing a task in natural language. There is no
account-wide Source CRUD screen, confirmation card or requirement to know an
internal ID.

## Why the legacy model is unsafe

`fetch_targets` currently combines three different responsibilities:

1. global knowledge of which acquisition capabilities Vane implements;
2. a user's configured query, URL, account and display metadata;
3. mutable scheduling health such as status, cursor, next-fetch time and
   failure count.

`task_fetch_targets` is only a join to that shared row. It cannot prevent:

- one tenant's failures disabling or postponing another tenant's task;
- one task edit re-enabling or resetting another task;
- private query/URL/config data surviving task or tenant deletion;
- a global URL uniqueness collision becoming a cross-tenant existence
  side-channel;
- card reconstruction displaying another task's first-seen Source metadata.

Adding `tenant_id` to an already shared row is not a safe migration. The old
row may belong to many tasks, and assigning one owner would either guess or
break frozen v1 references.

## New generation

### Tool definitions

Global, immutable, versioned and free of user data. One definition owns:

- model-visible Tool name and argument schema;
- strict decoder and canonicalizer;
- platform/capability and output kind;
- implementation and credential-reference generation;
- availability and the reason for deliberate unavailability.

Provider credentials are never stored in a Source or run snapshot. A Source
freezes only an authorized credential reference and generation.

### Task Sources

One Source belongs to exactly one tenant, user and task. It stores a canonical
Tool invocation and the derived execution configuration. Edits create a new
revision instead of mutating the identity used by an older run.

Sensitive Tool arguments and provider configuration must not be logged or
copied to an unscoped table. The database enforces ownership with composite
foreign keys and RLS; uniqueness is scoped to the task.

### Source state

Status, cursor, next-fetch time, last-fetch time and failure count are scoped
to a task Source. A failure or recovery action cannot affect another Source,
even when both monitor the same public blogger.

### Run Sources

A run binds append-only Source revisions to its immutable snapshot. It freezes
the Tool definition version, canonical arguments or their protected execution
form, and the acquisition identity digest. Runtime reads the frozen run Source;
current Source state is consulted only for scoped health and current
authorization.

### Content and evidence

New content records and appearances are scoped at least to tenant and user.
Every appearance links the task Source and run Source that observed it. Every
evidence claim links the exact run and Tool version.

Global public-content caching is a separate, explicit optimization. It may
share only content that is proven public; it never shares Source arguments,
configuration, health, appearance or evidence.

## Cutover sequence

1. **Containment.** Compiled runs stop reading and writing global due/health
   state. This shipped on the branch in `36d5011`.
2. **New schema.** Add scoped Source, state, run Source, content appearance and
   evidence tables with business-role RLS and composite ownership constraints.
3. **Writer cutover.** New task creation/edit and run admission write only the
   new generation. Revoke current application writes to legacy global
   Source/content tables. Do not dual-write private Source data.
4. **Active-task migration.** Reconstruct task Sources only from immutable
   approved definitions. Pause/quarantine empty, unknown or inconsistent
   tasks; never infer missing ownership or Tool arguments.
5. **Runtime cutover.** Fetch, health, candidate selection, card reconstruction
   and evidence rendering use task/run Source provenance.
6. **v1 recovery adapter.** Recoverable v1 runs read their frozen bytes,
   materialize a run-bound legacy Source and write new scoped content/evidence.
   They do not require mutable legacy rows and do not create new global
   content.
7. **Archive and removal.** When no recoverable v1 run or historical reader
   remains, move legacy tables to an explicit read-only v1 archive and remove
   unscoped APIs/types from the current runtime.

Historical appearances must not be copied to every task that shared a target.
Backfill only when an exact run, delivery or event proves that task observed
the content.

## Release gates

1. Two tenants × two users × two tasks: wrong Source/run/content IDs cannot be
   read or written through the business database role.
2. For every current task, approved Source set, stored task Source set and run
   admission set match exactly by ID, revision, Tool version and identity
   digest.
3. A Source edit creates a new revision; an older frozen run remains
   deterministic after edit, unlink or task deletion.
4. Current runtime execution produces zero writes to legacy
   `fetch_targets`, `task_fetch_targets`, `content_items` and
   `content_sources`.
5. Every new content record has a same-scope appearance and complete
   Source/run/Tool/evidence chain.
6. v1 retry/recovery succeeds using frozen bytes after its mutable global row
   is missing or changed, and writes only scoped evidence.
7. Tenant purge removes its Source configuration, state, content appearances
   and evidence without changing another tenant.
8. Static guards keep unscoped legacy Source APIs and SQL out of current
   runtime packages; only the named v1 archive/recovery boundary may refer to
   them.

7.10-B2 is not accepted merely because the containment test passes. It is
accepted only after the new generation, writer/runtime cutover and these gates
are green.
