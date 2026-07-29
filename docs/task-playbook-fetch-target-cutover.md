# Task Playbook / Fetch Target Cutover

> Status: implemented on `refactor/task-playbook-cutover` for the 2026-07-29
> source-model cleanup.
> This contract replaces account-level source subscriptions with task-owned
> intent while preserving immutable historical protocols and content evidence.

## Product invariant

The task playbook is the only user-facing and user-editable truth for recurring
information work.

- One-off research uses bounded read tools and creates no durable fetch state.
- Recurring work is always a task with a playbook and a versioned approved
  fetch plan.
- Users never create, list, remove, enable, or identify internal fetch targets.
- "Run now" always names an existing task and runs that task's frozen
  definition. There is no account-wide push.

## Internal model

The word "source" is retired from the current control plane because it
previously referred to three different concepts.

| Retired name | What it actually was | Current name |
|---|---|---|
| `sources` | Global materialized acquisition table | `fetch_targets` |
| `schedule_sources` | Exact task-to-acquisition join table | `task_fetch_targets` |
| `sourcecatalog` | Trusted Go capability registry, not a database table | `capabilitycatalog` |

1. `capabilitycatalog` is trusted code describing what the runtime can fetch.
2. `fetch_targets` are globally shared, materialized acquisition endpoints with
   mutable health and due state. Sharing avoids duplicate upstream calls and
   duplicate content acquisition when multiple tasks use the same endpoint.
3. `task_fetch_targets` is the exact many-to-many projection of the approved
   task plan. It is derived state, never a user-authored object.

`fetch_targets` and `task_fetch_targets` must remain separate. Combining them
would either duplicate paid fetching for every task or encode task ownership in
an array/JSON field that cannot enforce referential integrity, exact set
equality, or per-task provenance.

The approved playbook/fetch plan remains the identity truth. Materialized
targets provide only a stable internal ID and mutable acquisition health.
Run snapshots freeze the approved target identity before any paid or external
effect.

## Removed current behavior

- Agent tools `list_sources`, `add_source`, `remove_source`, and
  `enable_source`.
- Agent/API account-wide `push_now`.
- HTTP subscription CRUD and the standalone Web source-management product.
- Task-detail fetch-target tabs/counts and the `fetch_targets` user API field;
  the playbook is the product surface, while materialized targets stay internal.
- `existing_source_ids` and all task-creation entitlement through
  subscriptions.
- Runtime fallback from an empty task plan to all account subscriptions.
- Current writers/readers of the `subscriptions` table.
- Unregistered retired tools `update_schedule` and `edit_task_playbook`.
- Source-specific durable-action proposal/continuation runtime after all
  nonterminal actions have been proven absent.
- The unused source probe/URL-discovery admission path that existed only for
  `add_source` confirmation cards.

Migration `074_fetch_target_cutover.sql` performs the irreversible data-plane
cutover:

- takes exclusive locks across the task/action cutover relations so validation
  and destructive DDL observe one stable database state;
- refuses to proceed unless every active task has a compiled immutable
  Approved Definition v1 head, a non-empty approved plan, and exact equality
  between approved target IDs/URLs, the mutable playbook plan, and the
  task-target projection;
- refuses to proceed while a source action is nonterminal;
- rejects unknown action protocol versions, proves every retired v2
  source-action parent/child pair has a matching terminal state, and deletes
  both the terminal child audit and its v2 root;
- drops `subscriptions` and the two retired source-action tables;
- drops the three source-action database roles;
- renames `sources` to `fetch_targets`;
- renames `schedule_sources` to `task_fetch_targets`;
- constrains `task_creation_operations.execution_version` to the sole current
  version (`1`) and renames PostgreSQL 18 named constraints so no retired table
  prefix remains in catalog errors or introspection;
- creates no compatibility views or aliases.

Replacing a task's target projection is now one transaction. Re-approving a
task also resets automatic failure suspension for exactly that task's approved
targets, so recovery remains task-centric.

URL de-duplication may reuse an existing target only when acquisition identity
`{platform, capability, url, config}` is semantically equal; display-only title
does not participate. A compiled run repeats that check before calling a paid
fetcher, so drift is skipped before cost rather than discovered during
write-back and retried.

## Historical compatibility boundary

Applied migration files, immutable run snapshots, approved-definition versions,
general Agent ledgers, tool-call records, and content provenance are audit data. Their
old wire field names remain readable by retained version-specific decoders.
They do not justify keeping a current tool, API route, write path, or fallback.

The schema migration renames current physical acquisition tables but does not
rewrite immutable historical payload bytes. Versioned readers retain the old
`source_id`, `sources`, and `legacy_subscriptions` wire vocabulary only inside
those immutable formats.

The old physical table names may otherwise appear only in applied migrations
and migration tests that prove an existing database upgrades without aliases.
The old Agent tool names may otherwise appear only in immutable ledger/history
readers and explicit “retired tool stays absent” guards.

## Cutover gates

1. Production has no legacy schedule and no legacy approved-definition head.
2. Every active task has a non-empty approved fetch plan whose materialized
   target projection is exact.
3. Retired account subscriptions are explicitly discarded without deleting
   shared fetch targets or acquired content.
4. There are no nonterminal source-specific pending/durable actions.
5. New code contains no current account-subscription read/write path.
6. Empty or corrupt task plans fail closed before network, LLM, database
   business writes, or delivery.
7. A task can be located by natural-language description and run immediately
   without exposing an internal ID to the user.
