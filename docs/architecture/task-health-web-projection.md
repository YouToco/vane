# Task health Web projection

The task page should answer three user questions without exposing runtime
internals:

1. Did the latest check complete, and does the user need to do anything?
2. How much known usage did this task consume, and is that number complete?
3. Which task actions is the current account actually allowed to perform?

`taskhealth.ProjectV1` is the pure presentation boundary for those answers.

## Failure language

Only controlled issue and recommended-action enums reach the client. Provider
responses, SQL/driver errors, workflow IDs, stage names, and raw
`failure_message` values never enter this projection. Unknown codes become
`check_failed + contact_support`.

Successful-but-partial checks remain useful and are presented as incomplete
coverage, not as total failure. A known failing acquisition route takes
precedence because it gives the user a concrete action. The projection never
reintroduces a Source object or Source-management action: the user reviews the
task manual and lets the Agent recompile its internal Tool calls.

## Cost truthfulness

- Tool calls are attributed only through the exact immutable run snapshot
  `(tenant, user, task, Temporal workflow ID)` binding. No time-window or
  workflow-name prefix heuristic is allowed.
- A missing tool-cost attribution is `llm_only`, never a fabricated zero-cost
  tool total. Legacy calls without a run snapshot remain unattributed.
- Both LLM and tool cost/call pairs are nullable; a missing pair means
  unattributed, while an explicit zero pair means a verified zero.
- `known_cost_usd` is the sum of the components whose attribution is known.
- If only some Tool rows carry a provider cost receipt, their subtotal is
  included but coverage is `llm_and_tools_partial` (or `tools_partial`).
  `tool_calls` and `tool_priced_calls` make the lower bound auditable.
- `budget=ok` requires complete LLM-and-tool attribution. Partial attribution
  can prove only a lower-bound warning/exhaustion; otherwise it is incomplete.
- The reporting window is included only when both boundaries form a valid
  half-open interval.

## Permission truthfulness

The current control plane is task-owner based: after the exact
tenant/user/task tuple is admitted, that user may run, pause and delete the
task, and may edit it when definition editing is enabled. Tenant membership
role is display context, not a second RBAC system; an unavailable or unknown
role label does not narrow already verified task ownership. The Web client
must render from these booleans and must not infer authority merely because a
button component exists. When task access is not verified, every action
remains false and the usage object is omitted.

The package is exposed only through the task-scoped Brief Web projection after
the task-playbook cutover. It has no Feishu, workflow, or model call point.

## Acquisition failure language

The latest exact run may expose only a bounded failure category:
`timeout`, `provider_error`, `invalid_request`, `usage_limit`, or `internal`.
This category is projected only when the latest snapshot's Temporal workflow ID
names exactly one run snapshot in that task scope. Retained schedulers that
reuse a workflow ID are ambiguous and therefore expose no "latest" Tool fact;
history is never guessed into the current check.
Provider bodies, endpoint paths, SQL/driver details and raw error strings never
cross the API. The recommended action follows the category: transient failures
wait for retry, invalid requests review the task manual, usage limits review
usage, and internal failures contact support.
