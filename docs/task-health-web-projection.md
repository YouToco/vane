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

- A missing tool-cost attribution is `llm_only`, never a fabricated zero-cost
  tool total.
- Both LLM and tool cost/call pairs are nullable; a missing pair means
  unattributed, while an explicit zero pair means a verified zero.
- `known_cost_usd` is the sum of the components whose attribution is known.
- `budget=ok` requires complete LLM-and-tool attribution. Partial attribution
  can prove only a lower-bound warning/exhaustion; otherwise it is incomplete.
- The reporting window is included only when both boundaries form a valid
  half-open interval.

## Permission truthfulness

Permissions derive from the server-known membership role plus explicit runtime
capabilities. Unknown roles and ordinary members fail closed to read-only.
Deleting a task remains owner-only. The Web client must render from these
booleans and must not infer authority merely because a button component exists.
When `can_view_usage=false`, the server omits the usage object entirely.

The package is exposed only through the task-scoped Brief Web projection after
the task-playbook cutover. It has no Feishu, workflow, or model call point.
