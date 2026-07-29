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
coverage, not as total failure. A known failing source takes precedence because
it gives the user a concrete action.

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

The package currently has no API call point. It can be wired after the
task-playbook runtime cutover freezes the schedule detail contract. A repository
guard keeps production imports at zero until that rollout is intentional.
