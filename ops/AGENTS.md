# Operations agent contract

Scope changes under `ops/` to release orchestration, recovery, certificate,
audit, bootstrap, policy, or their tests.

- `ops/bin/vane` is the only operator-facing entry point.
- Repository code never reads production credentials or mutates production.
- Production requests require exact `origin/main`, a complete signed manifest
  chain, and a complete integrity lock. Unknowns fail closed.
- Do not add GitHub Actions, raw provider calls to the CLI, a second state
  owner, or an alternate operator entry point.
- Preserve the existing deploy/recovery scripts when moving them. Changes to
  their production behavior require the highest risk review and real recovery
  evidence.
- Keep component tests with components. `ops/tests` covers the control plane;
  cross-component release fixtures may live in `tests/release`.
- Never weaken forward-only migration or exact-artifact checks to make rollback
  appear available.

Run `python3 -m unittest discover -s ops/tests -p 'test_*.py'` and `bash -n` on
all operations shell scripts after changes.
