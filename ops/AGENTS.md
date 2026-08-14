# Operations agent contract

Scope changes under `ops/` to release orchestration, recovery, certificate,
audit, bootstrap, policy, or their tests.

- `ops/bin/vane` is the only operator-facing entry point.
- Local release code may read only the Aliyun credentials needed to upload the
  verified Web build directly to OSS and refresh its CDN entries. Server
  production credentials remain outside the checkout behind the VPS broker.
- Production requests require exact `origin/main`, a complete signed manifest
  chain, and a complete integrity lock. Unknowns fail closed.
- Do not add GitHub Actions, VM/container build dependencies, a second Server
  state owner, or an alternate operator entry point. Web publication owns a
  separate local lock and receipt because Web files never pass through the VPS.
- Changes to Server deploy/recovery behavior require the highest risk review
  and real recovery evidence.
- Keep component tests with components. `ops/tests` covers the control plane;
  cross-component release fixtures may live in `tests/release`.
- Never weaken forward-only migration or exact-artifact checks to make rollback
  appear available.

Run `python3 -m unittest discover -s ops/tests -p 'test_*.py'` and `bash -n` on
all operations shell scripts after changes.
