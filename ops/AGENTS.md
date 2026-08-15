# Operations agent contract

Scope changes under `ops/` to release orchestration, recovery, certificate,
audit, bootstrap, policy, or their tests.

- `ops/bin/vane` is the only operator-facing entry point.
- Local release code may read only the isolated Aliyun and Cloudflare
  credentials needed to publish one verified Web build to Pages overseas and
  OSS plus Ali CDN domestically. Server production credentials remain outside
  the checkout behind the VPS broker.
- Gate/test subprocesses must receive a sanitized environment without release
  signing paths, broker/SSH state, or Web provider credentials. The no-VM,
  single-UID Mac does not claim hostile-code isolation; exact merged main is
  the local trust root.
- Production requests require exact `origin/main`, a complete signed manifest
  chain, and a complete integrity lock. Unknowns fail closed.
- Do not add GitHub Actions, VM/container build dependencies, a second Server
  state owner, or an alternate operator entry point. Web publication owns a
  separate local lock, provider receipts, and one combined receipt because Web
  files never pass through the VPS. Publishing and retention must not mutate
  DNS; the exact default/oversea route split is a read-only finalize gate.
- Changes to Server deploy/recovery behavior require the highest risk review
  and real recovery evidence.
- Keep component tests with components. `ops/tests` covers the control plane;
  cross-component release fixtures may live in `tests/release`.
- Never weaken forward-only migration or exact-artifact checks to make rollback
  appear available.

Run `python3 -m unittest discover -s ops/tests -p 'test_*.py'` and `bash -n` on
all operations shell scripts after changes.
