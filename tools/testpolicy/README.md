# Test policy

`./tools/testpolicy/check-go-skips.sh` is the canonical Go skip-policy gate.
It runs the type-aware scanner in `server/internal/testgate` and validates this
directory's sole skip allowlist authority.

The allowlist starts empty. Capability-dependent Go tests do not use it: they
call the narrow `testgate` helpers, which produce standardized skips in the
quick loop and hard failures when `VANE_FULL_GATE=1`. Direct `testing.T`,
`testing.B`, or `testing.F` `Skip`, `Skipf`, and `SkipNow` methods are forbidden.
