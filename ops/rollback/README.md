# Rollback and recovery primitives

Rollback is admitted by `ops/bin/vane rollback` and the forced-command broker.
The active runtime switches the complete `/opt/vane/current` symlink between
immutable 40-SHA release directories; it never restores individual binaries.

The pre-monorepo mixed-path recovery program is retained only under
`ops/tests/fixtures/` so its historical failure matrix remains executable. It
is not an operator or broker entry point. Database migrations remain
forward-only, so post-finalize rollback stays blocked until signed compatibility
evidence proves the target is safe for the current schema.
