# Toolchain checks

`toolchain.py` validates cached artifacts, exact executable versions, npm lock
integrity, acme source identity, and canonical production image digests.

`server-coverage-baseline.json` is the first monorepo server baseline. It was
generated from the merged Store/non-Store atomic profiles for exact revision
`c2a03e8d0215e2cf5178346f828ede8c2adc9c11`: all three PostgreSQL 18 Store
race shards (902/902 tests, zero skips) and the complete non-Store race suite
passed before the profile was frozen. The same rehearsal later stopped at the
separate production-history replay gate because three Temporal histories had
already expired from the production namespace; that refusal does not turn the
completed test coverage into release evidence. A release remains blocked until
the remaining verification stages also succeed.
