# Store race test sharding

The monorepo full gate runs the migration-heavy `store` race suite as three
concurrent manifests, each backed by a fresh PostgreSQL 18 instance. The
remaining package race tests, Temporal recovery gate, vet, inventory check,
vulnerability scan, coverage merge, and builds run in the same exact-SHA gate.
The disposable test environment has no production credentials; production
mutation remains behind the external root-owned broker.

`storetestshard run`:

1. builds one race-enabled, atomic-coverage `store.test` binary;
2. obtains the authoritative top-level test list from that binary;
3. assigns every test to exactly one of three PostgreSQL 18 instances;
4. runs the three manifests concurrently and records `go test -json` compatible
   output through `go tool test2json`;
5. proves the observed top-level tests exactly match the manifests; and
6. requires the three coverage profiles to contain identical block sets before
   summing their atomic counters.

After a successful full run, the supervisor projects verified terminal events
to a canonical timing seed under the exact commit key. A candidate may restore
only its exact trusted base SHA. Historical longest-processing-time balancing is used only
when every current top-level test has authoritative timing. Missing, corrupt,
empty, or incomplete timing data falls back to stable FNV-1a assignment and is
reported in `store-shard-status.json`. Cache restore, seed construction, and
cache save are best-effort and cannot turn a correct test run red.

The cache restore and save steps deliberately declare the same
`tmp/store-timing-cache` path. GitHub includes the declared path in the cache
version fingerprint, so using a different staging directory for save creates a
valid-looking exact key that can never be restored. Main run `31364677761`
bootstrapped the first same-path seed for commit `c27d018` after this boundary
was corrected.

The same historical balancing remains available to explicit callers that
provide the timing file inside the checkout:

```text
(cd server && GOWORK=off go run ./cmd/storetestshard run ... --timings tmp/prior-store.test.json)
```

`--timings` accepts only a repository-relative regular file whose resolved path
remains inside the repository; absolute paths, parent traversal, symlink
escapes, permission failures, and other authority failures remain hard errors.
An omitted, ordinarily missing, corrupt, empty, or incomplete timing data file
safely falls back to stable FNV-1a and records `timing_input_status` in the run
status.

The runner writes
`store-shard-status.json` on best effort after setup even when build, listing,
shard execution, integrity verification, or coverage merge fails; `phase`,
`error`, `failed_shards`, wall timings, and `exit_code` identify the stopping
point.

Disposable PostgreSQL instances publish 5432 to dynamically allocated,
distinct host ports so shards cannot collide with a host database or fixed
ports. The full gate fails before test execution if any dependency cannot
become healthy and parses `go test -json`; any terminal skip is a failure.

The earlier six-job hosted experiment workflow was deleted after measurement;
it is not a retained dispatch or cost surface. Hosted run `31331916553` proved
880/880 exact tests in every repetition and reduced the median shard wall from
874.036 seconds to 840.479 seconds (3.84%). That did not meet the original 40%
experimental improvement target. On 2026-08-09, after GitHub-hosted billing
blocked further jobs, the owner explicitly chose the isolated Mac test runner
and retained the bounded LPT optimization with deterministic FNV fallback. This
is an operating-cost decision, not a claim that historical LPT achieved the
original experimental speed threshold.

## Local benchmark decision (2026-07-27)

Environment: Windows/amd64, Go 1.26.4, Docker 29.6.1, three
`postgres:18-alpine` containers. The baseline and final LPT run were serialized
on the same machine with fresh databases.

These measurements and the 576-test integrity count are bound to benchmark
commit `e779b196` and its then-current store test set. This branch has since
been rebased onto `40e10b8`, where the test set changed. The numbers below are
historical experiment evidence, not a performance claim for the current commit;
no expensive benchmark was rerun for this review fix.

| sample | result | wall time |
|---|---:|---:|
| uncontended store baseline | PASS | 691.579 s |
| uncontended historical LPT, shard 0 | PASS | 454.298 s |
| uncontended historical LPT, shard 1 | PASS | 463.905 s |
| uncontended historical LPT, shard 2 | PASS | 458.360 s |
| uncontended historical LPT, end to end | PASS | 478.909 s |

The end-to-end ratio was **69.25%** of baseline, a 30.75% reduction, and therefore
missed the required 60% threshold of 414.947 seconds. The three-shard
nearest-rank p95 equals the maximum shard, 463.905 seconds. This is not a
repeated-run p95; a statistically useful same-runner p95 has not been
established.

The baseline store process consumed 523.266 CPU seconds and peaked at
705,572,864 bytes RSS. The three shards consumed 819.016 CPU seconds in total
(56.5% more) and their largest per-process peak was 1,466,335,232 bytes
(2.08 times baseline); their individual peak RSS values sum to about 3.42 GB.
The final run still proved 576 expected and 576 observed top-level tests, with
zero missing and zero duplicate tests, identical coverage block sets, and 71.8%
merged store coverage.

Two earlier measurements are retained only as diagnostic artifacts and are
invalid for performance comparison:

- the 574.341-second stable-hash smoke overlapped another store test lane;
- the 383.477-second historical-LPT smoke overlapped UI typecheck, test, and
  build work.

This historical Windows result did not justify promotion on performance alone.
The later hosted measurement and the explicit 2026-08-09 operating-cost
decision above supersede its old "manual only" recommendation; the integrity
checks and stable fallback remain mandatory in the full gate.
