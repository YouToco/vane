# Store race test sharding experiment

This experiment keeps `.github/workflows/ci.yml` unchanged until it is measured on
the `vane-test` runner. Run the opt-in **Store race sharding experiment** workflow
with `workflow_dispatch`.

The manual workflow is a store-only, two-stage performance experiment. It does
not replace or alter `.github/workflows/ci.yml`, its package set, PostgreSQL 18
isolation, race/coverage flags, Temporal gate, vet, inventory, build, or trusted
deployment. Dispatch the experiment on its branch with:

```text
gh workflow run store-race-sharding-experiment.yml \
  --ref codex/ci-store-lpt-20260809
```

The seed job:

1. builds one race-enabled, atomic-coverage `store.test` binary;
2. obtains the authoritative top-level test list from that binary;
3. assigns every test to exactly one of three PostgreSQL 18 instances;
4. runs the three manifests concurrently and records `go test -json` compatible
   output through `go tool test2json`;
5. proves the observed top-level tests exactly match the manifests;
6. requires the three coverage profiles to contain identical block sets before
   summing their atomic counters;
7. projects the verified combined stream to sorted top-level terminal JSONL;
8. proves the seed and authoritative test list are exactly one-to-one, records
   the reviewed 880-test baseline, records the seed SHA-256 and exact checkout
   commit, and uploads all authority files under one checksum manifest.

Five fixed matrix jobs then run on independent GitHub-hosted runners, each with
three fresh PostgreSQL 18 service containers. Every job downloads the same
run-scoped seed artifact, rechecks the SHA-256 and exact test-list projection,
requires the downloaded source commit to equal both `github.sha` and the seed
job output, and invokes `storetestshard run --timings`. A successful repeat must
report `historical-lpt`, exactly 880 expected/observed/historical tests, zero
duplicate/missing tests, three per-shard wall measurements, and identical
coverage block sets.
Each repeat uploads its status, plan, manifests, top-level event stream, shard
JSONL, and coverage profiles under a run/attempt/repeat-unique artifact name.

Historical longest-processing-time balancing remains available to local or
other explicit callers that first provide the timing file inside the checkout:

```text
go run ./cmd/storetestshard run ... --timings tmp/prior-store.test.json
```

`--timings` accepts only a repository-relative regular file whose resolved path
remains inside the repository; absolute paths, parent traversal, symlink
escapes, permission failures, and other authority failures remain hard errors.
An omitted, ordinarily missing, corrupt, or empty timing data file safely falls
back to stable FNV-1a and records `timing_input_status` in the run status. Tests
absent from otherwise valid history use the median known duration.

Do not replace the default CI test step until repeated measurements on the same
runner show:

```text
sharded p95 wall time <= 0.60 * baseline p95 wall time
```

The uploaded artifacts contain the authoritative list, shard manifests and plan,
per-shard JSON, merged top-level JSON, per-shard coverage, merged store coverage,
the canonical timing seed, and integrity/timing status. The runner writes
`store-shard-status.json` on best effort after setup even when build, listing,
shard execution, integrity verification, or coverage merge fails; `phase`,
`error`, `failed_shards`, wall timings, and `exit_code` identify the stopping
point.

Repository-scoped workflow concurrency serializes manual experiments from this
repository. Service containers publish PostgreSQL 5432 to three dynamically
allocated, distinct host ports, so jobs from other repositories cannot collide
with fixed 5432/5433/5434 bindings. GitHub Actions fails the job before test
execution if a service cannot become healthy.

Artifact upload/download steps are pinned to immutable official action SHAs.
The workflow has only `contents: read`, receives no secrets, and runs only on
GitHub-hosted Ubuntu 24.04 runners.

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

Because the threshold failed locally, repeat-run p95 is absent, memory and CPU
costs increased, and the actual Linux/ARM64 `vane-test` runner has not been
measured, this workflow remains manual. Do not wire it into the default
`.github/workflows/ci.yml` test gate without a new same-runner benchmark and an
explicit decision.
