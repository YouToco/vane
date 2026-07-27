# Store race test sharding experiment

This experiment keeps `.github/workflows/ci.yml` unchanged until it is measured on
the `vane-test` runner. Run the opt-in **Store race sharding experiment** workflow
with `workflow_dispatch`.

The runner:

1. builds one race-enabled, atomic-coverage `store.test` binary;
2. obtains the authoritative top-level test list from that binary;
3. assigns every test to exactly one of three PostgreSQL 18 instances;
4. runs the three manifests concurrently and records `go test -json` compatible
   output through `go tool test2json`;
5. proves the observed top-level tests exactly match the manifests;
6. requires the three coverage profiles to contain identical block sets before
   summing their atomic counters;
7. runs every non-store package with the normal race and coverage flags, then
   merges its coverage with the store profile.

With no timing input, assignment is the stable FNV-1a hash of the test name. Pass
the repository-relative path of a prior `go test -json` artifact as the
`timings_path` workflow input to use deterministic longest-processing-time
balancing. Tests absent from the history use the median known duration.

Do not replace the default CI test step until repeated measurements on the same
runner show:

```text
sharded p95 wall time <= 0.60 * baseline p95 wall time
```

The uploaded artifact contains the authoritative list, shard manifests and plan,
per-shard JSON, merged JSON, per-shard coverage, merged store coverage, final
coverage, and the integrity/timing status.

The experiment assumes the repository's `vane-test` label admits one job at a
time. Service containers publish PostgreSQL 5432 to three dynamically allocated,
distinct host ports, so jobs from other repositories cannot collide with fixed
5432/5433/5434 bindings. GitHub Actions fails the job before test execution if a
service cannot become healthy.

The artifact uploader is pinned to official `actions/upload-artifact` v7.0.1
(Node.js 24). The self-hosted runner must be at least version 2.327.1; confirming
that runner prerequisite is part of the manual experiment and another reason the
workflow is not a default gate.

## Local benchmark decision (2026-07-27)

Environment: Windows/amd64, Go 1.26.4, Docker 29.6.1, three
`postgres:18-alpine` containers. The baseline and final LPT run were serialized
on the same machine with fresh databases.

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
