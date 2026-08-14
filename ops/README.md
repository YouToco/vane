# Operations control plane

`ops/bin/vane` is the repository's only operator-facing executable. It runs
local checks and validates immutable release evidence, but it cannot read
production credentials, acquire the production lock, or mutate production.
Those capabilities belong to a separately installed, root-owned VPS broker.

## Layout

| Path | Authority |
| --- | --- |
| `bin/vane` | Local doctor, test dispatch, manifest audit, and broker request validation |
| `release/` | Exact-source checkout, deterministic artifacts, SHA-directory cutover, and frontend publication |
| `rollback/` | Whole-release rollback admission; legacy mixed-path logic exists only as a test fixture |
| `certificates/` | Existing certificate issuance and edge-verification primitive |
| `audit/` | Binary and release evidence checks |
| `bootstrap/` | Integrity-pinned tool installers; never provider credentials |
| `policy/` | Release stage, signer, skip, and tool version policy |
| `tests/` | Operations unit and policy tests |

The imported GitHub workflows were intentionally deleted. Their reusable
scripts were moved without rewriting the underlying deployment transaction.
Workflow orchestration is replaced by the signed chain:

```text
plan -> gate -> artifact -> deploy -> verify -> finalize
```

Every production manifest carries one exact lowercase 40-character monorepo
revision. Each non-plan manifest links to the immediately previous sibling by
relative path, stage, and SHA-256. Detached signatures use the
`vane-release` SSH signing namespace. Missing stages, signatures, digests, or
trusted signers fail closed.

## Commands

```bash
./ops/bin/vane doctor
./ops/bin/vane quick --risk B --base origin/main --head HEAD
./ops/bin/vane full --sha <exact-clean-checked-out-sha>
./ops/bin/vane status --manifest /path/to/finalize.json
./ops/bin/vane audit --manifest /path/to/finalize.json
./ops/bin/vane release --sha <exact-origin-main-sha>
./ops/bin/vane retry --sha <sha> --stage deploy --manifest /path/to/deploy.json
./ops/bin/vane rollback --sha <current> --to <previous> \
  --manifest /path/to/current/finalize.json \
  --target-manifest /path/to/previous/finalize.json
./ops/bin/vane cert check --certificate /path/to/public-fullchain.pem
```

`doctor` verifies the checksum-locked downloads, exact installed executable
versions, canonical production image digests, and the trusted signer policy.
It refuses release when any required local installation or broker signer is
missing; versions alone are never treated as integrity pins.

`release --sha` performs doctor, the complete full gate, deterministic backend
and frontend artifact construction, a signed plan/gate/artifact chain, and one
submission to the configured root-owned broker. The user does not assemble
receipts, current-state documents, manifests, or CAS values. Its non-secret
local runtime is configured with `VANE_WORK_ROOT`, `VANE_RELEASE_SIGNING_KEY`,
`VANE_RELEASE_SIGNER`, `VANE_ALLOWED_SIGNERS`, and `VANE_BROKER_SUBMIT`.
Missing broker installation or signer material fails closed before production
mutation. Production credentials remain available only to the broker.

`release-receipt.json` is consumed as an exact ten-field object. The durable
`current-release.json` is also parsed strictly and compared by SHA-256 CAS.
The candidate N+1 document may be prepared with the artifact, but `audit`
authorizes its activation only when the same signed chain reaches `finalize`;
deploy or verify failure therefore leaves N authoritative.

## Tests

```bash
python3 -m unittest discover -s ops/tests -p 'test_*.py'
bash -n ops/release/*.sh ops/rollback/*.sh ops/certificates/*.sh \
  ops/audit/*.sh ops/bootstrap/*.sh
```
