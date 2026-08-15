# Operations control plane

`ops/bin/vane` is the repository's only operator-facing executable. It runs
local checks and validates immutable release evidence. The VPS broker alone
owns Server mutation, its lock, and CAS. The release Mac separately owns the
narrow Web mutation: verified Vite output goes directly to OSS/CDN and is never
copied to the VPS.

## Layout

| Path | Authority |
| --- | --- |
| `bin/vane` | Local doctor, test dispatch, manifest audit, and broker request validation |
| `release/` | Local exact-source Gate, Server artifact handoff, native SHA-directory cutover, and direct OSS publication |
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
./ops/bin/vane resume-web --sha <sha> --release-root /path/to/release-<sha>
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

`release --sha` first performs read-only OSS object, CDN domain, and public
marker preflight using the canonical production endpoint. Missing or invalid
Web credentials and an unavailable provider/public path therefore refuse
before the complete full gate or any Server mutation. It then performs the
complete full gate, deterministic Server
artifact construction, a signed plan/gate/artifact chain, and one Server-only
submission to the configured root-owned broker. After Server success, the same
command publishes the verified `dist/` directly from the Mac to OSS: immutable
assets first, then exact-byte provider readback, `index.html`, and finally the
root `vane-release.json` commit object. Bounded CDN refresh is followed by
exact public marker and entrypoint verification. The user does not assemble
receipts, current-state documents, manifests, or CAS values. Its non-secret
local runtime is configured with `VANE_WORK_ROOT`, `VANE_RELEASE_SIGNING_KEY`,
`VANE_RELEASE_SIGNER` and `VANE_ALLOWED_SIGNERS`. The broker submitter is the
user-scoped `~/.local/libexec/vane-broker-submit`; neither the checkout nor an
environment variable may replace its executable or fixed SSH destination.
Missing broker installation or signer material fails closed before Server
mutation. Aliyun credentials remain local and are passed only to the OSS/CDN
publisher; Server/runtime credentials remain available only to the broker.
After submission, the exact controller independently queries the policy-pinned
numeric VPS endpoint through `/usr/bin/ssh` and requires the requested Server
revision both before and after Web publication. A forged local client success
cannot authorize Web publication; concurrent Server drift is detected and the
release is not reported as finalized. Cross-machine serialization remains a
separate VPS release-lease concern.

`resume-web` is the narrow recovery path for a release whose Server already
finalized but whose Web publication did not. Run it from a clean checkout of
the requested revision, which may be an older commit still reachable from
`origin/main`. It accepts only an owner-private `release-<sha>` evidence root,
verifies the signed artifact chain binds its exact `full-gate.json`, re-hashes
the Gate's Web tree and marker, and independently requires the VPS Server SHA
before and after publication. It never re-runs the full Gate or submits Server
mutation. If `web-publication.json` already exists, it performs credential-free
exact public marker/index verification instead of publishing again.

`release-receipt.json` is consumed as an exact ten-field object. The durable
Server `current-release.json` is also parsed strictly and compared by SHA-256 CAS.
The candidate N+1 document may be prepared with the artifact, but `audit`
authorizes its activation only when the same signed chain reaches `finalize`;
deploy or verify failure therefore leaves N authoritative.

Controller code is deliberately one product release behind. Release M stages
controller M but remains authorized by the previously finalized controller.
Only when the following product release begins may the broker lock promote M;
the promotion is idempotent across a crash immediately after the symlink swap.
Candidate product helpers are never executed as root with broker credentials.

## Tests

```bash
python3 -m unittest discover -s ops/tests -p 'test_*.py'
bash -n ops/rollback/*.sh ops/certificates/*.sh \
  ops/audit/*.sh ops/bootstrap/*.sh
```
