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
| `release/` | Local exact-source Gate, Server artifact handoff, native SHA-directory cutover, and dual-provider Web publication |
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
./ops/bin/vane prune-cloudflare-pages --sha <sha> \
  --release-root /path/to/release-<sha> \
  --expected-total <n> --expected-candidate-count <n>
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

`release --sha` first performs read-only OSS, Aliyun CDN/GeoDNS, and Cloudflare
Pages project/custom-domain preflight. Missing credentials, a Git-bound Pages
project, or a route contract other than default→Ali CDN and oversea→Pages
therefore refuses
before the complete full gate or any Server mutation. It then performs the
complete full gate, deterministic Server
artifact construction, a signed plan/gate/artifact chain, and one Server-only
submission to the configured root-owned broker. After Server success, the same
command first publishes the verified `dist/` to the canonical Cloudflare Pages
production deployment and verifies its immutable URL and pages.dev alias. It
then publishes the same artifact to OSS: immutable assets first, exact-byte
readback, `index.html`, and finally `vane-release.json`. Bounded CDN refresh is
followed by OSS readback and a TLS-SNI-pinned Ali CDN edge verification. Only
then is the combined provider result finalized. Partial progress is durable and
`resume-web` adopts exact canonical provider state before mutating a missing
side. The user does not assemble
receipts, current-state documents, manifests, or CAS values. Its non-secret
local runtime is configured with `VANE_WORK_ROOT`, `VANE_RELEASE_SIGNING_KEY`,
`VANE_RELEASE_SIGNER` and `VANE_ALLOWED_SIGNERS`. The broker submitter is the
user-scoped `~/.local/libexec/vane-broker-submit`; neither the checkout nor an
environment variable may replace its executable or fixed SSH destination.
Missing broker installation or signer material fails closed before Server
mutation. Aliyun and Cloudflare credentials remain local, are isolated from
each other, and are passed only to their publisher subprocesses; Gate and
Server/runtime processes receive neither.
Immediately after the credential-free Gate, before signing, broker submission,
or Web mutation, the controller again requires the exact HEAD and a clean
checkout and re-hashes the controller, Web publisher/planner, release policy,
toolchain lock, signer authority, signing key, installed broker submitter, and
broker client credentials/configuration. Broker subprocesses receive neither
Web provider credentials nor the release signing-key path.

Production Web publication hashes the actual platform-specific Aliyun CLI,
ossutil, Node, and Wrangler entry bytes and the complete canonical Wrangler npm
tree before executing any of them. Those digests are pinned in
`toolchain.lock.json`; Wrangler must also report exactly `4.115.0`. The same
check runs again after Gate while provider/signing credentials are still absent.
A same-version replacement therefore fails closed even though the Mac's
single-UID tool cache is intentionally user-owned.
Web mutation is hard-locked by policy and runtime to the release Mac's
`darwin-arm64`; Linux/VPS remains the Server broker authority and cannot run the
Web publisher. Cross-platform pins for general Gate tools do not broaden this
mutation authority.

These byte checks are reliability and post-Gate integrity barriers within the
repository's documented single-UID trust model. They do not claim to contain
an exact trusted `origin/main` revision that deliberately launches a persistent
credential-stealing process; merged main remains the local trust root.
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

`prune-cloudflare-pages` is separate from the release transaction and is the
only supported retention entry. It requires a clean exact `origin/main`, the
signed Gate, an untampered combined Web result, credential-free exact public
verification, stable independent Server status, and a fresh full provider
route/data-plane preflight. The default is a reviewable dry-run. Deletion also
requires `--execute`, the reviewed `--expected-manifest-sha256`, and the
`darwin-arm64` release Mac; the library rechecks canonical/domain/deployment
authority after every deletion and always keeps the new and previous canonical
deployments. The internal Python module has no standalone CLI.
The previous canonical deployment is captured durably by the Web transaction
before any Cloudflare commit and is read only from the strict combined provider
receipt; operators cannot select an arbitrary historical deployment to keep.

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
