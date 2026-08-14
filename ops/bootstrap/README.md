# Bootstrap

Tool installers place integrity-pinned development dependencies below a
caller-owned private temporary directory. Version-only or unresolved pins are
not sufficient for release; `vane doctor` enforces
`tools/toolchain.lock.json`.

## One-time production control-plane cutover

`production_cutover.py` is the only exception to normal broker admission. It
exists because the first broker cannot install itself. The exception is
narrow: an exact merged `origin/main` revision B creates a signed plan, and the
VPS rechecks the frozen legacy bytes before creating any new authority.

The local, non-production phase is:

```bash
python3 ops/bootstrap/production_cutover.py create-plan \
  --output /private/bootstrap-B \
  --signing-key /private/release-signing-key \
  --transport-public-key /private/broker-transport-key.pub \
  --broker-public-key /private/broker-signing-key.pub
```

Copy only the generated plan, its detached signature, and controller archive
to a root-only VPS staging directory. Extract that same signed controller
archive to a separate root-only temporary directory, then run its copy of the
bootstrap tool. This ensures the installer and policy files come from B:

```bash
python3 ops/bootstrap/production_cutover.py audit \
  --plan /root/bootstrap-B/bootstrap-plan.json \
  --controller-archive /root/bootstrap-B/controller-B.tar.gz
python3 ops/bootstrap/production_cutover.py apply \
  --plan /root/bootstrap-B/bootstrap-plan.json \
  --controller-archive /root/bootstrap-B/controller-B.tar.gz
```

Both VPS commands require root. `audit` is read-only and must pass immediately
before `apply`. The apply transaction:

1. verifies the signed plan, controller archive, legacy state, live process,
   exact binaries/configuration, and middleware images;
2. creates an immutable legacy SHA release so the existing process has a
   complete rollback target;
3. stages controller B and installs the unprivileged SSH forced-command broker;
4. initializes the root-owned current-release CAS and activates the two
   canonical `current` links.

No persistent browser session is provisioned. Each later release creates a
10-minute owner UAT session through the local PostgreSQL container, passes it
to the UAT subprocess in a private temporary credential directory, and revokes
it even when UAT fails.

B must never publish product revision B through the broker it introduces. A
later exact-main revision C is the first normal `./ops/bin/vane release --sha C`:
installed B validates and mutates C, then stages controller C without activating
it. At the start of the following product release D, the global broker lock may
promote already-finalized controller C; C can authorize D but never C. No GitHub
runner or VM participates in this path.

The pre-hardening bootstrap controller is upgraded exactly once with
`controller_upgrade.py`. Its signed plan CAS-binds the live product/controller
state, changes only the root-owned controller authority, and records an exact
archive-bound bootstrap exception. The upgraded controller must publish a
later SHA; it cannot publish its own product revision. This command is not a
general controller deployment path.

Create the one-time plan from a clean exact-main checkout using read-only
snapshots of the live state, active controller revision, and its signer policy:

```bash
python3 ops/bootstrap/controller_upgrade.py create-plan \
  --output /private/controller-bootstrap \
  --current-release /private/current-release.json \
  --active-controller-revision <40-character-active-controller-sha> \
  --allowed-signers /private/current-allowed-signers \
  --signing-key /private/release-signing-key \
  --transport-public-key /private/broker-transport-key.pub
```

Copy the plan, detached signature, and exact controller archive into a
root-only VPS directory. Extract that archive into a separate root-only
directory and run its copy of the tool:

```bash
python3 ops/bootstrap/controller_upgrade.py apply \
  --plan /root/controller-bootstrap/controller-bootstrap-plan.json \
  --controller-archive /root/controller-bootstrap/controller-<sha>.tar.gz
```

Afterward, verify that the controller revision advanced while the product and
middleware revisions did not. A different exact-main SHA is then released
through the normal broker path.

On the release Mac, install the narrow client once from the merged controller.
The private transport key remains outside the repository and can invoke only
the VPS forced command:

```bash
sudo ./ops/bootstrap/install-client.sh \
  /absolute/path/to/broker_transport_key broker.example 22 \
  /absolute/path/to/known_hosts
/usr/local/libexec/vane-broker-submit --status
```
