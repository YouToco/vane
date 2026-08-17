# Firecracker sandbox runtime-dark production Gate

This package remains unavailable to Vane Server and the Skill runtime, and no
long-running sandbox service is enabled. One deliberately narrow executable
path exists: the trusted release controller runs a fixed Firecracker microVM
self-test after activating an exact candidate Server and before production UAT.
`sandboxd serve-dark` authenticates a fixed Vane service UID over a root-owned
Unix socket, validates the complete authority envelope, and always returns
`dark_foundation`.

## Trust and isolation contract

- Vane Server never opens `/dev/kvm`, invokes Firecracker, or executes uploaded
  code. Only the separate Linux `sandboxd` process performs host preflight.
- The request binds tenant, user, capability, immutable capability version,
  invocation, approved closed policy, and canonical request digest. A trusted
  registry maps the exact capability/version pair to a separately pinned code
  image digest; requests never carry host paths or custom images.
- Firecracker, jailer, kernel, initramfs root filesystem, and the fixed Gate
  code image are absolute, root-owned, single-link, non-writable paths with
  exact SHA-256 and byte-size pins.
  Firecracker and jailer must report the same exact non-debug release.
  The official x86_64 Firecracker v1.16.1 archive and Firecracker-CI kernel are
  HTTPS-downloaded only during the trusted artifact stage and checked against
  the committed lock. A deterministic `newc` initramfs contains only the exact
  release `sandboxd` binary and fixed directories. The signed backend manifest
  binds all five artifacts and `sandboxd`; the v1 release receipt in turn binds
  that backend manifest without changing its compatibility shape.
- The Gate invocation receives a derived jailer-safe ID, a distinct host UID/GID
  slot, a new work directory, a new microVM plan, read-only root/code drives,
  non-root guest identity, bounded tmpfs boot contract, CPU/memory/PID cgroups,
  wall timeout, input/output caps, process-group kill, checked cgroup removal,
  namespace unmount, and checked work-tree scrub.
- Guest input is a bounded, per-invocation read-only raw block device. PID 1
  first mounts the kernel `devtmpfs` on the initramfs `/dev` mountpoint, then
  opens the discovered `/dev/vdb` and `/dev/ttyS0` devices before spawning the
  receipt worker as the approved non-root UID/GID.
  The host accepts one canonical guest receipt only when it binds the invocation,
  full request digest, input and response digests, identity, loopback-only view,
  absence of a default route, and MMDS unavailability.
- V1 has no virtual NIC and no MMDS configuration. Jailer is always given a
  pre-created root-owned network namespace. Preflight enters it on a dedicated
  locked OS thread and proves that it contains loopback only and no
  non-loopback IPv4/IPv6 route, then restores the original namespace. A failed
  restore retires that OS thread.
- Socket startup fails if the path already exists. The immediate parent and
  socket have exact owner/mode contracts; cleanup removes only the inode that
  sandboxd created, and any cleanup mismatch or failure is returned.
- Socket messages use one explicit 32-bit length-prefixed JSON frame with a
  closed maximum; unknown fields, trailing bytes inside the frame, and an
  oversized declared or encoded payload are rejected without waiting for EOF.
- The dark daemon defaults to 16 concurrent connections and a 256 KiB frame,
  hard-caps configuration at 64 connections/256 KiB, applies a 30-second
  per-connection deadline, and returns a fixed `busy` response without spawning
  another handler when all slots are occupied.
  Raw request input is capped at one quarter of the configured frame so its
  base64 representation and the complete authority/policy JSON envelope remain
  representable within the same closed frame.
- Configuration is read only from an absolute root-owned 0400/0600 regular
  file with one link and a root-owned, non-writable, non-symlink parent chain.
  Wire responses contain fixed error codes, never internal paths or principals.

## Explicit release Gate and rollout boundary

`Service.Execute` and general `FirecrackerBackend.Run` still always return
`dark_foundation`; the Server cannot import or call the Gate. The only live path
is:

```text
/opt/vane/releases/<sha>/bin/sandboxd release-gate \
  --sha <sha> --receipt <new-root-owned-path>
```

The root broker starts it in a transient systemd service with a closed device
policy exposing only `/dev/kvm`, a private outer network/mount namespace,
delegated cgroup control, a 30-second host deadline, and write access only to
the receipt transaction. The command additionally requires that `<sha>` is the
exact `/opt/vane/current` target, validates the release receipt → backend
manifest → sandbox manifest chain, verifies its own executable digest, performs
real Linux/KVM preflight, boots the guest, and durably fsyncs a 0600 receipt only
after all cleanup succeeds. That receipt is signed into the release verify
manifest; a failure rolls the candidate Server back.

The controller rollout is intentionally two-stage because controller `N` must
authorize product `N+1`: first ship the compatibility bridge that accepts both
the legacy base artifact and the extended Firecracker artifact; after that
controller becomes active, a following exact-main release may add `sandboxd`
and the immutable sandbox bundle to the server inventory. Never collapse those
two releases or let candidate controller code authorize its own new artifact.

Before Skill scripts can execute, a separate reviewed project must still add:

1. a durable append-only user invocation receipt/lease ledger that survives
   sandboxd restarts and binds the full request digest;
2. the Skill code-image builder, validator, and immutable capability binding;
3. adversarial script evidence beyond this fixed known-answer guest; and
4. a separately approved canary that consumes the durable capability ledger.

Artifact pins already include `SizeBytes`; user-script activation must still put
large image acquisition/copy under the invocation deadline and make those
copies context-cancellable. The fixed Gate bundle is acquired before production
mutation and does not accept user-selected images or paths.

The current macOS unit suite cannot provide Linux namespace or KVM evidence.
Portable tests never claim KVM success. macOS cross-compiles the exact static
Linux binary and executes the portable contracts in Linux; only the signed VPS
receipt counts as a real microVM Gate.

The implementation follows the upstream Firecracker production guidance and
jailer interface:

- <https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md>
- <https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md>
- <https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md>

No package installation, long-running sandbox service, user Skill script, NIC,
MMDS, host directory mount, or general sandbox API is enabled by this Gate.
