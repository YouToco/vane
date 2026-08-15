# Firecracker sandbox dark foundation

This package is a deliberately non-executable security foundation. It is not
wired into Vane Server or the Skill runtime, and no deployment unit is enabled.
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
- Firecracker, jailer, kernel, root filesystem, and every code image are
  absolute, root-owned, single-link, non-writable paths with exact SHA-256 pins.
  Firecracker and jailer must report the same exact non-debug release.
  Those digests are authority only after an external trusted release-approval
  process; this dark foundation does not yet embed an official release manifest.
- Every invocation receives a derived jailer-safe ID, a distinct host UID/GID
  slot, a new work directory, a new microVM plan, read-only root/code drives,
  non-root guest identity, bounded tmpfs boot contract, CPU/memory/PID cgroups,
  wall timeout, output cap, process-group kill, and checked scrub.
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
- Configuration is read only from an absolute root-owned 0400/0600 regular
  file with one link and a root-owned, non-writable, non-symlink parent chain.
  Wire responses contain fixed error codes, never internal paths or principals.

## Intentionally missing before any execution rollout

Production execution is impossible in this version: both the exported service
and Firecracker backend remain dark. The package-internal launch switch exists
only so unit/Linux integration tests can inspect plans and cleanup behavior.
Activation requires a separate reviewed project that adds, at minimum:

1. a durable append-only invocation receipt/lease ledger that survives
   sandboxd restarts and binds the full request digest;
2. an authenticated, framed guest input/output protocol and a pinned guest
   init that enforces the tmpfs and non-root contracts;
3. real Linux/KVM Firecracker integration evidence for timeout, fork/output
   abuse, crash cleanup, concurrent tenants, no-network, and metadata denial;
4. independent security approval and a new explicit rollout gate.

That activation gate must also add an approved `SizeBytes` to every artifact
pin, place acquisition/plan/copy under the invocation wall deadline, make copy
context-cancellable, and prove that launcher output-limit errors always map to
`killed/output_limit`. These are not reachable while both production Run paths
remain permanently dark.

The current macOS unit suite cannot provide Linux namespace or KVM evidence.
`sandboxd self-test` therefore fails outside a correctly provisioned Linux
host; it never degrades to a portable success.

The implementation follows the upstream Firecracker production guidance and
jailer interface:

- <https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md>
- <https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md>
- <https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md>

No package installation, service enablement, VPS mutation, or Skill script
activation is part of this foundation.
