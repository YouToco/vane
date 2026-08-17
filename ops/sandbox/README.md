# Firecracker release artifacts

`prepare_artifacts.py` is called only by the trusted exact-main artifact stage.
It downloads the official non-debug Firecracker and jailer v1.16.1 x86_64
release archive plus the Firecracker-CI Linux 5.10.245 kernel over HTTPS, checks
the committed byte sizes and SHA-256 values, extracts only the two named regular
members, and builds a deterministic `newc` initramfs from the release `sandboxd`
binary. It also creates the fixed 4 KiB read-only code image used only by the
known-answer Gate.

The output manifest binds the source revision, sandboxd digest, Firecracker
version, and each artifact's digest and size. The backend artifact manifest
binds that file and every artifact byte. Acquisition never occurs on the VPS,
and the release will fail before submission on a redirect outside HTTPS,
partial output, unexpected member metadata, size drift, or digest drift.

This directory participates in a three-release controller transition. The
compatibility bridge and then the hardened base-only controller must each be
finalized and promoted before the product inventory activation is merged and
released; see `docs/sandbox/README.md`.
