# Tools

This directory is the authority for build and release tool integrity metadata.
`toolchain.lock.json` pins required versions and upstream bytes. An explicit
`UNRESOLVED` value is a release blocker, not a wildcard.

Build and deployment run in GitHub Actions on ubuntu runners. The `Deploy`
workflow installs the pinned Go and Node toolchains from `toolchain.lock.json`
with archive SHA-256 verification, then installs the pinned Aliyun CLI and
ossutil the same way for web publication.

Wrangler is installed from `tools/wrangler/package-lock.json`; every npm
transitive package carries registry integrity and the root Wrangler tarball
integrity is also bound by `toolchain.lock.json`. The publisher never resolves
Wrangler from `PATH`.

Publication and deployment credentials are GitHub Actions repository secrets;
they are never embedded in this directory, the workflow payload, or the release
scripts.
