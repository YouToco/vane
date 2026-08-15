# Tools

This directory is the authority for build and release tool integrity metadata.
`toolchain.lock.json` pins required versions and upstream bytes. An explicit
`UNRESOLVED` value is a release blocker, not a wildcard.

Web publication uses the pinned Aliyun CLI, ossutil, Node, and Wrangler. Wrangler
is installed from `ops/release/wrangler/package-lock.json`; every npm transitive
package carries registry integrity and the root Wrangler tarball integrity is
also bound by `toolchain.lock.json`. The publisher never resolves Wrangler from
`PATH` or Homebrew.

The package lock proves registry bytes. Platform-specific entry SHA-256 values
for Node, Aliyun CLI, ossutil, and Wrangler plus a canonical installed-Wrangler
tree digest prove the bytes in the single-UID Mac cache. The canonical tree
includes every regular-file byte and internal symlink target while ignoring
owner/mode; two clean `npm ci --ignore-scripts` installs must yield the same
digest. The publisher checks these pins before execution and again after Gate,
and requires Wrangler's version output to equal the lock exactly. This is an
integrity layer for the single-UID release workflow, not a sandbox for
malicious trusted main code.
Only `darwin-arm64` publication-entry/tree pins are valid because Web mutation
is a release-Mac operation. Other platforms fail closed rather than inferring
installed bytes from archive checksums.
