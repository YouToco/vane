# Tools

This directory is the authority for build and release tool integrity metadata.
`toolchain.lock.json` pins required versions and upstream bytes. An explicit
`UNRESOLVED` value is a release blocker, not a wildcard.

`wrangler/package-lock.json` is the complete npm integrity lock for the exact
Wrangler deployment tree.
