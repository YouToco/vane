# Tools

This directory is the authority for build and release tool integrity metadata.
`toolchain.lock.json` pins required versions and upstream bytes. An explicit
`UNRESOLVED` value is a release blocker, not a wildcard.

Web publication uses only the pinned Aliyun CLI and ossutil. Cloudflare Pages
and Wrangler are intentionally absent; OSS plus CDN is the single Web authority.
