# Release primitives

These are internal release primitives: Server artifact packing/validation,
native atomic cutover, binary receipts, provider-neutral Web planning, and the
Cloudflare Pages + Aliyun OSS/CDN combined publication transaction. They are
not independent operator commands;
`../bin/vane` is the only supported entry point.

The Web primitive hashes the locked Aliyun CLI, ossutil, Node 22.23.2, and
Wrangler 4.115.0 entry bytes plus Wrangler's complete canonical npm tree,
including safe internal `.bin` symlinks. Expected platform digests live in the
toolchain lock and are rechecked after Gate; version output alone is not
integrity evidence.

Before any Cloudflare direct-upload commit, the publisher records the observed
previous canonical deployment ID in durable pending state. A crash after the
commit therefore adopts the exact new deployment without replacing that
history with the new ID. Cloudflare provider receipt v2 binds the captured ID;
when no exact prior canonical can be proved it records `null`, which permits
publication but makes later retention fail closed.
