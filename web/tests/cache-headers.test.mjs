import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { test } from "node:test";

test("production headers revalidate HTML and keep hashed assets immutable", async () => {
  const headers = await readFile(resolve("public/_headers"), "utf8");

  assert.match(
    headers,
    /\/\*\s+Cache-Control: no-cache, must-revalidate/,
  );
  assert.match(
    headers,
    /\/assets\/\*\s+! Cache-Control\s+Cache-Control: public, max-age=31536000, immutable/,
  );
  assert.match(
    headers,
    /\/_preview\/\*\s+! Cache-Control\s+Cache-Control: no-store/,
  );
});

test("A2A keeps the well-known redirect while the release marker stays at root", async () => {
  const redirects = await readFile(resolve("public/_redirects"), "utf8");
  assert.match(
    redirects,
    /^\/\.well-known\/\* https:\/\/api\.vane\.zhuoqidev\.com\/\.well-known\/:splat 302$/m,
  );
  assert.doesNotMatch(redirects, /vane-release\.json/);
});
