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
