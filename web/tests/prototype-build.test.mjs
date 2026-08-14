import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { verifyP0aBuild } from "../scripts/verify-p0a-build.mjs";

async function fixture({ html, bundle }) {
  const root = await mkdtemp(join(tmpdir(), "vane-p0a-build-"));
  const output = join(root, ".prototype-dist/p0a");
  const preview = join(
    output,
    "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42",
  );
  await mkdir(preview, { recursive: true });
  await mkdir(join(output, "assets"), { recursive: true });
  await writeFile(join(preview, "index.html"), html);
  await writeFile(join(output, "assets/p0a.js"), bundle);
  return root;
}

test("accepts an isolated P0-A build", async () => {
  const root = await fixture({
    html: '<script type="module" src="../../assets/p0a.js"></script>',
    bundle: 'const marker="VANE_P0A_OWNER_PREVIEW";',
  });
  const result = await verifyP0aBuild(root);
  assert.deepEqual(result.javascript, ["p0a.js"]);
});

test("rejects a build coupled to the production API", async () => {
  const root = await fixture({
    html: '<script type="module" src="../../assets/p0a.js"></script>',
    bundle:
      'const marker="VANE_P0A_OWNER_PREVIEW";fetch("https://api.vane.zhuoqidev.com");',
  });
  await assert.rejects(() => verifyP0aBuild(root), /production API origin/);
});
