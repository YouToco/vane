import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export async function verifyP0aBuild(projectRoot) {
  const outputRoot = resolve(projectRoot, ".prototype-dist/p0a");
  const htmlPath = resolve(
    outputRoot,
    "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html",
  );
  const html = await readFile(htmlPath, "utf8");

  assert.match(
    html,
    /<script[^>]+src="\.\.\/\.\.\/assets\/[^"]+\.js"/,
    "prototype HTML must load only its emitted, relative JavaScript entry",
  );
  assert.doesNotMatch(
    html,
    /src\/main\.tsx|\/api\b/,
    "prototype HTML must not reference the production entry or API",
  );

  const assetsDir = resolve(outputRoot, "assets");
  const assetNames = await readdir(assetsDir);
  const javascript = assetNames.filter((name) => name.endsWith(".js"));
  assert.ok(javascript.length > 0, "prototype build must emit JavaScript");

  const bundles = await Promise.all(
    javascript.map((name) => readFile(resolve(assetsDir, name), "utf8")),
  );
  const bundledSource = bundles.join("\n");
  assert.match(
    bundledSource,
    /VANE_P0A_OWNER_PREVIEW/,
    "prototype marker must be present in the emitted bundle",
  );
  assert.doesNotMatch(
    bundledSource,
    /api\.vane\.zhuoqidev\.com/,
    "prototype bundle must not contain the production API origin",
  );

  return { htmlPath, javascript };
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  const projectRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
  const result = await verifyP0aBuild(projectRoot);
  console.log(
    `P0-A build verified: ${result.javascript.length} JavaScript bundle(s)`,
  );
}
