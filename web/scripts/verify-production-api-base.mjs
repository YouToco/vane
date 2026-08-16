import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const PRODUCTION_API_ORIGIN = "https://api.vane.zhuoqidev.com";

export async function verifyProductionApiBase(distRoot) {
  const assetsRoot = resolve(distRoot, "assets");
  const javascript = (await readdir(assetsRoot, { withFileTypes: true }))
    .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
    .map((entry) => resolve(assetsRoot, entry.name));

  assert.ok(javascript.length > 0, "production build has no JavaScript assets");
  const sources = await Promise.all(javascript.map((file) => readFile(file, "utf8")));
  assert.ok(
    sources.some((source) => source.includes(PRODUCTION_API_ORIGIN)),
    `production JavaScript does not bind ${PRODUCTION_API_ORIGIN}`,
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const projectRoot = resolve(import.meta.dirname, "..");
  await verifyProductionApiBase(resolve(projectRoot, "dist"));
  console.log(`Production API base verified: ${PRODUCTION_API_ORIGIN}`);
}
