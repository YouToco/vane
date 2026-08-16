import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { after, test } from "node:test";
import {
  PRODUCTION_API_ORIGIN,
  verifyProductionApiBase,
} from "../scripts/verify-production-api-base.mjs";

const fixtures = [];
after(async () => Promise.all(fixtures.map((path) => rm(path, { recursive: true, force: true }))));

async function fixture(source) {
  const root = await mkdtemp(join(tmpdir(), "vane-web-api-base-"));
  fixtures.push(root);
  await mkdir(resolve(root, "assets"));
  await writeFile(resolve(root, "assets/app.js"), source);
  return root;
}

test("accepts a production bundle bound to the public API", async () => {
  const root = await fixture(`export const api=${JSON.stringify(PRODUCTION_API_ORIGIN)};`);
  await verifyProductionApiBase(root);
});

test("rejects the relative-only bundle that sends login to static hosting", async () => {
  const root = await fixture('fetch("/api/auth/login",{method:"POST"});');
  await assert.rejects(
    () => verifyProductionApiBase(root),
    /production JavaScript does not bind https:\/\/api\.vane\.zhuoqidev\.com/,
  );
});
