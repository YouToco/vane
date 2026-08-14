import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { after, test } from "node:test";
import {
  RELEASE_MARKER_RELATIVE_PATH,
  generateReleaseMarker,
  sourceRevision,
  verifyReleaseMarker,
} from "../scripts/release-marker.mjs";

const fixtures = [];
after(async () => Promise.all(fixtures.map((path) => rm(path, { recursive: true, force: true }))));

test("release marker uses the CDN-safe root commit path", () => {
  assert.equal(RELEASE_MARKER_RELATIVE_PATH, "vane-release.json");
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "vane-web-release-"));
  fixtures.push(root);
  await mkdir(resolve(root, "assets"));
  await writeFile(resolve(root, "index.html"), "<!doctype html>\n");
  await writeFile(resolve(root, "assets/app.js"), "export const app = true;\n");
  return root;
}

test("generates and verifies a canonical release tree marker", async () => {
  const distRoot = await fixture();
  const revision = "1".repeat(40);
  const marker = await generateReleaseMarker({ distRoot, revision, sourceDirty: false });
  assert.equal(marker.file_count, 2);
  assert.equal((await readFile(resolve(distRoot, RELEASE_MARKER_RELATIVE_PATH), "utf8")), `${JSON.stringify(marker)}\n`);
  await verifyReleaseMarker({ distRoot, expectedRevision: revision });
});

test("rejects a release tree changed after marker generation", async () => {
  const distRoot = await fixture();
  const revision = "2".repeat(40);
  await generateReleaseMarker({ distRoot, revision, sourceDirty: true });
  await writeFile(resolve(distRoot, "assets/app.js"), "export const app = false;\n");
  await assert.rejects(
    () => verifyReleaseMarker({ distRoot, expectedRevision: revision }),
    /release tree digest differs/,
  );
});

test("clean-release verification rejects a dirty source marker", async () => {
  const distRoot = await fixture();
  const revision = "3".repeat(40);
  await generateReleaseMarker({ distRoot, revision, sourceDirty: true });
  await assert.rejects(
    () => verifyReleaseMarker({ distRoot, expectedRevision: revision, requireClean: true }),
    /release source is dirty/,
  );
});

test("configured release SHA must equal the checked-out monorepo HEAD", () => {
  const projectRoot = resolve(import.meta.dirname, "..");
  const actual = sourceRevision(projectRoot);
  assert.match(actual, /^[0-9a-f]{40}$/);
  const different = actual === "1".repeat(40) ? "2".repeat(40) : "1".repeat(40);
  assert.throws(
    () => sourceRevision(projectRoot, different),
    /must equal the checked-out monorepo HEAD/,
  );
});
