import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

export const RELEASE_MARKER_SCHEMA = "vane.web-release/v1";
export const RELEASE_MARKER_RELATIVE_PATH = "vane-release.json";

function sha256(payload) {
  return createHash("sha256").update(payload).digest("hex");
}

function validateRevision(revision) {
  assert.match(revision, /^[0-9a-f]{40}$/, "release revision must be a lowercase Git SHA");
  return revision;
}

async function releaseFiles(root, current = root) {
  const entries = await readdir(current, { withFileTypes: true });
  const files = [];
  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const absolute = resolve(current, entry.name);
    const path = relative(root, absolute).split(sep).join("/");
    if (path === RELEASE_MARKER_RELATIVE_PATH) continue;
    assert.ok(!path.includes("\n") && !path.includes("\r"), "release path contains a line break");
    assert.ok(!entry.isSymbolicLink(), `release tree contains a symlink: ${path}`);
    if (entry.isDirectory()) {
      files.push(...(await releaseFiles(root, absolute)));
    } else {
      assert.ok(entry.isFile(), `release tree contains a non-regular file: ${path}`);
      files.push(path);
    }
  }
  return files.sort();
}

export async function calculateReleaseTree(distRoot) {
  const files = await releaseFiles(distRoot);
  assert.ok(files.length > 0, "release tree is empty");
  const entries = [];
  for (const path of files) {
    const payload = await readFile(resolve(distRoot, path));
    entries.push(`${sha256(payload)}  ${path}\n`);
  }
  return { fileCount: files.length, treeSha256: sha256(entries.join("")) };
}

export async function generateReleaseMarker({ distRoot, revision, sourceDirty }) {
  validateRevision(revision);
  assert.equal(typeof sourceDirty, "boolean", "sourceDirty must be boolean");
  const tree = await calculateReleaseTree(distRoot);
  const marker = {
    schema: RELEASE_MARKER_SCHEMA,
    source_revision: revision,
    source_dirty: sourceDirty,
    tree_sha256: tree.treeSha256,
    file_count: tree.fileCount,
  };
  const markerPath = resolve(distRoot, RELEASE_MARKER_RELATIVE_PATH);
  await mkdir(dirname(markerPath), { recursive: true });
  await writeFile(markerPath, `${JSON.stringify(marker)}\n`, { encoding: "utf8", flag: "w" });
  return marker;
}

export async function verifyReleaseMarker({
  distRoot,
  expectedRevision,
  requireClean = false,
}) {
  validateRevision(expectedRevision);
  const markerPath = resolve(distRoot, RELEASE_MARKER_RELATIVE_PATH);
  const payload = await readFile(markerPath, "utf8");
  const marker = JSON.parse(payload);
  assert.deepEqual(Object.keys(marker), [
    "schema",
    "source_revision",
    "source_dirty",
    "tree_sha256",
    "file_count",
  ]);
  assert.equal(payload, `${JSON.stringify(marker)}\n`, "release marker is not canonical JSON");
  assert.equal(marker.schema, RELEASE_MARKER_SCHEMA);
  assert.equal(marker.source_revision, expectedRevision);
  assert.equal(typeof marker.source_dirty, "boolean");
  if (requireClean) assert.equal(marker.source_dirty, false, "release source is dirty");
  assert.match(marker.tree_sha256, /^[0-9a-f]{64}$/);
  assert.ok(Number.isSafeInteger(marker.file_count) && marker.file_count > 0);
  const tree = await calculateReleaseTree(distRoot);
  assert.equal(marker.tree_sha256, tree.treeSha256, "release tree digest differs");
  assert.equal(marker.file_count, tree.fileCount, "release tree file count differs");
  return marker;
}

function gitOutput(projectRoot, args) {
  return execFileSync("git", args, {
    cwd: projectRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  }).trim();
}

export function sourceRevision(projectRoot, configuredRevision = process.env.VANE_RELEASE_SHA?.trim()) {
  const actual = validateRevision(
    gitOutput(projectRoot, ["rev-parse", "--verify", "HEAD"]),
  );
  if (!configuredRevision) return actual;
  const configured = validateRevision(configuredRevision);
  assert.equal(
    configured,
    actual,
    "VANE_RELEASE_SHA must equal the checked-out monorepo HEAD",
  );
  return actual;
}

function sourceDirty(projectRoot) {
  return gitOutput(projectRoot, ["status", "--porcelain", "--untracked-files=all"]) !== "";
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  const command = process.argv[2];
  assert.ok(command === "generate" || command === "verify", "usage: release-marker.mjs generate|verify");
  const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const distRoot = resolve(projectRoot, "dist");
  const revision = sourceRevision(projectRoot);
  const requireClean = process.env.VANE_REQUIRE_CLEAN_RELEASE === "1";
  assert.ok(
    !process.env.VANE_REQUIRE_CLEAN_RELEASE ||
      process.env.VANE_REQUIRE_CLEAN_RELEASE === "0" ||
      requireClean,
    "VANE_REQUIRE_CLEAN_RELEASE must be 0 or 1",
  );
  const marker = command === "generate"
    ? await generateReleaseMarker({ distRoot, revision, sourceDirty: sourceDirty(projectRoot) })
    : await verifyReleaseMarker({ distRoot, expectedRevision: revision, requireClean });
  console.log(
    `Web release ${command} verified: ${marker.source_revision} ${marker.tree_sha256}`,
  );
}
