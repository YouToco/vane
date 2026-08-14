import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const projectRoot = resolve(import.meta.dirname, "..");
const distRoot = resolve(projectRoot, "dist");
const previewRelative =
  "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html";

function scriptSource(html, label) {
  const match = html.match(/<script[^>]+src="([^"]+\.js)"/);
  assert.ok(match, `${label} must reference an emitted JavaScript entry`);
  return match[1];
}

function emittedPath(source) {
  return resolve(distRoot, source.replace(/^\.?\//, ""));
}

const [appHtml, previewHtml, headers, mainSource, componentSource, fixtureSource] =
  await Promise.all([
    readFile(resolve(distRoot, "index.html"), "utf8"),
    readFile(resolve(distRoot, previewRelative), "utf8"),
    readFile(resolve(distRoot, "_headers"), "utf8"),
    readFile(
      resolve(projectRoot, "src/prototypes/p0a-task-brief/main.tsx"),
      "utf8",
    ),
    readFile(
      resolve(
        projectRoot,
        "src/prototypes/p0a-task-brief/PrototypeTaskDetail.tsx",
      ),
      "utf8",
    ),
    readFile(
      resolve(projectRoot, "src/prototypes/p0a-task-brief/fixture.ts"),
      "utf8",
    ),
  ]);

const appBundle = await readFile(emittedPath(scriptSource(appHtml, "app")), "utf8");
const previewBundle = await readFile(
  emittedPath(scriptSource(previewHtml, "owner preview")),
  "utf8",
);
const javascriptNames = (await readdir(resolve(distRoot, "assets"))).filter(
  (name) => name.endsWith(".js"),
);
const javascriptEntries = await Promise.all(
  javascriptNames.map(async (name) => ({
    name,
    source: await readFile(resolve(distRoot, "assets", name), "utf8"),
  })),
);
const markerEntries = javascriptEntries.filter(({ source }) =>
  source.includes("VANE_P0A_OWNER_PREVIEW"),
);

assert.deepEqual(
  markerEntries.map(({ name }) => name),
  javascriptNames.filter((name) => name.startsWith("ownerPreview-")),
  "only the isolated owner preview entry may contain its marker",
);
assert.doesNotMatch(
  appBundle,
  /VANE_P0A_OWNER_PREVIEW|p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/,
  "the normal app entry must not expose or import the owner preview",
);
assert.doesNotMatch(
  `${previewHtml}\n${previewBundle}`,
  /api\.vane\.zhuoqidev\.com|allan_guodpl/,
  "the owner preview must not contain the production API origin or account identifier",
);
assert.match(previewHtml, /noindex, nofollow, noarchive/);
assert.match(previewHtml, /connect-src 'none'/);
assert.match(headers, /\/_preview\/\*/);
assert.match(headers, /Cache-Control: no-store/);
assert.match(headers, /X-Robots-Tag: noindex, nofollow, noarchive/);

const isolatedSource = `${mainSource}\n${componentSource}\n${fixtureSource}`;
for (const forbidden of [
  /\bfetch\s*\(/,
  /\/api\b/,
  /@\/api/,
  /\bXMLHttpRequest\b/,
  /\baxios\b/,
  /\bimport\s*\(/,
  /\blocalStorage\b/,
  /\bsessionStorage\b/,
  /\bindexedDB\b/,
  /document\.cookie/,
  /\bsendBeacon\b/,
  /\bWebSocket\b/,
  /\bEventSource\b/,
  /\bserviceWorker\b/,
]) {
  assert.doesNotMatch(
    isolatedSource,
    forbidden,
    `owner preview source must not use ${forbidden}`,
  );
}

assert.doesNotMatch(
  isolatedSource,
  /allan_guodpl|AI 市场与产品情报群|deliveries7d:\s*28|llmCostUSD:\s*0\.0008/,
  "owner preview source must not contain known production-derived identifiers",
);

console.log(
  `P0-A production build verified: app + hidden owner preview ${previewRelative}`,
);
