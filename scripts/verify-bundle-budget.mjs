import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { gzipSync, constants as zlibConstants } from "node:zlib";

const projectRoot = resolve(import.meta.dirname, "..");
const distRoot = resolve(projectRoot, "dist");

const [budget, manifest, audit] = await Promise.all([
  readFile(resolve(projectRoot, "config/bundle-budget.json"), "utf8").then(
    JSON.parse,
  ),
  readFile(resolve(distRoot, ".vite/manifest.json"), "utf8").then(JSON.parse),
  readFile(resolve(distRoot, ".vite/bundle-modules.json"), "utf8").then(
    JSON.parse,
  ),
]);

assert.equal(budget.version, 2, "unsupported bundle budget version");
assert.equal(audit.version, 1, "unsupported bundle audit manifest version");

function requireManifestEntry(key) {
  const entry = manifest[key];
  assert.ok(entry, `Vite manifest is missing ${key}`);
  return entry;
}

const appEntry = requireManifestEntry(budget.entry);
const authenticatedEntry = requireManifestEntry(budget.authenticatedEntry);
const defaultAuthenticatedRoute = requireManifestEntry(
  budget.defaultAuthenticatedRoute,
);
assert.equal(appEntry.isEntry, true, `${budget.entry} must remain the app entry`);
assert.equal(
  authenticatedEntry.isDynamicEntry,
  true,
  "authenticated shell must be separated from the public app bootstrap",
);
assert.equal(
  defaultAuthenticatedRoute.isDynamicEntry,
  true,
  "default authenticated route must remain lazy",
);
for (const boundary of [
  budget.authenticatedEntry,
  "src/pages/Landing.tsx",
  "src/pages/Login.tsx",
]) {
  assert.ok(
    appEntry.dynamicImports?.includes(boundary),
    `${boundary} must be a direct lazy boundary of the app bootstrap`,
  );
}

for (const route of budget.requiredLazyRoutes) {
  assert.equal(
    requireManifestEntry(route).isDynamicEntry,
    true,
    `${route} must remain a lazy route`,
  );
}
const authenticatedRoutes = budget.requiredLazyRoutes.filter(
  (key) =>
    key !== "src/pages/Landing.tsx" && key !== "src/pages/Login.tsx",
);
assert.ok(
  authenticatedRoutes.includes(budget.defaultAuthenticatedRoute),
  "default authenticated route must be listed as a required lazy route",
);
for (const route of authenticatedRoutes) {
  assert.ok(
    authenticatedEntry.dynamicImports?.includes(route),
    `${route} must be a direct lazy route of the authenticated shell`,
  );
}

function collectStaticGraph(startKeys) {
  const pending = [...startKeys];
  const seen = new Set();
  while (pending.length > 0) {
    const key = pending.pop();
    if (seen.has(key)) continue;
    seen.add(key);
    const entry = requireManifestEntry(key);
    for (const imported of entry.imports ?? []) pending.push(imported);
  }
  return seen;
}

// The shell graph is useful for enforcing route boundaries, but it is not a
// usable authenticated screen. The default Home route is requested
// immediately after the shell renders, so the budget below measures the union
// of bootstrap + shell + Home and all of their static imports.
const authenticatedShellGraph = collectStaticGraph([
  budget.entry,
  budget.authenticatedEntry,
]);
for (const route of budget.requiredLazyRoutes) {
  assert.ok(
    !authenticatedShellGraph.has(route),
    `${route} leaked into the authenticated initial graph`,
  );
}

const routeGraphs = new Map(
  authenticatedRoutes.map((route) => [
    route,
    collectStaticGraph([
      budget.entry,
      budget.authenticatedEntry,
      route,
    ]),
  ]),
);
const defaultAuthenticatedGraph = routeGraphs.get(
  budget.defaultAuthenticatedRoute,
);
assert.ok(defaultAuthenticatedGraph, "default authenticated graph is missing");

function javascriptFiles(graph) {
  return [
    ...new Set(
      [...graph]
      .map((key) => requireManifestEntry(key).file)
      .filter((file) => file.endsWith(".js")),
    ),
  ].sort();
}

const modulesByFile = new Map(
  audit.chunks.map((chunk) => [chunk.file, chunk.modules]),
);

function assertNoForbiddenModules(label, graph) {
  const modules = javascriptFiles(graph).flatMap((file) => {
    const chunkModules = modulesByFile.get(file);
    assert.ok(chunkModules, `bundle audit manifest is missing ${file}`);
    return chunkModules;
  });
  for (const forbidden of budget.forbiddenAuthenticatedInitialModules) {
    const leaked = modules.filter(
      (moduleID) =>
        moduleID === forbidden ||
        (forbidden.endsWith("/") && moduleID.startsWith(forbidden)),
    );
    assert.deepEqual(
      leaked,
      [],
      `${forbidden} must not enter ${label}`,
    );
  }
}

assertNoForbiddenModules("the authenticated shell", authenticatedShellGraph);
for (const [route, graph] of routeGraphs) {
  assertNoForbiddenModules(`authenticated route ${route}`, graph);
}

const gzipByFile = new Map();
async function gzipBytes(file) {
  let size = gzipByFile.get(file);
  if (size == null) {
    const source = await readFile(resolve(distRoot, file));
    size = gzipSync(source, {
      level: zlibConstants.Z_BEST_COMPRESSION,
    }).byteLength;
    gzipByFile.set(file, size);
  }
  return size;
}

async function measureGraph(label, graph) {
  const files = javascriptFiles(graph);
  const sizes = await Promise.all(
    files.map(async (file) => ({ file, gzipBytes: await gzipBytes(file) })),
  );
  return {
    label,
    files: sizes,
    gzipBytes: sizes.reduce((total, item) => total + item.gzipBytes, 0),
  };
}

const [shellReport, ...routeReports] = await Promise.all([
  measureGraph("authenticated shell", authenticatedShellGraph),
  ...[...routeGraphs].map(([route, graph]) => measureGraph(route, graph)),
]);
const defaultReport = routeReports.find(
  (report) => report.label === budget.defaultAuthenticatedRoute,
);
assert.ok(defaultReport, "default authenticated route report is missing");

const reductionPercent =
  ((budget.baselineDefaultRouteJavaScriptGzipBytes -
    defaultReport.gzipBytes) /
    budget.baselineDefaultRouteJavaScriptGzipBytes) *
  100;

assert.ok(
  reductionPercent >= budget.minimumReductionPercent,
  `default authenticated route JS gzip reduction ${reductionPercent.toFixed(2)}% ` +
    `is below ${budget.minimumReductionPercent}%`,
);
console.log(
  [
    "Bundle budget verified:",
    `${defaultReport.gzipBytes} default authenticated Home gzip bytes`,
    `(${reductionPercent.toFixed(2)}% below ${budget.baselineDefaultRouteJavaScriptGzipBytes})`,
    `shell=${shellReport.gzipBytes}`,
    `routes=${routeReports.map(({ label, gzipBytes: bytes }) => `${label}:${bytes}`).join(",")}`,
  ].join(" "),
);
