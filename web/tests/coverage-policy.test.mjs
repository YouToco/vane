import assert from "node:assert/strict";
import test from "node:test";
import { evaluateCoverage, parseChangedLines, parseLcov } from "../scripts/coverage-policy.mjs";

const baseline = {
  maxRegressionPoints: 0.5,
  minimumChangedLineCoverage: 80,
  total: { lines: 50, statements: 50, branches: 50, functions: 50 },
  files: { "src/app.ts": 50 },
  packages: { src: 50 },
};
const summary = {
  total: Object.fromEntries(["lines", "statements", "branches", "functions"].map((key) => [key, { pct: 50 }])),
  "/repo/web/src/app.ts": { lines: { pct: 50, total: 2, covered: 1 } },
};

test("parses legacy-to-monorepo rename hunks and executable lines", () => {
  const diff = "+++ b/web/src/app.ts\n@@ -1,0 +1,2 @@\n";
  const changed = parseChangedLines(diff);
  const lcov = parseLcov("SF:src/app.ts\nDA:1,1\nDA:2,0\nend_of_record\n");
  const result = evaluateCoverage({ summary, lcov, changed, baseline });
  assert.equal(result.changedTotal, 2);
  assert.equal(result.changedCovered, 1);
  assert.match(result.failures.join("\n"), /changed-line coverage/);
});

test("fails closed on missing coverage and baseline regression", () => {
  const regressed = structuredClone(summary);
  regressed.total.lines.pct = 49.49;
  delete regressed.total.branches;
  const result = evaluateCoverage({ summary: regressed, lcov: new Map(), changed: new Map(), baseline });
  assert.match(result.failures.join("\n"), /lines coverage/);
  assert.match(result.failures.join("\n"), /missing branches coverage/);
});

test("accepts at most a half-point regression and 80 percent changed lines", () => {
  const diff = "+++ b/src/new.ts\n@@ -0,0 +1,5 @@\n";
  const lcov = parseLcov("SF:src/new.ts\nDA:1,1\nDA:2,1\nDA:3,1\nDA:4,1\nDA:5,0\nend_of_record\n");
  const atFloor = structuredClone(summary);
  for (const metric of Object.values(atFloor.total)) metric.pct = 49.5;
  const result = evaluateCoverage({ summary: atFloor, lcov, changed: parseChangedLines(diff), baseline });
  assert.deepEqual(result.failures, []);
  assert.equal(result.changedPercent, 80);
});
