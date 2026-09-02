import assert from "node:assert/strict";
import test from "node:test";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const testsDir = path.dirname(fileURLToPath(import.meta.url));
const packageJson = JSON.parse(
  readFileSync(path.resolve(testsDir, "../package.json"), "utf8"),
);

// The CI test scripts enumerate every test file explicitly, so a new test
// file is silently never executed unless it is added there. This inventory
// check fails closed: every test file under tests/ must be enumerated in
// test:node or test:unit.
test("every test file under tests/ is enumerated in package.json", () => {
  const enumerated = new Set(
    [packageJson.scripts["test:node"], packageJson.scripts["test:unit"]]
      .join(" ")
      .split(/\s+/)
      .filter((value) => value.startsWith("tests/")),
  );
  const files = readdirSync(testsDir).filter((name) =>
    /^.+\.(test|spec)\.(mjs|ts|tsx)$/.test(name),
  );
  const missing = files
    .map((name) => `tests/${name}`)
    .filter((name) => !enumerated.has(name));
  assert.deepEqual(
    missing,
    [],
    `test files never executed by CI; add them to test:node or test:unit: ${missing.join(", ")}`,
  );
});
