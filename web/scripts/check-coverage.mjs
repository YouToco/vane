import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { evaluateCoverage, loadPolicyInputs } from "./coverage-policy.mjs";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(webRoot, "..");
const head = process.env.VANE_COVERAGE_HEAD_SHA?.trim() || "HEAD";
const base = process.env.VANE_COVERAGE_BASE_SHA?.trim() || `${head}^1`;
for (const revision of [base, head]) {
  execFileSync("git", ["rev-parse", "--verify", `${revision}^{commit}`], { cwd: repoRoot, stdio: "ignore" });
}

const result = evaluateCoverage(loadPolicyInputs(webRoot, base, head));
if (result.failures.length) {
  for (const failure of result.failures) console.error(`coverage policy: ${failure}`);
  process.exit(1);
}
console.log(
  `Coverage policy verified: ${result.changedCovered}/${result.changedTotal} changed executable lines ` +
    `(${result.changedPercent.toFixed(2)}%), base=${base}, head=${head}`,
);
