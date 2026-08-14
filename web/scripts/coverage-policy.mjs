import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

export function parseLcov(source) {
  const files = new Map();
  let current;
  for (const line of source.split(/\r?\n/)) {
    if (line.startsWith("SF:")) {
      current = line.slice(3).replaceAll("\\", "/");
      files.set(current, new Map());
    } else if (current && line.startsWith("DA:")) {
      const [lineNumber, hits] = line.slice(3).split(",", 2).map(Number);
      files.get(current).set(lineNumber, hits);
    } else if (line === "end_of_record") {
      current = undefined;
    }
  }
  return files;
}

export function parseChangedLines(diff) {
  const changed = new Map();
  let file;
  for (const line of diff.split(/\r?\n/)) {
    if (line.startsWith("+++ b/")) {
      let name = line.slice(6).replaceAll("\\", "/");
      if (name.startsWith("web/")) name = name.slice(4);
      file = /^src\/.*\.(?:ts|tsx)$/.test(name) ? name : undefined;
      if (file && !changed.has(file)) changed.set(file, new Set());
      continue;
    }
    if (!file || !line.startsWith("@@ ")) continue;
    const match = /\+(\d+)(?:,(\d+))?/.exec(line);
    if (!match) continue;
    const start = Number(match[1]);
    const count = match[2] === undefined ? 1 : Number(match[2]);
    for (let offset = 0; offset < count; offset += 1) {
      changed.get(file).add(start + offset);
    }
  }
  return changed;
}

function percent(covered, total) {
  return total === 0 ? 100 : (covered * 100) / total;
}

function packageName(file) {
  return file.slice(0, file.lastIndexOf("/"));
}

export function evaluateCoverage({ summary, lcov, changed, baseline }) {
  const failures = [];
  const tolerance = baseline.maxRegressionPoints;
  for (const metric of ["lines", "statements", "branches", "functions"]) {
    const actual = summary.total?.[metric]?.pct;
    const frozen = baseline.total?.[metric];
    if (typeof actual !== "number" || typeof frozen !== "number") {
      failures.push(`missing ${metric} coverage`);
    } else if (actual + tolerance < frozen) {
      failures.push(`${metric} coverage ${actual}% regressed below ${frozen - tolerance}%`);
    }
  }

  let changedTotal = 0;
  let changedCovered = 0;
  const touchedPackages = new Set();
  for (const [file, lines] of changed) {
    touchedPackages.add(packageName(file));
    const measured = lcov.get(file) ?? new Map();
    for (const line of lines) {
      if (!measured.has(line)) continue;
      changedTotal += 1;
      if (measured.get(line) > 0) changedCovered += 1;
    }
    if (Object.hasOwn(baseline.files, file)) {
      const absolute = Object.keys(summary).find((candidate) => candidate.replaceAll("\\", "/").endsWith(`/web/${file}`));
      const actual = absolute ? summary[absolute]?.lines?.pct : undefined;
      if (typeof actual !== "number") failures.push(`missing touched-file coverage for ${file}`);
      else if (actual + tolerance < baseline.files[file]) {
        failures.push(`${file} line coverage ${actual}% regressed below ${baseline.files[file] - tolerance}%`);
      }
    }
  }

  for (const name of touchedPackages) {
    if (!Object.hasOwn(baseline.packages, name)) continue;
    let total = 0;
    let covered = 0;
    for (const [absolute, metrics] of Object.entries(summary)) {
      if (absolute === "total") continue;
      const normalized = absolute.replaceAll("\\", "/");
      if (!normalized.includes(`/web/${name}/`)) continue;
      total += metrics.lines.total;
      covered += metrics.lines.covered;
    }
    const actual = percent(covered, total);
    if (actual + tolerance < baseline.packages[name]) {
      failures.push(`${name} line coverage ${actual.toFixed(2)}% regressed below ${baseline.packages[name] - tolerance}%`);
    }
  }

  const changedPercent = percent(changedCovered, changedTotal);
  if (changedTotal > 0 && changedPercent < baseline.minimumChangedLineCoverage) {
    failures.push(`changed-line coverage ${changedPercent.toFixed(2)}% is below ${baseline.minimumChangedLineCoverage}%`);
  }
  return { failures, changedTotal, changedCovered, changedPercent };
}

export function repositoryDiff(repoRoot, base, head) {
  return execFileSync("git", ["diff", "--find-renames=50%", "--unified=0", base, head, "--", "src", "web/src"], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
}

export function loadPolicyInputs(webRoot, base, head) {
  const repoRoot = path.resolve(webRoot, "..");
  const summary = JSON.parse(fs.readFileSync(path.join(webRoot, "coverage/coverage-summary.json"), "utf8"));
  const lcov = parseLcov(fs.readFileSync(path.join(webRoot, "coverage/lcov.info"), "utf8"));
  const baseline = JSON.parse(fs.readFileSync(path.join(webRoot, "coverage-baseline.json"), "utf8"));
  const changed = parseChangedLines(repositoryDiff(repoRoot, base, head));
  return { summary, lcov, baseline, changed };
}
