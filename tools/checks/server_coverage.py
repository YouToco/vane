#!/usr/bin/env python3
"""Enforce frozen and changed-line coverage for the merged Go profile."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys


PROFILE_LINE = re.compile(
    r"^(?P<file>.+):(?P<start>\d+)\.\d+,(?P<end>\d+)\.\d+ "
    r"(?P<statements>\d+) (?P<count>\d+)$"
)
HUNK = re.compile(r"\+(?P<start>\d+)(?:,(?P<count>\d+))?")
EXACT_SHA = re.compile(r"^[0-9a-f]{40}$")


class CoverageError(RuntimeError):
    pass


@dataclass(frozen=True)
class Block:
    start: int
    end: int
    statements: int
    covered: bool


def normalize_profile_file(value: str) -> str:
    marker = "/vane/server/"
    if marker in value:
        value = value.split(marker, 1)[1]
    elif value.startswith("github.com/YouToco/vane/server/"):
        value = value.removeprefix("github.com/YouToco/vane/server/")
    value = value.replace("\\", "/")
    if value.startswith("server/"):
        return value
    if value.startswith("/") or ".." in Path(value).parts:
        raise CoverageError(f"unsafe coverage path: {value}")
    return f"server/{value}"


def parse_profile(payload: str) -> dict[str, list[Block]]:
    lines = payload.splitlines()
    if not lines or lines[0] not in {"mode: atomic", "mode: count", "mode: set"}:
        raise CoverageError("Go coverage profile has no supported mode header")
    result: dict[str, list[Block]] = {}
    for line_number, line in enumerate(lines[1:], start=2):
        match = PROFILE_LINE.fullmatch(line)
        if match is None:
            raise CoverageError(f"invalid Go coverage record at line {line_number}")
        file = normalize_profile_file(match.group("file"))
        block = Block(
            start=int(match.group("start")),
            end=int(match.group("end")),
            statements=int(match.group("statements")),
            covered=int(match.group("count")) > 0,
        )
        if block.start < 1 or block.end < block.start or block.statements < 0:
            raise CoverageError(f"invalid Go coverage range at line {line_number}")
        result.setdefault(file, []).append(block)
    if not result or not any(block.statements for blocks in result.values() for block in blocks):
        raise CoverageError("Go coverage profile contains zero statements")
    return result


def parse_changed_lines(payload: str) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    current: str | None = None
    for line in payload.splitlines():
        if line.startswith("+++ b/"):
            candidate = line[6:].replace("\\", "/")
            current = candidate if candidate.startswith("server/") and candidate.endswith(".go") else None
            if current is not None:
                result.setdefault(current, set())
            continue
        if current is None or not line.startswith("@@ "):
            continue
        match = HUNK.search(line)
        if match is None:
            continue
        start = int(match.group("start"))
        count = int(match.group("count") or "1")
        result[current].update(range(start, start + count))
    return result


def counts(blocks: list[Block]) -> tuple[int, int]:
    total = sum(block.statements for block in blocks)
    covered = sum(block.statements for block in blocks if block.covered)
    return covered, total


def pct(covered: int, total: int) -> float:
    return 100.0 if total == 0 else covered * 100.0 / total


def aggregate(profile: dict[str, list[Block]]) -> tuple[float, dict[str, float], dict[str, float]]:
    file_percentages: dict[str, float] = {}
    package_counts: dict[str, list[int]] = {}
    all_covered = 0
    all_total = 0
    for file, blocks in profile.items():
        covered, total = counts(blocks)
        all_covered += covered
        all_total += total
        file_percentages[file] = round(pct(covered, total), 2)
        package = file.rsplit("/", 1)[0]
        current = package_counts.setdefault(package, [0, 0])
        current[0] += covered
        current[1] += total
    packages = {
        package: round(pct(covered, total), 2)
        for package, (covered, total) in package_counts.items()
    }
    return round(pct(all_covered, all_total), 2), file_percentages, packages


def snapshot(profile: dict[str, list[Block]]) -> dict[str, object]:
    total, files, packages = aggregate(profile)
    return {
        "schema": "vane.server-coverage-baseline/v1",
        "max_regression_points": 0.5,
        "minimum_changed_line_coverage": 80,
        "total": total,
        "packages": dict(sorted(packages.items())),
        "files": dict(sorted(files.items())),
    }


def evaluate(
    profile: dict[str, list[Block]],
    changed: dict[str, set[int]],
    baseline: dict[str, object],
) -> tuple[list[str], int, int]:
    expected = {
        "schema", "max_regression_points", "minimum_changed_line_coverage",
        "total", "packages", "files",
    }
    if set(baseline) != expected or baseline.get("schema") != "vane.server-coverage-baseline/v1":
        raise CoverageError("server coverage baseline schema or keys are not exact")
    tolerance = baseline["max_regression_points"]
    minimum_changed = baseline["minimum_changed_line_coverage"]
    frozen_total = baseline["total"]
    frozen_files = baseline["files"]
    frozen_packages = baseline["packages"]
    if (
        isinstance(tolerance, bool) or not isinstance(tolerance, (int, float))
        or isinstance(minimum_changed, bool) or not isinstance(minimum_changed, (int, float))
        or isinstance(frozen_total, bool) or not isinstance(frozen_total, (int, float))
        or not isinstance(frozen_files, dict) or not isinstance(frozen_packages, dict)
    ):
        raise CoverageError("server coverage baseline contains invalid value types")

    total, files, packages = aggregate(profile)
    failures: list[str] = []
    if total + tolerance < frozen_total:
        failures.append(f"overall coverage {total:.2f}% regressed below {frozen_total - tolerance:.2f}%")

    touched_packages: set[str] = set()
    changed_total = 0
    changed_covered = 0
    for file, line_numbers in changed.items():
        touched_packages.add(file.rsplit("/", 1)[0])
        blocks = profile.get(file, [])
        for line_number in line_numbers:
            matching = [block for block in blocks if block.start <= line_number <= block.end]
            if not matching:
                continue
            changed_total += 1
            if all(block.covered for block in matching):
                changed_covered += 1
        if file in frozen_files:
            actual = files.get(file)
            if actual is None:
                failures.append(f"missing touched-file coverage for {file}")
            elif actual + tolerance < frozen_files[file]:
                failures.append(
                    f"{file} coverage {actual:.2f}% regressed below "
                    f"{frozen_files[file] - tolerance:.2f}%"
                )
    for package in touched_packages:
        if package in frozen_packages:
            actual = packages.get(package)
            if actual is None:
                failures.append(f"missing touched-package coverage for {package}")
            elif actual + tolerance < frozen_packages[package]:
                failures.append(
                    f"{package} coverage {actual:.2f}% regressed below "
                    f"{frozen_packages[package] - tolerance:.2f}%"
                )
    changed_percent = pct(changed_covered, changed_total)
    if changed_total and changed_percent < minimum_changed:
        failures.append(
            f"changed-line coverage {changed_percent:.2f}% is below {minimum_changed:.2f}%"
        )
    return failures, changed_covered, changed_total


def git_diff(root: Path, base: str, head: str) -> str:
    for revision in (base, head):
        resolved = subprocess.run(
            ["git", "rev-parse", "--verify", f"{revision}^{{commit}}"],
            cwd=root, text=True, capture_output=True, check=False,
        )
        if resolved.returncode != 0 or EXACT_SHA.fullmatch(resolved.stdout.strip()) is None:
            raise CoverageError(f"cannot resolve coverage revision: {revision}")
    result = subprocess.run(
        ["git", "diff", "--find-renames=50%", "--unified=0", base, head, "--", "server"],
        cwd=root, text=True, capture_output=True, check=False,
    )
    if result.returncode != 0:
        raise CoverageError(f"cannot compute coverage diff: {result.stderr.strip()}")
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args()
    try:
        if args.profile.is_symlink() or not args.profile.is_file():
            raise CoverageError("Go coverage profile is missing or unsafe")
        profile = parse_profile(args.profile.read_text(encoding="utf-8"))
        if args.snapshot:
            print(json.dumps(snapshot(profile), indent=2, sort_keys=True))
            return 0
        if None in (args.baseline, args.repo_root, args.base):
            raise CoverageError("coverage check requires baseline, repo root, base, and head")
        if args.baseline.is_symlink() or not args.baseline.is_file():
            raise CoverageError("server coverage baseline is missing or unsafe")
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        changed = parse_changed_lines(git_diff(args.repo_root, args.base, args.head))
        failures, covered, total = evaluate(profile, changed, baseline)
        if failures:
            raise CoverageError("; ".join(failures))
        print(f"Server coverage verified: {covered}/{total} changed executable lines")
        return 0
    except (CoverageError, OSError, UnicodeError, json.JSONDecodeError) as error:
        print(f"server coverage refusal: {error}", file=sys.stderr)
        return 78


if __name__ == "__main__":
    raise SystemExit(main())
