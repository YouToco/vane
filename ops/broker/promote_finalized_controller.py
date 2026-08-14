#!/usr/bin/env python3
"""Promote only a controller whose product revision was already finalized."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re


EXACT_SHA = re.compile(r"^[0-9a-f]{40}$")


def strict_json(path: Path) -> dict:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise RuntimeError(f"duplicate current-release key: {key}")
            value[key] = item
        return value

    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
        raise RuntimeError("current-release authority is unsafe")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise RuntimeError("current-release root is invalid")
    return value


def revision_from_target(target: Path, releases: Path) -> str:
    try:
        relative = target.relative_to(releases)
    except ValueError as error:
        raise RuntimeError("active controller target escapes release authority") from error
    if len(relative.parts) != 1 or EXACT_SHA.fullmatch(relative.name) is None:
        raise RuntimeError("active controller target is not an exact revision")
    return relative.name


def promote(*, state: Path, control_root: Path, evidence_root: Path) -> Path:
    value = strict_json(state)
    monorepo = value.get("monorepo_revision")
    controller = value.get("controller_revision")
    if not all(isinstance(item, str) and EXACT_SHA.fullmatch(item) for item in (monorepo, controller)):
        raise RuntimeError("current-release controller revisions are invalid")

    releases = control_root / "releases"
    current = control_root / "current"
    if control_root.is_symlink() or releases.is_symlink() or not releases.is_dir():
        raise RuntimeError("controller release authority is unsafe")
    if not current.is_symlink():
        raise RuntimeError("active controller authority is not a symlink")
    active_target = current.resolve(strict=True)
    active_revision = revision_from_target(active_target, releases.resolve(strict=True))
    active_launcher = active_target / "ops/broker/run-production-handler.sh"
    active_marker = active_target / ".controller-archive.sha256"
    if (
        active_launcher.is_symlink()
        or not active_launcher.is_file()
        or not os.access(active_launcher, os.X_OK)
        or active_marker.is_symlink()
        or not active_marker.is_file()
    ):
        raise RuntimeError("active controller is incomplete")
    if active_revision == monorepo:
        return active_launcher
    if active_revision != controller:
        raise RuntimeError("active controller and durable state are inconsistent")

    target_path = releases / monorepo
    finalize = evidence_root / "releases" / monorepo / "manifests/finalize.json"
    # The one-time bootstrap legitimately starts with controller B alongside a
    # legacy product N that never had a controller archive. B handles the first
    # normal release; thereafter every finalized product must have both target
    # and finalize evidence, so partial authority fails closed.
    if not target_path.exists() and not finalize.exists():
        return active_launcher
    if not target_path.exists() or not finalize.exists():
        raise RuntimeError("finalized next controller is unavailable")
    target = target_path.resolve(strict=True)
    revision_from_target(target, releases.resolve(strict=True))
    launcher = target / "ops/broker/run-production-handler.sh"
    marker = target / ".controller-archive.sha256"
    if (
        target.is_symlink()
        or not target.is_dir()
        or launcher.is_symlink()
        or not launcher.is_file()
        or not os.access(launcher, os.X_OK)
        or marker.is_symlink()
        or not marker.is_file()
        or finalize.is_symlink()
        or not finalize.is_file()
    ):
        raise RuntimeError("finalized next controller is unavailable")

    pending = control_root / f".current-{monorepo}.{os.getpid()}"
    if pending.exists() or pending.is_symlink():
        raise RuntimeError("controller promotion staging link already exists")
    pending.symlink_to(target)
    os.replace(pending, current)
    return launcher


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("/var/lib/vane-broker/state/current-release.json"),
    )
    parser.add_argument(
        "--control-root", type=Path, default=Path("/opt/vane-control")
    )
    parser.add_argument(
        "--evidence-root", type=Path, default=Path("/var/lib/vane-broker/evidence")
    )
    parser.add_argument(
        "--lock",
        type=Path,
        default=Path("/var/lib/vane-broker/state/broker-work/release.lock"),
    )
    args = parser.parse_args()
    if args.lock.is_symlink() or not args.lock.is_file():
        raise RuntimeError("controller promotion lock authority is unsafe")
    with args.lock.open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        launcher = promote(
            state=args.state,
            control_root=args.control_root,
            evidence_root=args.evidence_root,
        )
    forced_command = launcher.with_name("forced_command.py")
    if (
        forced_command.is_symlink()
        or not forced_command.is_file()
        or not os.access(forced_command, os.X_OK)
    ):
        raise RuntimeError("promoted forced-command controller is unavailable")
    print(forced_command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
