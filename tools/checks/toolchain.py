"""Validate installed executables, cached artifacts, and production image pins."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def machine_arch() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    architecture = "arm64" if machine in {"arm64", "aarch64"} else "amd64"
    return f"{system}-{architecture}"


def check(lock_path: Path, cache: Path, repo_root: Path) -> list[str]:
    lock = json.loads(lock_path.read_text(encoding="utf-8"))["tools"]
    errors: list[str] = []
    arch = machine_arch()
    commands = {
        "go": ([cache / "go" / lock["go"]["version"] / "bin/go", "version"], f"go version go{lock['go']['version']}") ,
        "node": ([cache / "node" / lock["node"]["version"] / "bin/node", "--version"], f"v{lock['node']['version']}"),
        "temporal_cli": ([cache / "temporal_cli" / lock["temporal_cli"]["version"] / "temporal", "--version"], f"temporal version {lock['temporal_cli']['version']}"),
        "shellcheck": ([cache / "shellcheck" / lock["shellcheck"]["version"] / "shellcheck", "--version"], f"version: {lock['shellcheck']['version']}"),
        "govulncheck": ([cache / "govulncheck" / lock["govulncheck"]["version"] / "govulncheck", "-version"], f"v{lock['govulncheck']['version']}"),
        "aliyun_cli": ([cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun", "version"], lock["aliyun_cli"]["version"]),
        "ossutil": ([cache / "ossutil" / lock["ossutil"]["version"] / "ossutil", "version"], lock["ossutil"]["version"]),
    }
    for tool, (command, expected) in commands.items():
        binary = Path(command[0])
        if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
            errors.append(f"locked executable is missing: {tool}: {binary}")
            continue
        result = subprocess.run([str(part) for part in command], text=True, capture_output=True, check=False)
        output = result.stdout + result.stderr
        if result.returncode != 0 or expected not in output:
            errors.append(f"locked executable version mismatch: {tool}: {output.strip()!r}")
    for tool in ("go", "node", "temporal_cli", "shellcheck"):
        artifact = lock[tool]["artifacts"][arch]
        path = cache / "downloads" / artifact["filename"]
        if path.is_symlink() or not path.is_file():
            errors.append(f"locked download is missing: {tool}: {path}")
        elif sha256(path) != artifact["sha256"]:
            errors.append(f"locked download checksum mismatch: {tool}: {path}")
    compose = (repo_root / "infra/production/compose/docker-compose.yml").read_text(encoding="utf-8")
    for tool in ("postgres", "temporal_server", "temporal_ui", "caddy"):
        expected = f"{lock[tool]['image']}@{lock[tool]['digest']}"
        if expected not in compose:
            errors.append(f"production image pin differs from lock: {tool}: {expected}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    errors = check(args.lock, args.cache, args.repo_root)
    if args.json:
        print(json.dumps({"ok": not errors, "errors": errors}, sort_keys=True, separators=(",", ":")))
    else:
        for error in errors:
            print(error)
    return 0 if not errors else 78


if __name__ == "__main__":
    raise SystemExit(main())
