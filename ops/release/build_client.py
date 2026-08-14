#!/usr/bin/env python3
"""Root-owned local client for the isolated build VM."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile


MAX_BUILD_OUTPUT = 2 * 1024 * 1024 * 1024


def strict_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("build client JSON root is not an object")
    return value


def safe_extract(archive_path: Path, output: Path) -> None:
    if archive_path.stat().st_size > MAX_BUILD_OUTPUT:
        raise RuntimeError("build output exceeds the client limit")
    pending = output.with_name(f".{output.name}.pending")
    if pending.exists() or pending.is_symlink():
        raise RuntimeError("build output pending path already exists")
    pending.mkdir(mode=0o700)
    try:
        seen: set[str] = set()
        with tarfile.open(archive_path, mode="r:") as bundle:
            members = bundle.getmembers()
            if not members or len(members) > 100_000:
                raise RuntimeError("build output member count is invalid")
            for info in members:
                member = Path(info.name)
                if (
                    member.is_absolute()
                    or any(part in {"", ".", ".."} for part in member.parts)
                    or info.name in seen
                    or not (info.isdir() or info.isreg())
                    or info.mode & 0o7000
                ):
                    raise RuntimeError(f"build output member is unsafe: {info.name}")
                seen.add(info.name)
                destination = pending.joinpath(*member.parts)
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                if info.isdir():
                    destination.mkdir(exist_ok=True, mode=0o700)
                    continue
                source = bundle.extractfile(info)
                if source is None:
                    raise RuntimeError(f"cannot read build output member: {info.name}")
                with destination.open("xb") as handle:
                    shutil.copyfileobj(source, handle, length=1024 * 1024)
                destination.chmod(0o700 if info.mode & 0o111 else 0o600)
        os.replace(pending, output)
    except BaseException:
        shutil.rmtree(pending, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.sha):
        raise RuntimeError("build client revision is not an exact SHA")
    if not args.output.is_absolute() or args.output.exists() or args.output.is_symlink():
        raise RuntimeError("build client output must be a new absolute path")
    config_path = Path("/etc/vane-build/client.json")
    if os.environ.get("VANE_BUILD_CLIENT_TESTING") == "1" and os.geteuid() != 0:
        config_path = Path(os.environ.get("VANE_BUILD_CLIENT_CONFIG", ""))
    config = strict_json(config_path)
    if set(config) != {"schema", "ssh_command"} or config.get("schema") != "vane.build-client/v1":
        raise RuntimeError("build client configuration is invalid")
    command = config["ssh_command"]
    if not isinstance(command, list) or not command or any(not isinstance(item, str) or not item for item in command):
        raise RuntimeError("build client SSH command is invalid")
    with tempfile.TemporaryDirectory(prefix="vane-build-client-") as temporary:
        archive = Path(temporary) / "output.tar"
        with archive.open("wb") as handle:
            result = subprocess.run(
                [*command, f"vane-build {args.sha}"],
                stdout=handle,
                check=False,
            )
        if result.returncode != 0:
            raise RuntimeError(f"remote build supervisor failed with exit {result.returncode}")
        safe_extract(archive, args.output)
    evidence = strict_json(args.output / "full-gate.json")
    if evidence.get("schema") != "vane.full-gate-evidence/v1" or evidence.get("revision") != args.sha:
        shutil.rmtree(args.output, ignore_errors=True)
        raise RuntimeError("remote build evidence differs from requested exact SHA")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"build client refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
