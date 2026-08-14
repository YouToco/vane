#!/usr/bin/env python3
"""Forced-command streaming endpoint for the root-owned build supervisor."""

from __future__ import annotations

import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile


def stream_tar(root: Path) -> None:
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|", format=tarfile.USTAR_FORMAT) as archive:
        for path in sorted(root.rglob("*")):
            if path.is_symlink() or (not path.is_file() and not path.is_dir()):
                raise RuntimeError("supervisor output contains an unsafe member")
            relative = path.relative_to(root).as_posix()
            info = tarfile.TarInfo(relative)
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = 0
            if path.is_dir():
                info.type = tarfile.DIRTYPE
                info.mode = 0o700
                archive.addfile(info)
            else:
                info.size = path.stat().st_size
                info.mode = 0o700 if os.access(path, os.X_OK) else 0o600
                with path.open("rb") as handle:
                    archive.addfile(info, handle)


def main() -> int:
    original = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    match = re.fullmatch(r"vane-build ([0-9a-f]{40})", original)
    if match is None:
        raise RuntimeError("build command is not allowlisted")
    controller = Path("/opt/vane-build-control/current")
    export_root = Path("/var/lib/vane-build/exports")
    if os.environ.get("VANE_BUILD_FORCED_TESTING") == "1" and os.geteuid() != 0:
        controller = Path(os.environ.get("VANE_BUILD_CONTROL_ROOT", ""))
        export_root = Path(os.environ.get("VANE_BUILD_EXPORT_ROOT", ""))
    supervisor = controller / "ops/release/build_supervisor.py"
    if supervisor.is_symlink() or not supervisor.is_file() or not os.access(supervisor, os.X_OK):
        raise RuntimeError("active root-owned build supervisor is unavailable")
    export_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    transaction = Path(tempfile.mkdtemp(prefix=f"{match.group(1)}.", dir=str(export_root)))
    output = transaction / "output"
    try:
        result = subprocess.run(
            [str(supervisor), "--sha", match.group(1), "--output", str(output)],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"build supervisor failed with exit {result.returncode}: {result.stderr.strip()}"
            )
        stream_tar(output)
        return 0
    finally:
        shutil.rmtree(transaction, ignore_errors=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"build endpoint refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
