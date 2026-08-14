#!/usr/bin/env python3
"""Package and submit one release to the fixed forced-command broker.

This file is installed as a root-owned local helper.  Repository code passes
only a release directory; the SSH destination and identity remain in the
installed helper configuration rather than the checkout or process arguments.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys
import tarfile
import tempfile


ALLOWED_TOP_LEVEL = {
    "artifacts",
    "full-gate.json",
    "gate-evidence",
    "manifests",
    "submission.json",
}
MAX_BYTES = 768 * 1024 * 1024


def strict_json(path: Path) -> dict:
    def pairs(values: list[tuple[str, object]]) -> dict:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise RuntimeError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return value


def safe_files(root: Path) -> list[Path]:
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise RuntimeError("release submission must be a safe absolute directory")
    actual = {path.name for path in root.iterdir()}
    if actual != ALLOWED_TOP_LEVEL:
        raise RuntimeError(
            "release submission top-level paths are not exact: "
            f"expected={sorted(ALLOWED_TOP_LEVEL)} actual={sorted(actual)}"
        )
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part in {"", ".", ".."} for part in PurePosixPath(relative.as_posix()).parts):
            raise RuntimeError(f"unsafe release member: {relative}")
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode) or not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            raise RuntimeError(f"release submission contains a special member: {relative}")
        if mode & 0o7000:
            raise RuntimeError(f"release submission contains privileged mode bits: {relative}")
        if stat.S_ISREG(mode):
            files.append(path)
    if not files:
        raise RuntimeError("release submission is empty")
    return files


def deterministic_tar(root: Path, output: Path) -> tuple[str, int]:
    files = safe_files(root)
    with tarfile.open(output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        directories = sorted(
            {parent for file in files for parent in file.relative_to(root).parents if str(parent) != "."},
            key=lambda item: (len(item.parts), item.as_posix()),
        )
        for relative in [*directories, *(file.relative_to(root) for file in files)]:
            source = root / relative
            info = tarfile.TarInfo(relative.as_posix())
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = 0
            if source.is_dir():
                info.type = tarfile.DIRTYPE
                info.mode = 0o700
                archive.addfile(info)
            else:
                info.size = source.stat().st_size
                info.mode = 0o700 if os.access(source, os.X_OK) else 0o600
                with source.open("rb") as handle:
                    archive.addfile(info, handle)
    size = output.stat().st_size
    if size <= 0 or size > MAX_BYTES:
        raise RuntimeError("release submission archive is outside the size limit")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    return digest, size


def main() -> int:
    if len(sys.argv) != 2:
        print(
            "usage: vane-broker-submit ABSOLUTE_RELEASE_DIRECTORY|--status",
            file=sys.stderr,
        )
        return 2
    config_path = Path("/etc/vane-broker/client.json")
    testing = os.environ.get("VANE_BROKER_CLIENT_TESTING") == "1" and os.geteuid() != 0
    if testing:
        config_path = Path(os.environ.get("VANE_BROKER_CLIENT_CONFIG", ""))
    if config_path.is_symlink() or not config_path.is_file():
        raise RuntimeError("root-owned broker client configuration is unavailable")
    config = strict_json(config_path)
    if set(config) != {"schema", "ssh_command"} or config.get("schema") != "vane.broker-client/v1":
        raise RuntimeError("broker client configuration is invalid")
    command = config["ssh_command"]
    if (
        not isinstance(command, list)
        or not command
        or any(not isinstance(item, str) or not item for item in command)
    ):
        raise RuntimeError("broker client SSH command is invalid")
    status = subprocess.run(
        [*command, "vane-broker status"],
        input=b"{}",
        capture_output=True,
        check=False,
    )
    if status.returncode != 0:
        raise RuntimeError(f"broker status failed with exit {status.returncode}")
    try:
        current = json.loads(status.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("broker status returned invalid JSON") from error
    expected = current.get("current_digest") if isinstance(current, dict) else None
    server_revision = current.get("server_revision") if isinstance(current, dict) else None
    if not isinstance(expected, str) or len(expected) != 64:
        raise RuntimeError("broker status did not return an exact current CAS digest")
    if (
        not isinstance(server_revision, str)
        or len(server_revision) != 40
        or any(character not in "0123456789abcdef" for character in server_revision)
    ):
        raise RuntimeError("broker status did not return an exact server revision")
    if sys.argv[1] == "--status":
        print(
            json.dumps(
                {"current_digest": expected, "server_revision": server_revision},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    release = Path(sys.argv[1])
    with tempfile.TemporaryDirectory(prefix="vane-submit-") as temp:
        archive = Path(temp) / "submission.tar"
        digest, size = deterministic_tar(release, archive)
        with archive.open("rb") as handle:
            upload = subprocess.run(
                [*command, f"vane-broker upload {digest} {size}"],
                stdin=handle,
                check=False,
            )
        if upload.returncode != 0:
            raise RuntimeError(f"broker upload failed with exit {upload.returncode}")
        request = json.dumps(
            {"request_id": digest, "expected_current_digest": expected},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        released = subprocess.run(
            [*command, "vane-broker release"], input=request, check=False
        )
        if released.returncode != 0:
            raise RuntimeError(f"broker release failed with exit {released.returncode}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"broker submit refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
