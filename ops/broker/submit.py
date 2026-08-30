#!/usr/bin/env python3
"""Package and submit one release to the fixed forced-command broker.

This file is installed into the release user's private local directory.
Repository code passes only a release directory; the SSH destination and
identity remain in a mode-0600 configuration outside the checkout. The remote
forced-command broker, not this unprivileged client, owns production mutation.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
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
HOST = re.compile(r"^[A-Za-z0-9.-]+$")
KNOWN_HOSTS_SHA256 = "d9b593ae3ad6d7e683565ce446c8d81d2884fc07971363cfb0f6694d21044f31"


def release_user_home() -> Path:
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    try:
        metadata = home.lstat()
    except OSError as error:
        raise RuntimeError("release user home is unavailable or unsafe") from error
    if (
        not home.is_absolute()
        or home.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise RuntimeError("release user home is unavailable or unsafe")
    return home


def default_config_path() -> Path:
    return release_user_home() / ".config" / "vane" / "broker-client.json"


def validate_directory_chain(home: Path, parent: Path) -> None:
    try:
        relative = parent.relative_to(home)
    except ValueError as error:
        raise RuntimeError("broker client configuration is outside account home") from error
    current = home
    for part in relative.parts:
        current = current / part
        metadata = current.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or current.is_symlink()
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise RuntimeError("broker client configuration path is unsafe")
    if stat.S_IMODE(parent.lstat().st_mode) != 0o700:
        raise RuntimeError(
            "broker client configuration directory must be user-owned mode 0700"
        )


def validate_config_file(path: Path, *, account_home: Path | None = None) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise RuntimeError(
            "user-scoped broker client configuration is unavailable"
        ) from error
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise RuntimeError("user-scoped broker client configuration is unavailable")
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise RuntimeError("broker client configuration must be user-owned mode 0600")
    validate_directory_chain(account_home or release_user_home(), path.parent)


def validate_credential_file(path: Path, name: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise RuntimeError(f"broker client {name} is unavailable") from error
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeError(f"broker client {name} must be a private user-owned file")


def fixed_ssh_command(
    config: dict, *, known_hosts_sha256: str = KNOWN_HOSTS_SHA256
) -> list[str]:
    if set(config) != {
        "schema", "host", "port", "identity_file", "known_hosts_file"
    } or config.get("schema") != "vane.broker-client/v1":
        raise RuntimeError("broker client configuration is invalid")
    host = config["host"]
    port = config["port"]
    identity = config["identity_file"]
    known_hosts = config["known_hosts_file"]
    if (
        not isinstance(host, str)
        or not HOST.fullmatch(host)
        or host.startswith(".")
        or host.endswith(".")
        or isinstance(port, bool)
        or not isinstance(port, int)
        or not 1 <= port <= 65535
        or not isinstance(identity, str)
        or not isinstance(known_hosts, str)
    ):
        raise RuntimeError("broker client configuration is invalid")
    identity_path = Path(identity)
    known_hosts_path = Path(known_hosts)
    validate_credential_file(identity_path, "identity")
    validate_credential_file(known_hosts_path, "known-hosts file")
    if hashlib.sha256(known_hosts_path.read_bytes()).hexdigest() != known_hosts_sha256:
        raise RuntimeError("broker client known-hosts file differs from release policy")
    return [
        "/usr/bin/ssh", "-F", "/dev/null", "-T",
        "-i", str(identity_path), "-p", str(port),
        "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes",
        "-o", "ProxyCommand=none", "-o", "ProxyJump=none",
        "-o", "PermitLocalCommand=no", "-o", "ClearAllForwardings=yes",
        "-o", f"UserKnownHostsFile={known_hosts_path}",
        f"vane-broker@{host}",
    ]


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
    config_path = default_config_path()
    validate_config_file(config_path)
    config = strict_json(config_path)
    command = fixed_ssh_command(config)
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
