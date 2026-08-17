#!/usr/bin/env python3
"""Run the release-bound workspace-memory UAT in a transient systemd unit."""

from __future__ import annotations

import argparse
import grp
import json
import os
from pathlib import Path
import pwd
import re
import stat
import subprocess
import sys
import tempfile
import uuid


CURRENT_RELEASE = Path("/opt/vane/current")
RELEASE_ROOT = Path("/opt/vane/releases")
SERVER_ENV = Path("/opt/vane/env/server.env")
MIGRATION_CREDENTIAL = Path("/etc/vane/credentials/migration_db_url")
TRUSTED_UID = 0
MIGRATION_ACCOUNT = "vane-migrate"
SCHEMA = "vane.workspace-memory-runtime-uat/v1"
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
CAPTURE_BYTES_MAX = 64 * 1024


def strict_json(payload: str) -> object:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise RuntimeError(f"workspace memory UAT returned duplicate key: {key}")
            value[key] = item
        return value

    return json.loads(payload, object_pairs_hook=pairs)


def require_file(path: Path, *, uid: int, forbidden_mode: int) -> None:
    info = path.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != uid
        or info.st_mode & forbidden_mode
        or info.st_size <= 0
    ):
        raise RuntimeError(f"unsafe workspace memory UAT authority: {path}")


def migration_identity() -> tuple[int, int]:
    account = pwd.getpwnam(MIGRATION_ACCOUNT)
    group = grp.getgrnam(MIGRATION_ACCOUNT)
    if account.pw_gid != group.gr_gid:
        raise RuntimeError("vane-migrate account and group differ")
    return account.pw_uid, group.gr_gid


def require_migration_credential(path: Path, uid: int, gid: int) -> None:
    info = path.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != uid
        or info.st_gid != gid
        or stat.S_IMODE(info.st_mode) != 0o400
        or info.st_nlink != 1
        or info.st_size <= 0
    ):
        raise RuntimeError(f"unsafe workspace memory UAT credential: {path}")


def runtime_database_url(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != TRUSTED_UID
            or stat.S_IMODE(info.st_mode) & 0o027
            or info.st_nlink != 1
            or not 0 < info.st_size <= 64 * 1024
        ):
            raise RuntimeError("workspace memory UAT server env is unsafe")
        payload = os.read(descriptor, info.st_size + 1)
    finally:
        os.close(descriptor)
    if len(payload) != info.st_size:
        raise RuntimeError("workspace memory UAT server env changed while reading")
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeError as error:
        raise RuntimeError("workspace memory UAT server env is not UTF-8") from error
    matches = [line.removeprefix("VANE_DB_URL=") for line in lines
               if line.startswith("VANE_DB_URL=")]
    if len(matches) != 1:
        raise RuntimeError("workspace memory UAT runtime database URL is not unique")
    value = matches[0]
    if (
        not value.startswith(("postgres://", "postgresql://"))
        or any(character.isspace() or ord(character) < 0x20 for character in value)
    ):
        raise RuntimeError("workspace memory UAT runtime database URL is invalid")
    return value


def write_runtime_credential(directory: Path, value: str, uid: int, gid: int) -> Path:
    path = directory / "runtime_db_url"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, (value + "\n").encode("utf-8"))
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    require_migration_credential(path, uid, gid)
    return path


def create_capture_file(directory: Path, name: str) -> Path:
    path = directory / name
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    os.close(descriptor)
    return path


def read_capture_file(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != TRUSTED_UID
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > CAPTURE_BYTES_MAX
        ):
            raise RuntimeError("unsafe workspace memory UAT capture")
        payload = os.read(descriptor, info.st_size + 1)
    finally:
        os.close(descriptor)
    if len(payload) != info.st_size:
        raise RuntimeError("workspace memory UAT capture changed while reading")
    try:
        return payload.decode("utf-8")
    except UnicodeError as error:
        raise RuntimeError("workspace memory UAT capture is not UTF-8") from error


def validate_report(value: object, revision: str, operation_id: str) -> dict[str, object]:
    expected = {
        "schema",
        "revision",
        "operation_id",
        "runtime_boundary_verified",
        "personal_write_verified",
        "team_write_verified",
        "cross_member_recall_verified",
        "personal_excluded_from_team",
        "team_excluded_from_personal",
        "cross_user_personal_denied",
        "cleanup_verified",
        "personal_evidence_digest",
        "team_evidence_digest",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise RuntimeError("workspace memory UAT report shape is invalid")
    if (
        value["schema"] != SCHEMA
        or value["revision"] != revision
        or value["operation_id"] != operation_id
    ):
        raise RuntimeError("workspace memory UAT report authority differs")
    for field in expected - {
        "schema",
        "revision",
        "operation_id",
        "personal_evidence_digest",
        "team_evidence_digest",
    }:
        if value[field] is not True:
            raise RuntimeError(f"workspace memory UAT check failed: {field}")
    personal = value["personal_evidence_digest"]
    team = value["team_evidence_digest"]
    if (
        not isinstance(personal, str)
        or not DIGEST.fullmatch(personal)
        or not isinstance(team, str)
        or not DIGEST.fullmatch(team)
        or personal == team
    ):
        raise RuntimeError("workspace memory UAT evidence digest is invalid")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--operation-id", required=True)
    args = parser.parse_args()
    if not SHA.fullmatch(args.sha):
        raise RuntimeError("workspace memory UAT revision is invalid")
    try:
        operation = uuid.UUID(args.operation_id)
    except ValueError as error:
        raise RuntimeError("workspace memory UAT operation ID is invalid") from error
    if operation.int == 0 or str(operation) != args.operation_id:
        raise RuntimeError("workspace memory UAT operation ID is not canonical")
    release = CURRENT_RELEASE.resolve(strict=True)
    if release.name != args.sha or release.parent != RELEASE_ROOT:
        raise RuntimeError("workspace memory UAT release pointer differs")
    executable = release / "bin" / "vane-migrate"
    require_file(executable, uid=TRUSTED_UID, forbidden_mode=0o022)
    require_file(SERVER_ENV, uid=TRUSTED_UID, forbidden_mode=0o027)
    migrate_uid, migrate_gid = migration_identity()
    require_migration_credential(
        MIGRATION_CREDENTIAL, migrate_uid, migrate_gid
    )
    runtime_url = runtime_database_url(SERVER_ENV)
    unit = "vane-workspace-memory-uat-" + operation.hex
    with tempfile.TemporaryDirectory(prefix="vane-memory-uat-runtime-") as raw:
        directory = Path(raw)
        directory.chmod(0o700)
        runtime_credential = write_runtime_credential(
            directory, runtime_url, migrate_uid, migrate_gid
        )
        stdout_path = create_capture_file(directory, "stdout")
        stderr_path = create_capture_file(directory, "stderr")
        command = [
            "systemd-run",
            "--quiet",
            "--wait",
            "--collect",
            f"--unit={unit}",
            "--property=Type=oneshot",
            "--property=User=vane-migrate",
            "--property=Group=vane-migrate",
            "--property=WorkingDirectory=/opt/vane",
            f"--property=LoadCredential=migration_db_url:{MIGRATION_CREDENTIAL}",
            f"--property=LoadCredential=runtime_db_url:{runtime_credential}",
            f"--property=StandardOutput=file:{stdout_path}",
            f"--property=StandardError=file:{stderr_path}",
            "--property=NoNewPrivileges=yes",
            "--property=ProtectSystem=strict",
            "--property=ProtectHome=yes",
            "--property=PrivateTmp=yes",
            "--property=PrivateDevices=yes",
            "--property=ProtectProc=invisible",
            "--property=RestrictSUIDSGID=yes",
            "--property=LockPersonality=yes",
            "--property=MemoryDenyWriteExecute=yes",
            "--property=RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
            "--property=TimeoutStartSec=6min",
            str(executable),
            "workspace-memory-uat",
            "--operation-id",
            args.operation_id,
            "--expected-revision",
            args.sha,
            "--confirm",
            SCHEMA,
        ]
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=390,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin"},
        )
        stdout = read_capture_file(stdout_path)
        stderr = read_capture_file(stderr_path)
    if completed.returncode != 0:
        raise RuntimeError(
            f"workspace memory UAT transient unit failed with exit {completed.returncode}"
        )
    if completed.stdout.strip() or completed.stderr.strip():
        raise RuntimeError("workspace memory UAT systemd runner wrote unexpected output")
    if stderr.strip():
        raise RuntimeError("workspace memory UAT transient unit wrote stderr")
    report = validate_report(strict_json(stdout), args.sha, args.operation_id)
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as error:
        print(f"workspace memory UAT refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
