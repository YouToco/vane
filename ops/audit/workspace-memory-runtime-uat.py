#!/usr/bin/env python3
"""Run the release-bound workspace-memory UAT in a transient systemd unit."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import uuid


CURRENT_RELEASE = Path("/opt/vane/current")
RELEASE_ROOT = Path("/opt/vane/releases")
SERVER_ENV = Path("/opt/vane/env/server.env")
MIGRATION_CREDENTIAL = Path("/etc/vane/credentials/migration_db_url")
TRUSTED_UID = 0
SCHEMA = "vane.workspace-memory-runtime-uat/v1"
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")


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
    require_file(MIGRATION_CREDENTIAL, uid=TRUSTED_UID, forbidden_mode=0o077)
    unit = "vane-workspace-memory-uat-" + operation.hex
    command = [
        "systemd-run",
        "--quiet",
        "--wait",
        "--collect",
        "--pipe",
        f"--unit={unit}",
        "--property=Type=oneshot",
        "--property=User=vane-migrate",
        "--property=Group=vane-migrate",
        "--property=WorkingDirectory=/opt/vane",
        f"--property=EnvironmentFile={SERVER_ENV}",
        f"--property=LoadCredential=migration_db_url:{MIGRATION_CREDENTIAL}",
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
        "--property=TimeoutStartSec=4min",
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
        timeout=270,
        env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin"},
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"workspace memory UAT transient unit failed with exit {completed.returncode}"
        )
    if completed.stderr.strip():
        raise RuntimeError("workspace memory UAT transient unit wrote stderr")
    report = validate_report(strict_json(completed.stdout), args.sha, args.operation_id)
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as error:
        print(f"workspace memory UAT refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
