#!/usr/bin/env python3
"""Build and strictly validate Vane deployment artifacts."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tarfile
from typing import Iterable

SCHEMA = 2
MAX_TOTAL_SIZE = 1_000_000_000
MAX_FILE_SIZE = 500_000_000
SERVER_RELEASE_CONTRACT = (
    "vane.server-release-contract/v2 primary_store=owner_compat_v1 "
    "research_control_store=restricted_v1 research_store=restricted_v1"
)
BACKEND_FILES = {
    "bin/vane": 0o755,
    "bin/useradmin": 0o755,
    "bin/gate": 0o755,
    "bin/runtimeadmin": 0o755,
    "bin/vane-migrate": 0o755,
    "bin/agentfirstretention": 0o755,
    "bin/vane-research-gateway": 0o755,
    "bin/vane-research-prepare": 0o755,
    "bin/researchshadow": 0o755,
    "bin/researchcutover": 0o755,
    "deploy/Caddyfile": 0o644,
    "deploy/docker-compose.yml": 0o644,
    "deploy/vane.service": 0o644,
    "deploy/vane-migrate.service": 0o644,
    "deploy/vane-research-gateway.service": 0o644,
    "deploy/vane-research-gateway.socket": 0o644,
    "deploy/agent-first-retention-prepared-control.sh": 0o755,
    "deploy/dynamicconfig/development-sql.yaml": 0o644,
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_sha(value: str) -> str:
    if len(value) != 40 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"invalid exact source SHA: {value!r}")
    return value


def validate_archive_path(value: str, component: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        any(ord(char) < 32 or ord(char) == 127 for char in value)
        or "\\" in value
        or path.is_absolute()
        or str(path) != value
        or not path.parts
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise ValueError(f"unsafe archive path: {value!r}")
    if component == "frontend" and path.parts[0] != "dist":
        raise ValueError(f"frontend path is outside dist/: {value!r}")
    return path


def source_files(component: str, source: Path) -> list[tuple[str, Path, int]]:
    if component == "backend":
        candidates = [
            (name, source / name, mode) for name, mode in BACKEND_FILES.items()
        ]
    else:
        dist = source / "dist"
        if not (dist / "index.html").is_file():
            raise ValueError("frontend dist/index.html is missing")
        candidates = []
        for file_path in sorted(dist.rglob("*")):
            if file_path.is_symlink():
                raise ValueError(f"frontend artifact contains symlink: {file_path}")
            if file_path.is_dir():
                continue
            if not file_path.is_file():
                raise ValueError(f"frontend artifact contains non-file: {file_path}")
            archive_path = file_path.relative_to(source).as_posix()
            candidates.append((archive_path, file_path, 0o644))

    if not candidates:
        raise ValueError(f"{component} artifact has no files")

    total_size = 0
    for archive_path, file_path, _ in candidates:
        validate_archive_path(archive_path, component)
        file_stat = file_path.lstat()
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError(f"artifact member is not a regular file: {file_path}")
        if file_stat.st_size > MAX_FILE_SIZE:
            raise ValueError(f"artifact member is oversized: {file_path}")
        total_size += file_stat.st_size
    if total_size > MAX_TOTAL_SIZE:
        raise ValueError(f"{component} artifact is oversized")
    return candidates


def pack(
    component: str,
    source: Path,
    source_sha: str,
    output: Path,
    server_release_contract: str | None = None,
    control_plane_revision: str | None = None,
    deploy_run_id: str | None = None,
    build_run_attempt: int | None = None,
) -> None:
    source_sha = validate_sha(source_sha)
    if component == "backend":
        if server_release_contract != SERVER_RELEASE_CONTRACT:
            raise ValueError("backend server release contract is not exact")
        if control_plane_revision is None:
            raise ValueError("backend control-plane revision is required")
        control_plane_revision = validate_sha(control_plane_revision)
        if not deploy_run_id or not deploy_run_id.isascii() or not deploy_run_id.isdigit():
            raise ValueError("backend deploy run ID is invalid")
        if not isinstance(build_run_attempt, int) or build_run_attempt <= 0:
            raise ValueError("backend deploy run attempt is invalid")
    elif server_release_contract is not None:
        raise ValueError("frontend artifact cannot carry a server release contract")
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)

    archive_name = f"{component}-{source_sha}.tar.gz"
    archive_path = output / archive_name
    files = source_files(component, source)
    manifest_files = []

    with archive_path.open("xb") as raw_archive:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_archive, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                for name, file_path, mode in files:
                    file_stat = file_path.stat()
                    info = tarfile.TarInfo(name=name)
                    info.size = file_stat.st_size
                    info.mode = mode
                    info.mtime = 0
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    with file_path.open("rb") as handle:
                        archive.addfile(info, handle)
                    manifest_files.append(
                        {
                            "path": name,
                            "sha256": sha256_file(file_path),
                            "size": file_stat.st_size,
                            "mode": mode,
                        }
                    )

    archive_sha256 = sha256_file(archive_path)
    manifest = {
        "schema": SCHEMA,
        "component": component,
        "source_sha": source_sha,
        "archive": archive_name,
        "archive_sha256": archive_sha256,
        "archive_size": archive_path.stat().st_size,
        "files": manifest_files,
    }
    if component == "backend":
        manifest["server_release_contract"] = server_release_contract
        manifest["control_plane_revision"] = control_plane_revision
        manifest["deploy_run_id"] = deploy_run_id
        manifest["build_run_attempt"] = build_run_attempt
    manifest_path = output / f"{component}-{source_sha}.manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / f"{component}-{source_sha}.sha256").write_text(
        f"{archive_sha256}  {archive_name}\n", encoding="ascii"
    )


def exact_keys(value: dict, expected: Iterable[str], subject: str) -> None:
    if set(value) != set(expected):
        raise ValueError(f"{subject} has unexpected keys: {sorted(value)}")


def strict_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"JSON object has duplicate key: {key}")
        value[key] = item
    return value


def validate(
    component: str,
    source_sha: str,
    input_dir: Path,
    output_dir: Path,
    control_plane_revision: str | None = None,
    deploy_run_id: str | None = None,
) -> None:
    source_sha = validate_sha(source_sha)
    archive_name = f"{component}-{source_sha}.tar.gz"
    manifest_name = f"{component}-{source_sha}.manifest.json"
    checksum_name = f"{component}-{source_sha}.sha256"
    expected_names = {archive_name, manifest_name, checksum_name}
    actual_names = {path.name for path in input_dir.iterdir()}
    if actual_names != expected_names:
        raise ValueError(
            f"artifact has missing or extra files: expected={sorted(expected_names)} "
            f"actual={sorted(actual_names)}"
        )
    input_size_limits = {
        archive_name: MAX_TOTAL_SIZE,
        manifest_name: 10_000_000,
        checksum_name: 1_000,
    }
    for name in expected_names:
        artifact_file = input_dir / name
        file_stat = artifact_file.lstat()
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError(f"artifact input is not a regular file: {name}")
        if file_stat.st_size > input_size_limits[name]:
            raise ValueError(f"artifact input is oversized: {name}")

    manifest = json.loads(
        (input_dir / manifest_name).read_text(encoding="utf-8"),
        object_pairs_hook=strict_object,
    )
    if not isinstance(manifest, dict):
        raise ValueError("manifest root must be an object")
    manifest_keys = [
        "schema",
        "component",
        "source_sha",
        "archive",
        "archive_sha256",
        "archive_size",
        "files",
    ]
    if component == "backend":
        manifest_keys.extend((
            "server_release_contract", "control_plane_revision",
            "deploy_run_id", "build_run_attempt",
        ))
    exact_keys(manifest, manifest_keys, "manifest")
    if manifest["schema"] != SCHEMA:
        raise ValueError("unsupported manifest schema")
    if manifest["component"] != component or manifest["source_sha"] != source_sha:
        raise ValueError("manifest component or source SHA does not match the plan")
    if component == "backend" and (
        manifest["server_release_contract"] != SERVER_RELEASE_CONTRACT
    ):
        raise ValueError("backend server release contract is not exact")
    if component == "backend":
        if control_plane_revision is None:
            raise ValueError("backend expected control-plane revision is required")
        control_plane_revision = validate_sha(control_plane_revision)
        if (
            not isinstance(manifest["deploy_run_id"], str)
            or not manifest["deploy_run_id"].isascii()
            or not manifest["deploy_run_id"].isdigit()
            or not isinstance(manifest["build_run_attempt"], int)
            or manifest["build_run_attempt"] <= 0
        ):
            raise ValueError("backend deployment identity is invalid")
        if (
            manifest["control_plane_revision"] != control_plane_revision
            or manifest["deploy_run_id"] != deploy_run_id
        ):
            raise ValueError("backend deployment identity differs from this run")
    elif any(value is not None for value in (
        control_plane_revision, deploy_run_id
    )):
        raise ValueError("frontend validation cannot carry deployment identity")
    if manifest["archive"] != archive_name:
        raise ValueError("manifest archive name is not exact")
    archive_path = input_dir / archive_name
    if (
        not isinstance(manifest["archive_size"], int)
        or manifest["archive_size"] < 0
        or manifest["archive_size"] > MAX_TOTAL_SIZE
        or archive_path.stat().st_size != manifest["archive_size"]
    ):
        raise ValueError("archive size is invalid or does not match")
    archive_sha256 = sha256_file(archive_path)
    if manifest["archive_sha256"] != archive_sha256:
        raise ValueError("archive SHA256 does not match manifest")
    expected_checksum = f"{archive_sha256}  {archive_name}\n"
    if (input_dir / checksum_name).read_text(encoding="ascii") != expected_checksum:
        raise ValueError("SHA256 sidecar is not exact")

    raw_files = manifest["files"]
    if not isinstance(raw_files, list) or not raw_files:
        raise ValueError("manifest file allowlist is empty or invalid")
    manifest_files: dict[str, dict] = {}
    total_size = 0
    for entry in raw_files:
        if not isinstance(entry, dict):
            raise ValueError("manifest file entry must be an object")
        exact_keys(entry, ("path", "sha256", "size", "mode"), "manifest file")
        path = entry["path"]
        if not isinstance(path, str):
            raise ValueError("manifest path must be a string")
        validate_archive_path(path, component)
        if path in manifest_files:
            raise ValueError(f"duplicate manifest path: {path}")
        if (
            not isinstance(entry["size"], int)
            or entry["size"] < 0
            or entry["size"] > MAX_FILE_SIZE
        ):
            raise ValueError(f"invalid size for {path}")
        if (
            not isinstance(entry["sha256"], str)
            or len(entry["sha256"]) != 64
            or any(char not in "0123456789abcdef" for char in entry["sha256"])
        ):
            raise ValueError(f"invalid SHA256 for {path}")
        if entry["mode"] not in (0o644, 0o755):
            raise ValueError(f"invalid mode for {path}")
        total_size += entry["size"]
        manifest_files[path] = entry
    if total_size > MAX_TOTAL_SIZE:
        raise ValueError("manifest content is oversized")
    if component == "backend":
        if set(manifest_files) != set(BACKEND_FILES):
            raise ValueError("backend allowlist is not exact")
        for path, expected_mode in BACKEND_FILES.items():
            if manifest_files[path]["mode"] != expected_mode:
                raise ValueError(f"backend mode is not exact: {path}")
    elif "dist/index.html" not in manifest_files:
        raise ValueError("frontend allowlist lacks dist/index.html")

    if output_dir.exists():
        raise ValueError(f"validation output already exists: {output_dir}")
    output_dir.mkdir(parents=True, mode=0o700)
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            if len(members) != len(manifest_files):
                raise ValueError("tar member count does not match allowlist")
            seen = set()
            for member in members:
                path = member.name
                validate_archive_path(path, component)
                if path in seen or path not in manifest_files:
                    raise ValueError(f"tar has duplicate or extra member: {path}")
                seen.add(path)
                expected = manifest_files[path]
                if not member.isfile():
                    raise ValueError(f"tar member is not a regular file: {path}")
                if member.linkname or member.devmajor or member.devminor:
                    raise ValueError(f"tar member has link/device metadata: {path}")
                if member.size != expected["size"] or member.mode != expected["mode"]:
                    raise ValueError(f"tar metadata does not match manifest: {path}")
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError(f"tar member cannot be read: {path}")
                destination = output_dir.joinpath(*PurePosixPath(path).parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                written = 0
                with destination.open("xb") as target:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        written += len(chunk)
                        if written > expected["size"]:
                            raise ValueError(f"tar member exceeds declared size: {path}")
                        digest.update(chunk)
                        target.write(chunk)
                os.chmod(destination, expected["mode"])
                if written != expected["size"] or digest.hexdigest() != expected["sha256"]:
                    raise ValueError(f"tar member content does not match manifest: {path}")
            if seen != set(manifest_files):
                raise ValueError("tar is missing allowlisted files")

        if component == "backend":
            for binary in (
                "vane", "useradmin", "gate", "runtimeadmin", "vane-migrate",
                "agentfirstretention",
                "vane-research-gateway", "vane-research-prepare",
                "researchshadow", "researchcutover",
            ):
                data = (output_dir / "bin" / binary).read_bytes()
                if f"vcs.revision={source_sha}".encode() not in data:
                    raise ValueError(f"{binary} lacks exact vcs.revision build info")
                if b"vcs.modified=false" not in data or b"vcs.modified=true" in data:
                    raise ValueError(f"{binary} was not built from a clean worktree")
            release_receipt = {
                "schema_version": "vane.release-receipt/v1",
                "source_revision": source_sha,
                "control_plane_revision": manifest["control_plane_revision"],
                "deploy_run_id": manifest["deploy_run_id"],
                "build_run_attempt": manifest["build_run_attempt"],
                "backend_archive_sha256": archive_sha256,
                "backend_manifest_sha256": sha256_file(input_dir / manifest_name),
                "server_release_contract_sha256": hashlib.sha256(
                    SERVER_RELEASE_CONTRACT.encode("utf-8")
                ).hexdigest(),
                "vane_sha256": manifest_files["bin/vane"]["sha256"],
                "agentfirstretention_sha256": manifest_files[
                    "bin/agentfirstretention"
                ]["sha256"],
            }
            (output_dir / "release-receipt.json").write_text(
                json.dumps(release_receipt, separators=(",", ":")),
                encoding="utf-8",
            )
            os.chmod(output_dir / "release-receipt.json", 0o644)
    except Exception:
        shutil.rmtree(output_dir)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("pack", "validate"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--component", choices=("backend", "frontend"), required=True)
        sub.add_argument("--sha", required=True)
        if command == "pack":
            sub.add_argument("--source", type=Path, required=True)
            sub.add_argument("--server-release-contract")
            sub.add_argument("--control-plane-revision")
            sub.add_argument("--deploy-run-id")
            sub.add_argument("--build-run-attempt", type=int)
        else:
            sub.add_argument("--input", type=Path, required=True)
            sub.add_argument("--control-plane-revision")
            sub.add_argument("--deploy-run-id")
        sub.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.command == "pack":
        pack(
            args.component,
            args.source,
            args.sha,
            args.output,
            args.server_release_contract,
            args.control_plane_revision,
            args.deploy_run_id,
            args.build_run_attempt,
        )
    else:
        validate(
            args.component,
            args.sha,
            args.input,
            args.output,
            args.control_plane_revision,
            args.deploy_run_id,
        )


if __name__ == "__main__":
    main()
