"""Acquire and install checksum-locked release tools without credentials."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import posixpath
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request


INSTALLABLE = {"go", "node", "temporal_cli", "shellcheck", "govulncheck"}


def arch() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    architecture = "amd64" if machine in {"x86_64", "amd64"} else (
        "arm64" if machine in {"aarch64", "arm64"} else ""
    )
    if system not in {"linux", "darwin"} or not architecture:
        raise RuntimeError(f"unsupported platform: {system}/{machine}")
    return f"{system}-{architecture}"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def safe_members(archive: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members = archive.getmembers()
    for member in members:
        path = PurePosixPath(member.name)
        if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
            raise RuntimeError(f"unsafe archive member: {member.name!r}")
        if member.issym():
            if member.linkname.startswith("/"):
                raise RuntimeError(f"absolute archive symlink is forbidden: {member.name!r}")
            resolved = posixpath.normpath(
                posixpath.join(posixpath.dirname(member.name), member.linkname)
            )
            if resolved == ".." or resolved.startswith("../"):
                raise RuntimeError(f"escaping archive symlink is forbidden: {member.name!r}")
        elif not (member.isdir() or member.isfile()):
            raise RuntimeError(f"non-file archive member is forbidden: {member.name!r}")
    return members


def install(tool: str, lock_path: Path, cache: Path) -> Path:
    if tool not in INSTALLABLE:
        raise RuntimeError(f"tool is not archive-installable: {tool}")
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    entry = lock["tools"][tool]
    if tool == "govulncheck":
        go = cache / "go" / lock["tools"]["go"]["version"] / "bin/go"
        if go.is_symlink() or not go.is_file():
            raise RuntimeError("locked Go must be installed before govulncheck")
        metadata = subprocess.run(
            [str(go), "mod", "download", "-json", f"{entry['module']}@v{entry['version']}"],
            text=True,
            capture_output=True,
            check=True,
            env={**os.environ, "GOSUMDB": "sum.golang.org", "GOTOOLCHAIN": "local"},
        )
        module = json.loads(metadata.stdout)
        if module.get("Sum") != entry["module_sum"] or module.get("Origin", {}).get("Hash") != entry["source_commit"]:
            raise RuntimeError("govulncheck module checksum or source commit mismatch")
        target = cache / tool / entry["version"]
        if target.exists():
            raise RuntimeError(f"install target already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        staging = Path(tempfile.mkdtemp(prefix=f".{tool}.", dir=target.parent))
        try:
            subprocess.run(
                [str(go), "install", f"{entry['module']}/cmd/govulncheck@v{entry['version']}"],
                check=True,
                env={**os.environ, "GOBIN": str(staging), "GOSUMDB": "sum.golang.org", "GOTOOLCHAIN": "local"},
            )
            os.replace(staging, target)
            return target
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    artifact = entry["artifacts"][arch()]
    downloads = cache / "downloads"
    downloads.mkdir(parents=True, exist_ok=True, mode=0o700)
    archive_path = downloads / artifact["filename"]
    if archive_path.exists() and digest(archive_path) != artifact["sha256"]:
        raise RuntimeError(f"cached artifact checksum mismatch: {archive_path}")
    if not archive_path.exists():
        pending = archive_path.with_name(archive_path.name + ".pending")
        try:
            with urllib.request.urlopen(artifact["url"], timeout=60) as response, pending.open("xb") as output:
                shutil.copyfileobj(response, output)
            if digest(pending) != artifact["sha256"]:
                raise RuntimeError(f"downloaded artifact checksum mismatch: {artifact['filename']}")
            os.replace(pending, archive_path)
        finally:
            pending.unlink(missing_ok=True)
    target = cache / tool / entry["version"]
    if target.exists():
        raise RuntimeError(f"install target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    staging = Path(tempfile.mkdtemp(prefix=f".{tool}.", dir=target.parent))
    try:
        with tarfile.open(archive_path, "r:*") as archive:
            members = safe_members(archive)
            names = [PurePosixPath(member.name) for member in members]
            prefixes = {
                "go": PurePosixPath("go"),
                "node": names[0].parts[0] if names else "",
                "temporal_cli": None,
                "shellcheck": names[0].parts[0] if names else "",
            }
            prefix = prefixes[tool]
            for member in members:
                if not member.isfile():
                    continue
                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeError(f"cannot read archive member: {member.name}")
                relative = PurePosixPath(member.name)
                if prefix is not None:
                    if relative.parts[0] != str(prefix):
                        raise RuntimeError(f"archive has inconsistent root: {member.name}")
                    relative = PurePosixPath(*relative.parts[1:])
                destination = staging.joinpath(*relative.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                with destination.open("xb") as output:
                    shutil.copyfileobj(source, output)
                os.chmod(destination, member.mode & 0o777)
            for member in members:
                if not member.issym():
                    continue
                relative = PurePosixPath(member.name)
                if prefix is not None:
                    relative = PurePosixPath(*relative.parts[1:])
                destination = staging.joinpath(*relative.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.symlink(member.linkname, destination)
        binary = {
            "go": staging / "bin/go",
            "node": staging / "bin/node",
            "temporal_cli": staging / "temporal",
            "shellcheck": staging / "shellcheck",
        }[tool]
        if tool == "shellcheck" and not binary.is_file():
            candidates = [
                path for path in staging.rglob("shellcheck")
                if path.is_file() and path.parent.name == "bin"
            ]
            if len(candidates) == 1:
                shutil.copy2(candidates[0], binary)
        if not binary.is_file():
            raise RuntimeError(f"installed archive lacks expected executable: {binary}")
        os.chmod(binary, 0o755)
        os.replace(staging, target)
        return target
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tool", choices=sorted(INSTALLABLE))
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    args = parser.parse_args()
    print(install(args.tool, args.lock, args.cache))


if __name__ == "__main__":
    main()
