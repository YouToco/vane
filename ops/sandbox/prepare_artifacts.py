#!/usr/bin/env python3
"""Materialize the immutable Firecracker self-test bundle for one release."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import stat
import struct
import tarfile
import tempfile
import urllib.request


MAX_DOWNLOAD = 64 * 1024 * 1024
CPIO_ALIGNMENT = 4


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def strict_json(path: Path) -> dict:
    def pairs(items: list[tuple[str, object]]) -> dict:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"duplicate lock key: {key}")
            value[key] = item
        return value

    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ValueError("artifact lock root must be an object")
    return value


def checked_download(url: str, destination: Path, size: int, sha256: str) -> None:
    if not url.startswith("https://") or size <= 0 or size > MAX_DOWNLOAD:
        raise ValueError("artifact download contract is invalid")
    request = urllib.request.Request(url, headers={"User-Agent": "vane-release/1"})
    written = 0
    value = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=60) as source, destination.open("xb") as target:
        if not source.geturl().startswith("https://"):
            raise ValueError("artifact download redirected outside HTTPS")
        while chunk := source.read(1024 * 1024):
            written += len(chunk)
            if written > size:
                raise ValueError("artifact download exceeds locked size")
            value.update(chunk)
            target.write(chunk)
    if written != size or value.hexdigest() != sha256:
        raise ValueError("artifact download differs from content lock")


def extract_regular(archive_path: Path, member_name: str, destination: Path, size: int, sha256: str) -> None:
    with tarfile.open(archive_path, mode="r:gz") as archive:
        matches = [member for member in archive.getmembers() if member.name == member_name]
        if len(matches) != 1:
            raise ValueError("locked release member is missing or duplicated")
        member = matches[0]
        if not member.isfile() or member.linkname or member.size != size:
            raise ValueError("locked release member metadata differs")
        source = archive.extractfile(member)
        if source is None:
            raise ValueError("locked release member cannot be read")
        payload = source.read(size + 1)
    if len(payload) != size or hashlib.sha256(payload).hexdigest() != sha256:
        raise ValueError("locked release member content differs")
    destination.write_bytes(payload)
    destination.chmod(0o755)


def verify_static_elf(path: Path) -> None:
    payload = path.read_bytes()
    if len(payload) < 64 or payload[:6] != b"\x7fELF\x02\x01":
        raise ValueError("Firecracker artifact is not little-endian ELF64")
    elf_type = struct.unpack_from("<H", payload, 16)[0]
    phoff = struct.unpack_from("<Q", payload, 32)[0]
    phentsize = struct.unpack_from("<H", payload, 54)[0]
    phnum = struct.unpack_from("<H", payload, 56)[0]
    if elf_type not in (2, 3) or phentsize < 56 or phnum < 1 or phnum > 256:
        raise ValueError("Firecracker artifact is not static executable or static PIE")
    if phoff + phentsize * phnum > len(payload):
        raise ValueError("Firecracker ELF program headers are truncated")
    for index in range(phnum):
        program_type = struct.unpack_from("<I", payload, phoff + index * phentsize)[0]
        if program_type == 3:
            raise ValueError("dynamic ELF interpreter is forbidden")


def pad4(payload: bytes) -> bytes:
    return payload + b"\0" * ((-len(payload)) % CPIO_ALIGNMENT)


def newc_entry(name: str, mode: int, payload: bytes, inode: int) -> bytes:
    encoded = name.encode("ascii") + b"\0"
    fields = (
        inode, mode, 0, 0, 1, 0, len(payload), 0, 0, 0, 0, len(encoded), 0,
    )
    header = b"070701" + b"".join(f"{field:08x}".encode("ascii") for field in fields)
    return pad4(header + encoded) + pad4(payload)


def build_rootfs(sandboxd: Path, destination: Path) -> None:
    metadata = sandboxd.lstat()
    if sandboxd.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise ValueError("sandboxd release binary is unsafe")
    binary = sandboxd.read_bytes()
    archive = bytearray()
    for inode, name in enumerate(("dev", "proc", "sbin", "sys"), start=1):
        archive += newc_entry(name, stat.S_IFDIR | 0o755, b"", inode)
    archive += newc_entry("sbin/vane-sandbox-init", stat.S_IFREG | 0o755, binary, 5)
    archive += newc_entry("TRAILER!!!", 0, b"", 6)
    archive += b"\0" * ((512 - len(archive) % 512) % 512)
    destination.write_bytes(archive)
    destination.chmod(0o644)


def materialize(lock_path: Path, sandboxd: Path, output: Path, revision: str) -> None:
    if output.exists():
        raise ValueError("sandbox artifact output already exists")
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        raise ValueError("sandbox artifact source revision is invalid")
    lock = strict_json(lock_path)
    if set(lock) != {"schema", "architecture", "firecracker_version", "archive", "firecracker", "jailer", "kernel"}:
        raise ValueError("sandbox artifact lock keys are not exact")
    if lock["schema"] != "vane.firecracker-artifact-lock/v1" or lock["architecture"] != "x86_64":
        raise ValueError("unsupported sandbox artifact lock")
    output.mkdir(parents=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix="vane-firecracker-acquire-") as temporary:
        archive = Path(temporary) / "firecracker.tgz"
        checked_download(lock["archive"]["url"], archive, lock["archive"]["size_bytes"], lock["archive"]["sha256"])
        for name in ("firecracker", "jailer"):
            item = lock[name]
            extract_regular(archive, item["member"], output / name, item["size_bytes"], item["sha256"])
            verify_static_elf(output / name)
    kernel = lock["kernel"]
    checked_download(kernel["url"], output / "vmlinux", kernel["size_bytes"], kernel["sha256"])
    (output / "vmlinux").chmod(0o644)
    build_rootfs(sandboxd, output / "rootfs.cpio")
    (output / "code.raw").write_bytes(b"\0" * 4096)
    (output / "code.raw").chmod(0o644)
    artifacts = {}
    for name in ("firecracker", "jailer", "vmlinux", "rootfs.cpio", "code.raw"):
        path = output / name
        artifacts[name] = {"sha256": digest(path), "size_bytes": path.stat().st_size}
    manifest = {
        "schema": "vane.firecracker-release-artifacts/v1",
        "source_revision": revision,
        "architecture": "x86_64",
        "firecracker_version": lock["firecracker_version"],
        "sandboxd_sha256": digest(sandboxd),
        "artifacts": artifacts,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    (output / "manifest.json").chmod(0o644)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--sandboxd", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    arguments = parser.parse_args()
    materialize(arguments.lock, arguments.sandboxd, arguments.output, arguments.revision)


if __name__ == "__main__":
    main()
