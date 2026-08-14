"""Root broker admission boundary.

The forced-command wrapper accepts a content-addressed release bundle first and
then a tiny JSON request that refers to that bundle by digest.  This module
never accepts caller-controlled paths.  Mutation is delegated to one fixed,
root-owned handler only after the bundle, manifest chain, and current-state CAS
have all been checked while holding the global lock.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile
from typing import Optional


ALLOWED = {"release", "retry", "rollback", "status", "audit", "cert-check"}
MUTATING = {"release", "retry", "rollback"}
EXACT_SHA256 = re.compile(r"^[0-9a-f]{64}$")
MAX_UPLOAD_BYTES = 768 * 1024 * 1024
MAX_UPLOAD_MEMBERS = 100_000
ALLOWED_TOP_LEVEL = {
    "artifacts",
    "full-gate.json",
    "gate-evidence",
    "manifests",
    "submission.json",
}


def load_request() -> dict:
    raw = sys.stdin.buffer.read(1_048_577)
    if len(raw) > 1_048_576:
        raise RuntimeError("broker request exceeds 1 MiB")
    pairs: dict[str, object] = {}

    def strict(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise RuntimeError(f"duplicate request key: {key}")
            value[key] = item
        return value

    value = json.loads(raw, object_pairs_hook=strict)
    if not isinstance(value, dict):
        raise RuntimeError("broker request must be an object")
    return value


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def request_file(root: Path, value: object) -> Path:
    if not isinstance(value, str) or not value or "/" in value or value in {".", ".."}:
        raise RuntimeError("broker request file must be a plain basename")
    path = (root / value).resolve()
    if path.parent != root.resolve() or path.is_symlink() or not path.is_file():
        raise RuntimeError(f"unsafe or missing broker request file: {value!r}")
    return path


def request_directory(root: Path, request_id: object) -> Path:
    if not isinstance(request_id, str) or not EXACT_SHA256.fullmatch(request_id):
        raise RuntimeError("broker request_id must be an exact SHA-256")
    path = root / request_id
    if path.is_symlink() or not path.is_dir():
        raise RuntimeError("broker request bundle is unavailable")
    marker = path / ".bundle.sha256"
    if marker.is_symlink() or not marker.is_file():
        raise RuntimeError("broker request bundle marker is unavailable")
    if marker.read_text(encoding="ascii") != request_id + "\n":
        raise RuntimeError("broker request bundle marker differs from request_id")
    return path


def _safe_member_name(name: str) -> PurePosixPath:
    if not name or "\\" in name or "\x00" in name:
        raise RuntimeError("release bundle contains an unsafe member name")
    member = PurePosixPath(name)
    if member.is_absolute() or any(part in {"", ".", ".."} for part in member.parts):
        raise RuntimeError("release bundle contains an unsafe member path")
    if member.parts[0] not in ALLOWED_TOP_LEVEL:
        raise RuntimeError(f"release bundle contains an unknown top-level path: {member.parts[0]}")
    return member


def receive_bundle(
    stream: object,
    *,
    expected_digest: str,
    expected_size: int,
    root: Path,
) -> dict:
    """Receive and safely extract one content-addressed uncompressed tar."""
    if not EXACT_SHA256.fullmatch(expected_digest):
        raise RuntimeError("upload digest is invalid")
    if expected_size <= 0 or expected_size > MAX_UPLOAD_BYTES:
        raise RuntimeError("upload size is outside the broker limit")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("broker request root is unsafe")
    existing = root / expected_digest
    if existing.is_dir() and not existing.is_symlink():
        request_directory(root, expected_digest)
        return {"ok": True, "verb": "upload", "request_id": expected_digest, "reused": True}
    temporary_archive = tempfile.NamedTemporaryFile(
        prefix=".upload-", suffix=".tar", dir=str(root), delete=False
    )
    archive_path = Path(temporary_archive.name)
    digest = hashlib.sha256()
    received = 0
    try:
        with temporary_archive:
            while received < expected_size:
                chunk = stream.read(min(1024 * 1024, expected_size - received))
                if not chunk:
                    raise RuntimeError("upload ended before the declared size")
                if not isinstance(chunk, bytes):
                    raise RuntimeError("upload stream is not binary")
                temporary_archive.write(chunk)
                digest.update(chunk)
                received += len(chunk)
            if stream.read(1):
                raise RuntimeError("upload exceeds the declared size")
            temporary_archive.flush()
            os.fsync(temporary_archive.fileno())
        if digest.hexdigest() != expected_digest:
            raise RuntimeError("upload SHA-256 mismatch")
        extracted = Path(tempfile.mkdtemp(prefix=".request-", dir=str(root)))
        seen: set[str] = set()
        try:
            with tarfile.open(archive_path, mode="r:") as archive:
                members = archive.getmembers()
                if not members or len(members) > MAX_UPLOAD_MEMBERS:
                    raise RuntimeError("release bundle member count is invalid")
                for info in members:
                    member = _safe_member_name(info.name)
                    key = member.as_posix()
                    if key in seen:
                        raise RuntimeError(f"release bundle contains duplicate member: {key}")
                    seen.add(key)
                    if not (info.isdir() or info.isreg()):
                        raise RuntimeError(f"release bundle contains a non-regular member: {key}")
                    if info.mode & 0o7000:
                        raise RuntimeError(f"release bundle contains privileged mode bits: {key}")
                    destination = extracted.joinpath(*member.parts)
                    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    if info.isdir():
                        destination.mkdir(exist_ok=True, mode=0o700)
                        continue
                    source = archive.extractfile(info)
                    if source is None:
                        raise RuntimeError(f"cannot read release bundle member: {key}")
                    with destination.open("xb") as output:
                        while True:
                            chunk = source.read(1024 * 1024)
                            if not chunk:
                                break
                            output.write(chunk)
                    destination.chmod(0o700 if info.mode & 0o111 else 0o600)
            required = {"submission.json", "full-gate.json", "manifests", "artifacts"}
            if not required.issubset({path.name for path in extracted.iterdir()}):
                raise RuntimeError("release bundle is missing required top-level members")
            (extracted / ".bundle.sha256").write_text(expected_digest + "\n", encoding="ascii")
            (extracted / ".bundle.sha256").chmod(0o600)
            os.replace(extracted, existing)
        except BaseException:
            if extracted.exists():
                import shutil

                shutil.rmtree(extracted)
            raise
        return {"ok": True, "verb": "upload", "request_id": expected_digest, "reused": False}
    finally:
        archive_path.unlink(missing_ok=True)


def load_submission(request_root: Path) -> dict:
    path = request_root / "submission.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
        raise RuntimeError("release submission metadata is unavailable")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=lambda pairs: _strict_pairs(pairs))
    expected = {
        "schema",
        "revision",
        "deploy_run_id",
        "artifact_manifest",
        "backend_pack",
        "controller_archive",
        "evidence",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise RuntimeError("release submission metadata keys are not exact")
    if value["schema"] != "vane.broker-submission/v1":
        raise RuntimeError("release submission schema is unsupported")
    if not isinstance(value["revision"], str) or not re.fullmatch(r"[0-9a-f]{40}", value["revision"]):
        raise RuntimeError("release submission revision is invalid")
    if not isinstance(value["deploy_run_id"], str) or not value["deploy_run_id"].isascii() or not value["deploy_run_id"].isdigit():
        raise RuntimeError("release submission deploy_run_id is invalid")
    submission_path(path.parent, value["controller_archive"])
    return value


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise RuntimeError(f"duplicate request key: {key}")
        value[key] = item
    return value


def submission_path(root: Path, value: object, *, directory: bool = False) -> Path:
    if not isinstance(value, str):
        raise RuntimeError("release submission path is not a string")
    member = _safe_member_name(value)
    path = root.joinpath(*member.parts)
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise RuntimeError("release submission path escapes its bundle") from error
    if path.is_symlink() or (not path.is_dir() if directory else not path.is_file()):
        raise RuntimeError("release submission path has the wrong type")
    return path


def invoke_cli(repo: Path, arguments: list[str], *, quiet: bool = False) -> None:
    cli = repo / "ops/bin/vane"
    if cli.is_symlink() or not cli.is_file():
        raise RuntimeError("fixed repository CLI is unavailable")
    result = subprocess.run(
        [str(cli), *arguments],
        check=False,
        stdout=subprocess.DEVNULL if quiet else None,
    )
    if result.returncode != 0:
        raise RuntimeError(f"repository admission failed with exit {result.returncode}")


def verify_submission_evidence(request_root: Path, submission: dict) -> Path:
    manifest = submission_path(request_root, submission["artifact_manifest"])
    if manifest.parent.name != "manifests" or manifest.name != "artifact.json":
        raise RuntimeError("release submission artifact manifest path is not canonical")
    evidence = submission["evidence"]
    if not isinstance(evidence, dict) or not evidence:
        raise RuntimeError("release submission evidence map is invalid")
    current = manifest
    visited: set[str] = set()
    expected_stages = ["artifact", "gate", "plan"]
    for expected_stage in expected_stages:
        value = json.loads(
            current.read_text(encoding="utf-8"), object_pairs_hook=lambda pairs: _strict_pairs(pairs)
        )
        if not isinstance(value, dict) or value.get("stage") != expected_stage:
            raise RuntimeError("release submission manifest order is invalid")
        for item in value.get("evidence", []):
            if not isinstance(item, dict) or set(item) != {"name", "sha256"}:
                raise RuntimeError("release submission manifest evidence shape is invalid")
            key = f"{expected_stage}:{item['name']}"
            if key in visited or key not in evidence:
                raise RuntimeError(f"release submission evidence mapping is incomplete: {key}")
            visited.add(key)
            binding = evidence[key]
            if not isinstance(binding, dict) or set(binding) != {"path", "sha256"}:
                raise RuntimeError(f"release submission evidence binding is invalid: {key}")
            bound = submission_path(request_root, binding["path"])
            if binding["sha256"] != item["sha256"] or sha256(bound) != item["sha256"]:
                raise RuntimeError(f"release submission evidence digest mismatch: {key}")
        parent = value.get("parent")
        if expected_stage == "plan":
            if parent is not None:
                raise RuntimeError("release submission plan manifest has a parent")
            continue
        if not isinstance(parent, dict) or set(parent) != {"path", "sha256", "stage"}:
            raise RuntimeError("release submission manifest parent is invalid")
        current = submission_path(request_root, f"manifests/{parent['path']}")
        if sha256(current) != parent["sha256"]:
            raise RuntimeError("release submission manifest parent digest mismatch")
    if set(evidence) != visited:
        raise RuntimeError("release submission evidence map contains unknown bindings")
    return manifest


def validate_artifacts(
    *, repo: Path, request_root: Path, submission: dict, output_root: Path
) -> Path:
    backend_pack = submission_path(request_root, submission["backend_pack"], directory=True)
    backend = output_root / "backend"
    tool = repo / "ops/release/artifact.py"
    if tool.is_symlink() or not tool.is_file():
        raise RuntimeError("fixed artifact validator is unavailable")
    command = [
        sys.executable,
        str(tool),
        "validate",
        "--component",
        "backend",
        "--sha",
        submission["revision"],
        "--input",
        str(backend_pack),
        "--output",
        str(backend),
        "--control-plane-revision",
        submission["revision"],
        "--deploy-run-id",
        submission["deploy_run_id"],
    ]
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError("backend artifact validation failed")
    return backend


def invoke_handler(
    handler: Path,
    *,
    verb: str,
    request_root: Path,
    validated_root: Path,
    state_root: Path,
    repo: Path,
    expected_current_digest: str,
    privileged: bool = False,
) -> dict:
    if handler.is_symlink() or not handler.is_file() or not os.access(handler, os.X_OK):
        raise RuntimeError("root-owned production handler is unavailable")
    command = [
        str(handler),
        verb,
        str(request_root),
        str(validated_root),
        str(state_root),
        str(repo),
        expected_current_digest,
    ]
    if privileged:
        command = ["/usr/bin/sudo", "--non-interactive", "--", *command]
    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        # The SSH caller receives only protocol state, never privileged child
        # output. Detailed diagnostics remain in root-owned handler evidence.
        raise RuntimeError(f"production handler failed with exit {result.returncode}")
    try:
        value = json.loads(result.stdout, object_pairs_hook=lambda pairs: _strict_pairs(pairs))
    except json.JSONDecodeError as error:
        raise RuntimeError("production handler returned invalid JSON") from error
    if not isinstance(value, dict) or value.get("ok") is not True:
        raise RuntimeError("production handler did not report success")
    return value


def handle(
    verb: str,
    request: dict,
    *,
    root: Path,
    repo: Path,
    state_root: Path,
    handler: Optional[Path] = None,
) -> dict:
    if verb not in ALLOWED:
        raise RuntimeError("broker verb is not allowlisted")
    expected = {
        "release": {"request_id", "expected_current_digest"},
        "retry": {"request_id", "expected_current_digest"},
        "rollback": {"request_id", "target_request_id", "expected_current_digest"},
        "audit": {"request_id"},
        "status": set(),
        "cert-check": {"certificate", "min_days"},
    }[verb]
    if set(request) != expected:
        raise RuntimeError(f"broker request keys are not exact for {verb}")
    if verb == "status":
        current = state_root / "current-release.json"
        invoke_cli(repo, ["status", "--current-release", str(current)], quiet=True)
        release = json.loads(
            current.read_text(encoding="utf-8"),
            object_pairs_hook=lambda pairs: _strict_pairs(pairs),
        )
        server = release.get("server") if isinstance(release, dict) else None
        server_revision = (
            server.get("deployed_revision") if isinstance(server, dict) else None
        )
        if not isinstance(server_revision, str) or not re.fullmatch(
            r"[0-9a-f]{40}", server_revision
        ):
            raise RuntimeError("broker current server revision is invalid")
        return {
            "ok": True,
            "verb": verb,
            "current_digest": sha256(current),
            "server_revision": server_revision,
        }
    if verb == "cert-check":
        certificate = request_file(root, request["certificate"])
        days = request["min_days"]
        if type(days) is not int or days < 0:
            raise RuntimeError("min_days must be a non-negative integer")
        invoke_cli(repo, ["cert", "check", "--certificate", str(certificate), "--min-days", str(days)])
        return {"ok": True, "verb": verb}
    request_root = request_directory(root, request["request_id"])
    submission = load_submission(request_root)
    manifest = verify_submission_evidence(request_root, submission)
    invoke_cli(repo, ["audit", "--manifest", str(manifest)])
    if verb == "audit":
        return {"ok": True, "verb": verb}

    if state_root.is_symlink() or not state_root.is_dir():
        raise RuntimeError("broker state root is unavailable")
    work_root = state_root / "broker-work"
    if work_root.is_symlink() or not work_root.is_dir():
        raise RuntimeError("broker work root is unavailable")
    lock_path = work_root / "release.lock"
    if lock_path.is_symlink():
        raise RuntimeError("broker release lock must not be a symlink")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = state_root / "current-release.json"
        expected_digest = request["expected_current_digest"]
        if not isinstance(expected_digest, str) or not EXACT_SHA256.fullmatch(expected_digest):
            raise RuntimeError("expected_current_digest is invalid")
        if not current.is_file() or current.is_symlink() or sha256(current) != expected_digest:
            raise RuntimeError("broker current-release CAS mismatch")
        if verb == "rollback":
            target_root = request_directory(root, request["target_request_id"])
            target_submission = load_submission(target_root)
            target_manifest = verify_submission_evidence(target_root, target_submission)
            invoke_cli(repo, ["audit", "--manifest", str(target_manifest)])
        inflight_root = work_root / "inflight"
        if inflight_root.is_symlink():
            raise RuntimeError("broker inflight root must not be a symlink")
        inflight_root.mkdir(mode=0o700, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=f"{request['request_id']}.", dir=str(inflight_root)) as temp:
            validated_root = Path(temp)
            if verb in {"release", "retry"}:
                validate_artifacts(
                    repo=repo,
                    request_root=request_root,
                    submission=submission,
                    output_root=validated_root,
                )
            selected_handler = handler or Path(__file__).with_name("run-production-handler.sh")
            result = invoke_handler(
                selected_handler,
                verb=verb,
                request_root=request_root,
                validated_root=validated_root,
                state_root=state_root,
                repo=repo,
                expected_current_digest=expected_digest,
                privileged=handler is None,
            )
        if not current.is_file() or current.is_symlink():
            raise RuntimeError("production handler removed current-release authority")
        if verb in {"release", "retry"} and sha256(current) == expected_digest:
            if result.get("status") != "already-current":
                raise RuntimeError("production handler succeeded without advancing current-release CAS")
        return {"ok": True, "verb": verb, "admitted": True, "result": result}
