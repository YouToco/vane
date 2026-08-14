"""Root broker admission boundary; production mutation handlers are installed separately."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


ALLOWED = {"release", "retry", "rollback", "status", "audit", "cert-check"}
MUTATING = {"release", "retry", "rollback"}


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


def invoke_cli(repo: Path, arguments: list[str]) -> None:
    cli = repo / "ops/bin/vane"
    if cli.is_symlink() or not cli.is_file():
        raise RuntimeError("fixed repository CLI is unavailable")
    result = subprocess.run([str(cli), *arguments], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"repository admission failed with exit {result.returncode}")


def handle(verb: str, request: dict, *, root: Path, repo: Path, state_root: Path) -> dict:
    if verb not in ALLOWED:
        raise RuntimeError("broker verb is not allowlisted")
    expected = {
        "release": {"manifest", "current_release", "candidate_release", "release_receipt", "expected_current_digest"},
        "retry": {"manifest"},
        "rollback": {"manifest", "target_manifest", "current_release", "expected_current_digest"},
        "audit": {"manifest"},
        "status": set(),
        "cert-check": {"certificate", "min_days"},
    }[verb]
    if set(request) != expected:
        raise RuntimeError(f"broker request keys are not exact for {verb}")
    if verb == "status":
        current = state_root / "current-release.json"
        invoke_cli(repo, ["status", "--current-release", str(current)])
        return {"ok": True, "verb": verb}
    if verb == "cert-check":
        certificate = request_file(root, request["certificate"])
        days = request["min_days"]
        if type(days) is not int or days < 0:
            raise RuntimeError("min_days must be a non-negative integer")
        invoke_cli(repo, ["cert", "check", "--certificate", str(certificate), "--min-days", str(days)])
        return {"ok": True, "verb": verb}
    manifest = request_file(root, request["manifest"])
    if verb in {"audit", "retry"}:
        invoke_cli(repo, ["audit", "--manifest", str(manifest)])
        return {"ok": True, "verb": verb}

    lock_path = state_root / "release.lock"
    state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = state_root / "current-release.json"
        expected_digest = request["expected_current_digest"]
        if not isinstance(expected_digest, str) or len(expected_digest) != 64:
            raise RuntimeError("expected_current_digest is invalid")
        if not current.is_file() or current.is_symlink() or sha256(current) != expected_digest:
            raise RuntimeError("broker current-release CAS mismatch")
        if verb == "release":
            supplied_current = request_file(root, request["current_release"])
            if sha256(supplied_current) != expected_digest:
                raise RuntimeError("request current-release differs from broker state")
            candidate = request_file(root, request["candidate_release"])
            receipt = request_file(root, request["release_receipt"])
            invoke_cli(
                repo,
                [
                    "audit", "--manifest", str(manifest),
                    "--current-release", str(current),
                    "--candidate-release", str(candidate),
                    "--expected-current-digest", expected_digest,
                    "--release-receipt", str(receipt),
                ],
            )
        else:
            target = request_file(root, request["target_manifest"])
            invoke_cli(repo, ["audit", "--manifest", str(manifest)])
            invoke_cli(repo, ["audit", "--manifest", str(target)])
        # Admission succeeds, but mutation remains disabled until a separately
        # installed fixed handler is configured and exercised on the VPS.
        return {"ok": False, "verb": verb, "admitted": True, "mutation": "not-installed"}
