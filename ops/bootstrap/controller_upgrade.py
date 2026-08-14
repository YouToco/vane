#!/usr/bin/env python3
"""One-time signed upgrade from the bootstrap controller to the hardened broker.

This changes only the root-owned controller authority. The product, Web and
middleware revisions remain unchanged, and the new controller may authorize
only a later exact-main product revision.
"""

from __future__ import annotations

import argparse
import fcntl
import grp
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
SCHEMA = "vane.controller-bootstrap-plan/v1"
EVIDENCE_SCHEMA = "vane.controller-bootstrap-evidence/v1"
EXACT_SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
PUBLIC_KEY = re.compile(r"^ssh-ed25519 [A-Za-z0-9+/]+={0,2}$")


class UpgradeError(RuntimeError):
    pass


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def strict_json(path: Path, *, limit: int = 1024 * 1024) -> dict[str, Any]:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in items:
            if key in result:
                raise UpgradeError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise UpgradeError(f"unsafe JSON input: {path}")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise UpgradeError("JSON root is not an object")
    return value


def read_public_key(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise UpgradeError("transport public key is unavailable")
    fields = path.read_text(encoding="ascii").strip().split()
    if len(fields) < 2:
        raise UpgradeError("transport public key is malformed")
    value = f"{fields[0]} {fields[1]}"
    if PUBLIC_KEY.fullmatch(value) is None:
        raise UpgradeError("transport public key is not Ed25519")
    return value


def validate_plan(value: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "schema",
        "controller_revision",
        "controller_archive_sha256",
        "expected_current_release_sha256",
        "expected_updated_current_release_sha256",
        "expected_product_revision",
        "expected_controller_revision",
        "expected_active_controller_revision",
        "expected_allowed_signers_sha256",
        "release_signer",
        "transport_public_key",
    }
    if set(value) != expected or value.get("schema") != SCHEMA:
        raise UpgradeError("controller bootstrap plan keys are not exact")
    for field in (
        "controller_revision",
        "expected_product_revision",
        "expected_controller_revision",
        "expected_active_controller_revision",
    ):
        if not isinstance(value[field], str) or EXACT_SHA.fullmatch(value[field]) is None:
            raise UpgradeError(f"{field} is not an exact SHA")
    for field in (
        "controller_archive_sha256",
        "expected_current_release_sha256",
        "expected_updated_current_release_sha256",
        "expected_allowed_signers_sha256",
    ):
        if not isinstance(value[field], str) or DIGEST.fullmatch(value[field]) is None:
            raise UpgradeError(f"{field} is not an exact digest")
    if value["controller_revision"] in {
        value["expected_product_revision"],
        value["expected_controller_revision"],
        value["expected_active_controller_revision"],
    }:
        raise UpgradeError("controller bootstrap must advance to a new revision")
    if value["release_signer"] != "vane-release-local":
        raise UpgradeError("controller bootstrap signer is not fixed")
    if not isinstance(value["transport_public_key"], str) or PUBLIC_KEY.fullmatch(
        value["transport_public_key"]
    ) is None:
        raise UpgradeError("controller bootstrap transport key is invalid")
    return value


def verify_signature(plan_path: Path, signer: str, allowed_signers: Path) -> None:
    signature = plan_path.with_name(plan_path.name + ".sig")
    if signature.is_symlink() or not signature.is_file():
        raise UpgradeError("controller bootstrap signature is unavailable")
    result = subprocess.run(
        [
            "ssh-keygen", "-Y", "verify", "-f", str(allowed_signers),
            "-I", signer, "-n", "vane-release", "-s", str(signature),
        ],
        input=plan_path.read_bytes(), capture_output=True, check=False,
    )
    if result.returncode != 0:
        raise UpgradeError("controller bootstrap signature verification failed")


def atomic_json(path: Path, value: dict[str, Any], *, mode: int, gid: int | None = None) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    with temporary.open("xb") as handle:
        handle.write(canonical(value))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(mode)
    if gid is not None:
        os.chown(temporary, 0, gid)
    os.replace(temporary, path)
    descriptor = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_bytes(path: Path, value: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.pending")
    if temporary.exists() or temporary.is_symlink():
        if temporary.is_symlink() or not temporary.is_file() or temporary.stat().st_uid != os.getuid():
            raise UpgradeError("stable signer staging authority is unsafe")
        temporary.unlink()
    with temporary.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(mode)
    os.replace(temporary, path)
    descriptor = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def switch_link(link: Path, target: Path) -> None:
    pending = link.with_name(f".{link.name}.{os.getpid()}")
    if pending.exists() or pending.is_symlink():
        raise UpgradeError("controller bootstrap staging link already exists")
    pending.symlink_to(target)
    os.replace(pending, link)
    descriptor = os.open(str(link.parent), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def clean_evidence_staging(directory: Path, final: Path, *, testing: bool) -> None:
    prefix = f".{final.name}."
    expected_uid = os.getuid() if testing else 0
    for path in directory.iterdir():
        if path == final:
            continue
        suffix = path.name.removeprefix(prefix)
        if (
            not path.name.startswith(prefix)
            or not suffix.isdigit()
            or path.is_symlink()
            or not path.is_file()
            or path.stat().st_uid != expected_uid
        ):
            raise UpgradeError("controller bootstrap was already consumed")
        path.unlink()


def apply(
    *,
    plan_path: Path,
    archive: Path,
    state: Path = Path("/var/lib/vane-broker/state/current-release.json"),
    control_root: Path = Path("/opt/vane-control"),
    evidence_root: Path = Path("/var/lib/vane-broker/evidence"),
    lock_path: Path = Path("/var/lib/vane-broker/state/broker-work/release.lock"),
    current_allowed_signers: Path = Path("/opt/vane-control/current/ops/policy/allowed_signers"),
    stable_allowed_signers: Path = Path("/etc/vane-broker/bootstrap_allowed_signers"),
    testing: bool = False,
) -> dict[str, Any]:
    if not testing and os.geteuid() != 0:
        raise UpgradeError("controller bootstrap apply requires root")
    plan = validate_plan(strict_json(plan_path))
    if archive.is_symlink() or not archive.is_file() or sha256(archive) != plan["controller_archive_sha256"]:
        raise UpgradeError("controller bootstrap archive digest differs")
    if lock_path.is_symlink() or not lock_path.is_file():
        raise UpgradeError("controller bootstrap lock authority is unsafe")

    with lock_path.open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        signer_authority = (
            stable_allowed_signers
            if stable_allowed_signers.exists()
            else current_allowed_signers
        )
        if (
            signer_authority.is_symlink()
            or not signer_authority.is_file()
            or sha256(signer_authority) != plan["expected_allowed_signers_sha256"]
        ):
            raise UpgradeError("controller bootstrap signer authority differs")
        verify_signature(plan_path, plan["release_signer"], signer_authority)
        from ops.cli.controller import validate_current_release
        from ops.broker.production_handler import stage_controller

        current = validate_current_release(state)
        active = (control_root / "current").resolve(strict=True)
        releases = (control_root / "releases").resolve(strict=True)
        try:
            active_relative = active.relative_to(releases)
        except ValueError as error:
            raise UpgradeError("active controller escapes release authority") from error
        if len(active_relative.parts) != 1 or EXACT_SHA.fullmatch(active_relative.name) is None:
            raise UpgradeError("active controller target is not an exact revision")
        active_revision = active_relative.name
        expected_old = plan["expected_controller_revision"]
        expected_active = plan["expected_active_controller_revision"]
        new = plan["controller_revision"]
        product = plan["expected_product_revision"]
        if current["monorepo_revision"] != product or current["server"]["deployed_revision"] != product:
            raise UpgradeError("controller bootstrap product revision changed")
        current_digest = sha256(state)
        already_applied = current["controller_revision"] == new and active_revision == new
        recovering_link = current["controller_revision"] == expected_old and active_revision == new
        recovering_state = current["controller_revision"] == new and active_revision == expected_active
        fresh = current["controller_revision"] == expected_old and active_revision == expected_active
        if not (already_applied or recovering_link or recovering_state or fresh):
            raise UpgradeError("controller bootstrap authority is inconsistent")
        if (fresh or recovering_link) and current_digest != plan["expected_current_release_sha256"]:
            raise UpgradeError("controller bootstrap current-release CAS mismatch")
        if (recovering_state or already_applied) and current_digest != plan[
            "expected_updated_current_release_sha256"
        ]:
            raise UpgradeError("controller bootstrap updated current-release CAS mismatch")

        evidence = {
            "schema": EVIDENCE_SCHEMA,
            "product_revision": product,
            "controller_revision": new,
            "controller_archive_sha256": plan["controller_archive_sha256"],
        }
        evidence_dir = evidence_root / "controller-bootstrap"
        evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        evidence_path = evidence_dir / f"{new}.json"
        if evidence_dir.is_symlink() or not evidence_dir.is_dir():
            raise UpgradeError("controller bootstrap evidence authority is unsafe")
        clean_evidence_staging(evidence_dir, evidence_path, testing=testing)
        existing_evidence = list(evidence_dir.iterdir())
        if any(path != evidence_path for path in existing_evidence):
            raise UpgradeError("controller bootstrap was already consumed")
        if evidence_path.exists() and strict_json(evidence_path) != evidence:
            raise UpgradeError("controller bootstrap evidence differs")

        if not stable_allowed_signers.exists():
            stable_allowed_signers.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if stable_allowed_signers.parent.is_symlink():
                raise UpgradeError("stable signer authority parent is unsafe")
            atomic_bytes(stable_allowed_signers, signer_authority.read_bytes(), mode=0o600)

        target = stage_controller(archive=archive, revision=new, controller_root=control_root)
        if not evidence_path.exists():
            atomic_json(evidence_path, evidence, mode=0o600)

        if not testing:
            descriptor, name = tempfile.mkstemp(prefix="vane-controller-transport-")
            os.close(descriptor)
            transport = Path(name)
            try:
                transport.write_text(plan["transport_public_key"] + "\n", encoding="ascii")
                transport.chmod(0o600)
                result = subprocess.run(
                    [str(target / "ops/bootstrap/install-broker.sh"), str(transport)], check=False,
                )
                if result.returncode != 0:
                    raise UpgradeError("hardened broker installation failed")
            finally:
                transport.unlink(missing_ok=True)

        if active_revision != new:
            switch_link(control_root / "current", target)
        if current["controller_revision"] != new:
            updated = dict(current)
            updated["controller_revision"] = new
            broker_gid = None if testing else grp.getgrnam("vane-broker").gr_gid
            atomic_json(state, updated, mode=0o640, gid=broker_gid)
        marker = target / ".controller-archive.sha256"
        if marker.read_text(encoding="ascii") != plan["controller_archive_sha256"] + "\n":
            raise UpgradeError("activated controller archive marker differs")
        return {
            "schema": "vane.controller-bootstrap-result/v1",
            "ok": True,
            "product_revision": product,
            "controller_revision": new,
            "already_current": already_applied,
        }


def create_plan(args: argparse.Namespace) -> int:
    from ops.cli.controller import assert_origin_main, git_revision, validate_current_release, write_control_plane_archive

    revision = git_revision("HEAD")
    assert_origin_main(revision)
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT,
        text=True, capture_output=True, check=False,
    )
    if dirty.returncode != 0 or dirty.stdout:
        raise UpgradeError("controller bootstrap plan requires a clean exact-main checkout")
    current = validate_current_release(args.current_release)
    if EXACT_SHA.fullmatch(args.active_controller_revision) is None:
        raise UpgradeError("active controller revision is not an exact SHA")
    for ancestor_revision in {
        current["controller_revision"], args.active_controller_revision
    }:
        ancestor = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor_revision, revision],
            cwd=ROOT, check=False,
        )
        if ancestor.returncode != 0:
            raise UpgradeError("controller bootstrap revision does not descend from current authority")
    args.output.mkdir(parents=True, mode=0o700)
    if any(args.output.iterdir()):
        raise UpgradeError("controller bootstrap output directory must be empty")
    archive = args.output / f"controller-{revision}.tar.gz"
    archive_digest = write_control_plane_archive(archive)
    updated_current = dict(current)
    updated_current["controller_revision"] = revision
    plan = validate_plan({
        "schema": SCHEMA,
        "controller_revision": revision,
        "controller_archive_sha256": archive_digest,
        "expected_current_release_sha256": sha256(args.current_release),
        "expected_updated_current_release_sha256": hashlib.sha256(
            canonical(updated_current)
        ).hexdigest(),
        "expected_product_revision": current["monorepo_revision"],
        "expected_controller_revision": current["controller_revision"],
        "expected_active_controller_revision": args.active_controller_revision,
        "expected_allowed_signers_sha256": sha256(args.allowed_signers),
        "release_signer": "vane-release-local",
        "transport_public_key": read_public_key(args.transport_public_key),
    })
    plan_path = args.output / "controller-bootstrap-plan.json"
    plan_path.write_bytes(canonical(plan))
    result = subprocess.run(
        ["ssh-keygen", "-Y", "sign", "-f", str(args.signing_key), "-n", "vane-release", str(plan_path)],
        check=False,
    )
    if result.returncode != 0:
        raise UpgradeError("controller bootstrap signing failed")
    print(json.dumps({"plan": str(plan_path), "archive": str(archive)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-plan")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--current-release", type=Path, required=True)
    create.add_argument("--signing-key", type=Path, required=True)
    create.add_argument("--transport-public-key", type=Path, required=True)
    create.add_argument("--active-controller-revision", required=True)
    create.add_argument("--allowed-signers", type=Path, required=True)
    apply_command = commands.add_parser("apply")
    apply_command.add_argument("--plan", type=Path, required=True)
    apply_command.add_argument("--controller-archive", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "create-plan":
        return create_plan(args)
    result = apply(plan_path=args.plan, archive=args.controller_archive)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (UpgradeError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"controller bootstrap refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
