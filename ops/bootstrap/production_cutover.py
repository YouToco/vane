#!/usr/bin/env python3
"""One-time signed legacy-to-broker production bootstrap.

The first controller cannot be installed by itself through the broker it is
meant to create.  This tool is the deliberately narrow, root-authorized
exception: a merged exact-main controller creates and signs a plan locally;
the VPS verifies that plan, rechecks every frozen legacy byte, constructs a
rollbackable release, and only then exposes the forced-command broker.
"""

from __future__ import annotations

import argparse
import grp
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Callable, Optional, Sequence


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
BASELINE_PATH = Path(__file__).with_name("legacy-production-baseline.json")
PLAN_SCHEMA = "vane.production-bootstrap-plan/v1"
BASELINE_SCHEMA = "vane.legacy-production-baseline/v1"
EXACT_SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
PUBLIC_KEY = re.compile(r"^ssh-ed25519 [A-Za-z0-9+/]+={0,2}$")


class BootstrapError(RuntimeError):
    """Fail-closed production bootstrap refusal."""


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def strict_json(path: Path) -> dict[str, Any]:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise BootstrapError(f"duplicate JSON key in {path}: {key}")
            value[key] = item
        return value

    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise BootstrapError(f"unsafe or oversized JSON: {path}")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise BootstrapError(f"JSON root is not an object: {path}")
    return value


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def read_public_key(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise BootstrapError(f"public key is unavailable: {path}")
    fields = path.read_text(encoding="ascii").strip().split()
    if len(fields) < 2:
        raise BootstrapError(f"public key is malformed: {path}")
    value = f"{fields[0]} {fields[1]}"
    if not PUBLIC_KEY.fullmatch(value):
        raise BootstrapError(f"public key is not Ed25519: {path}")
    return value


def validate_baseline(value: dict[str, Any]) -> dict[str, Any]:
    expected = {
        "schema",
        "server_revision",
        "server_tree_archive_sha256",
        "web_revision",
        "deploy_run_id",
        "backend_archive_sha256",
        "receipt",
        "binaries",
        "infra",
        "middleware_images",
        "uat_identity",
    }
    if set(value) != expected or value.get("schema") != BASELINE_SCHEMA:
        raise BootstrapError("legacy production baseline keys are not exact")
    for field in ("server_revision", "web_revision"):
        if not isinstance(value[field], str) or not EXACT_SHA.fullmatch(value[field]):
            raise BootstrapError(f"baseline {field} is not an exact SHA")
    for field in ("server_tree_archive_sha256", "backend_archive_sha256"):
        if not isinstance(value[field], str) or not DIGEST.fullmatch(value[field]):
            raise BootstrapError(f"baseline {field} is not an exact digest")
    if not isinstance(value["deploy_run_id"], str) or not value["deploy_run_id"].isdigit():
        raise BootstrapError("baseline deploy_run_id is invalid")
    expected_binaries = {
        "vane",
        "vane-research-gateway",
        "vane-migrate",
        "gate",
        "agentfirstretention",
    }
    expected_infra = {
        "Caddyfile",
        "docker-compose.yml",
        "dynamicconfig/development-sql.yaml",
        "vane.service",
        "vane-research-gateway.service",
        "vane-research-gateway.socket",
    }
    if set(value["binaries"]) != expected_binaries or set(value["infra"]) != expected_infra:
        raise BootstrapError("baseline release inventory is not exact")
    for section in ("binaries", "infra"):
        for name, entry in value[section].items():
            if not isinstance(entry, dict) or set(entry) != {"source", "sha256"}:
                raise BootstrapError(f"baseline {section}.{name} is malformed")
            source = entry["source"]
            if not isinstance(source, str) or not source.startswith("/") or ".." in Path(source).parts:
                raise BootstrapError(f"baseline {section}.{name} source is unsafe")
            if not isinstance(entry["sha256"], str) or not DIGEST.fullmatch(entry["sha256"]):
                raise BootstrapError(f"baseline {section}.{name} digest is invalid")
    receipt = value["receipt"]
    if not isinstance(receipt, dict) or set(receipt) != {"source", "sha256"}:
        raise BootstrapError("baseline receipt is malformed")
    if not isinstance(receipt["source"], str) or not receipt["source"].startswith("/"):
        raise BootstrapError("baseline receipt source is unsafe")
    if not isinstance(receipt["sha256"], str) or not DIGEST.fullmatch(receipt["sha256"]):
        raise BootstrapError("baseline receipt digest is invalid")
    identity = value["uat_identity"]
    if (
        not isinstance(identity, dict)
        or set(identity) != {"user_id", "tenant_id"}
        or type(identity["user_id"]) is not int
        or type(identity["tenant_id"]) is not int
        or identity["user_id"] <= 0
        or identity["tenant_id"] <= 0
    ):
        raise BootstrapError("baseline UAT identity is invalid")
    images = value["middleware_images"]
    if not isinstance(images, dict) or set(images) != {
        "vane-caddy-1",
        "vane-postgres-1",
        "vane-temporal-1",
        "vane-temporal-ui-1",
    }:
        raise BootstrapError("baseline middleware inventory is invalid")
    for container, item in images.items():
        if not isinstance(item, dict) or set(item) != {"reference", "image_id"}:
            raise BootstrapError(f"baseline middleware image is malformed: {container}")
        if not isinstance(item["reference"], str) or not item["reference"]:
            raise BootstrapError(f"baseline middleware reference is invalid: {container}")
        if (
            not isinstance(item["image_id"], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", item["image_id"])
        ):
            raise BootstrapError(f"baseline middleware image ID is invalid: {container}")
    return value


def validate_plan(value: dict[str, Any], baseline_digest: str) -> dict[str, Any]:
    expected = {
        "schema",
        "controller_revision",
        "controller_archive_sha256",
        "baseline_sha256",
        "release_signer",
        "broker_signer",
        "broker_signer_public_key",
        "transport_public_key",
    }
    if set(value) != expected or value.get("schema") != PLAN_SCHEMA:
        raise BootstrapError("production bootstrap plan keys are not exact")
    if not isinstance(value["controller_revision"], str) or not EXACT_SHA.fullmatch(
        value["controller_revision"]
    ):
        raise BootstrapError("bootstrap controller revision is invalid")
    if not isinstance(value["controller_archive_sha256"], str) or not DIGEST.fullmatch(
        value["controller_archive_sha256"]
    ):
        raise BootstrapError("bootstrap controller archive digest is invalid")
    if value["baseline_sha256"] != baseline_digest:
        raise BootstrapError("bootstrap plan binds a different legacy baseline")
    if value["release_signer"] != "vane-release-local":
        raise BootstrapError("bootstrap plan release signer is not the fixed local authority")
    if value["broker_signer"] != "vane-production-broker":
        raise BootstrapError("bootstrap plan broker signer is not the fixed production authority")
    for field in ("broker_signer_public_key", "transport_public_key"):
        if not isinstance(value[field], str) or not PUBLIC_KEY.fullmatch(value[field]):
            raise BootstrapError(f"bootstrap {field} is invalid")
    return value


class Layout:
    """Map fixed production paths under a test root without weakening real CLI paths."""

    def __init__(self, testing_root: Optional[Path] = None) -> None:
        self.testing_root = testing_root

    def path(self, absolute: str | Path) -> Path:
        path = Path(absolute)
        if not path.is_absolute():
            raise BootstrapError(f"production path is not absolute: {path}")
        if self.testing_root is None:
            return path
        return self.testing_root / str(path).lstrip("/")


def verify_file_inventory(baseline: dict[str, Any], layout: Layout) -> None:
    for section in ("binaries", "infra"):
        for name, entry in baseline[section].items():
            path = layout.path(entry["source"])
            if path.is_symlink() or not path.is_file() or sha256(path) != entry["sha256"]:
                raise BootstrapError(f"legacy {section} drift: {name}")
            if section == "binaries" and not os.access(path, os.X_OK):
                raise BootstrapError(f"legacy binary is not executable: {name}")
    receipt = layout.path(baseline["receipt"]["source"])
    if receipt.is_symlink() or not receipt.is_file() or sha256(receipt) != baseline["receipt"]["sha256"]:
        raise BootstrapError("legacy release receipt drift")
    from ops.cli.controller import validate_release_receipt

    value = validate_release_receipt(receipt)
    if (
        value["source_revision"] != baseline["server_revision"]
        or value["deploy_run_id"] != baseline["deploy_run_id"]
        or value["backend_archive_sha256"] != baseline["backend_archive_sha256"]
        or value["vane_sha256"] != baseline["binaries"]["vane"]["sha256"]
        or value["agentfirstretention_sha256"]
        != baseline["binaries"]["agentfirstretention"]["sha256"]
    ):
        raise BootstrapError("legacy release receipt does not describe the live baseline")


def command_output(command: list[str], *, input_text: Optional[str] = None) -> str:
    result = subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise BootstrapError(f"production fact command failed: {command[0]}")
    return result.stdout.strip()


def verify_live_baseline(
    baseline: dict[str, Any],
    layout: Layout,
    *,
    capture: Callable[..., str] = command_output,
) -> None:
    verify_file_inventory(baseline, layout)
    server_state = layout.path(
        "/var/lib/vane-deploy-runner/.local/state/vane-deploy/deployed-vane.sha"
    )
    web_state = layout.path(
        "/var/lib/vane-deploy-runner/.local/state/vane-deploy/deployed-vane-web.sha"
    )
    if server_state.read_text(encoding="ascii").strip() != baseline["server_revision"]:
        raise BootstrapError("legacy deployed Server revision drift")
    if web_state.read_text(encoding="ascii").strip() != baseline["web_revision"]:
        raise BootstrapError("legacy deployed Web revision drift")
    for path in (
        "/opt/vane/current",
        "/opt/vane-control/current",
        "/var/lib/vane-broker/state/current-release.json",
    ):
        target = layout.path(path)
        if target.exists() or target.is_symlink():
            raise BootstrapError(f"new production authority already exists: {path}")
    if layout.testing_root is not None:
        return
    if capture(["systemctl", "is-active", "vane.service"]) != "active":
        raise BootstrapError("legacy vane.service is not active")
    if capture(["systemctl", "is-active", "vane-research-gateway.socket"]) != "active":
        raise BootstrapError("legacy gateway socket is not active")
    pid = capture(["systemctl", "show", "vane.service", "--property=MainPID", "--value"])
    if not pid.isdigit() or int(pid) <= 0:
        raise BootstrapError("legacy vane.service has no live PID")
    if Path(f"/proc/{pid}/exe").resolve() != Path(baseline["binaries"]["vane"]["source"]):
        raise BootstrapError("legacy live process executable drift")
    for container, image in baseline["middleware_images"].items():
        actual_reference = capture(
            ["docker", "inspect", container, "--format", "{{.Config.Image}}"]
        )
        actual_id = capture(["docker", "inspect", container, "--format", "{{.Image}}"])
        if actual_reference != image["reference"] or actual_id != image["image_id"]:
            raise BootstrapError(f"legacy middleware image drift: {container}")


def manifest_bytes(root: Path, prefixes: tuple[str, ...]) -> bytes:
    lines: list[str] = []
    for prefix in prefixes:
        for path in sorted((root / prefix).rglob("*")):
            if path.is_file() and not path.is_symlink():
                lines.append(f"{sha256(path)}  {path.relative_to(root).as_posix()}\n")
    return "".join(lines).encode("ascii")


def build_legacy_release(baseline: dict[str, Any], layout: Layout) -> tuple[Path, str]:
    release_root = layout.path("/opt/vane/releases")
    release_root.mkdir(parents=True, exist_ok=True, mode=0o755)
    release_root.chmod(0o755)
    target = release_root / baseline["server_revision"]
    if target.exists() or target.is_symlink():
        raise BootstrapError("legacy current release target already exists")
    pending = Path(tempfile.mkdtemp(prefix=".bootstrap-legacy.", dir=str(release_root)))
    try:
        (pending / "bin").mkdir(mode=0o755)
        (pending / "deploy/dynamicconfig").mkdir(parents=True, mode=0o755)
        for name, entry in baseline["binaries"].items():
            shutil.copy2(layout.path(entry["source"]), pending / "bin" / name)
            (pending / "bin" / name).chmod(0o755)
        for name, entry in baseline["infra"].items():
            destination = pending / "deploy" / name
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            shutil.copy2(layout.path(entry["source"]), destination)
            destination.chmod(0o644)
        shutil.copy2(
            layout.path(baseline["receipt"]["source"]),
            pending / "release-receipt.json",
        )
        (pending / "release-receipt.json").chmod(0o644)
        (pending / "infra-bound-files.sha256").write_bytes(
            manifest_bytes(pending, ("bin", "deploy"))
        )
        (pending / "infra-manifest.sha256").write_bytes(
            manifest_bytes(pending, ("deploy",))
        )
        (pending / "monorepo-revision").write_text(
            baseline["server_revision"] + "\n", encoding="ascii"
        )
        for name in ("infra-bound-files.sha256", "infra-manifest.sha256", "monorepo-revision"):
            (pending / name).chmod(0o644)
        pending.chmod(0o755)
        infra_digest = sha256(pending / "infra-manifest.sha256")
        os.replace(pending, target)
        return target, infra_digest
    except BaseException:
        shutil.rmtree(pending, ignore_errors=True)
        raise


def write_current_release(
    *,
    baseline: dict[str, Any],
    controller_revision: str,
    infra_digest: str,
    layout: Layout,
    broker_gid: int,
) -> Path:
    state_root = layout.path("/var/lib/vane-broker/state")
    if state_root.is_symlink() or not state_root.is_dir():
        raise BootstrapError("preprovisioned broker state root is unavailable")
    current = state_root / "current-release.json"
    if current.exists() or current.is_symlink():
        raise BootstrapError("initial current-release authority already exists")
    value = {
        "schema": "vane.current-release/v2",
        "monorepo_revision": baseline["server_revision"],
        "server": {
            "tree_digest": baseline["server_tree_archive_sha256"],
            "artifact_digest": baseline["backend_archive_sha256"],
            "deployed_revision": baseline["server_revision"],
        },
        "infra_manifest_digest": infra_digest,
        "controller_revision": controller_revision,
    }
    temporary = state_root / f".current-release.{os.getpid()}.json"
    with temporary.open("xb") as handle:
        handle.write(canonical(value))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o640)
    if layout.testing_root is None:
        os.chown(temporary, 0, broker_gid)
    os.replace(temporary, current)
    provider = state_root / "provider/vane-deploy"
    provider.mkdir(parents=True, mode=0o700)
    (provider / "deployed-vane.sha").write_text(
        baseline["server_revision"] + "\n", encoding="ascii"
    )
    (provider / "deployed-vane.sha").chmod(0o600)
    return current


def activate_link(link: Path, target: Path) -> None:
    if link.exists() or link.is_symlink():
        raise BootstrapError(f"initial authority link already exists: {link}")
    link.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    pending = link.with_name(f".{link.name}.{os.getpid()}")
    pending.symlink_to(target)
    os.replace(pending, link)


def verify_plan_signature(plan_path: Path, plan: dict[str, Any]) -> None:
    from ops.cli.controller import verify_signature

    verify_signature(plan_path, plan["release_signer"], ROOT / "ops/policy/allowed_signers")


def verify_broker_key(plan: dict[str, Any], layout: Layout) -> None:
    private = layout.path("/etc/vane-broker/credentials/broker_signing_key")
    if private.is_symlink() or not private.is_file():
        raise BootstrapError("VPS broker signing key is unavailable")
    actual = command_output(["ssh-keygen", "-y", "-f", str(private)])
    if actual != plan["broker_signer_public_key"]:
        raise BootstrapError("VPS broker signing key differs from signed bootstrap plan")


def write_production_config(baseline: dict[str, Any], layout: Layout) -> Path:
    target = layout.path("/etc/vane-broker/production.json")
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    value = {
        "schema": "vane.production-handler/v1",
        "uat_command": [
            "/opt/vane-control/current/ops/audit/production-uat.py",
            "--origin",
            "https://vane.zhuoqidev.com",
        ],
        "evidence_root": "/var/lib/vane-broker/evidence",
        "signer": "vane-production-broker",
        "controller_root": "/opt/vane-control",
        "state_reader_group": "vane-broker",
        "uat_identity": baseline["uat_identity"],
    }
    temporary = target.with_name(f".{target.name}.{os.getpid()}")
    temporary.write_bytes(canonical(value))
    temporary.chmod(0o600)
    os.replace(temporary, target)
    return target


def audit(plan_path: Path, archive: Path, layout: Layout) -> tuple[dict[str, Any], dict[str, Any]]:
    baseline = validate_baseline(strict_json(BASELINE_PATH))
    plan = validate_plan(strict_json(plan_path), sha256(BASELINE_PATH))
    verify_plan_signature(plan_path, plan)
    if archive.is_symlink() or not archive.is_file() or sha256(archive) != plan["controller_archive_sha256"]:
        raise BootstrapError("controller archive differs from signed bootstrap plan")
    if plan["controller_revision"] == baseline["server_revision"]:
        raise BootstrapError("initial controller must be newer than the legacy product revision")
    verify_live_baseline(baseline, layout)
    if layout.testing_root is None:
        verify_broker_key(plan, layout)
    return plan, baseline


def apply(plan_path: Path, archive: Path, layout: Layout) -> dict[str, Any]:
    if layout.testing_root is None and os.geteuid() != 0:
        raise BootstrapError("production bootstrap apply requires root")
    plan, baseline = audit(plan_path, archive, layout)
    release_target: Optional[Path] = None
    controller_root = layout.path("/opt/vane-control")
    current_state: Optional[Path] = None
    activated_links: list[Path] = []
    config_path: Optional[Path] = None
    try:
        release_target, infra_digest = build_legacy_release(baseline, layout)
        from ops.broker.production_handler import stage_controller

        controller_target = stage_controller(
            archive=archive,
            revision=plan["controller_revision"],
            controller_root=controller_root,
        )
        transport_fd, transport_name = tempfile.mkstemp(
            prefix="vane-broker-transport-", text=True
        )
        os.close(transport_fd)
        transport = Path(transport_name)
        try:
            transport.write_text(plan["transport_public_key"] + "\n", encoding="ascii")
            transport.chmod(0o600)
            if layout.testing_root is None:
                installer = ROOT / "ops/bootstrap/install-broker.sh"
                result = subprocess.run([str(installer), str(transport)], check=False)
                if result.returncode != 0:
                    raise BootstrapError("forced-command broker installation failed")
            else:
                state = layout.path("/var/lib/vane-broker/state")
                (state / "broker-work").mkdir(parents=True, exist_ok=True)
                layout.path("/var/lib/vane-broker/evidence").mkdir(parents=True, exist_ok=True)
        finally:
            transport.unlink(missing_ok=True)
        broker_gid = os.getgid() if layout.testing_root is not None else grp.getgrnam("vane-broker").gr_gid
        current_state = write_current_release(
            baseline=baseline,
            controller_revision=plan["controller_revision"],
            infra_digest=infra_digest,
            layout=layout,
            broker_gid=broker_gid,
        )
        product_current = layout.path("/opt/vane/current")
        activate_link(product_current, release_target)
        activated_links.append(product_current)
        controller_current = controller_root / "current"
        activate_link(controller_current, controller_target)
        activated_links.append(controller_current)
        config_path = write_production_config(baseline, layout)
        return {
            "schema": "vane.production-bootstrap-result/v1",
            "ok": True,
            "legacy_server_revision": baseline["server_revision"],
            "controller_revision": plan["controller_revision"],
            "current_release_sha256": sha256(current_state),
            "infra_manifest_digest": infra_digest,
        }
    except BaseException:
        for link in reversed(activated_links):
            link.unlink(missing_ok=True)
        if config_path is not None:
            config_path.unlink(missing_ok=True)
        if current_state is not None:
            current_state.unlink(missing_ok=True)
        if release_target is not None:
            shutil.rmtree(release_target, ignore_errors=True)
        shutil.rmtree(controller_root, ignore_errors=True)
        raise


def create_plan(args: argparse.Namespace) -> int:
    from ops.cli.controller import (
        assert_origin_main,
        git_revision,
        write_control_plane_archive,
    )

    revision = git_revision("HEAD")
    assert_origin_main(revision)
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if dirty.returncode != 0 or dirty.stdout:
        raise BootstrapError("bootstrap plan requires a clean exact-main checkout")
    baseline = validate_baseline(strict_json(BASELINE_PATH))
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", baseline["server_revision"], revision],
        cwd=ROOT,
        check=False,
    )
    if ancestor.returncode != 0:
        raise BootstrapError("legacy Server revision is not an ancestor of controller main")
    args.output.mkdir(parents=True, mode=0o700)
    if any(args.output.iterdir()):
        raise BootstrapError("bootstrap plan output directory must be empty")
    archive = args.output / f"controller-{revision}.tar.gz"
    archive_digest = write_control_plane_archive(archive)
    plan = {
        "schema": PLAN_SCHEMA,
        "controller_revision": revision,
        "controller_archive_sha256": archive_digest,
        "baseline_sha256": sha256(BASELINE_PATH),
        "release_signer": args.signer,
        "broker_signer": "vane-production-broker",
        "broker_signer_public_key": read_public_key(args.broker_public_key),
        "transport_public_key": read_public_key(args.transport_public_key),
    }
    validate_plan(plan, sha256(BASELINE_PATH))
    plan_path = args.output / "bootstrap-plan.json"
    plan_path.write_bytes(canonical(plan))
    result = subprocess.run(
        [
            "ssh-keygen",
            "-Y",
            "sign",
            "-f",
            str(args.signing_key),
            "-n",
            "vane-release",
            str(plan_path),
        ],
        check=False,
    )
    if result.returncode != 0:
        raise BootstrapError("bootstrap plan signing failed")
    print(json.dumps({"plan": str(plan_path), "archive": str(archive)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-plan")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--signing-key", type=Path, required=True)
    create.add_argument("--transport-public-key", type=Path, required=True)
    create.add_argument("--broker-public-key", type=Path, required=True)
    create.add_argument("--signer", default="vane-release-local")
    create.set_defaults(handler=create_plan)
    for name in ("audit", "apply"):
        command = commands.add_parser(name)
        command.add_argument("--plan", type=Path, required=True)
        command.add_argument("--controller-archive", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "create-plan":
        return args.handler(args)
    if os.geteuid() != 0:
        raise BootstrapError(f"production bootstrap {args.command} requires root")
    layout = Layout()
    if args.command == "audit":
        plan, baseline = audit(args.plan, args.controller_archive, layout)
        result = {
            "schema": "vane.production-bootstrap-audit/v1",
            "ok": True,
            "legacy_server_revision": baseline["server_revision"],
            "controller_revision": plan["controller_revision"],
        }
    else:
        result = apply(args.plan, args.controller_archive, layout)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BootstrapError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"production bootstrap refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
