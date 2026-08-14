#!/usr/bin/env python3
"""Root-owned single-VPS production release state machine.

The broker has already verified the signed artifact chain and unpacked both
artifacts before entering this handler.  This process owns product mutation,
recovery, production verification, UAT, durable evidence, and the final CAS.
Application services are native systemd binaries; Docker is invoked only by
the canonical infra transition scripts for PostgreSQL, Temporal, Temporal UI,
and Caddy.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Any, Optional
from urllib.request import Request, urlopen


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def strict_json(path: Path) -> dict:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise RuntimeError(f"duplicate JSON key in {path}: {key}")
            value[key] = item
        return value

    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise RuntimeError(f"unsafe or oversized JSON evidence: {path}")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON evidence root is not an object: {path}")
    return value


def run(command: list[str], *, env: Optional[dict[str, str]] = None) -> None:
    result = subprocess.run(command, env=env, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"production command failed with exit {result.returncode}: {command[0]}")


def write_json(path: Path, value: dict) -> None:
    path.write_bytes(canonical(value))
    path.chmod(0o600)


def write_revision(path: Path, revision: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    temporary.write_text(revision + "\n", encoding="ascii")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def credential(name: str) -> str:
    root = Path(os.environ.get("CREDENTIALS_DIRECTORY", ""))
    path = root / name
    if not root.is_absolute() or path.is_symlink() or not path.is_file():
        raise RuntimeError(f"systemd credential is unavailable: {name}")
    value = path.read_text(encoding="utf-8").rstrip("\r\n")
    if not value:
        raise RuntimeError(f"systemd credential is empty: {name}")
    return value


def load_config() -> dict:
    path = Path("/etc/vane-broker/production.json")
    if os.environ.get("VANE_BROKER_HANDLER_TESTING") == "1" and os.geteuid() != 0:
        path = Path(os.environ.get("VANE_BROKER_HANDLER_CONFIG", ""))
    value = strict_json(path)
    expected = {
        "schema",
        "public_origin",
        "cloudflare_origin",
        "aliyun_bin",
        "ossutil_bin",
        "wrangler_bin",
        "uat_command",
        "evidence_root",
        "signer",
        "controller_root",
    }
    if set(value) != expected or value.get("schema") != "vane.production-handler/v1":
        raise RuntimeError("production handler configuration keys are not exact")
    for key in ("public_origin", "cloudflare_origin"):
        if not isinstance(value[key], str) or not value[key].startswith("https://"):
            raise RuntimeError(f"production handler {key} is invalid")
    for key in (
        "aliyun_bin",
        "ossutil_bin",
        "wrangler_bin",
        "evidence_root",
        "controller_root",
    ):
        path_value = Path(value[key])
        if not path_value.is_absolute():
            raise RuntimeError(f"production handler {key} is not absolute")
    command = value["uat_command"]
    if not isinstance(command, list) or not command or any(not isinstance(item, str) or not item for item in command):
        raise RuntimeError("production handler UAT command is invalid")
    if not isinstance(value["signer"], str) or not value["signer"].isascii() or any(
        character.isspace() for character in value["signer"]
    ):
        raise RuntimeError("production handler signer is invalid")
    return value


def ensure_provider_state(
    state_root: Path, *, server_revision: str, web_revision: str
) -> Path:
    provider_root = state_root / "provider"
    state = provider_root / "vane-deploy"
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name, current_revision in (
        ("deployed-vane.sha", server_revision),
        ("deployed-vane-web.sha", web_revision),
    ):
        path = state / name
        if path.exists():
            if path.is_symlink() or path.read_text(encoding="ascii").strip() != current_revision:
                raise RuntimeError(f"provider compatibility state differs from current-release: {name}")
        else:
            path.write_text(current_revision + "\n", encoding="ascii")
            path.chmod(0o600)
    return provider_root


def provider_environment(
    *, config: dict, state_root: Path, receipt_root: Path, expected_revision: str
) -> dict[str, str]:
    return {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/nonexistent",
        "XDG_STATE_HOME": str(state_root),
        "EXPECTED_DEPLOYED_SHA": expected_revision,
        "FRONTEND_RECEIPT_DIR": str(receipt_root),
        "ALIYUN_BIN": config["aliyun_bin"],
        "OSSUTIL_BIN": config["ossutil_bin"],
        "WRANGLER_BIN": config["wrangler_bin"],
        "ALIYUN_ACCESS_KEY_ID": credential("aliyun_access_key_id"),
        "ALIYUN_ACCESS_KEY_SECRET": credential("aliyun_access_key_secret"),
        "CLOUDFLARE_API_TOKEN": credential("cloudflare_api_token"),
        "CLOUDFLARE_ACCOUNT_ID": credential("cloudflare_account_id"),
    }


def stage_server(validated: Path, revision: str) -> Path:
    backend = validated / "backend"
    stage = Path(f"/opt/vane/.deploy-{revision}-{os.getpid()}-1")
    if stage.exists() or stage.is_symlink():
        raise RuntimeError("server release stage already exists")
    (stage / "bin").mkdir(parents=True, mode=0o700)
    (stage / "dynamicconfig").mkdir(mode=0o700)
    for path in (backend / "bin").iterdir():
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("validated server payload contains an unsafe binary")
        shutil.copy2(path, stage / "bin" / path.name)
    deploy = backend / "deploy"
    for path in deploy.rglob("*"):
        if path.is_dir():
            continue
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("validated server payload contains unsafe infra")
        relative = path.relative_to(deploy)
        destination = stage / relative
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copy2(path, destination)
    shutil.copy2(backend / "release-receipt.json", stage / "release-receipt.json")
    return stage


def public_marker(origin: str, revision: str) -> dict:
    request = Request(origin.rstrip("/") + "/.well-known/vane-release.json")
    with urlopen(request, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError(f"release marker returned HTTP {response.status}")
        value = json.loads(response.read(64 * 1024))
    if not isinstance(value, dict) or value.get("source_revision") != revision or value.get("source_dirty") is not False:
        raise RuntimeError(f"public release marker differs from exact revision at {origin}")
    return value


def run_uat(command: list[str], revision: str, output: Path) -> dict:
    executable = Path(command[0])
    if executable.is_symlink() or not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError("fixed production UAT command is unavailable")
    result = subprocess.run(
        [*command, "--sha", revision], text=True, capture_output=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"production UAT failed with exit {result.returncode}")
    value = json.loads(result.stdout)
    if (
        not isinstance(value, dict)
        or value.get("schema") != "vane.production-uat/v1"
        or value.get("revision") != revision
        or value.get("ok") is not True
    ):
        raise RuntimeError("production UAT evidence is invalid")
    write_json(output, value)
    return value


def restore(
    *,
    repo: Path,
    previous: dict,
    candidate_revision: str,
    provider_root: Path,
    provider_env: dict[str, str],
    evidence_root: Path,
) -> None:
    previous_revision = previous["monorepo_revision"]
    previous_frontend = evidence_root / "releases" / previous_revision / "frontend"
    if previous_frontend.is_symlink() or not previous_frontend.is_dir():
        raise RuntimeError("previous validated Web artifact is unavailable for recovery")
    provider_web_state = provider_root / "vane-deploy/deployed-vane-web.sha"
    if provider_web_state.is_symlink() or not provider_web_state.is_file():
        raise RuntimeError("provider Web state is unavailable during recovery")
    provider_expected = provider_web_state.read_text(encoding="ascii").strip()
    restore_env = {
        **provider_env,
        "XDG_STATE_HOME": str(provider_root),
        "EXPECTED_DEPLOYED_SHA": provider_expected,
    }
    deploy = repo / "ops/release/deploy.sh"
    for component in ("frontend-aliyun-restore", "frontend-cloudflare-restore"):
        run([str(deploy), component, str(previous_frontend), previous_revision], env=restore_env)
    run([str(deploy), "frontend-finalize", str(previous_frontend), previous_revision], env=restore_env)
    rollback = repo / "ops/rollback/switch-server-release.sh"
    run([str(rollback), previous["server"]["deployed_revision"], candidate_revision])
    write_revision(
        provider_root / "vane-deploy/deployed-vane.sha",
        previous["server"]["deployed_revision"],
    )


def atomic_current_release(path: Path, value: dict, expected_digest: str) -> None:
    if digest(path) != expected_digest:
        raise RuntimeError("current-release CAS changed before finalize")
    temporary = path.with_name(f".current-release.{os.getpid()}.json")
    with temporary.open("xb") as handle:
        handle.write(canonical(value))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o600)
    os.replace(temporary, path)
    directory = os.open(str(path.parent), os.O_RDONLY)
    controller_activated = False
    durable = evidence_root / "releases" / revision
    controller_root = Path(config["controller_root"])
    active_controller = controller_root / "current"
    if not active_controller.is_symlink():
        raise RuntimeError("active controller authority is not an atomic symlink")
    previous_controller_target = active_controller.resolve(strict=True)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def stage_controller(*, archive: Path, revision: str, controller_root: Path) -> Path:
    releases = controller_root / "releases"
    releases.mkdir(parents=True, exist_ok=True, mode=0o755)
    target = releases / revision
    marker_name = ".controller-archive.sha256"
    archive_digest = digest(archive)
    if target.is_dir() and not target.is_symlink():
        marker = target / marker_name
        if marker.is_file() and not marker.is_symlink() and marker.read_text(encoding="ascii") == archive_digest + "\n":
            return target
        raise RuntimeError("existing controller release differs from candidate archive")
    pending = Path(tempfile.mkdtemp(prefix=f".controller-{revision}.", dir=str(releases)))
    try:
        seen: set[str] = set()
        with tarfile.open(archive, mode="r:gz") as bundle:
            members = bundle.getmembers()
            if not members or len(members) > 20_000:
                raise RuntimeError("controller archive member count is invalid")
            for info in members:
                parts = Path(info.name).parts
                if (
                    not parts
                    or parts[0] not in {"ops", "contracts", "infra", "tools", "server"}
                    or Path(info.name).is_absolute()
                    or any(part in {"", ".", ".."} for part in parts)
                    or info.name in seen
                    or not info.isreg()
                    or info.mode & 0o7000
                ):
                    raise RuntimeError(f"controller archive member is unsafe: {info.name}")
                seen.add(info.name)
                destination = pending.joinpath(*parts)
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                source = bundle.extractfile(info)
                if source is None:
                    raise RuntimeError(f"cannot read controller archive member: {info.name}")
                with destination.open("xb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                destination.chmod(info.mode & 0o777)
        for required in (
            "ops/bin/vane",
            "ops/broker/forced_command.py",
            "ops/broker/production_handler.py",
            "ops/release/artifact.py",
            "tools/toolchain.lock.json",
            "server/go.mod",
            "server/internal/testgate/cmd/testpolicyscan/main.go",
        ):
            path = pending / required
            if path.is_symlink() or not path.is_file():
                raise RuntimeError(f"controller archive lacks required member: {required}")
        (pending / marker_name).write_text(archive_digest + "\n", encoding="ascii")
        (pending / marker_name).chmod(0o600)
        os.replace(pending, target)
    except BaseException:
        shutil.rmtree(pending, ignore_errors=True)
        raise
    return target


def activate_controller(*, target: Path, controller_root: Path) -> None:
    current = controller_root / "current"
    next_link = controller_root / f".current-{target.name}.{os.getpid()}"
    if next_link.exists() or next_link.is_symlink():
        raise RuntimeError("controller activation staging link already exists")
    next_link.symlink_to(target)
    os.replace(next_link, current)


def active_controller_target(controller_root: Path) -> Path:
    """Resolve the installed N controller without accepting an arbitrary link."""
    releases = controller_root / "releases"
    current = controller_root / "current"
    if not current.is_symlink() or not releases.is_dir() or releases.is_symlink():
        raise RuntimeError("active controller authority is unavailable")
    try:
        target = current.resolve(strict=True)
        target.relative_to(releases.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise RuntimeError("active controller escapes the release authority") from error
    if target.is_symlink() or not target.is_dir():
        raise RuntimeError("active controller target is unsafe")
    return target


def preserve_failed_evidence(
    *, revision: str, evidence_root: Path, durable: Path, transaction: Path
) -> Optional[Path]:
    """Move the most complete failed transaction aside, even after recovery errors."""
    source = durable if durable.exists() and not durable.is_symlink() else transaction
    if not source.exists():
        return None
    if source.is_symlink() or not source.is_dir():
        raise RuntimeError("failed release evidence source is unsafe")
    failed_root = evidence_root / "failed"
    failed_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    failed = failed_root / (
        revision + "-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )
    if failed.exists() or failed.is_symlink():
        raise RuntimeError("failed release evidence destination already exists")
    os.replace(source, failed)
    return failed


def release(
    *,
    request_root: Path,
    validated: Path,
    state_root: Path,
    repo: Path,
    expected_digest: str,
    config: dict,
) -> dict:
    # Import the already-installed N controller.  Candidate M code is data for
    # this process and cannot authorize or execute itself.
    sys.path.insert(0, str(repo))
    from ops.cli.controller import (  # pylint: disable=import-outside-toplevel
        DEFAULT_SIGNERS,
        require_release_chain,
        validate_current_release,
        validate_current_release_transition,
        validate_release_receipt,
        write_signed_manifest,
    )
    from ops.broker.controller import load_submission  # pylint: disable=import-outside-toplevel

    current_path = state_root / "current-release.json"
    current = validate_current_release(current_path)
    submission = load_submission(request_root)
    revision = submission["revision"]
    gate = strict_json(request_root / "full-gate.json")
    if gate.get("revision") != revision or gate.get("server_rollback_safe") is not True:
        raise RuntimeError("release lacks exact rollback compatibility evidence")
    receipt = validate_release_receipt(validated / "backend/release-receipt.json")
    if receipt["source_revision"] != revision:
        raise RuntimeError("validated server receipt differs from release revision")
    evidence_root = Path(config["evidence_root"])
    evidence_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    controller_root = Path(config["controller_root"])
    previous_controller_target = active_controller_target(controller_root)
    controller_activated = False
    durable = evidence_root / "releases" / revision
    transaction = evidence_root / "inflight" / revision
    if transaction.exists() or transaction.is_symlink():
        raise RuntimeError("production evidence transaction already exists")
    transaction.mkdir(parents=True, mode=0o700)
    manifests = transaction / "manifests"
    shutil.copytree(request_root / "manifests", manifests)
    receipt_root = transaction / "provider-receipts"
    receipt_root.mkdir(mode=0o700)
    provider_root = ensure_provider_state(
        state_root,
        server_revision=current["server"]["deployed_revision"],
        web_revision=current["web"]["deployed_revision"],
    )
    environment = provider_environment(
        config=config,
        state_root=provider_root,
        receipt_root=receipt_root,
        expected_revision=current["web"]["deployed_revision"],
    )
    server_stage = stage_server(validated, revision)
    server_script = repo / "ops/release/remote-atomic-release.sh"
    deploy_script = repo / "ops/release/deploy.sh"
    server_activated = False
    try:
        run(
            [
                str(server_script),
                str(server_stage),
                current["server"]["deployed_revision"],
            ]
        )
        server_activated = True
        write_revision(provider_root / "vane-deploy/deployed-vane.sha", revision)
        run([str(deploy_script), "frontend-aliyun", str(validated / "frontend"), revision], env=environment)
        run([str(deploy_script), "frontend-cloudflare", str(validated / "frontend"), revision], env=environment)
        run([str(deploy_script), "frontend-finalize", str(validated / "frontend"), revision], env=environment)
        write_json(
            transaction / "deploy-result.json",
            {
                "schema": "vane.production-deploy/v1",
                "revision": revision,
                "server": "native-systemd",
                "middleware": "compose-only-if-infra-digest-changed",
                "web": ["aliyun", "cloudflare"],
            },
        )
        markers = {
            "aliyun": public_marker(config["public_origin"], revision),
            "cloudflare": public_marker(config["cloudflare_origin"], revision),
        }
        write_json(transaction / "machine-verify.json", {
            "schema": "vane.production-verify/v1",
            "revision": revision,
            "ready": True,
            "markers": markers,
        })
        run_uat(config["uat_command"], revision, transaction / "uat.json")

        aliyun_receipt = receipt_root / "aliyun.sha"
        cloudflare_receipt = receipt_root / "cloudflare.sha"
        candidate = {
            "schema": "vane.current-release/v1",
            "monorepo_revision": revision,
            "server": {
                "tree_digest": gate["server_source_tree_sha256"],
                "artifact_digest": receipt["backend_archive_sha256"],
                "deployed_revision": revision,
            },
            "web": {
                "tree_digest": gate["web_tree_sha256"],
                "aliyun_receipt_digest": digest(aliyun_receipt),
                "cloudflare_receipt_digest": digest(cloudflare_receipt),
                "deployed_revision": revision,
            },
            "infra_manifest_digest": gate["infra_tree_sha256"],
            "controller_revision": revision,
        }
        write_json(transaction / "candidate-current-release.json", candidate)
        controller_archive = request_root / submission["controller_archive"]
        controller_target = stage_controller(
            archive=controller_archive,
            revision=revision,
            controller_root=controller_root,
        )
        write_json(
            transaction / "controller-activation.json",
            {
                "schema": "vane.controller-activation/v1",
                "from_revision": current["controller_revision"],
                "to_revision": revision,
                "archive_sha256": digest(controller_archive),
                "target": str(controller_target),
                "activation": "after-product-verify-before-next-request",
            },
        )
        signing_key = Path(os.environ.get("CREDENTIALS_DIRECTORY", "")) / "broker_signing_key"
        if signing_key.is_symlink() or not signing_key.is_file():
            raise RuntimeError("broker signing credential is unavailable")
        artifact_manifest = manifests / "artifact.json"
        deploy_manifest = write_signed_manifest(
            directory=manifests,
            stage="deploy",
            revision=revision,
            signer=config["signer"],
            signing_key=signing_key,
            evidence={
                "deploy-result.json": transaction / "deploy-result.json",
                "aliyun.sha": aliyun_receipt,
                "cloudflare.sha": cloudflare_receipt,
            },
            parent=artifact_manifest,
        )
        verify_manifest = write_signed_manifest(
            directory=manifests,
            stage="verify",
            revision=revision,
            signer=config["signer"],
            signing_key=signing_key,
            evidence={
                "machine-verify.json": transaction / "machine-verify.json",
                "uat.json": transaction / "uat.json",
            },
            parent=deploy_manifest,
        )
        finalize_manifest = write_signed_manifest(
            directory=manifests,
            stage="finalize",
            revision=revision,
            signer=config["signer"],
            signing_key=signing_key,
            evidence={
                "candidate-current-release.json": transaction / "candidate-current-release.json",
                "controller-activation.json": transaction / "controller-activation.json",
            },
            parent=verify_manifest,
        )
        chain = require_release_chain(
            finalize_manifest,
            revision,
            "finalize",
            DEFAULT_SIGNERS,
        )
        validate_current_release_transition(
            current_path=current_path,
            candidate_path=transaction / "candidate-current-release.json",
            expected_current_digest=expected_digest,
            receipt_path=validated / "backend/release-receipt.json",
            chain=chain,
            activation=True,
        )
        # Product mutation and UAT are complete. Persist recovery artifacts and
        # the old-controller-signed activation before exposing controller M.
        durable.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if durable.exists() or durable.is_symlink():
            raise RuntimeError("durable release evidence already exists")
        shutil.copytree(transaction, durable / "evidence")
        shutil.copytree(validated / "frontend", durable / "frontend")
        shutil.copytree(validated / "backend", durable / "backend")
        shutil.copytree(manifests, durable / "manifests")
        shutil.copy2(request_root / "submission.json", durable / "submission.json")
        shutil.copy2(controller_archive, durable / controller_archive.name)
        activate_controller(target=controller_target, controller_root=controller_root)
        controller_activated = True
        atomic_current_release(current_path, candidate, expected_digest)
        shutil.rmtree(transaction, ignore_errors=True)
        return {
            "ok": True,
            "stage": "finalize",
            "revision": revision,
            "current_digest": digest(current_path),
            "finalized_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    except BaseException as release_error:
        # Any failure before the one durable CAS restores both product
        # components and the previously active controller.  Failed evidence is
        # retained separately without blocking a clean retry of the same SHA.
        recovery_errors: list[BaseException] = []
        if current_path.is_file() and digest(current_path) == expected_digest:
            if controller_activated:
                try:
                    activate_controller(
                        target=previous_controller_target,
                        controller_root=controller_root,
                    )
                except BaseException as error:  # recovery must not hide evidence
                    recovery_errors.append(error)
            if server_activated:
                try:
                    restore(
                        repo=repo,
                        previous=current,
                        candidate_revision=revision,
                        provider_root=provider_root,
                        provider_env=environment,
                        evidence_root=evidence_root,
                    )
                except BaseException as error:  # recovery must not hide evidence
                    recovery_errors.append(error)
        try:
            preserve_failed_evidence(
                revision=revision,
                evidence_root=evidence_root,
                durable=durable,
                transaction=transaction,
            )
        except BaseException as error:
            recovery_errors.append(error)
        if recovery_errors:
            details = "; ".join(str(error) for error in recovery_errors)
            raise RuntimeError(
                f"production release failed and recovery was incomplete: {details}"
            ) from release_error
        raise


def main() -> int:
    if len(sys.argv) != 7:
        print(
            "usage: production_handler.py VERB REQUEST_ROOT VALIDATED_ROOT STATE_ROOT REPO EXPECTED_DIGEST",
            file=sys.stderr,
        )
        return 2
    _, verb, request_raw, validated_raw, state_raw, repo_raw, expected = sys.argv
    if verb != "release":
        raise RuntimeError(f"production handler verb is not implemented safely: {verb}")
    result = release(
        request_root=Path(request_raw),
        validated=Path(validated_raw),
        state_root=Path(state_raw),
        repo=Path(repo_raw),
        expected_digest=expected,
        config=load_config(),
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"production handler refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
