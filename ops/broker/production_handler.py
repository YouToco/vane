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

import base64
from datetime import datetime, timezone
import hashlib
import grp
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Any, Optional


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
    # The forced-command broker stdout is a machine-only JSON protocol.  Child
    # commands must never inherit it, even when a successful deploy script is
    # verbose, or the broker response becomes unparsable after mutation.
    result = subprocess.run(
        command,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"production command failed with exit {result.returncode}: "
            f"{command[0]}"
        )


def write_json(path: Path, value: dict) -> None:
    path.write_bytes(canonical(value))
    path.chmod(0o600)


def write_revision(path: Path, revision: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    temporary.write_text(revision + "\n", encoding="ascii")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def load_config() -> dict:
    path = Path("/etc/vane-broker/production.json")
    if os.environ.get("VANE_BROKER_HANDLER_TESTING") == "1" and os.geteuid() != 0:
        path = Path(os.environ.get("VANE_BROKER_HANDLER_CONFIG", ""))
    value = strict_json(path)
    expected = {
        "schema",
        "uat_command",
        "evidence_root",
        "signer",
        "controller_root",
        "state_reader_group",
        "uat_identity",
    }
    if set(value) != expected or value.get("schema") != "vane.production-handler/v1":
        raise RuntimeError("production handler configuration keys are not exact")
    for key in ("evidence_root", "controller_root"):
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
    group = value["state_reader_group"]
    if not isinstance(group, str) or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", group):
        raise RuntimeError("production handler state reader group is invalid")
    identity = value["uat_identity"]
    if (
        not isinstance(identity, dict)
        or set(identity) != {"user_id", "tenant_id"}
        or type(identity["user_id"]) is not int
        or type(identity["tenant_id"]) is not int
        or identity["user_id"] <= 0
        or identity["tenant_id"] <= 0
    ):
        raise RuntimeError("production handler UAT identity is invalid")
    return value


def ensure_provider_state(
    state_root: Path, *, server_revision: str, candidate_revision: Optional[str] = None
) -> Path:
    provider_root = state_root / "provider"
    state = provider_root / "vane-deploy"
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name, current_revision in (("deployed-vane.sha", server_revision),):
        path = state / name
        if path.exists():
            allowed = {current_revision}
            if candidate_revision is not None:
                allowed.add(candidate_revision)
            if path.is_symlink() or path.read_text(encoding="ascii").strip() not in allowed:
                raise RuntimeError(f"provider compatibility state differs from current-release: {name}")
        else:
            path.write_text(current_revision + "\n", encoding="ascii")
            path.chmod(0o600)
    return provider_root


def active_server_revision(
    current_link: Path = Path("/opt/vane/current"),
    release_root: Path = Path("/opt/vane/releases"),
) -> str:
    if not current_link.is_symlink():
        raise RuntimeError("active Server authority is not a symlink")
    target = os.readlink(current_link)
    match = re.fullmatch(re.escape(str(release_root)) + r"/([0-9a-f]{40})", target)
    if match is None:
        raise RuntimeError("active Server authority has an unsafe target")
    resolved = current_link.resolve(strict=True)
    if resolved.is_symlink() or not resolved.is_dir():
        raise RuntimeError("active Server release is unavailable")
    return match.group(1)


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


def postgres_query(sql: str) -> str:
    """Run one fixed local-container query without putting a DB secret in argv."""
    result = subprocess.run(
        [
            "/usr/bin/docker",
            "exec",
            "-i",
            "vane-postgres-1",
            "psql",
            "-X",
            "-U",
            "vane",
            "-d",
            "vane",
            "-v",
            "ON_ERROR_STOP=1",
            "-Atq",
        ],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("temporary production UAT session database operation failed")
    return result.stdout.strip()


def create_uat_session(identity: dict, *, token: Optional[str] = None) -> tuple[str, str]:
    token = token or base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(
        "ascii"
    ).rstrip("=")
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", token):
        raise RuntimeError("generated production UAT session token is invalid")
    token_hash = hashlib.sha256(token.encode("ascii")).hexdigest()
    sql = f"""
WITH eligible AS (
  SELECT 1
    FROM memberships m JOIN tenants t ON t.id=m.tenant_id
   WHERE m.user_id={identity['user_id']} AND m.tenant_id={identity['tenant_id']}
     AND m.role='owner' AND t.status='active' AND t.deleted_at IS NULL
), inserted AS (
  INSERT INTO user_sessions(token_hash,user_id,tenant_id,expires_at)
  SELECT decode('{token_hash}','hex'),{identity['user_id']},{identity['tenant_id']},
         now()+interval '10 minutes' FROM eligible RETURNING 1
)
SELECT count(*) FROM inserted;
"""
    if postgres_query(sql) != "1":
        raise RuntimeError("temporary production UAT owner session was not inserted exactly once")
    return token, token_hash


def revoke_uat_session(token_hash: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", token_hash):
        raise RuntimeError("temporary production UAT session hash is invalid")
    sql = f"""
WITH deleted AS (
  DELETE FROM user_sessions WHERE token_hash=decode('{token_hash}','hex') RETURNING 1
)
SELECT count(*) FROM deleted;
"""
    if postgres_query(sql) != "1":
        raise RuntimeError("temporary production UAT session was not revoked exactly once")


def run_uat(command: list[str], revision: str, output: Path, identity: dict) -> dict:
    executable = Path(command[0])
    if executable.is_symlink() or not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError("fixed production UAT command is unavailable")
    token, token_hash = create_uat_session(identity)
    try:
        with tempfile.TemporaryDirectory(prefix="vane-production-uat-") as directory:
            credential_path = Path(directory) / "uat_session_cookie"
            credential_path.write_text(token + "\n", encoding="ascii")
            credential_path.chmod(0o600)
            environment = os.environ.copy()
            environment["CREDENTIALS_DIRECTORY"] = directory
            result = subprocess.run(
                [*command, "--sha", revision],
                text=True,
                capture_output=True,
                check=False,
                env=environment,
            )
            if result.returncode != 0:
                detail = result.stderr.strip()
                if len(detail) > 2000:
                    detail = detail[-2000:]
                suffix = f": {detail}" if detail else ""
                raise RuntimeError(
                    f"production UAT failed with exit {result.returncode}{suffix}"
                )
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
    finally:
        revoke_uat_session(token_hash)


def restore(
    *,
    repo: Path,
    previous: dict,
    candidate_revision: str,
    provider_root: Path,
) -> None:
    rollback = repo / "ops/rollback/switch-server-release.sh"
    run([str(rollback), previous["server"]["deployed_revision"], candidate_revision])
    verify_active_server(previous["server"]["deployed_revision"])
    write_revision(
        provider_root / "vane-deploy/deployed-vane.sha",
        previous["server"]["deployed_revision"],
    )


def verify_active_server(revision: str) -> None:
    if active_server_revision() != revision:
        raise RuntimeError("active Server revision differs after recovery")
    for unit in (
        "vane.service",
        "vane-research-gateway.socket",
        "vane-research-gateway.service",
    ):
        # systemctl's aggregate exit status for multiple units is successful
        # when any unit is active. Probe each authority separately so one dead
        # companion cannot masquerade as complete recovery.
        run(["/usr/bin/systemctl", "is-active", "--quiet", unit])
    run(
        [
            "/usr/bin/curl",
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "5",
            "http://127.0.0.1:8080/readyz",
        ]
    )
    pid = subprocess.run(
        ["/usr/bin/systemctl", "show", "vane.service", "--property=MainPID", "--value"],
        text=True,
        capture_output=True,
        check=False,
    )
    value = pid.stdout.strip()
    if pid.returncode != 0 or not re.fullmatch(r"[1-9][0-9]*", value):
        raise RuntimeError("active Server process ID is unavailable after recovery")
    executable = Path(f"/proc/{value}/exe")
    if os.readlink(executable) != f"/opt/vane/releases/{revision}/bin/vane":
        raise RuntimeError("active Server process is not bound to the recovered revision")


def reconcile_server_before_release(
    *, repo: Path, current: dict, candidate_revision: str, provider_root: Path
) -> None:
    expected = current["server"]["deployed_revision"]
    provider_revision_path = provider_root / "vane-deploy/deployed-vane.sha"
    actual = active_server_revision()
    provider_revision = provider_revision_path.read_text(encoding="ascii").strip()
    if actual == candidate_revision or provider_revision == candidate_revision:
        # A previous process died after the product link moved but before the
        # durable CAS. Restore N first, then restart the immutable request.
        restore(
            repo=repo,
            previous=current,
            candidate_revision=candidate_revision,
            provider_root=provider_root,
        )
    elif actual != expected:
        raise RuntimeError("active Server differs from both current state and candidate")


def atomic_current_release(
    path: Path,
    value: dict,
    expected_digest: str,
    *,
    reader_group: Optional[str] = None,
) -> None:
    if digest(path) != expected_digest:
        raise RuntimeError("current-release CAS changed before finalize")
    temporary = path.with_name(f".current-release.{os.getpid()}.json")
    with temporary.open("xb") as handle:
        handle.write(canonical(value))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o640 if reader_group else 0o600)
    if reader_group:
        group_id = grp.getgrnam(reader_group).gr_gid
        os.chown(temporary, 0, group_id)
    os.replace(temporary, path)
    directory = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def make_tree_durable(root: Path) -> None:
    """Persist every extracted controller byte before publishing its dirname."""
    directories = [root]
    for path in root.rglob("*"):
        if path.is_symlink():
            raise RuntimeError("controller durability walk found a symlink")
        if path.is_file():
            descriptor = os.open(str(path), os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        elif path.is_dir():
            directories.append(path)
        else:
            raise RuntimeError("controller durability walk found an unsafe member")
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        fsync_directory(directory)


def stage_controller(*, archive: Path, revision: str, controller_root: Path) -> Path:
    controller_root.mkdir(parents=True, exist_ok=True, mode=0o755)
    if controller_root.is_symlink() or not controller_root.is_dir():
        raise RuntimeError("controller root is unsafe")
    controller_root.chmod(0o755)
    releases = controller_root / "releases"
    releases.mkdir(parents=True, exist_ok=True, mode=0o755)
    if releases.is_symlink() or not releases.is_dir():
        raise RuntimeError("controller release root is unsafe")
    releases.chmod(0o755)
    target = releases / revision
    marker_name = ".controller-archive.sha256"
    archive_digest = digest(archive)
    if target.is_dir() and not target.is_symlink():
        marker = target / marker_name
        if marker.is_file() and not marker.is_symlink() and marker.read_text(encoding="ascii") == archive_digest + "\n":
            make_tree_durable(target)
            fsync_directory(releases)
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
        required_files = (
            "ops/bin/vane",
            "ops/broker/forced_command.py",
            "ops/broker/production_handler.py",
            "ops/broker/promote_finalized_controller.py",
            "ops/broker/run-production-handler.sh",
            "ops/audit/production-uat.py",
            "ops/release/artifact.py",
            "ops/release/remote-atomic-release.sh",
            "ops/rollback/switch-server-release.sh",
            "tools/toolchain.lock.json",
            "server/go.mod",
            "server/internal/testgate/cmd/testpolicyscan/main.go",
        )
        required_executables = {
            "ops/bin/vane",
            "ops/broker/forced_command.py",
            "ops/broker/production_handler.py",
            "ops/broker/promote_finalized_controller.py",
            "ops/broker/run-production-handler.sh",
            "ops/audit/production-uat.py",
            "ops/release/artifact.py",
            "ops/release/remote-atomic-release.sh",
            "ops/rollback/switch-server-release.sh",
        }
        for required in required_files:
            path = pending / required
            if (
                path.is_symlink()
                or not path.is_file()
                or (required in required_executables and not os.access(path, os.X_OK))
            ):
                raise RuntimeError(f"controller archive lacks required member: {required}")
        (pending / marker_name).write_text(archive_digest + "\n", encoding="ascii")
        (pending / marker_name).chmod(0o600)
        pending.chmod(0o755)
        make_tree_durable(pending)
        os.replace(pending, target)
        fsync_directory(releases)
    except BaseException:
        shutil.rmtree(pending, ignore_errors=True)
        raise
    return target


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


def active_controller_revision_for_release(
    *, current: dict, controller_root: Path, evidence_root: Path
) -> str:
    """Require normal delayed authority or the exact one-time bootstrap pair."""
    active_controller = active_controller_target(controller_root)
    active_revision = active_controller.name
    if re.fullmatch(r"[0-9a-f]{40}", active_revision) is None:
        raise RuntimeError("active controller revision is invalid")
    if active_revision == current["controller_revision"]:
        from ops.broker.promote_finalized_controller import (  # pylint: disable=import-outside-toplevel
            bootstrap_authorizes_active,
        )

        if bootstrap_authorizes_active(
            evidence_root=evidence_root,
            product_revision=current["monorepo_revision"],
            controller_revision=current["controller_revision"],
            marker=active_controller / ".controller-archive.sha256",
        ):
            return active_revision
    finalized_product_controller = (
        controller_root / "releases" / current["monorepo_revision"]
    )
    required = (
        current["monorepo_revision"]
        if finalized_product_controller.is_dir()
        and not finalized_product_controller.is_symlink()
        else current["controller_revision"]
    )
    if active_revision != required:
        raise RuntimeError("active controller is not the eligible finalized revision")
    return active_revision


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
        revision
        + "-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        + "-"
        + secrets.token_hex(4)
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
    active_controller_revision = active_controller_revision_for_release(
        current=current,
        controller_root=controller_root,
        evidence_root=evidence_root,
    )
    durable = evidence_root / "releases" / revision
    transaction = evidence_root / "inflight" / revision
    if revision == current["monorepo_revision"]:
        if durable.is_symlink() or not durable.is_dir():
            raise RuntimeError("current release lacks durable idempotency evidence")
        durable_submission = durable / "submission.json"
        durable_receipt = durable / "backend/release-receipt.json"
        durable_finalize = durable / "manifests/finalize.json"
        if (
            durable_submission.is_symlink()
            or not durable_submission.is_file()
            or digest(durable_submission) != digest(request_root / "submission.json")
            or durable_receipt.is_symlink()
            or not durable_receipt.is_file()
            or digest(durable_receipt)
            != digest(validated / "backend/release-receipt.json")
            or current["server"]["artifact_digest"]
            != validate_release_receipt(durable_receipt)["backend_archive_sha256"]
        ):
            raise RuntimeError("current release differs from the immutable retry request")
        require_release_chain(
            durable_finalize,
            revision,
            "finalize",
            DEFAULT_SIGNERS,
        )
        provider_root = ensure_provider_state(
            state_root,
            server_revision=revision,
        )
        verify_active_server(revision)
        return {
            "ok": True,
            "stage": "finalize",
            "status": "already-current",
            "revision": revision,
            "current_digest": digest(current_path),
        }
    if durable.is_symlink() or transaction.is_symlink():
        raise RuntimeError("production evidence transaction is unsafe")
    if durable.exists():
        # A kill after durable evidence copy but before the one CAS leaves a
        # complete-looking release tree that is not authoritative. Archive it
        # before reconciling the product link and restarting the request.
        preserve_failed_evidence(
            revision=revision,
            evidence_root=evidence_root,
            durable=durable,
            transaction=transaction,
        )
    if transaction.exists():
        # A process can fail after creating the transaction but before the
        # main recovery try block (older controllers did this during provider
        # compatibility admission).  Preserve that evidence and restart the
        # same immutable request from pre-deploy admission.
        preserve_failed_evidence(
            revision=revision,
            evidence_root=evidence_root,
            durable=transaction,
            transaction=transaction,
        )
    transaction.mkdir(parents=True, mode=0o700)
    server_script = repo / "ops/release/remote-atomic-release.sh"
    server_may_have_changed = False
    provider_root: Optional[Path] = None
    try:
        manifests = transaction / "manifests"
        shutil.copytree(request_root / "manifests", manifests)
        provider_root = ensure_provider_state(
            state_root,
            server_revision=current["server"]["deployed_revision"],
            candidate_revision=revision,
        )
        reconcile_server_before_release(
            repo=repo,
            current=current,
            candidate_revision=revision,
            provider_root=provider_root,
        )
        server_stage = stage_server(validated, revision)
        server_may_have_changed = True
        run(
            [
                str(server_script),
                str(server_stage),
                current["server"]["deployed_revision"],
            ]
        )
        write_revision(provider_root / "vane-deploy/deployed-vane.sha", revision)
        write_json(
            transaction / "deploy-result.json",
            {
                "schema": "vane.production-deploy/v1",
                "revision": revision,
                "server": "native-systemd",
                "middleware": "compose-only-if-infra-digest-changed",
                "web": "published-locally-after-server-finalize",
            },
        )
        write_json(transaction / "machine-verify.json", {
            "schema": "vane.production-verify/v1",
            "revision": revision,
            "ready": True,
            "server": "ready-and-exact-process-bound",
        })
        run_uat(
            config["uat_command"],
            revision,
            transaction / "uat.json",
            config["uat_identity"],
        )

        infra_manifest = Path(f"/opt/vane/releases/{revision}/infra-manifest.sha256")
        if infra_manifest.is_symlink() or not infra_manifest.is_file():
            raise RuntimeError("activated Server release lacks its bound infra manifest")

        candidate = {
            "schema": "vane.current-release/v2",
            "monorepo_revision": revision,
            "server": {
                "tree_digest": gate["server_source_tree_sha256"],
                "artifact_digest": receipt["backend_archive_sha256"],
                "deployed_revision": revision,
            },
            "infra_manifest_digest": digest(infra_manifest),
            # Controller M is staged now but cannot authorize M.  The active
            # controller is the already-finalized product N and advances only
            # when the following product release begins.
            "controller_revision": active_controller_revision,
        }
        write_json(transaction / "candidate-current-release.json", candidate)
        controller_archive = request_root / submission["controller_archive"]
        controller_target = stage_controller(
            archive=controller_archive,
            revision=revision,
            controller_root=controller_root,
        )
        write_json(
            transaction / "controller-staging.json",
            {
                "schema": "vane.controller-staging/v1",
                "active_revision": active_controller_revision,
                "candidate_revision": revision,
                "archive_sha256": digest(controller_archive),
                "target": str(controller_target),
                "activation": "at-start-of-following-product-release",
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
                "controller-staging.json": transaction / "controller-staging.json",
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
        # Product mutation and UAT are complete. Persist the candidate
        # controller as data; it is not exposed during its own product release.
        durable.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if durable.exists() or durable.is_symlink():
            raise RuntimeError("durable release evidence already exists")
        shutil.copytree(transaction, durable / "evidence")
        shutil.copytree(validated / "backend", durable / "backend")
        shutil.copytree(manifests, durable / "manifests")
        shutil.copy2(request_root / "submission.json", durable / "submission.json")
        shutil.copy2(controller_archive, durable / controller_archive.name)
        atomic_current_release(
            current_path,
            candidate,
            expected_digest,
            reader_group=config["state_reader_group"],
        )
        shutil.rmtree(transaction, ignore_errors=True)
        return {
            "ok": True,
            "stage": "finalize",
            "revision": revision,
            "current_digest": digest(current_path),
            "finalized_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    except BaseException as release_error:
        # Any failure before the one durable CAS restores the product release.
        # The candidate controller was only staged, never activated. Failed
        # evidence is retained without blocking a clean retry of the same SHA.
        recovery_errors: list[BaseException] = []
        if current_path.is_file() and digest(current_path) == expected_digest:
            if server_may_have_changed:
                try:
                    if provider_root is None:
                        raise RuntimeError("provider recovery authority is unavailable")
                    actual_server_revision = active_server_revision()
                    if actual_server_revision in {
                        revision,
                        current["server"]["deployed_revision"],
                    }:
                        restore(
                            repo=repo,
                            previous=current,
                            candidate_revision=revision,
                            provider_root=provider_root,
                        )
                    else:
                        raise RuntimeError("Server recovery found an unknown active revision")
                except BaseException as error:  # recovery must not hide evidence
                    recovery_errors.append(error)
        try:
            if transaction.is_dir() and not transaction.is_symlink():
                write_json(
                    transaction / "failure.json",
                    {
                        "schema": "vane.production-failure/v1",
                        "revision": revision,
                        "error": str(release_error)[-4000:],
                        "recovery_errors": [str(error)[-2000:] for error in recovery_errors],
                    },
                )
        except BaseException as error:
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
    if verb not in {"release", "retry"}:
        raise RuntimeError(f"production handler verb is not implemented safely: {verb}")
    # Retry deliberately restarts the immutable, content-addressed request from
    # pre-deploy admission. It never resumes from caller-selected loose files or
    # an unverified intermediate stage.
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
