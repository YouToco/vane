"""The single repository-side Vane operations CLI.

This command never reads production credentials and never mutates production.
Production operations are submitted to the separately installed, root-owned
broker only after this CLI has verified an exact revision and signed manifest
chain. The repository contains the audited forced-command broker source, while
its root-owned installed copy and production handlers remain outside checkout.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import shutil
import stat
import subprocess
import sys
import tarfile
from typing import Any

from ops.broker import submit as broker_client


ROOT = Path(__file__).resolve().parents[2]
OPS_ROOT = ROOT / "ops"
DEFAULT_LOCK = ROOT / "tools" / "toolchain.lock.json"
DEFAULT_POLICY = OPS_ROOT / "policy" / "release-policy.json"
DEFAULT_SIGNERS = Path(
    os.environ.get("VANE_ALLOWED_SIGNERS", str(OPS_ROOT / "policy" / "allowed_signers"))
)
DEFAULT_EVIDENCE = ROOT / ".vane" / "evidence"
MANIFEST_SCHEMA = "vane.ops-manifest/v1"
STAGES = ("plan", "gate", "artifact", "deploy", "verify", "finalize")
EXACT_SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
BROKER_HOST = "188.253.125.238"
BROKER_PORT = 10058
BROKER_KNOWN_HOSTS_SHA256 = "755558dd29ed29400289842f639f31856f12eb0c25654a6d9007fd572163e5d3"
GATE_ENV_ALLOWLIST = {
    "ALL_PROXY", "DOCKER_CONTEXT", "DOCKER_HOST", "GOCACHE", "GOMODCACHE",
    "GOPATH", "GOPROXY", "GOROOT", "GOSUMDB", "GOTOOLCHAIN", "HTTP_PROXY",
    "HTTPS_PROXY", "LANG", "LC_ALL", "LC_CTYPE", "LOGNAME", "NO_PROXY",
    "PATH", "SHELL", "SSL_CERT_DIR", "SSL_CERT_FILE", "TERM", "TMPDIR",
    "TMP", "TEMP", "USER", "VANE_TOOL_CACHE",
}
CREATED_AT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
EXIT_POLICY = 78
RELEASE_RECEIPT_SCHEMA = "vane.release-receipt/v1"
CURRENT_RELEASE_SCHEMA = "vane.current-release/v2"
RELEASE_RECEIPT_KEYS = {
    "schema_version",
    "source_revision",
    "control_plane_revision",
    "deploy_run_id",
    "build_run_attempt",
    "backend_archive_sha256",
    "backend_manifest_sha256",
    "server_release_contract_sha256",
    "vane_sha256",
    "agentfirstretention_sha256",
}


class PolicyError(RuntimeError):
    """A fail-closed policy rejection."""


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PolicyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Any:
    if path.is_symlink() or not path.is_file():
        raise PolicyError(f"not a regular file: {path}")
    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise PolicyError(f"invalid JSON file {path}: {error}") from error


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def directory_tree_sha256(root: Path) -> str:
    """Hash a directory as canonical path/content/size/mode records."""
    if root.is_symlink() or not root.is_dir():
        raise PolicyError(f"artifact tree is missing or unsafe: {root}")
    entries: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise PolicyError(f"artifact tree contains an unsafe member: {path}")
        if path.is_dir():
            continue
        relative = path.relative_to(root).as_posix()
        entries.append(
            {
                "path": relative,
                "sha256": sha256_file(path),
                "size": path.stat().st_size,
                "mode": path.stat().st_mode & 0o777,
            }
        )
    if not entries:
        raise PolicyError(f"artifact tree is empty: {root}")
    return hashlib.sha256(canonical_json({"schema": "vane.directory-tree/v1", "files": entries})).hexdigest()


CONTROL_PLANE_PATHS = (
    "ops", "contracts", "infra", "tools", "server/go.mod", "server/go.sum",
    "server/internal/testgate",
)


def committed_files(
    revision: str, pathspecs: tuple[str, ...], *, repository: Path = ROOT
) -> list[tuple[Path, int, bytes]]:
    result = subprocess.run(
        ["git", "archive", "--format=tar", revision, "--", *pathspecs],
        cwd=repository,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError("cannot export exact committed release files")
    files: list[tuple[Path, int, bytes]] = []
    seen: set[str] = set()
    with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:") as archive:
        for member in archive.getmembers():
            relative = PurePosixPath(member.name)
            if (
                relative.is_absolute()
                or any(part in {"", ".", ".."} for part in relative.parts)
                or member.issym()
                or member.islnk()
            ):
                raise PolicyError("committed release export contains an unsafe member")
            if member.isdir():
                continue
            if not member.isfile() or member.name in seen:
                raise PolicyError("committed release export member is invalid")
            seen.add(member.name)
            source = archive.extractfile(member)
            if source is None:
                raise PolicyError("cannot read committed release export member")
            mode = 0o755 if member.mode & 0o111 else 0o644
            files.append((Path(member.name), mode, source.read()))
    if not files:
        raise PolicyError("committed release export is empty")
    return sorted(files, key=lambda value: value[0].as_posix())


def export_committed_files(
    revision: str, destination: Path, pathspecs: tuple[str, ...]
) -> list[Path]:
    files = committed_files(revision, pathspecs)
    for relative, mode, payload in files:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        target.write_bytes(payload)
        target.chmod(mode)
    return [relative for relative, _, _ in files]


def write_control_plane_archive(output: Path, revision: str | None = None) -> str:
    exact_revision = revision or git_revision("HEAD")
    with output.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                for relative, mode, payload in committed_files(
                    exact_revision, CONTROL_PLANE_PATHS
                ):
                    info = tarfile.TarInfo(relative.as_posix())
                    info.uid = 0
                    info.gid = 0
                    info.uname = "root"
                    info.gname = "root"
                    info.mtime = 0
                    info.mode = mode
                    info.size = len(payload)
                    archive.addfile(info, io.BytesIO(payload))
    return sha256_file(output)


def exact_sha(value: str) -> str:
    if not EXACT_SHA.fullmatch(value):
        raise argparse.ArgumentTypeError("must be an exact lowercase 40-character SHA")
    return value


def digest_value(value: str, *, field: str) -> str:
    if not DIGEST.fullmatch(value):
        raise PolicyError(f"{field} must be an exact lowercase SHA-256")
    return value


def git_revision(ref: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    revision = result.stdout.strip()
    if result.returncode != 0 or not EXACT_SHA.fullmatch(revision):
        raise PolicyError(f"cannot resolve exact commit for ref: {ref}")
    return revision


def assert_origin_main(revision: str) -> None:
    remote_main = git_revision("refs/remotes/origin/main")
    if revision != remote_main:
        raise PolicyError(
            f"release revision is not exact origin/main: requested={revision} "
            f"origin/main={remote_main}"
        )


def unresolved_values(value: Any, path: str = "") -> list[str]:
    unresolved: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            unresolved.extend(unresolved_values(child, f"{path}.{key}" if path else key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            unresolved.extend(unresolved_values(child, f"{path}[{index}]"))
    elif value == "UNRESOLVED":
        unresolved.append(path)
    return unresolved


def validate_toolchain(lock_path: Path, policy_path: Path) -> list[str]:
    lock = load_json(lock_path)
    policy = load_json(policy_path)
    errors: list[str] = []
    if not isinstance(lock, dict) or lock.get("schema") != "vane.toolchain-lock/v1":
        errors.append("toolchain lock schema is not vane.toolchain-lock/v1")
        return errors
    if not isinstance(policy, dict) or policy.get("schema") != "vane.release-policy/v1":
        errors.append("release policy schema is not vane.release-policy/v1")
        return errors
    tools = lock.get("tools")
    expected = policy.get("required_tool_versions")
    if not isinstance(tools, dict) or not isinstance(expected, dict):
        errors.append("toolchain lock or release policy has no tool version map")
        return errors
    for name, version in expected.items():
        entry = tools.get(name)
        if not isinstance(entry, dict):
            errors.append(f"required tool is missing: {name}")
        elif entry.get("version") != version:
            errors.append(
                f"tool version mismatch for {name}: expected={version} "
                f"actual={entry.get('version')!r}"
            )
    for field in unresolved_values(lock):
        errors.append(f"unresolved toolchain integrity value: {field}")
    return errors


def validate_release_receipt(path: Path) -> dict[str, Any]:
    receipt = load_json(path)
    if path.stat().st_size > 64 * 1024:
        raise PolicyError(f"release receipt is oversized: {path}")
    if not isinstance(receipt, dict) or set(receipt) != RELEASE_RECEIPT_KEYS:
        actual = sorted(receipt) if isinstance(receipt, dict) else type(receipt).__name__
        raise PolicyError(f"release receipt keys are not exact: {actual}")
    if receipt["schema_version"] != RELEASE_RECEIPT_SCHEMA:
        raise PolicyError("unsupported release receipt schema")
    for field in ("source_revision", "control_plane_revision"):
        value = receipt[field]
        if not isinstance(value, str) or not EXACT_SHA.fullmatch(value):
            raise PolicyError(f"release receipt {field} is not an exact SHA")
    run_id = receipt["deploy_run_id"]
    if (
        not isinstance(run_id, str)
        or not run_id.isascii()
        or not run_id.isdigit()
        or not run_id
    ):
        raise PolicyError("release receipt deploy_run_id is invalid")
    attempt = receipt["build_run_attempt"]
    if type(attempt) is not int or attempt <= 0:
        raise PolicyError("release receipt build_run_attempt is invalid")
    for field in RELEASE_RECEIPT_KEYS - {
        "schema_version",
        "source_revision",
        "control_plane_revision",
        "deploy_run_id",
        "build_run_attempt",
    }:
        value = receipt[field]
        if not isinstance(value, str):
            raise PolicyError(f"release receipt {field} is not a string")
        digest_value(value, field=f"release receipt {field}")
    return receipt


def validate_current_release(path: Path) -> dict[str, Any]:
    release = load_json(path)
    if path.stat().st_size > 64 * 1024:
        raise PolicyError(f"current-release document is oversized: {path}")
    root_keys = {
        "schema",
        "monorepo_revision",
        "server",
        "infra_manifest_digest",
        "controller_revision",
    }
    if not isinstance(release, dict) or set(release) != root_keys:
        actual = sorted(release) if isinstance(release, dict) else type(release).__name__
        raise PolicyError(f"current-release keys are not exact: {actual}")
    if release["schema"] != CURRENT_RELEASE_SCHEMA:
        raise PolicyError("unsupported current-release schema")
    for field in ("monorepo_revision", "controller_revision"):
        value = release[field]
        if not isinstance(value, str) or not EXACT_SHA.fullmatch(value):
            raise PolicyError(f"current-release {field} is not an exact SHA")
    if not isinstance(release["infra_manifest_digest"], str):
        raise PolicyError("current-release infra_manifest_digest is not a string")
    digest_value(
        release["infra_manifest_digest"], field="current-release infra_manifest_digest"
    )
    component_keys = {
        "server": {"tree_digest", "artifact_digest", "deployed_revision"},
    }
    for component, expected_keys in component_keys.items():
        value = release[component]
        if not isinstance(value, dict) or set(value) != expected_keys:
            actual = sorted(value) if isinstance(value, dict) else type(value).__name__
            raise PolicyError(f"current-release {component} keys are not exact: {actual}")
        revision = value["deployed_revision"]
        if not isinstance(revision, str) or not EXACT_SHA.fullmatch(revision):
            raise PolicyError(
                f"current-release {component}.deployed_revision is not an exact SHA"
            )
        for field in expected_keys - {"deployed_revision"}:
            digest = value[field]
            if not isinstance(digest, str):
                raise PolicyError(f"current-release {component}.{field} is not a string")
            digest_value(digest, field=f"current-release {component}.{field}")
    return release


def evidence_digest(
    chain: list[tuple[Path, dict[str, Any]]], stage: str, name: str
) -> str:
    matches = [
        item["sha256"]
        for _, manifest in chain
        if manifest["stage"] == stage
        for item in manifest["evidence"]
        if item["name"] == name
    ]
    if len(matches) != 1:
        raise PolicyError(f"{stage} manifest must bind exactly one {name} evidence")
    return matches[0]


def validate_current_release_transition(
    *,
    current_path: Path,
    candidate_path: Path,
    expected_current_digest: str,
    receipt_path: Path,
    chain: list[tuple[Path, dict[str, Any]]],
    activation: bool,
) -> dict[str, Any]:
    digest_value(expected_current_digest, field="expected current-release digest")
    actual_current_digest = sha256_file(current_path)
    if actual_current_digest != expected_current_digest:
        raise PolicyError(
            "current-release CAS mismatch: "
            f"expected={expected_current_digest} actual={actual_current_digest}"
        )
    current = validate_current_release(current_path)
    candidate = validate_current_release(candidate_path)
    receipt = validate_release_receipt(receipt_path)
    revision = chain[-1][1]["revision"]
    if current["monorepo_revision"] == candidate["monorepo_revision"]:
        raise PolicyError("current-release transition does not advance N to N+1")
    if candidate["monorepo_revision"] != revision:
        raise PolicyError("candidate current-release differs from manifest revision")
    if receipt["control_plane_revision"] != revision:
        raise PolicyError("release receipt candidate controller revision differs from manifest")
    if candidate["controller_revision"] not in {
        current["monorepo_revision"],
        current["controller_revision"],
    } or candidate["controller_revision"] == revision:
        raise PolicyError(
            "candidate active controller is not an eligible previously finalized revision"
        )
    if candidate["server"]["deployed_revision"] != receipt["source_revision"]:
        raise PolicyError("candidate server revision differs from release receipt")
    if candidate["server"]["artifact_digest"] != receipt["backend_archive_sha256"]:
        raise PolicyError("candidate server artifact differs from release receipt")
    receipt_digest = sha256_file(receipt_path)
    if evidence_digest(chain, "artifact", "release-receipt.json") != receipt_digest:
        raise PolicyError("artifact manifest does not bind the exact release receipt")
    if activation and chain[-1][1]["stage"] != "finalize":
        raise PolicyError("current-release activation requires the finalized N+1 chain")
    return {
        "current_digest": actual_current_digest,
        "candidate_digest": sha256_file(candidate_path),
        "current_revision": current["monorepo_revision"],
        "candidate_revision": candidate["monorepo_revision"],
        "receipt_digest": receipt_digest,
    }


def signer_entries(path: Path) -> list[str]:
    if path.is_symlink() or not path.is_file():
        return []
    return [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def validate_manifest_shape(manifest: Any, path: Path) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        raise PolicyError(f"manifest must be an object: {path}")
    required = {"schema", "stage", "revision", "created_at", "parent", "signer", "evidence"}
    if set(manifest) != required:
        raise PolicyError(
            f"manifest keys are not exact at {path}: "
            f"expected={sorted(required)} actual={sorted(manifest)}"
        )
    if manifest["schema"] != MANIFEST_SCHEMA:
        raise PolicyError(f"unsupported manifest schema: {path}")
    stage = manifest["stage"]
    if stage not in STAGES:
        raise PolicyError(f"unsupported manifest stage at {path}: {stage!r}")
    revision = manifest["revision"]
    if not isinstance(revision, str) or not EXACT_SHA.fullmatch(revision):
        raise PolicyError(f"manifest revision is not an exact SHA: {path}")
    if not isinstance(manifest["created_at"], str) or not CREATED_AT.fullmatch(
        manifest["created_at"]
    ):
        raise PolicyError(f"manifest created_at is not canonical UTC: {path}")
    if not isinstance(manifest["signer"], str) or not manifest["signer"].strip():
        raise PolicyError(f"manifest signer is empty: {path}")
    evidence = manifest["evidence"]
    if not isinstance(evidence, list):
        raise PolicyError(f"manifest evidence must be a list: {path}")
    seen_names: set[str] = set()
    for item in evidence:
        if not isinstance(item, dict) or set(item) != {"name", "sha256"}:
            raise PolicyError(f"manifest evidence entry is not exact: {path}")
        name = item["name"]
        if not isinstance(name, str) or not name or name in seen_names:
            raise PolicyError(f"manifest evidence name is empty or duplicated: {path}")
        seen_names.add(name)
        if not isinstance(item["sha256"], str):
            raise PolicyError(f"manifest evidence digest is not a string: {path}")
        digest_value(item["sha256"], field=f"{path}:{name}")
    return manifest


def safe_parent_path(current: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if pure.is_absolute() or not pure.parts or any(part in ("", ".", "..") for part in pure.parts):
        raise PolicyError(f"unsafe parent manifest path: {relative!r}")
    candidate = current.parent.joinpath(*pure.parts)
    evidence_root = current.parent.resolve()
    resolved = candidate.resolve()
    if resolved.parent != evidence_root:
        raise PolicyError("parent manifest must be a sibling in the same evidence directory")
    return resolved


def verify_signature(path: Path, signer: str, allowed_signers: Path) -> None:
    signature = path.with_name(path.name + ".sig")
    if signature.is_symlink() or not signature.is_file():
        raise PolicyError(f"detached manifest signature is missing: {signature}")
    if not signer_entries(allowed_signers):
        raise PolicyError(f"allowed signer policy has no trusted keys: {allowed_signers}")
    result = subprocess.run(
        [
            "ssh-keygen",
            "-Y",
            "verify",
            "-f",
            str(allowed_signers),
            "-I",
            signer,
            "-n",
            "vane-release",
            "-s",
            str(signature),
        ],
        input=path.read_bytes(),
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError(f"manifest signature verification failed: {path}")


def load_manifest_chain(
    path: Path,
    *,
    allowed_signers: Path,
    require_signatures: bool,
) -> list[tuple[Path, dict[str, Any]]]:
    chain: list[tuple[Path, dict[str, Any]]] = []
    visited: set[Path] = set()
    current = path.resolve()
    while True:
        if current in visited:
            raise PolicyError("manifest chain contains a cycle")
        visited.add(current)
        manifest = validate_manifest_shape(load_json(current), current)
        if require_signatures:
            verify_signature(current, manifest["signer"], allowed_signers)
        chain.append((current, manifest))
        parent = manifest["parent"]
        if parent is None:
            break
        if not isinstance(parent, dict) or set(parent) != {"path", "sha256", "stage"}:
            raise PolicyError(f"manifest parent link is not exact: {current}")
        if not isinstance(parent["path"], str) or not isinstance(parent["sha256"], str):
            raise PolicyError(f"manifest parent path/digest has wrong type: {current}")
        digest_value(parent["sha256"], field=f"{current}:parent.sha256")
        parent_path = safe_parent_path(current, parent["path"])
        if sha256_file(parent_path) != parent["sha256"]:
            raise PolicyError(f"manifest parent digest mismatch: {parent_path}")
        expected_parent_stage = STAGES[STAGES.index(manifest["stage"]) - 1]
        if STAGES.index(manifest["stage"]) == 0 or parent["stage"] != expected_parent_stage:
            raise PolicyError(f"manifest chain skips or reverses a stage: {current}")
        current = parent_path
    chain.reverse()
    stages = [manifest["stage"] for _, manifest in chain]
    expected = list(STAGES[: len(stages)])
    if stages != expected:
        raise PolicyError(f"manifest chain is incomplete: expected={expected} actual={stages}")
    revisions = {manifest["revision"] for _, manifest in chain}
    if len(revisions) != 1:
        raise PolicyError("manifest chain changes revision")
    return chain


def run_checked(command: list[str], *, cwd: Path) -> None:
    print("+", " ".join(command), flush=True)
    result = subprocess.run(command, cwd=cwd, check=False)
    if result.returncode != 0:
        raise PolicyError(f"local check failed with exit {result.returncode}: {' '.join(command)}")


def run_go_tests_json(
    command: list[str], *, cwd: Path, fail_on_skips: bool
) -> list[str]:
    print("+", " ".join(command), flush=True)
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        raise PolicyError(f"Go test gate failed with exit {result.returncode}")
    skipped: list[str] = []
    events = 0
    for line_number, line in enumerate(result.stdout.splitlines(), start=1):
        try:
            event = json.loads(line, object_pairs_hook=reject_duplicate_keys)
        except (json.JSONDecodeError, PolicyError) as error:
            raise PolicyError(
                f"go test -json emitted invalid JSON at line {line_number}: {error}"
            ) from error
        if not isinstance(event, dict):
            raise PolicyError(f"go test -json event is not an object at line {line_number}")
        events += 1
        if event.get("Action") == "skip":
            test_name = event.get("Test")
            if not isinstance(test_name, str) or not test_name:
                test_name = "<package-level>"
            skipped.append(f"{event.get('Package', '<unknown>')}::{test_name}")
    if events == 0:
        raise PolicyError("Go test gate produced no machine-readable events")
    if skipped and fail_on_skips:
        raise PolicyError("Go test gate observed skipped tests: " + ", ".join(skipped))
    return skipped


def run_go_tests_no_skips(command: list[str], *, cwd: Path) -> None:
    run_go_tests_json(command, cwd=cwd, fail_on_skips=True)


def command_doctor(args: argparse.Namespace) -> int:
    errors = validate_toolchain(args.lock, args.policy)
    checker = ROOT / "tools" / "checks" / "toolchain.py"
    tool_cache = Path(
        os.environ.get("VANE_TOOL_CACHE", str(ROOT / ".vane" / "tool-cache"))
    )
    if checker.is_symlink() or not checker.is_file():
        errors.append(f"toolchain executable checker is unavailable: {checker}")
    else:
        checked = subprocess.run(
            [
                sys.executable,
                str(checker),
                "--lock",
                str(args.lock),
                "--cache",
                str(tool_cache),
                "--repo-root",
                str(ROOT),
                "--json",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        try:
            check_report = json.loads(
                checked.stdout, object_pairs_hook=reject_duplicate_keys
            )
            if not isinstance(check_report, dict) or set(check_report) != {"ok", "errors"}:
                raise ValueError("unexpected checker report")
            if not isinstance(check_report["errors"], list):
                raise ValueError("checker errors are not a list")
            errors.extend(str(error) for error in check_report["errors"])
        except (json.JSONDecodeError, PolicyError, ValueError) as error:
            errors.append(f"toolchain executable checker failed: {error}")
    if not signer_entries(args.allowed_signers):
        errors.append(f"allowed signer policy has no trusted keys: {args.allowed_signers}")
    result = {"ok": not errors, "errors": errors}
    if getattr(args, "json", False):
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        if not errors:
            print("doctor: toolchain integrity and signer policy are complete")
    return 0 if not errors else EXIT_POLICY


def changed_paths(base: str, head: str) -> list[str]:
    base_sha = git_revision(base)
    head_sha = git_revision(head)
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}...{head_sha}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError("cannot compute quick-check change set")
    return [line for line in result.stdout.splitlines() if line]


def command_quick(args: argparse.Namespace) -> int:
    if args.risk in {"A", "S"}:
        raise PolicyError("quick cannot satisfy risk A/S; use full plus required validators")
    paths = changed_paths(args.base, args.head)
    skipped: list[str] = []
    if any(path.startswith("server/") for path in paths):
        skipped.extend(
            run_go_tests_json(
                ["go", "test", "-json", "./..."],
                cwd=ROOT / "server",
                fail_on_skips=False,
            )
        )
    if any(path.startswith("web/") for path in paths):
        run_checked(["npm", "test"], cwd=ROOT / "web")
    if any(
        path.startswith(("ops/", "infra/", "tools/", "contracts/", "tests/"))
        or path in {"AGENTS.md", "Makefile"}
        for path in paths
    ):
        run_checked(
            [sys.executable, "-m", "unittest", "discover", "-s", "ops/tests", "-p", "test_*.py"],
            cwd=ROOT,
        )
        run_checked(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests/contract", "-p", "test_*.py"],
            cwd=ROOT,
        )
    print(
        json.dumps(
            {
                "base": git_revision(args.base),
                "head": git_revision(args.head),
                "paths": paths,
                "risk": args.risk,
                "status": "passed",
                "skipped_tests": skipped,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def command_full(args: argparse.Namespace) -> int:
    scanner = ROOT / "tools" / "testpolicy" / "check-go-skips.sh"
    if scanner.is_symlink() or not scanner.is_file():
        raise PolicyError(f"test-policy scanner is unavailable: {scanner}")
    if git_revision("HEAD") != args.sha:
        raise PolicyError("full checkout differs from requested exact SHA")
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT,
        text=True, capture_output=True, check=False,
    )
    if dirty.returncode != 0 or dirty.stdout:
        raise PolicyError("full requires a clean exact-source worktree")
    work_root = Path(
        os.environ.get("VANE_WORK_ROOT", str(ROOT / ".vane" / "work"))
    )
    if not work_root.is_absolute() or work_root.is_symlink():
        raise PolicyError("VANE_WORK_ROOT must be a safe absolute directory")
    work_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not work_root.is_dir():
        raise PolicyError("VANE_WORK_ROOT is unavailable")
    old_full_sha = os.environ.get("VANE_FULL_SHA")
    old_work_root = os.environ.get("VANE_WORK_ROOT")
    os.environ["VANE_FULL_SHA"] = args.sha
    os.environ["VANE_WORK_ROOT"] = str(work_root)
    try:
        run_checked([str(scanner)], cwd=ROOT)
        run_checked([sys.executable, "-m", "ops.audit.full_gate"], cwd=ROOT)
    finally:
        if old_full_sha is None:
            os.environ.pop("VANE_FULL_SHA", None)
        else:
            os.environ["VANE_FULL_SHA"] = old_full_sha
        if old_work_root is None:
            os.environ.pop("VANE_WORK_ROOT", None)
        else:
            os.environ["VANE_WORK_ROOT"] = old_work_root
    run_checked(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests/contract", "-p", "test_*.py"],
        cwd=ROOT,
    )
    run_checked(
        [sys.executable, "-m", "unittest", "discover", "-s", "ops/tests", "-p", "test_*.py"],
        cwd=ROOT,
    )
    return 0


def default_manifest(revision: str, stage: str) -> Path:
    return DEFAULT_EVIDENCE / revision / f"{stage}.json"


def require_release_chain(
    manifest_path: Path,
    revision: str,
    target_stage: str,
    allowed_signers: Path,
) -> list[tuple[Path, dict[str, Any]]]:
    chain = load_manifest_chain(
        manifest_path,
        allowed_signers=allowed_signers,
        require_signatures=True,
    )
    if chain[-1][1]["stage"] != target_stage:
        raise PolicyError(
            f"operation requires a signed {target_stage} manifest, got {chain[-1][1]['stage']}"
        )
    if chain[-1][1]["revision"] != revision:
        raise PolicyError("manifest chain revision does not match requested revision")
    return chain


def broker_required(operation: str) -> int:
    print(
        f"{operation}: verified repository-side inputs; production mutation requires "
        "the separately installed root-owned broker",
        file=sys.stderr,
    )
    return EXIT_POLICY


def default_broker_submit_path() -> Path:
    release_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return release_home / ".local" / "libexec" / "vane-broker-submit"


def validate_broker_submitter(
    path: Path, *, account_home: Path | None = None
) -> None:
    home = account_home or Path(pwd.getpwuid(os.getuid()).pw_dir)
    try:
        relative_parent = path.parent.relative_to(home)
        home_metadata = home.lstat()
    except (OSError, ValueError) as error:
        raise PolicyError("broker submitter is outside the release account home") from error
    if (
        home.is_symlink()
        or not stat.S_ISDIR(home_metadata.st_mode)
        or home_metadata.st_uid != os.getuid()
        or stat.S_IMODE(home_metadata.st_mode) & 0o022
    ):
        raise PolicyError("release account home is unsafe")
    current = home
    for part in relative_parent.parts:
        current = current / part
        metadata = current.lstat()
        if (
            current.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise PolicyError("broker submitter directory chain is unsafe")
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or sha256_file(path) != sha256_file(ROOT / "ops/broker/submit.py")
    ):
        raise PolicyError("broker submitter differs from exact release source")


def verify_production_revision(revision: str) -> str:
    config_path = broker_client.default_config_path()
    broker_client.validate_config_file(config_path)
    config = broker_client.strict_json(config_path)
    if config.get("host") != BROKER_HOST or config.get("port") != BROKER_PORT:
        raise PolicyError("broker client endpoint differs from exact release policy")
    if broker_client.KNOWN_HOSTS_SHA256 != BROKER_KNOWN_HOSTS_SHA256:
        raise PolicyError("broker client known-hosts authority differs from controller")
    command = broker_client.fixed_ssh_command(config)
    status = subprocess.run(
        [*command, "vane-broker status"],
        input=b"{}",
        capture_output=True,
        check=False,
    )
    if status.returncode != 0:
        raise PolicyError("independent production status verification failed")
    try:
        value = json.loads(status.stdout, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise PolicyError("independent production status returned invalid JSON") from error
    if (
        not isinstance(value, dict)
        or set(value) != {"ok", "verb", "current_digest", "server_revision"}
        or value.get("ok") is not True
        or value.get("verb") != "status"
        or value.get("server_revision") != revision
        or not isinstance(value.get("current_digest"), str)
        or not DIGEST.fullmatch(value["current_digest"])
    ):
        raise PolicyError("independent production status differs from released revision")
    return value["current_digest"]


def sanitized_gate_environment(
    source: dict[str, str], *, gate_home: Path, release_root: Path,
    gate_evidence: Path, rollback_base: str,
) -> dict[str, str]:
    result = {
        name: value for name, value in source.items() if name in GATE_ENV_ALLOWLIST
    }
    result.update({
        "HOME": str(gate_home),
        "VANE_WORK_ROOT": str(release_root),
        "VANE_FULL_GATE_EVIDENCE": str(gate_evidence),
        "VANE_ROLLBACK_BASE_SHA": rollback_base,
    })
    return result


def require_release_runtime() -> tuple[Path, Path, str, Path]:
    work_root = Path(os.environ.get("VANE_WORK_ROOT", ""))
    signing_key = Path(os.environ.get("VANE_RELEASE_SIGNING_KEY", ""))
    signer = os.environ.get("VANE_RELEASE_SIGNER", "").strip()
    broker_submit = default_broker_submit_path()
    for name, path, executable in (
        ("VANE_WORK_ROOT", work_root, False),
        ("VANE_RELEASE_SIGNING_KEY", signing_key, False),
        ("VANE_BROKER_SUBMIT", broker_submit, True),
    ):
        valid = path.is_absolute() and not path.is_symlink()
        valid = valid and (path.is_dir() if name == "VANE_WORK_ROOT" else path.is_file())
        if executable:
            valid = valid and os.access(path, os.X_OK)
        if not valid:
            raise PolicyError(f"{name} must name a safe existing absolute path")
    validate_broker_submitter(broker_submit)
    if not signer or not signer.isascii() or any(char.isspace() for char in signer):
        raise PolicyError("VANE_RELEASE_SIGNER must be a non-empty ASCII principal")
    return work_root, signing_key, signer, broker_submit


def write_signed_manifest(
    *,
    directory: Path,
    stage: str,
    revision: str,
    signer: str,
    signing_key: Path,
    evidence: dict[str, Path],
    parent: Path | None,
) -> Path:
    path = directory / f"{stage}.json"
    parent_value = None
    if parent is not None:
        parent_manifest = validate_manifest_shape(load_json(parent), parent)
        parent_value = {
            "path": parent.name,
            "sha256": sha256_file(parent),
            "stage": parent_manifest["stage"],
        }
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "stage": stage,
        "revision": revision,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "parent": parent_value,
        "signer": signer,
        "evidence": [
            {"name": name, "sha256": sha256_file(value)}
            for name, value in sorted(evidence.items())
        ],
    }
    path.write_bytes(canonical_json(manifest))
    signed = subprocess.run(
        ["ssh-keygen", "-Y", "sign", "-f", str(signing_key), "-n", "vane-release", str(path)],
        text=True,
        capture_output=True,
        check=False,
    )
    if signed.returncode != 0 or not path.with_name(path.name + ".sig").is_file():
        raise PolicyError(f"cannot sign {stage} manifest: {signed.stderr.strip()}")
    return path


def validate_rollback_compatibility_proof(
    *, gate: dict[str, Any], gate_evidence: Path, revision: str
) -> Path:
    if gate.get("server_rollback_safe") is not True:
        raise PolicyError("release lacks previous-binary rollback compatibility proof")
    base_revision = gate.get("rollback_base_revision")
    if not isinstance(base_revision, str) or not EXACT_SHA.fullmatch(base_revision):
        raise PolicyError("full gate rollback base is not an exact revision")
    proof_value = gate.get("rollback_compatibility_path")
    if not isinstance(proof_value, str):
        raise PolicyError("full gate rollback proof path is not a string")
    rollback_proof = Path(proof_value)
    if rollback_proof.is_absolute():
        raise PolicyError("full gate rollback proof path must be relative")
    rollback_proof = gate_evidence.parent / rollback_proof
    try:
        rollback_proof.resolve(strict=True).relative_to(gate_evidence.parent.resolve())
    except (OSError, ValueError) as error:
        raise PolicyError("full gate rollback proof escapes the evidence root") from error
    if rollback_proof.is_symlink() or not rollback_proof.is_file():
        raise PolicyError("full gate rollback proof is unavailable")
    proof_digest = gate.get("rollback_compatibility_sha256")
    if not isinstance(proof_digest, str):
        raise PolicyError("full gate rollback compatibility digest is not a string")
    digest_value(proof_digest, field="full gate rollback compatibility digest")
    if sha256_file(rollback_proof) != proof_digest:
        raise PolicyError("full gate rollback proof changed after verification")
    proof = load_json(rollback_proof)
    expected_keys = {
        "schema", "base_revision", "current_revision", "mode",
        "added_migrations", "previous_gate_sha256",
        "previous_gate_output_sha256", "status",
    }
    if not isinstance(proof, dict) or set(proof) != expected_keys:
        raise PolicyError("rollback compatibility proof shape is not exact")
    mode = proof["mode"]
    added = proof["added_migrations"]
    if (
        proof["schema"] != "vane.server-rollback-compatibility/v1"
        or proof["base_revision"] != base_revision
        or proof["current_revision"] != revision
        or proof["status"] != "passed"
        or mode not in {
            "identical-migration-history", "previous-binary-on-upgraded-schema"
        }
        or not isinstance(added, list)
    ):
        raise PolicyError("rollback compatibility proof does not bind the release")
    if mode == "identical-migration-history":
        if added or proof["previous_gate_sha256"] is not None or proof[
            "previous_gate_output_sha256"
        ] is not None:
            raise PolicyError("identical migration proof contains contradictory evidence")
    else:
        if not added:
            raise PolicyError("upgraded-schema proof does not name added migrations")
        for item in added:
            if not isinstance(item, dict) or set(item) != {"path", "git_blob", "sha256"}:
                raise PolicyError("added migration proof entry shape is not exact")
            if (
                not isinstance(item["path"], str)
                or not re.fullmatch(r"[0-9]{3}_[a-z0-9_]+\.sql", item["path"])
                or not isinstance(item["git_blob"], str)
                or not re.fullmatch(r"[0-9a-f]{40,64}", item["git_blob"])
                or not isinstance(item["sha256"], str)
            ):
                raise PolicyError("added migration proof entry is invalid")
            digest_value(item["sha256"], field="added migration proof digest")
        for field in ("previous_gate_sha256", "previous_gate_output_sha256"):
            value = proof[field]
            if not isinstance(value, str):
                raise PolicyError(f"rollback compatibility {field} is not a string")
            digest_value(value, field=f"rollback compatibility {field}")
    return rollback_proof


def build_release_submission(
    *, revision: str, release_root: Path, gate_evidence: Path,
    signing_key: Path, signer: str, allowed_signers: Path,
) -> Path:
    gate = load_json(gate_evidence)
    expected_gate_keys = {
        "schema", "revision", "binary_dir", "web_dist",
        "binary_tree_sha256", "web_tree_sha256", "web_coverage_sha256",
        "server_coverage_sha256", "release_marker_sha256",
        "server_source_tree_sha256", "infra_tree_sha256",
        "migration_tree_object", "rollback_base_revision",
        "rollback_compatibility_path", "rollback_compatibility_sha256",
        "server_rollback_safe",
    }
    if not isinstance(gate, dict) or set(gate) != expected_gate_keys:
        raise PolicyError("full gate evidence shape is not exact")
    if gate["schema"] != "vane.full-gate-evidence/v1" or gate["revision"] != revision:
        raise PolicyError("full gate evidence does not bind the release revision")
    for field in (
        "server_source_tree_sha256",
        "infra_tree_sha256",
    ):
        if not isinstance(gate[field], str):
            raise PolicyError(f"full gate {field} is not a string")
        digest_value(gate[field], field=f"full gate {field}")
    rollback_proof = validate_rollback_compatibility_proof(
        gate=gate, gate_evidence=gate_evidence, revision=revision
    )
    binary_dir = Path(gate["binary_dir"])
    web_dist = Path(gate["web_dist"])
    if binary_dir.is_absolute() or web_dist.is_absolute():
        raise PolicyError("full gate artifact paths must be relative to the evidence root")
    binary_dir = gate_evidence.parent / binary_dir
    web_dist = gate_evidence.parent / web_dist
    if binary_dir.is_symlink() or not binary_dir.is_dir() or web_dist.is_symlink() or not web_dist.is_dir():
        raise PolicyError("full gate artifact directories are unavailable")
    if directory_tree_sha256(binary_dir) != gate["binary_tree_sha256"]:
        raise PolicyError("full gate binary tree changed after verification")
    if directory_tree_sha256(web_dist) != gate["web_tree_sha256"]:
        raise PolicyError("full gate Web tree changed after verification")
    gate_artifact_root = binary_dir.parent
    web_coverage = gate_artifact_root / "web-coverage-summary.json"
    server_coverage = gate_artifact_root / "coverage.out"
    if (
        sha256_file(web_coverage) != gate["web_coverage_sha256"]
        or sha256_file(server_coverage) != gate["server_coverage_sha256"]
    ):
        raise PolicyError("full gate coverage evidence changed after verification")
    handoff = release_root / "server-submission"
    handoff.mkdir()
    source = handoff / "artifact-source"
    (source / "server/bin").mkdir(parents=True)
    (source / "infra").mkdir()
    for binary in binary_dir.iterdir():
        if binary.is_symlink() or not binary.is_file():
            raise PolicyError(f"unsafe full-gate binary: {binary}")
        shutil.copy2(binary, source / "server/bin" / binary.name)
    if directory_tree_sha256(source / "server/bin") != gate["binary_tree_sha256"]:
        raise PolicyError("copied binary tree differs from full Gate evidence")
    committed_source = handoff / "committed-source"
    export_committed_files(revision, committed_source, CONTROL_PLANE_PATHS)
    committed_infra = committed_source / "infra/production"
    if directory_tree_sha256(committed_infra) != gate["infra_tree_sha256"]:
        raise PolicyError("committed infra tree differs from full Gate evidence")
    shutil.copytree(
        committed_infra, source / "infra/production", symlinks=False
    )
    artifacts = handoff / "artifacts"
    backend_pack = artifacts / "backend-pack"
    backend_payload = artifacts / "backend-payload"
    controller_archive = artifacts / f"controller-{revision}.tar.gz"
    run_id = str(int(datetime.now(timezone.utc).timestamp() * 1_000_000))
    artifact_tool = committed_source / "ops/release/artifact.py"
    run_checked(
        [sys.executable, str(artifact_tool), "pack", "--component", "backend",
         "--sha", revision, "--source", str(source), "--output", str(backend_pack),
         "--server-release-contract", "vane.server-release-contract/v2 primary_store=owner_compat_v1 research_control_store=restricted_v1 research_store=restricted_v1",
         "--control-plane-revision", revision, "--deploy-run-id", run_id,
         "--build-run-attempt", "1"],
        cwd=committed_source,
    )
    write_control_plane_archive(controller_archive, revision)
    run_checked(
        [sys.executable, str(artifact_tool), "validate", "--component", "backend",
         "--sha", revision, "--input", str(backend_pack), "--output", str(backend_payload),
         "--control-plane-revision", revision, "--deploy-run-id", run_id],
        cwd=committed_source,
    )
    manifests = handoff / "manifests"
    manifests.mkdir()
    durable_gate = handoff / "gate-evidence"
    durable_gate.mkdir()
    shutil.copy2(web_coverage, durable_gate / web_coverage.name)
    shutil.copy2(server_coverage, durable_gate / server_coverage.name)
    shutil.copy2(rollback_proof, durable_gate / rollback_proof.name)
    shutil.copy2(DEFAULT_POLICY, durable_gate / "release-policy.json")
    shutil.copy2(DEFAULT_LOCK, durable_gate / "toolchain.lock.json")
    shutil.copy2(
        backend_payload / "release-receipt.json",
        durable_gate / "release-receipt.json",
    )
    handoff_gate = handoff / "full-gate.json"
    shutil.copy2(gate_evidence, handoff_gate)
    plan = write_signed_manifest(
        directory=manifests, stage="plan", revision=revision, signer=signer,
        signing_key=signing_key,
        evidence={
            "release-policy.json": durable_gate / "release-policy.json",
            "toolchain.lock.json": durable_gate / "toolchain.lock.json",
        },
        parent=None,
    )
    gate_manifest = write_signed_manifest(
        directory=manifests, stage="gate", revision=revision, signer=signer,
        signing_key=signing_key,
        evidence={
            "full-gate.json": handoff_gate,
            "server-coverage.out": durable_gate / server_coverage.name,
            "server-rollback-compatibility.json": durable_gate / rollback_proof.name,
            "web-coverage-summary.json": durable_gate / web_coverage.name,
        },
        parent=plan,
    )
    artifact_manifest = write_signed_manifest(
        directory=manifests, stage="artifact", revision=revision, signer=signer,
        signing_key=signing_key,
        evidence={
            "release-receipt.json": durable_gate / "release-receipt.json",
            "backend-manifest.json": backend_pack / f"backend-{revision}.manifest.json",
            "controller-archive.tar.gz": controller_archive,
        },
        parent=gate_manifest,
    )
    require_release_chain(artifact_manifest, revision, "artifact", allowed_signers)
    submission = {
        "schema": "vane.broker-submission/v1",
        "revision": revision,
        "deploy_run_id": run_id,
        "artifact_manifest": "manifests/artifact.json",
        "backend_pack": "artifacts/backend-pack",
        "controller_archive": f"artifacts/controller-{revision}.tar.gz",
        "evidence": {
            "plan:release-policy.json": {
                "path": "gate-evidence/release-policy.json",
                "sha256": sha256_file(durable_gate / "release-policy.json"),
            },
            "plan:toolchain.lock.json": {
                "path": "gate-evidence/toolchain.lock.json",
                "sha256": sha256_file(durable_gate / "toolchain.lock.json"),
            },
            "gate:full-gate.json": {
                "path": "full-gate.json",
                "sha256": sha256_file(handoff_gate),
            },
            "gate:server-coverage.out": {
                "path": "gate-evidence/coverage.out",
                "sha256": sha256_file(durable_gate / server_coverage.name),
            },
            "gate:server-rollback-compatibility.json": {
                "path": "gate-evidence/server-rollback-compatibility.json",
                "sha256": sha256_file(durable_gate / rollback_proof.name),
            },
            "gate:web-coverage-summary.json": {
                "path": "gate-evidence/web-coverage-summary.json",
                "sha256": sha256_file(durable_gate / web_coverage.name),
            },
            "artifact:release-receipt.json": {
                "path": "gate-evidence/release-receipt.json",
                "sha256": sha256_file(durable_gate / "release-receipt.json"),
            },
            "artifact:backend-manifest.json": {
                "path": f"artifacts/backend-pack/backend-{revision}.manifest.json",
                "sha256": sha256_file(backend_pack / f"backend-{revision}.manifest.json"),
            },
            "artifact:controller-archive.tar.gz": {
                "path": f"artifacts/controller-{revision}.tar.gz",
                "sha256": sha256_file(controller_archive),
            },
        },
    }
    (handoff / "submission.json").write_bytes(canonical_json(submission))
    shutil.rmtree(source)
    shutil.rmtree(backend_payload)
    return handoff


def publish_web_after_server(
    *, revision: str, release_root: Path, gate_evidence: Path
) -> Path:
    gate = load_json(gate_evidence)
    web_dist = Path(gate["web_dist"])
    if web_dist.is_absolute():
        raise PolicyError("full gate Web dist path must be relative")
    web_dist = release_root / web_dist
    if directory_tree_sha256(web_dist) != gate["web_tree_sha256"]:
        raise PolicyError("verified Web dist changed before local publication")
    web_state = Path(
        os.environ.get("VANE_WEB_STATE_ROOT", str(Path.home() / ".local/state/vane"))
    )
    web_state.mkdir(parents=True, exist_ok=True, mode=0o700)
    web_origin = os.environ.get("VANE_WEB_ORIGIN", "https://vane.zhuoqidev.com")
    tool_cache = Path(
        os.environ.get("VANE_TOOL_CACHE", str(ROOT / ".vane/tool-cache"))
    )
    web_result = release_root / "web-publication.json"
    run_checked(
        [
            sys.executable, str(ROOT / "ops/release/publish_web.py"),
            "--dist", str(web_dist), "--sha", revision,
            "--work-root", str(release_root), "--state-root", str(web_state),
            "--tool-cache", str(tool_cache), "--origin", web_origin,
            "--result", str(web_result),
        ],
        cwd=ROOT,
    )
    return web_result


def command_release(args: argparse.Namespace) -> int:
    if (
        args.lock != DEFAULT_LOCK
        or args.policy != DEFAULT_POLICY
        or args.allowed_signers != DEFAULT_SIGNERS
    ):
        raise PolicyError("release authorities must be the exact repository defaults")
    assert_origin_main(args.sha)
    if git_revision("HEAD") != args.sha:
        raise PolicyError("release checkout is not the requested exact origin/main")
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT,
        text=True, capture_output=True, check=False,
    )
    if dirty.returncode != 0 or dirty.stdout:
        raise PolicyError("release requires a clean exact-source worktree")
    work_root, signing_key, signer, broker_submit = require_release_runtime()
    preflight_errors = validate_toolchain(args.lock, args.policy)
    if not signer_entries(args.allowed_signers):
        preflight_errors.append(f"allowed signer policy has no trusted keys: {args.allowed_signers}")
    if preflight_errors:
        raise PolicyError("release preflight failed: " + "; ".join(preflight_errors))
    broker_status = subprocess.run(
        [str(broker_submit), "--status"],
        text=True,
        capture_output=True,
        check=False,
    )
    if broker_status.returncode != 0:
        raise PolicyError(
            f"broker status failed before local Gate: {broker_status.stderr.strip()}"
        )
    try:
        current = json.loads(broker_status.stdout)
    except json.JSONDecodeError as error:
        raise PolicyError("broker status returned invalid JSON before local Gate") from error
    if not isinstance(current, dict) or set(current) != {
        "current_digest", "server_revision"
    }:
        raise PolicyError("broker status shape is not exact before local Gate")
    rollback_base = current["server_revision"]
    if not isinstance(rollback_base, str) or not EXACT_SHA.fullmatch(rollback_base):
        raise PolicyError("broker status lacks an exact deployed server revision")
    release_root = work_root / f"release-{args.sha}"
    if release_root.exists() or release_root.is_symlink():
        raise PolicyError(f"release evidence path already exists: {release_root}")
    release_root.mkdir(mode=0o700)
    gate_evidence = release_root / "full-gate.json"
    gate_home = release_root / "gate-home"
    gate_home.mkdir(mode=0o700)
    prior_environment = dict(os.environ)
    gate_environment = sanitized_gate_environment(
        prior_environment,
        gate_home=gate_home,
        release_root=release_root,
        gate_evidence=gate_evidence,
        rollback_base=rollback_base,
    )
    os.environ.clear()
    os.environ.update(gate_environment)
    try:
        command_full(argparse.Namespace(sha=args.sha))
    finally:
        os.environ.clear()
        os.environ.update(prior_environment)
    if gate_evidence.is_symlink() or not gate_evidence.is_file():
        raise PolicyError("local exact-SHA full gate returned no evidence")
    submission = build_release_submission(
        revision=args.sha, release_root=release_root, gate_evidence=gate_evidence,
        signing_key=signing_key, signer=signer, allowed_signers=args.allowed_signers,
    )
    submitted = subprocess.run([str(broker_submit), str(submission)], check=False)
    if submitted.returncode != 0:
        raise PolicyError(f"broker submission failed with exit {submitted.returncode}")
    verify_production_revision(args.sha)
    web_result = publish_web_after_server(
        revision=args.sha, release_root=release_root, gate_evidence=gate_evidence
    )
    verify_production_revision(args.sha)
    print(json.dumps({
        "revision": args.sha,
        "server_submission": str(submission),
        "web_evidence": str(web_result),
        "status": "server-and-web-published",
    }, sort_keys=True, separators=(",", ":")))
    return 0


def command_status(args: argparse.Namespace) -> int:
    if args.release_receipt is not None:
        receipt = validate_release_receipt(args.release_receipt)
        if args.sha is not None and receipt["source_revision"] != args.sha:
            raise PolicyError("release receipt revision differs from requested revision")
        print(
            json.dumps(
                {
                    "schema": receipt["schema_version"],
                    "revision": receipt["source_revision"],
                    "sha256": sha256_file(args.release_receipt),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    if args.current_release is not None:
        release = validate_current_release(args.current_release)
        print(
            json.dumps(
                {
                    "schema": release["schema"],
                    "revision": release["monorepo_revision"],
                    "sha256": sha256_file(args.current_release),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    if args.manifest is None:
        manifests = sorted(DEFAULT_EVIDENCE.glob("*/*.json")) if DEFAULT_EVIDENCE.is_dir() else []
        print(json.dumps({"evidence_root": str(DEFAULT_EVIDENCE), "manifests": len(manifests)}))
        return 0
    chain = load_manifest_chain(
        args.manifest,
        allowed_signers=args.allowed_signers,
        require_signatures=False,
    )
    path, manifest = chain[-1]
    print(
        json.dumps(
            {
                "manifest": str(path),
                "sha256": sha256_file(path),
                "revision": manifest["revision"],
                "stage": manifest["stage"],
                "chain_length": len(chain),
                "signed": all(item_path.with_name(item_path.name + ".sig").is_file() for item_path, _ in chain),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def command_audit(args: argparse.Namespace) -> int:
    chain = load_manifest_chain(
        args.manifest,
        allowed_signers=args.allowed_signers,
        require_signatures=not args.structural_only,
    )
    transition = None
    cas_arguments = (
        args.current_release,
        args.candidate_release,
        args.expected_current_digest,
        args.release_receipt,
    )
    if any(value is not None for value in cas_arguments):
        if any(value is None for value in cas_arguments):
            raise PolicyError("current-release activation requires all CAS inputs")
        transition = validate_current_release_transition(
            current_path=args.current_release,
            candidate_path=args.candidate_release,
            expected_current_digest=args.expected_current_digest,
            receipt_path=args.release_receipt,
            chain=chain,
            activation=True,
        )
    print(
        json.dumps(
            {
                "ok": True,
                "revision": chain[-1][1]["revision"],
                "stage": chain[-1][1]["stage"],
                "chain": [sha256_file(path) for path, _ in chain],
                "signatures_verified": not args.structural_only,
                "activation": transition,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def command_retry(args: argparse.Namespace) -> int:
    require_release_chain(args.manifest, args.sha, args.stage, args.allowed_signers)
    return broker_required("retry")


def command_rollback(args: argparse.Namespace) -> int:
    if args.to == args.sha:
        raise PolicyError("rollback target must differ from current revision")
    require_release_chain(args.manifest, args.sha, "finalize", args.allowed_signers)
    require_release_chain(args.target_manifest, args.to, "finalize", args.allowed_signers)
    return broker_required("rollback")


def command_cert_check(args: argparse.Namespace) -> int:
    certificate = args.certificate
    if certificate.is_symlink() or not certificate.is_file():
        raise PolicyError(f"certificate is not a regular file: {certificate}")
    if args.min_days < 0:
        raise PolicyError("certificate minimum days cannot be negative")
    seconds = args.min_days * 86400
    result = subprocess.run(
        ["openssl", "x509", "-in", str(certificate), "-noout", "-checkend", str(seconds)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError(f"certificate expires within {args.min_days} days or is invalid")
    fingerprint = subprocess.run(
        ["openssl", "x509", "-in", str(certificate), "-noout", "-fingerprint", "-sha256"],
        capture_output=True,
        text=True,
        check=False,
    )
    if fingerprint.returncode != 0:
        raise PolicyError("cannot calculate certificate fingerprint")
    print(fingerprint.stdout.strip())
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="vane")
    commands = result.add_subparsers(dest="command", required=True)

    doctor = commands.add_parser("doctor", help="validate local release prerequisites")
    doctor.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    doctor.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    doctor.add_argument("--allowed-signers", type=Path, default=DEFAULT_SIGNERS)
    doctor.add_argument("--json", action="store_true")
    doctor.set_defaults(handler=command_doctor)

    quick = commands.add_parser("quick", help="run local tests selected by changed paths")
    quick.add_argument("--risk", choices=("B", "A", "S"), required=True)
    quick.add_argument("--base", required=True)
    quick.add_argument("--head", required=True)
    quick.set_defaults(handler=command_quick)

    full = commands.add_parser("full", help="run all checks for a clean exact commit")
    full.add_argument("--sha", type=exact_sha, required=True)
    full.set_defaults(handler=command_full)

    release = commands.add_parser(
        "release", help="preflight, gate, build, sign, and submit one exact release"
    )
    release.add_argument("--sha", type=exact_sha, required=True)
    release.set_defaults(
        handler=command_release,
        lock=DEFAULT_LOCK,
        policy=DEFAULT_POLICY,
        allowed_signers=DEFAULT_SIGNERS,
    )

    status = commands.add_parser("status", help="read local release evidence")
    status_inputs = status.add_mutually_exclusive_group()
    status_inputs.add_argument("--manifest", type=Path)
    status_inputs.add_argument("--release-receipt", type=Path)
    status_inputs.add_argument("--current-release", type=Path)
    status.add_argument("--sha", type=exact_sha)
    status.add_argument("--allowed-signers", type=Path, default=DEFAULT_SIGNERS)
    status.set_defaults(handler=command_status)

    audit = commands.add_parser("audit", help="verify a manifest chain")
    audit.add_argument("--manifest", type=Path, required=True)
    audit.add_argument("--allowed-signers", type=Path, default=DEFAULT_SIGNERS)
    audit.add_argument("--structural-only", action="store_true")
    audit.add_argument("--current-release", type=Path)
    audit.add_argument("--candidate-release", type=Path)
    audit.add_argument("--expected-current-digest")
    audit.add_argument("--release-receipt", type=Path)
    audit.set_defaults(handler=command_audit)

    retry = commands.add_parser("retry", help="validate a production retry request")
    retry.add_argument("--sha", type=exact_sha, required=True)
    retry.add_argument("--manifest", type=Path, required=True)
    retry.add_argument("--stage", choices=("deploy", "verify"), required=True)
    retry.add_argument("--allowed-signers", type=Path, default=DEFAULT_SIGNERS)
    retry.set_defaults(handler=command_retry)

    rollback = commands.add_parser("rollback", help="validate a rollback request")
    rollback.add_argument("--sha", type=exact_sha, required=True)
    rollback.add_argument("--to", type=exact_sha, required=True)
    rollback.add_argument("--manifest", type=Path, required=True)
    rollback.add_argument("--target-manifest", type=Path, required=True)
    rollback.add_argument("--allowed-signers", type=Path, default=DEFAULT_SIGNERS)
    rollback.set_defaults(handler=command_rollback)

    cert = commands.add_parser("cert", help="certificate read-only operations")
    cert_commands = cert.add_subparsers(dest="cert_command", required=True)
    cert_check = cert_commands.add_parser("check", help="check a local public certificate")
    cert_check.add_argument("--certificate", type=Path, required=True)
    cert_check.add_argument("--min-days", type=int, default=60)
    cert_check.set_defaults(handler=command_cert_check)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return args.handler(args)
    except PolicyError as error:
        print(f"policy refusal: {error}", file=sys.stderr)
        return EXIT_POLICY


if __name__ == "__main__":
    raise SystemExit(main())
