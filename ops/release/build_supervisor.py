#!/usr/bin/env python3
"""Root-owned exact-main build supervisor.

Docker is controlled only by this installed N controller. Candidate M executes
inside a one-shot read-only runner with an empty Home, no Docker socket, and no
production credentials. Five PostgreSQL 18 instances plus canonical Temporal
1.29.7 are provisioned outside the candidate container.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid


CONTROL_ROOT = Path(__file__).resolve().parents[2]
EXACT_SHA = re.compile(r"^[0-9a-f]{40}$")


def command(args: list[str], *, capture: bool = False) -> str:
    result = subprocess.run(
        args,
        text=True,
        capture_output=capture,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() if capture else ""
        raise RuntimeError(f"supervisor command failed ({result.returncode}): {args[0]} {detail}")
    return result.stdout.strip() if capture else ""


def strict_json(path: Path) -> dict:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise RuntimeError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"unsafe supervisor JSON: {path}")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise RuntimeError(f"supervisor JSON root is not an object: {path}")
    return value


def load_config() -> dict:
    path = Path("/etc/vane-build/supervisor.json")
    if os.environ.get("VANE_BUILD_SUPERVISOR_TESTING") == "1" and os.geteuid() != 0:
        path = Path(os.environ.get("VANE_BUILD_SUPERVISOR_CONFIG", ""))
    value = strict_json(path)
    expected = {
        "schema",
        "source_mirror",
        "work_root",
        "tool_cache",
        "history_dir",
        "runner_image",
    }
    if set(value) != expected or value.get("schema") != "vane.build-supervisor/v1":
        raise RuntimeError("build supervisor configuration keys are not exact")
    for key in ("source_mirror", "work_root", "tool_cache", "history_dir"):
        candidate = Path(value[key])
        if not candidate.is_absolute() or candidate.is_symlink() or not candidate.exists():
            raise RuntimeError(f"build supervisor {key} is unsafe or unavailable")
    image = value["runner_image"]
    lock = strict_json(CONTROL_ROOT / "tools/toolchain.lock.json")["tools"]
    expected_image = f"{lock['gate_runner']['image']}@{lock['gate_runner']['digest']}"
    if image != expected_image:
        raise RuntimeError("build supervisor runner image differs from the locked digest")
    return value


def docker_output(args: list[str]) -> str:
    return command(["docker", *args], capture=True)


def wait_healthy(name: str) -> None:
    for _ in range(90):
        if docker_output(["inspect", "-f", "{{.State.Health.Status}}", name]) == "healthy":
            return
        time.sleep(1)
    raise RuntimeError(f"supervisor dependency did not become healthy: {name}")


def write_dependencies(path: Path, names: list[str], temporal_name: str) -> None:
    postgres = []
    for index, name in enumerate(names):
        postgres.append(
            {
                "container_name": name,
                "container_id": docker_output(["inspect", "-f", "{{.Id}}", name]),
                "host": f"pg-{index}",
                "url": f"postgres://vane:vane_test@pg-{index}:5432/vane_test?sslmode=disable",
            }
        )
    value = {
        "schema": "vane.full-gate-dependencies/v1",
        "postgres": postgres,
        "temporal_address": "temporal:7233",
    }
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    path.chmod(0o444)


def rewrite_evidence(path: Path, output_root: Path) -> None:
    value = strict_json(path)
    for key in ("binary_dir", "web_dist"):
        raw = value.get(key)
        if not isinstance(raw, str) or Path(raw).is_absolute() or ".." in Path(raw).parts:
            raise RuntimeError("runner evidence artifact path is not a safe relative path")
        relative = Path(raw)
        host = output_root / relative
        if host.is_symlink() or not host.is_dir():
            raise RuntimeError("runner evidence artifact path is unavailable on the supervisor")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not EXACT_SHA.fullmatch(args.sha):
        raise RuntimeError("build supervisor revision is not an exact SHA")
    if not args.output.is_absolute() or args.output.exists() or args.output.is_symlink():
        raise RuntimeError("build supervisor output must be a new absolute path")
    config = load_config()
    mirror = Path(config["source_mirror"])
    remote_main = command(
        ["git", "-C", str(mirror), "rev-parse", "--verify", "refs/remotes/origin/main^{commit}"],
        capture=True,
    )
    if remote_main != args.sha:
        raise RuntimeError("build supervisor accepts only exact origin/main")

    work_root = Path(config["work_root"])
    run_id = f"vane-full-{uuid.uuid4().hex[:12]}"
    transaction = Path(tempfile.mkdtemp(prefix=run_id + ".", dir=str(work_root)))
    source = transaction / "source"
    output = transaction / "output"
    dependencies = transaction / "dependencies.json"
    output.mkdir(mode=0o777)
    network = run_id
    containers: list[str] = []
    try:
        command(["git", "clone", "--no-local", "--no-checkout", str(mirror), str(source)])
        command(["git", "-C", str(source), "checkout", "--detach", args.sha])
        if command(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"], capture=True):
            raise RuntimeError("supervisor exact checkout is dirty")
        command(["chown", "-R", "10001:10001", str(source), str(output)])
        command(["docker", "network", "create", network])
        lock = strict_json(CONTROL_ROOT / "tools/toolchain.lock.json")["tools"]
        postgres_image = f"{lock['postgres']['image']}@{lock['postgres']['digest']}"
        for index in range(5):
            name = f"{run_id}-pg-{index}"
            command(
                [
                    "docker", "run", "-d", "--name", name,
                    "--network", network, "--network-alias", f"pg-{index}",
                    "-e", "POSTGRES_DB=vane_test", "-e", "POSTGRES_USER=vane",
                    "-e", "POSTGRES_PASSWORD=vane_test",
                    "--health-cmd", "pg_isready -U vane -d vane_test",
                    "--health-interval", "2s", "--health-timeout", "2s", "--health-retries", "30",
                    postgres_image,
                ]
            )
            containers.append(name)
        for name in containers:
            wait_healthy(name)
        temporal_name = f"{run_id}-temporal"
        temporal_image = f"{lock['temporal_server']['image']}@{lock['temporal_server']['digest']}"
        command(
            [
                "docker", "run", "-d", "--name", temporal_name,
                "--network", network, "--network-alias", "temporal",
                "-e", "DB=postgres12", "-e", "DB_PORT=5432",
                "-e", "POSTGRES_USER=vane", "-e", "POSTGRES_PWD=vane_test",
                "-e", "POSTGRES_SEEDS=pg-4", temporal_image,
            ]
        )
        containers.append(temporal_name)
        write_dependencies(dependencies, containers[:5], temporal_name)

        tool_cache = Path(config["tool_cache"])
        go = tool_cache / "go" / lock["go"]["version"] / "bin/go"
        scanner = transaction / "testpolicyscan"
        build_env = {
            **os.environ,
            "GOWORK": "off",
            "GOTOOLCHAIN": "local",
            "CGO_ENABLED": "0",
        }
        result = subprocess.run(
            [str(go), "build", "-o", str(scanner), "./internal/testgate/cmd/testpolicyscan"],
            cwd=CONTROL_ROOT / "server",
            env=build_env,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError("fixed test-policy scanner build failed")

        common = [
            "docker", "run", "--rm", "--read-only", "--network", network,
            "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
            "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=2g",
            "--tmpfs", "/home/vane-build:rw,noexec,nosuid,nodev,size=64m,uid=10001,gid=10001,mode=0700",
            "-v", f"{source}:/workspace:rw",
            "-v", f"{CONTROL_ROOT}:/control:ro",
            "-v", f"{tool_cache}:/toolcache:ro",
            "-v", f"{config['history_dir']}:/histories:ro",
            "-v", f"{output}:/output:rw",
            "-v", f"{dependencies}:/dependencies.json:ro",
            "-v", f"{scanner}:/control-bin/testpolicyscan:ro",
        ]
        command(
            [
                *common,
                "--entrypoint", "/control-bin/testpolicyscan",
                config["runner_image"],
                "/workspace/server",
            ]
        )
        command(
            [
                *common,
                "-e", "PYTHONPATH=/control",
                "-e", "VANE_SOURCE_ROOT=/workspace",
                "-e", "VANE_WORK_ROOT=/output",
                "-e", "VANE_TOOL_CACHE=/toolcache",
                "-e", "VANE_TEMPORAL_HISTORY_DIR=/histories",
                "-e", "VANE_FULL_GATE_DEPENDENCIES=/dependencies.json",
                "-e", f"VANE_FULL_SHA={args.sha}",
                "-e", "VANE_FULL_GATE_EVIDENCE=/output/full-gate.json",
                config["runner_image"],
                "/control/ops/audit/full_gate.py",
            ]
        )
        evidence = output / "full-gate.json"
        rewrite_evidence(evidence, output)
        os.replace(output, args.output)
        print(json.dumps({"ok": True, "revision": args.sha, "evidence": str(args.output / "full-gate.json")}, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        for name in reversed(containers):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        subprocess.run(["docker", "network", "rm", network], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if transaction.exists():
            shutil.rmtree(transaction, ignore_errors=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"build supervisor refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
