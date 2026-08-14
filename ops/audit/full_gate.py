"""Credential-free PG18/race/coverage/Temporal/Web release gate orchestration."""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import urlparse
import uuid

from ops.cli.controller import (
    PolicyError,
    directory_tree_sha256,
    run_checked,
    run_go_tests_no_skips,
)


ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
WEB = ROOT / "web"
LOCK = json.loads((ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8"))["tools"]


def assert_disposable_database(*, container_name: str, database_url: str, container_id: str) -> None:
    """Prove a destructive store target is a localhost port of this Docker run."""
    parsed = urlparse(database_url)
    if (
        not container_name.startswith("vane-full-")
        or parsed.scheme not in ("postgres", "postgresql")
        or parsed.hostname != "127.0.0.1"
        or not parsed.port
        or len(container_id) != 64
        or any(char not in "0123456789abcdef" for char in container_id)
    ):
        raise PolicyError("destructive migration test requires proven disposable localhost containers")


def output(command: list[str], *, cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise PolicyError(f"full gate command failed ({result.returncode}): {' '.join(command)}\n{result.stderr}")
    return result.stdout.strip()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    work_root_raw = os.environ.get("VANE_WORK_ROOT", "")
    history_raw = os.environ.get("VANE_TEMPORAL_HISTORY_DIR", "")
    work_root = Path(work_root_raw)
    history = Path(history_raw)
    if not work_root.is_absolute() or work_root.is_symlink() or not work_root.is_dir():
        raise PolicyError("VANE_WORK_ROOT must be an existing absolute directory")
    if not history.is_absolute() or history.is_symlink() or not history.is_dir():
        raise PolicyError("VANE_TEMPORAL_HISTORY_DIR must be a broker-provided absolute directory")
    head = output(["git", "rev-parse", "HEAD"], cwd=ROOT, env=os.environ.copy())
    requested = os.environ.get("VANE_FULL_SHA", "")
    if (
        head != requested
        or len(requested) != 40
        or any(char not in "0123456789abcdef" for char in requested)
    ):
        raise PolicyError("full gate checkout differs from requested exact SHA")
    if output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT, env=os.environ.copy()):
        raise PolicyError("full gate requires a clean exact-source worktree")
    cache = Path(os.environ.get("VANE_TOOL_CACHE", str(ROOT / ".vane/tool-cache")))
    go = cache / "go" / LOCK["go"]["version"] / "bin/go"
    node_bin = cache / "node" / LOCK["node"]["version"] / "bin"
    temporal = cache / "temporal_cli" / LOCK["temporal_cli"]["version"] / "temporal"
    govuln = cache / "govulncheck" / LOCK["govulncheck"]["version"] / "govulncheck"
    shellcheck = cache / "shellcheck" / LOCK["shellcheck"]["version"] / "shellcheck"
    for binary in (go, node_bin / "node", temporal, govuln, shellcheck):
        if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
            raise PolicyError(f"full gate locked executable is unavailable: {binary}")
    npm = node_bin / "npm"
    try:
        npm.resolve().relative_to((cache / "node" / LOCK["node"]["version"]).resolve())
    except (OSError, ValueError) as error:
        raise PolicyError("locked npm executable escapes the Node installation") from error
    if not npm.is_file() or not os.access(npm, os.X_OK):
        raise PolicyError(f"full gate locked executable is unavailable: {npm}")
    run_id = f"vane-full-{uuid.uuid4().hex[:12]}"
    artifacts = work_root / run_id
    artifacts.mkdir(mode=0o700)
    previous_has_coverage_policy = subprocess.run(
        ["git", "cat-file", "-e", f"{head}^1:web/coverage-baseline.json"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0
    coverage_base = f"{head}^1" if previous_has_coverage_policy else "legacy/vane-web/final"
    output(["git", "rev-parse", "--verify", coverage_base], cwd=ROOT, env=os.environ.copy())
    previous_has_server_coverage_policy = subprocess.run(
        ["git", "cat-file", "-e", f"{head}^1:tools/checks/server-coverage-baseline.json"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0
    server_coverage_base = (
        f"{head}^1" if previous_has_server_coverage_policy else "legacy/vane/pre-monorepo"
    )
    output(
        ["git", "rev-parse", "--verify", server_coverage_base],
        cwd=ROOT,
        env=os.environ.copy(),
    )
    env = {
        **os.environ,
        "GOWORK": "off",
        "GOTOOLCHAIN": "local",
        "GOSUMDB": "sum.golang.org",
        "VANE_FULL_GATE": "1",
        "VANE_TEMPORAL_CLI_PATH": str(temporal),
        "VANE_TEMPORAL_HISTORY_DIR": str(history),
        "VANE_RELEASE_SHA": head,
        "VANE_REQUIRE_CLEAN_RELEASE": "1",
        "VANE_COVERAGE_HEAD_SHA": head,
        "VANE_COVERAGE_BASE_SHA": coverage_base,
        "PATH": f"{go.parent}:{node_bin}:{os.environ.get('PATH', '')}",
    }
    image = f"{LOCK['postgres']['image']}@{LOCK['postgres']['digest']}"
    containers: list[str] = []
    network = f"{run_id}-network"
    original_environment = os.environ.copy()
    os.environ.update(env)
    try:
        subprocess.run(["docker", "network", "create", network], check=True, stdout=subprocess.DEVNULL)
        urls = []
        for index in range(5):
            name = f"{run_id}-pg-{index}"
            subprocess.run(
                [
                    "docker", "run", "-d", "--name", name,
                    "--network", network, "--network-alias", f"pg-{index}",
                    "-e", "POSTGRES_DB=vane_test", "-e", "POSTGRES_USER=vane",
                    "-e", "POSTGRES_PASSWORD=vane_test", "-p", "127.0.0.1::5432",
                    "--health-cmd", "pg_isready -U vane -d vane_test",
                    "--health-interval", "2s", "--health-timeout", "2s", "--health-retries", "30",
                    image,
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            containers.append(name)
        for name in containers:
            for _ in range(60):
                if output(["docker", "inspect", "-f", "{{.State.Health.Status}}", name], cwd=ROOT, env=env) == "healthy":
                    break
                time.sleep(1)
            else:
                raise PolicyError(f"PostgreSQL 18 container did not become healthy: {name}")
            port = output(["docker", "port", name, "5432/tcp"], cwd=ROOT, env=env).rsplit(":", 1)[-1]
            urls.append(f"postgres://vane:vane_test@127.0.0.1:{port}/vane_test?sslmode=disable")

        temporal_name = f"{run_id}-temporal"
        temporal_image = f"{LOCK['temporal_server']['image']}@{LOCK['temporal_server']['digest']}"
        subprocess.run(
            [
                "docker", "run", "-d", "--name", temporal_name, "--network", network,
                "-e", "DB=postgres12", "-e", "DB_PORT=5432",
                "-e", "POSTGRES_USER=vane", "-e", "POSTGRES_PWD=vane_test",
                "-e", "POSTGRES_SEEDS=pg-4", "-p", "127.0.0.1::7233", temporal_image,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        containers.append(temporal_name)
        temporal_port = output(["docker", "port", temporal_name, "7233/tcp"], cwd=ROOT, env=env).rsplit(":", 1)[-1]
        temporal_address = f"127.0.0.1:{temporal_port}"
        for _ in range(90):
            health = subprocess.run(
                [str(temporal), "operator", "cluster", "health", "--address", temporal_address],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if health.returncode == 0:
                break
            time.sleep(1)
        else:
            raise PolicyError("canonical Temporal Server did not become healthy")
        run_checked([str(temporal), "operator", "cluster", "system-info", "--address", temporal_address], cwd=ROOT)
        os.environ["VANE_TEMPORAL_ADDRESS"] = temporal_address
        run_go_tests_no_skips(
            [
                str(go), "test", "-json", "-tags=integration",
                "./internal/temporalintegration",
                "-run", "^TestCanonicalTemporalServerPostgreSQLRoundTrip$", "-count=1",
            ],
            cwd=SERVER,
        )

        run_checked([str(go), "mod", "download"], cwd=SERVER)
        run_checked([str(govuln), "./..."], cwd=SERVER)
        run_checked([str(go), "vet", "./..."], cwd=SERVER)
        run_checked([str(go), "run", "./cmd/agenttoolinventory", "-check"], cwd=SERVER)
        store_artifacts = artifacts / "store"
        # Keep this Python 3.9 compatible: the controller is deliberately usable
        # on the Ubuntu 24.04 broker before its locked Python is provisioned.
        if len(containers[:3]) != len(urls[:3]):
            raise PolicyError("store shard container/database URL cardinality differs")
        for name, url in zip(containers[:3], urls[:3]):
            container_id = output(["docker", "inspect", "-f", "{{.Id}}", name], cwd=ROOT, env=env)
            assert_disposable_database(
                container_name=name, database_url=url, container_id=container_id
            )
        os.environ["VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST"] = "1"
        run_checked(
            [str(go), "run", "./cmd/storetestshard", "run", "--artifacts", str(store_artifacts)]
            + [item for url in urls[:3] for item in ("--database-url", url)],
            cwd=SERVER,
        )
        os.environ.pop("VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST", None)
        store_package = output([str(go), "list", "./store"], cwd=SERVER, env=env)
        packages = [item for item in output([str(go), "list", "./..."], cwd=SERVER, env=env).splitlines() if item != store_package]
        non_store = artifacts / "non-store.coverage.out"
        test_env = {**env, "DATABASE_URL": urls[3]}
        os.environ.update(test_env)
        run_go_tests_no_skips(
            [str(go), "test", "-json", "-race", "-p=1", "-parallel=1", "-count=1", "-timeout=40m", "-covermode=atomic", f"-coverprofile={non_store}", *packages],
            cwd=SERVER,
        )
        for package, test in (
            ("./internal/agentfirstaudit", "^TestRetentionClockEvidenceRealTemporalHistory$"),
            ("./periodicbrief", "^TestPeriodicWorkflowExternalTerminationReplaysAndRecoveryConverges$"),
        ):
            run_go_tests_no_skips([str(go), "test", "-json", "-tags=integration", package, "-run", test, "-count=1"], cwd=SERVER)
        run_go_tests_no_skips([str(go), "test", "-json", "-tags=productionreplay", "./cmd/server", "-run", "^TestProductionHistoryReplay$", "-count=1"], cwd=SERVER)
        server_coverage = artifacts / "coverage.out"
        run_checked([str(go), "run", "./cmd/storetestshard", "merge-coverage", "--output", str(server_coverage), str(store_artifacts / "store.coverage.out"), str(non_store)], cwd=SERVER)
        coverage_function_report = output(
            [str(go), "tool", "cover", f"-func={server_coverage}"], cwd=SERVER, env=env
        )
        total_match = re.search(r"^total:\s+\(statements\)\s+([0-9]+(?:\.[0-9]+)?)%$", coverage_function_report, re.MULTILINE)
        if total_match is None or float(total_match.group(1)) <= 0:
            raise PolicyError("server merged coverage is missing or zero")
        (artifacts / "coverage-functions.txt").write_text(
            coverage_function_report + "\n", encoding="utf-8"
        )
        run_checked(
            [
                sys.executable,
                str(ROOT / "tools/checks/server_coverage.py"),
                "--profile", str(server_coverage),
                "--baseline", str(ROOT / "tools/checks/server-coverage-baseline.json"),
                "--repo-root", str(ROOT),
                "--base", server_coverage_base,
                "--head", head,
            ],
            cwd=ROOT,
        )

        inventory = json.loads((ROOT / "contracts/release/server-binaries.json").read_text(encoding="utf-8"))["binaries"]
        binary_dir = artifacts / "bin"
        binary_dir.mkdir()
        build_env = {**env, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"}
        for item in inventory:
            subprocess.run([str(go), "build", "-buildvcs=true", "-o", str(binary_dir / item["name"]), item["package"]], cwd=SERVER, env=build_env, check=True)
        for binary in binary_dir.iterdir():
            run_checked([str(ROOT / "ops/audit/check-go-build-info.sh"), str(binary), output(["git", "rev-parse", "HEAD"], cwd=ROOT, env=env)], cwd=ROOT)

        scripts = [str(path) for path in (ROOT / "ops").glob("**/*.sh")]
        run_checked([str(shellcheck), *scripts], cwd=ROOT)
        run_checked([str(node_bin / "npm"), "ci"], cwd=WEB)
        run_checked([str(node_bin / "npm"), "audit", "--audit-level=high"], cwd=WEB)
        for command in (
            "test:coverage",
            "typecheck",
            "prototype:p0a:build",
            "build",
        ):
            run_checked([str(node_bin / "npm"), "run", command], cwd=WEB)
        marker = WEB / "dist/.well-known/vane-release.json"
        if marker.is_symlink() or not marker.is_file():
            raise PolicyError("Web release marker evidence is missing")
        marker_value = json.loads(marker.read_text(encoding="utf-8"))
        if (
            list(marker_value) != [
                "schema", "source_revision", "source_dirty", "tree_sha256", "file_count"
            ]
            or marker_value["schema"] != "vane.web-release/v1"
            or marker_value["source_revision"] != head
            or marker_value["source_dirty"] is not False
            or not isinstance(marker_value["tree_sha256"], str)
            or len(marker_value["tree_sha256"]) != 64
            or any(char not in "0123456789abcdef" for char in marker_value["tree_sha256"])
            or isinstance(marker_value["file_count"], bool)
            or not isinstance(marker_value["file_count"], int)
            or marker_value["file_count"] < 1
        ):
            raise PolicyError("Web release marker is not bound to exact revision/tree digest")
        verified_web_dist = artifacts / "web-dist"
        shutil.copytree(WEB / "dist", verified_web_dist, symlinks=False)
        web_coverage = artifacts / "web-coverage-summary.json"
        shutil.copy2(WEB / "coverage/coverage-summary.json", web_coverage)
        evidence_raw = os.environ.get("VANE_FULL_GATE_EVIDENCE", "")
        if evidence_raw:
            evidence_path = Path(evidence_raw)
            try:
                evidence_path.resolve().relative_to(work_root.resolve())
            except (OSError, ValueError) as error:
                raise PolicyError("full gate evidence must stay under VANE_WORK_ROOT") from error
            if evidence_path.is_symlink() or evidence_path.exists():
                raise PolicyError("full gate evidence path already exists or is unsafe")
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            evidence = {
                "schema": "vane.full-gate-evidence/v1",
                "revision": head,
                "binary_dir": str(binary_dir.resolve()),
                "web_dist": str(verified_web_dist.resolve()),
                "binary_tree_sha256": directory_tree_sha256(binary_dir),
                "web_tree_sha256": directory_tree_sha256(verified_web_dist),
                "web_coverage_sha256": file_sha256(web_coverage),
                "server_coverage_sha256": file_sha256(server_coverage),
                "release_marker_sha256": file_sha256(marker),
            }
            evidence_path.write_text(
                json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
        return 0
    finally:
        for name in reversed(containers):
            subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "network", "rm", network], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.environ.clear()
        os.environ.update(original_environment)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PolicyError as error:
        print(f"full gate refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
