"""Run the exact-SHA release gate directly on the local Mac.

Go and Web compilation happen on the trusted release workstation. PostgreSQL
and Temporal use short-lived native development processes; no runner, VM,
container, Docker socket, or production credential participates in the gate.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse
import uuid

from ops.cli.controller import (
    PolicyError,
    directory_tree_sha256,
    run_checked,
    run_go_tests_no_skips,
)


CONTROL_ROOT = Path(__file__).resolve().parents[2]
ROOT = Path(os.environ.get("VANE_SOURCE_ROOT", str(CONTROL_ROOT))).resolve()
SERVER = ROOT / "server"
WEB = ROOT / "web"
LOCK = json.loads(
    (CONTROL_ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
)["tools"]


def output(command: list[str], *, cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(
        command, cwd=cwd, env=env, text=True, capture_output=True, check=False
    )
    if result.returncode != 0:
        raise PolicyError(
            f"full gate command failed ({result.returncode}): {' '.join(command)}\n"
            f"{result.stderr}"
        )
    return result.stdout.strip()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def migration_tree_object(*, revision: str, path: str, env: dict[str, str]) -> str:
    value = output(
        ["git", "rev-parse", "--verify", f"{revision}:{path}"], cwd=ROOT, env=env
    )
    if not re.fullmatch(r"[0-9a-f]{40,64}", value):
        raise PolicyError("migration tree object is not an exact Git object ID")
    return value


def free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def require_native_postgres() -> dict[str, Path]:
    binaries: dict[str, Path] = {}
    for name in ("postgres", "initdb", "pg_ctl", "createdb"):
        resolved = shutil.which(name)
        if not resolved and sys.platform == "darwin":
            for prefix in (Path("/opt/homebrew"), Path("/usr/local")):
                candidate = prefix / "opt/postgresql@18/bin" / name
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    resolved = str(candidate)
                    break
        if not resolved:
            raise PolicyError(f"native PostgreSQL 18 executable is missing: {name}")
        path = Path(resolved)
        if path.is_symlink():
            path = path.resolve(strict=True)
        if not path.is_file() or not os.access(path, os.X_OK):
            raise PolicyError(f"native PostgreSQL executable is unsafe: {path}")
        binaries[name] = path
    version = output(
        [str(binaries["postgres"]), "--version"], cwd=ROOT, env=os.environ.copy()
    )
    if not re.search(r"PostgreSQL\) 18(?:\.|\s|$)", version):
        raise PolicyError(f"full gate requires native PostgreSQL 18, got: {version}")
    return binaries


def assert_disposable_database(
    *, data_dir: Path, database_url: str, owner_root: Path, expected_database: str
) -> None:
    """Prove destructive tests target this gate's private native cluster."""
    parsed = urlparse(database_url)
    try:
        data_dir.resolve(strict=True).relative_to(owner_root.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise PolicyError(
            "destructive migration test requires a proven disposable native cluster"
        ) from error
    if (
        data_dir.is_symlink()
        or not (data_dir / "PG_VERSION").is_file()
        or (data_dir / "PG_VERSION").read_text(encoding="ascii").strip() != "18"
        or parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname != "127.0.0.1"
        or not parsed.port
        or parsed.path != f"/{expected_database}"
        or not expected_database.startswith("vane_full_")
    ):
        raise PolicyError(
            "destructive migration test requires a proven disposable native cluster"
        )


def wait_temporal(temporal: Path, address: str, env: dict[str, str]) -> None:
    for _ in range(90):
        result = subprocess.run(
            [
                str(temporal), "operator", "cluster", "health",
                "--address", address,
                "--disable-config-file", "--disable-config-env",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise PolicyError("native Temporal development server did not become healthy")


def main() -> int:
    work_root_raw = os.environ.get("VANE_WORK_ROOT", "")
    work_root = Path(work_root_raw)
    if not work_root.is_absolute() or work_root.is_symlink() or not work_root.is_dir():
        raise PolicyError("VANE_WORK_ROOT must be an existing absolute directory")
    head = output(["git", "rev-parse", "HEAD"], cwd=ROOT, env=os.environ.copy())
    requested = os.environ.get("VANE_FULL_SHA", "")
    if head != requested or not re.fullmatch(r"[0-9a-f]{40}", requested):
        raise PolicyError("full gate checkout differs from requested exact SHA")
    if output(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        env=os.environ.copy(),
    ):
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
    postgres = require_native_postgres()

    run_id = f"vane-full-{uuid.uuid4().hex[:12]}"
    artifacts = work_root / run_id
    artifacts.mkdir(mode=0o700)
    runtime = Path(tempfile.mkdtemp(prefix=f".{run_id}-runtime-", dir=work_root))

    previous_web_policy = subprocess.run(
        ["git", "cat-file", "-e", f"{head}^1:web/coverage-baseline.json"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
    ).returncode == 0
    coverage_base = f"{head}^1" if previous_web_policy else "legacy/vane-web/final"
    previous_server_policy = subprocess.run(
        ["git", "cat-file", "-e", f"{head}^1:tools/checks/server-coverage-baseline.json"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
    ).returncode == 0
    server_coverage_base = (
        f"{head}^1" if previous_server_policy else "legacy/vane/pre-monorepo"
    )
    base_migration_path = "server/store/migrations" if previous_server_policy else "store/migrations"
    current_migration_tree = migration_tree_object(
        revision=head, path="server/store/migrations", env=os.environ.copy()
    )
    base_migration_tree = migration_tree_object(
        revision=server_coverage_base, path=base_migration_path, env=os.environ.copy()
    )
    server_rollback_safe = current_migration_tree == base_migration_tree
    if not server_rollback_safe:
        raise PolicyError(
            "migration bytes changed; release requires explicit previous-binary/"
            "upgraded-schema compatibility before rollback is permitted"
        )

    env = {
        **os.environ,
        "GOWORK": "off",
        "GOTOOLCHAIN": "local",
        "GOSUMDB": "sum.golang.org",
        "VANE_FULL_GATE": "1",
        "VANE_TEMPORAL_CLI_PATH": str(temporal),
        "VANE_RELEASE_SHA": head,
        "VANE_REQUIRE_CLEAN_RELEASE": "1",
        "VANE_COVERAGE_HEAD_SHA": head,
        "VANE_COVERAGE_BASE_SHA": coverage_base,
        "PATH": f"{go.parent}:{node_bin}:{os.environ.get('PATH', '')}",
    }
    original_environment = os.environ.copy()
    temporal_process: subprocess.Popen[bytes] | None = None
    postgres_clusters: list[Path] = []
    try:
        os.environ.update(env)
        urls: list[str] = []
        for index in range(4):
            # Store migration tests create and revoke cluster-wide roles. A
            # separate database in one cluster is not isolation: parallel
            # shards would race through pg_authid. Give each shard its own
            # short-lived native PostgreSQL cluster instead.
            data_dir = runtime / f"postgres-{index}"
            run_checked(
                [
                    str(postgres["initdb"]), "-D", str(data_dir),
                    "--no-locale", "--encoding=UTF8", "--auth=trust",
                    "--username=vane",
                ],
                cwd=ROOT,
            )
            pg_port = free_local_port()
            run_checked(
                [
                    str(postgres["pg_ctl"]), "-D", str(data_dir),
                    "-l", str(artifacts / f"postgres-{index}.log"),
                    "-o", f"-h 127.0.0.1 -p {pg_port}", "-w", "start",
                ],
                cwd=ROOT,
            )
            postgres_clusters.append(data_dir)
            database = f"vane_full_{index}"
            run_checked(
                [
                    str(postgres["createdb"]), "-h", "127.0.0.1", "-p", str(pg_port),
                    "-U", "vane", database,
                ],
                cwd=ROOT,
            )
            url = (
                f"postgres://vane@127.0.0.1:{pg_port}/{database}?sslmode=disable"
            )
            assert_disposable_database(
                data_dir=data_dir, database_url=url, owner_root=runtime,
                expected_database=database,
            )
            urls.append(url)

        temporal_port = free_local_port()
        temporal_address = f"127.0.0.1:{temporal_port}"
        with (artifacts / "temporal.log").open("wb") as temporal_log:
            temporal_process = subprocess.Popen(
                [
                    str(temporal), "server", "start-dev", "--headless",
                    "--ip", "127.0.0.1", "--port", str(temporal_port),
                    "--db-filename", str(runtime / "temporal.db"),
                    "--disable-config-file", "--disable-config-env",
                    "--log-format", "json", "--log-level", "warn",
                ],
                cwd=ROOT,
                env=env,
                stdout=temporal_log,
                stderr=subprocess.STDOUT,
            )
        wait_temporal(temporal, temporal_address, env)
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
        os.environ["VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST"] = "1"
        run_checked(
            [str(go), "run", "./cmd/storetestshard", "run", "--artifacts", str(store_artifacts)]
            + [item for url in urls[:3] for item in ("--database-url", url)],
            cwd=SERVER,
        )
        os.environ.pop("VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST", None)
        store_package = output([str(go), "list", "./store"], cwd=SERVER, env=env)
        packages = [
            item for item in output([str(go), "list", "./..."], cwd=SERVER, env=env).splitlines()
            if item != store_package
        ]
        non_store = artifacts / "non-store.coverage.out"
        os.environ["DATABASE_URL"] = urls[3]
        run_go_tests_no_skips(
            [
                str(go), "test", "-json", "-race", "-p=1", "-parallel=1",
                "-count=1", "-timeout=40m", "-covermode=atomic",
                f"-coverprofile={non_store}", *packages,
            ],
            cwd=SERVER,
        )
        for package, test in (
            ("./internal/agentfirstaudit", "^TestRetentionClockEvidenceRealTemporalHistory$"),
            ("./periodicbrief", "^TestPeriodicWorkflowExternalTerminationReplaysAndRecoveryConverges$"),
        ):
            run_go_tests_no_skips(
                [str(go), "test", "-json", "-tags=integration", package, "-run", test, "-count=1"],
                cwd=SERVER,
            )

        server_coverage = artifacts / "coverage.out"
        run_checked(
            [
                str(go), "run", "./cmd/storetestshard", "merge-coverage",
                "--output", str(server_coverage),
                str(store_artifacts / "store.coverage.out"), str(non_store),
            ],
            cwd=SERVER,
        )
        coverage_report = output(
            [str(go), "tool", "cover", f"-func={server_coverage}"], cwd=SERVER, env=env
        )
        total = re.search(
            r"^total:\s+\(statements\)\s+([0-9]+(?:\.[0-9]+)?)%$",
            coverage_report,
            re.MULTILINE,
        )
        if total is None or float(total.group(1)) <= 0:
            raise PolicyError("server merged coverage is missing or zero")
        (artifacts / "coverage-functions.txt").write_text(
            coverage_report + "\n", encoding="utf-8"
        )
        run_checked(
            [
                sys.executable, str(CONTROL_ROOT / "tools/checks/server_coverage.py"),
                "--profile", str(server_coverage),
                "--baseline", str(ROOT / "tools/checks/server-coverage-baseline.json"),
                "--repo-root", str(ROOT), "--base", server_coverage_base, "--head", head,
            ],
            cwd=ROOT,
        )

        inventory = json.loads(
            (ROOT / "contracts/release/server-binaries.json").read_text(encoding="utf-8")
        )["binaries"]
        binary_dir = artifacts / "bin"
        binary_dir.mkdir()
        build_env = {
            **env, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"
        }
        linker_flags = (
            f"-buildid=vane/{head}/clean "
            f"-X=github.com/YouToco/vane/server/internal/releaseinfo.revision={head} "
            "-X=github.com/YouToco/vane/server/internal/releaseinfo.clean=true"
        )
        for item in inventory:
            subprocess.run(
                [
                    str(go), "build", "-buildvcs=true", "-trimpath",
                    "-ldflags", linker_flags,
                    "-o", str(binary_dir / item["name"]), item["package"],
                ],
                cwd=SERVER,
                env=build_env,
                check=True,
            )
        for binary in binary_dir.iterdir():
            run_checked(
                [str(CONTROL_ROOT / "ops/audit/check-go-build-info.sh"), str(binary), head],
                cwd=ROOT,
            )

        scripts = [str(path) for path in (ROOT / "ops").glob("**/*.sh")]
        run_checked([str(shellcheck), *scripts], cwd=ROOT)
        run_checked([str(npm), "ci"], cwd=WEB)
        run_checked([str(npm), "audit", "--audit-level=high"], cwd=WEB)
        for command in ("test:coverage", "typecheck", "prototype:p0a:build", "build"):
            run_checked([str(npm), "run", command], cwd=WEB)
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
            or not re.fullmatch(r"[0-9a-f]{64}", marker_value.get("tree_sha256", ""))
            or isinstance(marker_value.get("file_count"), bool)
            or not isinstance(marker_value.get("file_count"), int)
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
                "binary_dir": binary_dir.resolve().relative_to(work_root.resolve()).as_posix(),
                "web_dist": verified_web_dist.resolve().relative_to(work_root.resolve()).as_posix(),
                "binary_tree_sha256": directory_tree_sha256(binary_dir),
                "web_tree_sha256": directory_tree_sha256(verified_web_dist),
                "web_coverage_sha256": file_sha256(web_coverage),
                "server_coverage_sha256": file_sha256(server_coverage),
                "release_marker_sha256": file_sha256(marker),
                "server_source_tree_sha256": directory_tree_sha256(SERVER),
                "infra_tree_sha256": directory_tree_sha256(ROOT / "infra/production"),
                "migration_tree_object": current_migration_tree,
                "server_rollback_safe": server_rollback_safe,
            }
            evidence_path.write_text(
                json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
        return 0
    finally:
        os.environ.pop("VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST", None)
        if temporal_process is not None:
            temporal_process.terminate()
            try:
                temporal_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                temporal_process.kill()
                temporal_process.wait(timeout=5)
        for data_dir in reversed(postgres_clusters):
            subprocess.run(
                [str(postgres["pg_ctl"]), "-D", str(data_dir), "-m", "fast", "-w", "stop"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        shutil.rmtree(runtime, ignore_errors=True)
        os.environ.clear()
        os.environ.update(original_environment)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PolicyError as error:
        print(f"full gate refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
