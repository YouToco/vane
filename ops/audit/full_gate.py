"""Run the exact-SHA release gate directly on the local Mac.

Go and Web compilation happen on the trusted release workstation. PostgreSQL
and Temporal use short-lived native development processes; no runner, VM,
container, Docker socket, or production credential participates in the gate.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
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

# Each package below contains PostgreSQL-backed tests. It gets a private
# database and remains internally serial. Every other package runs without
# database environment variables, so a missing registration fails closed in
# VANE_FULL_GATE instead of racing on shared schema state or silently skipping.
NON_STORE_DATABASE_PACKAGE_DIRS = (
    "a2a",
    "api",
    "evolver",
    "feishu",
    "llm",
    "periodicbrief",
    "researchgateway",
    "scheduler",
    "task",
)
DATABASE_TEST_MARKER = re.compile(
    r"DATABASE_URL|VANE_TEST_DATABASE_URL|"
    r"testgate\.(?:Database|CreateDatabase|DestructiveDatabase|PostgreSQLURL)"
)
STORE_TIMING_CACHE_VERSION = "store-race-timings-v1"
STORE_TIMING_SEED_NAME = "store.timings.jsonl"


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


def validate_database_package_inventory(
    *, server_root: Path, declared: tuple[str, ...]
) -> None:
    if tuple(sorted(set(declared))) != declared:
        raise PolicyError("non-Store database package inventory is not sorted and unique")
    detected: set[str] = set()
    for test_file in server_root.rglob("*_test.go"):
        relative = test_file.relative_to(server_root)
        if not relative.parts or relative.parts[0] == "store":
            continue
        if DATABASE_TEST_MARKER.search(test_file.read_text(encoding="utf-8")):
            detected.add(relative.parent.as_posix())
    expected = set(declared)
    if detected != expected:
        raise PolicyError(
            "non-Store database package inventory drift: "
            f"missing={sorted(detected - expected)} stale={sorted(expected - detected)}"
        )


def partition_non_store_packages(
    *, packages: list[str], module_path: str
) -> tuple[list[str], list[str]]:
    database_packages = [
        f"{module_path}/{relative}" for relative in NON_STORE_DATABASE_PACKAGE_DIRS
    ]
    package_set = set(packages)
    missing = sorted(set(database_packages) - package_set)
    if missing:
        raise PolicyError(f"declared non-Store database packages are missing: {missing}")
    database_set = set(database_packages)
    pure_packages = [package for package in packages if package not in database_set]
    if not pure_packages:
        raise PolicyError("non-Store pure package group is empty")
    return pure_packages, database_packages


def database_package_lanes(
    packages: list[str], *, lane_count: int
) -> tuple[tuple[tuple[int, str], ...], ...]:
    if lane_count < 1 or lane_count > len(packages):
        raise PolicyError("non-Store database lane count is invalid")
    lanes: list[list[tuple[int, str]]] = [[] for _ in range(lane_count)]
    for index, package in enumerate(packages):
        lanes[index % lane_count].append((index, package))
    return tuple(tuple(lane) for lane in lanes)


def require_private_cache_directory(path: Path) -> None:
    if path.is_symlink():
        raise PolicyError(f"Store timing cache directory is unsafe: {path}")
    path.mkdir(parents=False, exist_ok=True, mode=0o700)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise PolicyError(f"Store timing cache directory is unsafe: {path}")


def select_store_timing_seed(
    *, cache_root: Path, revisions: tuple[str, ...]
) -> tuple[Path, str] | None:
    if (
        not cache_root.is_absolute()
        or cache_root.is_symlink()
        or cache_root.parent.is_symlink()
    ):
        raise PolicyError("Store timing cache root is unsafe")
    require_private_cache_directory(cache_root)
    timing_root = cache_root / STORE_TIMING_CACHE_VERSION
    require_private_cache_directory(timing_root)
    for revision in dict.fromkeys(revisions):
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise PolicyError("Store timing cache revision is invalid")
        revision_root = timing_root / revision
        if not revision_root.exists():
            continue
        require_private_cache_directory(revision_root)
        seed = revision_root / STORE_TIMING_SEED_NAME
        if not seed.exists():
            continue
        metadata = seed.lstat()
        if (
            seed.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o022
            or metadata.st_size <= 0
            or metadata.st_size > 16 * 1024 * 1024
        ):
            raise PolicyError(f"Store timing seed is unsafe: {seed}")
        return revision_root, revision
    return None


def publish_store_timing_seed(
    *, cache_root: Path, revision: str, source: Path
) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise PolicyError("Store timing cache revision is invalid")
    selected = select_store_timing_seed(cache_root=cache_root, revisions=(revision,))
    timing_root = cache_root / STORE_TIMING_CACHE_VERSION
    revision_root = timing_root / revision
    if selected is None:
        require_private_cache_directory(revision_root)
    if source.is_symlink() or not source.is_file():
        raise PolicyError("generated Store timing seed is unsafe")
    payload = source.read_bytes()
    if not payload or len(payload) > 16 * 1024 * 1024:
        raise PolicyError("generated Store timing seed has an invalid size")
    target = revision_root / STORE_TIMING_SEED_NAME
    temporary = revision_root / f".{STORE_TIMING_SEED_NAME}.{os.getpid()}.tmp"
    try:
        with temporary.open("xb") as handle:
            os.chmod(temporary, 0o600)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory_fd = os.open(revision_root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def migration_tree_object(*, revision: str, path: str, env: dict[str, str]) -> str:
    value = output(
        ["git", "rev-parse", "--verify", f"{revision}:{path}"], cwd=ROOT, env=env
    )
    if not re.fullmatch(r"[0-9a-f]{40,64}", value):
        raise PolicyError("migration tree object is not an exact Git object ID")
    return value


def git_path_exists(*, revision: str, path: str) -> bool:
    return subprocess.run(
        ["git", "cat-file", "-e", f"{revision}:{path}"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def migration_inventory(*, revision: str, path: str) -> dict[str, str]:
    result = subprocess.run(
        [
            "git", "ls-tree", "-r", "-z", "--full-tree",
            "--format=%(objectname)%x09%(path)", revision, "--", path,
        ],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError(f"cannot inventory migrations at {revision}:{path}")
    prefix = path.rstrip("/") + "/"
    inventory: dict[str, str] = {}
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        try:
            object_id_raw, file_raw = raw.split(b"\t", 1)
            object_id = object_id_raw.decode("ascii")
            file_name = file_raw.decode("utf-8")
        except (UnicodeDecodeError, ValueError) as error:
            raise PolicyError("migration inventory contains an invalid Git entry") from error
        if not file_name.startswith(prefix):
            raise PolicyError("migration inventory escaped its canonical directory")
        relative = file_name[len(prefix):]
        if (
            not relative
            or "/" in relative
            or not relative.endswith(".sql")
            or not re.fullmatch(r"[0-9]{3}_[a-z0-9_]+\.sql", relative)
            or not re.fullmatch(r"[0-9a-f]{40,64}", object_id)
            or relative in inventory
        ):
            raise PolicyError(f"migration inventory entry is not canonical: {relative!r}")
        inventory[relative] = object_id
    if not inventory:
        raise PolicyError(f"migration inventory is empty: {revision}:{path}")
    return inventory


def additive_migration_delta(
    *, base: dict[str, str], current: dict[str, str]
) -> dict[str, str]:
    removed = sorted(set(base) - set(current))
    changed = sorted(name for name in set(base) & set(current) if base[name] != current[name])
    if removed or changed:
        raise PolicyError(
            "migration history is not append-only: "
            f"removed={removed} changed={changed}"
        )
    added = {name: current[name] for name in sorted(set(current) - set(base))}
    if added:
        highest_base = max(int(name[:3]) for name in base)
        non_forward = [name for name in added if int(name[:3]) <= highest_base]
        if non_forward:
            raise PolicyError(
                "new migrations must advance beyond the retained migration history: "
                f"{non_forward}"
            )
    return added


def extract_git_revision(*, revision: str, destination: Path) -> None:
    destination.mkdir(mode=0o700)
    archive = subprocess.Popen(
        ["git", "archive", "--format=tar", revision],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert archive.stdout is not None
    assert archive.stderr is not None
    try:
        with tarfile.open(fileobj=archive.stdout, mode="r|") as payload:
            for member in payload:
                name = PurePosixPath(member.name)
                if (
                    name.is_absolute()
                    or not name.parts
                    or any(part in {"", ".", ".."} for part in name.parts)
                    or not (member.isdir() or member.isreg())
                    or member.mode & 0o7000
                ):
                    raise PolicyError("previous revision archive contains an unsafe member")
                target = destination.joinpath(*name.parts)
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                if member.isdir():
                    target.mkdir(exist_ok=True, mode=0o700)
                    continue
                source = payload.extractfile(member)
                if source is None:
                    raise PolicyError("cannot read previous revision archive member")
                with target.open("xb") as output_file:
                    shutil.copyfileobj(source, output_file)
                target.chmod(0o755 if member.mode & 0o111 else 0o600)
    finally:
        archive.stdout.close()
    stderr = archive.stderr.read().decode("utf-8", errors="replace")
    if archive.wait() != 0:
        raise PolicyError(f"cannot extract rollback base revision: {stderr.strip()}")


def run_psql(*, psql: Path, database_url: str, script: str, cwd: Path) -> str:
    result = subprocess.run(
        [str(psql), database_url, "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1"],
        cwd=cwd,
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise PolicyError(
            "rollback compatibility database fixture failed: "
            + result.stderr.strip()
        )
    return result.stdout.strip()


def prove_previous_binary_compatibility(
    *,
    base_revision: str,
    current_revision: str,
    added_migrations: dict[str, str],
    database_url: str,
    runtime: Path,
    artifacts: Path,
    go: Path,
    postgres: dict[str, Path],
    env: dict[str, str],
) -> tuple[Path, bool]:
    proof_path = artifacts / "server-rollback-compatibility.json"
    if not added_migrations:
        proof = {
            "schema": "vane.server-rollback-compatibility/v1",
            "base_revision": base_revision,
            "current_revision": current_revision,
            "mode": "identical-migration-history",
            "added_migrations": [],
            "previous_gate_sha256": None,
            "previous_gate_output_sha256": None,
            "status": "passed",
        }
        proof_path.write_text(
            json.dumps(proof, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        return proof_path, True

    migrate_env = {**env, "VANE_MIGRATION_DB_URL": database_url}
    migration = subprocess.run(
        [str(go), "run", "./cmd/migrate"],
        cwd=SERVER,
        env=migrate_env,
        capture_output=True,
        text=True,
        check=False,
    )
    if migration.returncode != 0:
        raise PolicyError(
            "current migrations failed in rollback compatibility database: "
            + migration.stderr.strip()
        )

    password = "vane-full-compat-" + uuid.uuid4().hex
    user_value = run_psql(
        psql=postgres["psql"],
        database_url=database_url,
        cwd=ROOT,
        script=(
            "ALTER ROLE vane_server_runtime LOGIN PASSWORD '" + password + "';\n"
            "WITH inserted AS (\n"
            "  INSERT INTO users(feishu_open_id,name)\n"
            "  VALUES ('vane-full-rollback-compat','rollback compatibility')\n"
            "  RETURNING id\n"
            ")\n"
            "INSERT INTO memberships(tenant_id,user_id,role)\n"
            "SELECT 1,id,'owner' FROM inserted RETURNING user_id;\n"
        ),
    )
    if not re.fullmatch(r"[1-9][0-9]*", user_value):
        raise PolicyError("rollback compatibility fixture did not return one exact user")

    previous_source = runtime / "rollback-base-source"
    extract_git_revision(revision=base_revision, destination=previous_source)
    if (previous_source / "server/go.mod").is_file():
        previous_module = previous_source / "server"
    elif (previous_source / "go.mod").is_file():
        previous_module = previous_source
    else:
        raise PolicyError("rollback base has no supported Go server module")
    previous_gate = runtime / "rollback-base-gate"
    build = subprocess.run(
        [str(go), "build", "-trimpath", "-o", str(previous_gate), "./cmd/gate"],
        cwd=previous_module,
        env={**env, "GOWORK": "off", "GOTOOLCHAIN": "local"},
        capture_output=True,
        text=True,
        check=False,
    )
    if build.returncode != 0:
        raise PolicyError(
            "rollback base gate does not build with the locked toolchain: "
            + build.stderr.strip()
        )

    parsed = urlparse(database_url)
    runtime_url = (
        f"postgres://vane_server_runtime:{password}@127.0.0.1:{parsed.port}"
        f"{parsed.path}?sslmode=disable"
    )
    (runtime / "rollback-base-home").mkdir(mode=0o700)
    gate = subprocess.run(
        [str(previous_gate), "-user", user_value, "-json"],
        cwd=runtime,
        env={
            **env,
            "HOME": str(runtime / "rollback-base-home"),
            "VANE_DB_URL": runtime_url,
            "VANE_LLM_API_KEY": "rollback-compatibility-not-a-secret",
            "VANE_LLM_AGENT_PROVIDER": "openai",
            "VANE_LLM_AGENT_BASE_URL": "https://invalid.example/v1",
            "VANE_LLM_AGENT_API_KEY": "rollback-compatibility-not-a-secret",
        },
        capture_output=True,
        check=False,
    )
    if gate.returncode != 0:
        raise PolicyError(
            "rollback base gate failed against the upgraded schema: "
            + gate.stderr.decode("utf-8", errors="replace").strip()
        )
    try:
        gate_output = json.loads(gate.stdout)
    except json.JSONDecodeError as error:
        raise PolicyError("rollback base gate returned invalid JSON") from error
    if not isinstance(gate_output, dict):
        raise PolicyError("rollback base gate JSON root is not an object")

    added = []
    current_path = "server/store/migrations"
    for name, object_id in added_migrations.items():
        blob = subprocess.run(
            ["git", "show", f"{current_revision}:{current_path}/{name}"],
            cwd=ROOT,
            capture_output=True,
            check=False,
        )
        if blob.returncode != 0:
            raise PolicyError(f"cannot read added migration bytes: {name}")
        added.append({
            "path": name,
            "git_blob": object_id,
            "sha256": hashlib.sha256(blob.stdout).hexdigest(),
        })
    proof = {
        "schema": "vane.server-rollback-compatibility/v1",
        "base_revision": base_revision,
        "current_revision": current_revision,
        "mode": "previous-binary-on-upgraded-schema",
        "added_migrations": added,
        "previous_gate_sha256": file_sha256(previous_gate),
        "previous_gate_output_sha256": hashlib.sha256(gate.stdout).hexdigest(),
        "status": "passed",
    }
    proof_path.write_text(
        json.dumps(proof, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return proof_path, True


def free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def require_native_postgres() -> dict[str, Path]:
    binaries: dict[str, Path] = {}
    for name in ("postgres", "initdb", "pg_ctl", "createdb", "psql"):
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
    rollback_base = os.environ.get("VANE_ROLLBACK_BASE_SHA", f"{head}^1")
    rollback_base = output(
        ["git", "rev-parse", "--verify", f"{rollback_base}^{{commit}}"],
        cwd=ROOT,
        env=os.environ.copy(),
    )
    if not re.fullmatch(r"[0-9a-f]{40}", rollback_base):
        raise PolicyError("rollback base is not an exact Git commit")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", rollback_base, head],
        cwd=ROOT,
        check=False,
    )
    if ancestor.returncode != 0:
        raise PolicyError("rollback base is not an ancestor of the release revision")
    gate_cache_root = Path(os.environ.get("VANE_GATE_CACHE_ROOT", ""))
    selected_timing = select_store_timing_seed(
        cache_root=gate_cache_root, revisions=(head, rollback_base)
    )
    base_migration_path = (
        "server/store/migrations"
        if git_path_exists(revision=rollback_base, path="server/store/migrations")
        else "store/migrations"
    )
    current_migration_tree = migration_tree_object(
        revision=head, path="server/store/migrations", env=os.environ.copy()
    )
    base_migration_tree = migration_tree_object(
        revision=rollback_base, path=base_migration_path, env=os.environ.copy()
    )
    base_migrations = migration_inventory(
        revision=rollback_base, path=base_migration_path
    )
    current_migrations = migration_inventory(
        revision=head, path="server/store/migrations"
    )
    added_migrations = additive_migration_delta(
        base=base_migrations, current=current_migrations
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
        postgres_ports: list[int] = []
        for index in range(5):
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
            postgres_ports.append(pg_port)
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

        rollback_proof, server_rollback_safe = prove_previous_binary_compatibility(
            base_revision=rollback_base,
            current_revision=head,
            added_migrations=added_migrations,
            database_url=urls[4],
            runtime=runtime,
            artifacts=artifacts,
            go=go,
            postgres=postgres,
            env=env,
        )

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
        timing_arguments: list[str] = []
        if selected_timing is not None:
            timing_root, timing_revision = selected_timing
            timing_arguments = [
                "--timing-root", str(timing_root),
                "--timings", STORE_TIMING_SEED_NAME,
            ]
            print(f"Using trusted Store timing seed from {timing_revision}", flush=True)
        run_checked(
            [str(go), "run", "./cmd/storetestshard", "run", "--artifacts", str(store_artifacts)]
            + timing_arguments
            + [item for url in urls[:3] for item in ("--database-url", url)],
            cwd=SERVER,
        )
        os.environ.pop("VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST", None)
        generated_timing_seed = store_artifacts / STORE_TIMING_SEED_NAME
        generated_timing_manifest = store_artifacts / "store.timings.manifest.json"
        seed_command = [
            str(go), "run", "./cmd/storetestshard", "timing-seed",
            "--repo", str(store_artifacts),
            "--tests", "store-tests.list.txt",
            "--output", STORE_TIMING_SEED_NAME,
            "--manifest", generated_timing_manifest.name,
            "store-shard-0.test.json", "store-shard-1.test.json",
            "store-shard-2.test.json",
        ]
        seed_result = subprocess.run(
            seed_command, cwd=SERVER, env=env, text=True, capture_output=True,
            check=False,
        )
        generated_timing_ready = False
        if seed_result.returncode != 0:
            print(
                "Store timing seed generation failed; continuing without cache update: "
                + seed_result.stderr.strip(),
                file=sys.stderr,
                flush=True,
            )
        else:
            try:
                store_status = json.loads(
                    (store_artifacts / "store-shard-status.json").read_text(
                        encoding="utf-8"
                    )
                )
                seed_manifest = json.loads(
                    generated_timing_manifest.read_text(encoding="utf-8")
                )
                generated_timing_ready = (
                    seed_manifest.get("version") == 1
                    and seed_manifest.get("test_count")
                    == store_status.get("expected_tests")
                    and seed_manifest.get("terminal_events")
                    == store_status.get("expected_tests")
                    and isinstance(seed_manifest.get("zero_duration_tests"), int)
                )
            except (OSError, json.JSONDecodeError, AttributeError):
                generated_timing_ready = False
            if not generated_timing_ready:
                print(
                    "Store timing seed manifest is incomplete; continuing without cache update",
                    file=sys.stderr,
                    flush=True,
                )
        store_package = output([str(go), "list", "./store"], cwd=SERVER, env=env)
        packages = [
            item for item in output([str(go), "list", "./..."], cwd=SERVER, env=env).splitlines()
            if item != store_package
        ]
        validate_database_package_inventory(
            server_root=SERVER, declared=NON_STORE_DATABASE_PACKAGE_DIRS
        )
        module_path = output([str(go), "list", "-m"], cwd=SERVER, env=env)
        pure_packages, database_packages = partition_non_store_packages(
            packages=packages, module_path=module_path
        )
        non_store_profiles: list[Path] = []
        pure_profile = artifacts / "non-store-pure.coverage.out"
        pure_env = {**env}
        pure_env.pop("DATABASE_URL", None)
        pure_env.pop("VANE_TEST_DATABASE_URL", None)
        run_go_tests_no_skips(
            [
                str(go), "test", "-json", "-race", "-p=4", "-parallel=4",
                "-count=1", "-timeout=40m", "-covermode=atomic",
                f"-coverprofile={pure_profile}", *pure_packages,
            ],
            cwd=SERVER,
            env=pure_env,
        )
        non_store_profiles.append(pure_profile)
        database_lanes = database_package_lanes(database_packages, lane_count=3)

        def run_database_lane(
            lane_index: int, lane: tuple[tuple[int, str], ...]
        ) -> list[Path]:
            profiles: list[Path] = []
            for package_index, package in lane:
                database = f"vane_full_non_store_{package_index}"
                run_checked(
                    [
                        str(postgres["createdb"]), "-h", "127.0.0.1", "-p",
                        str(postgres_ports[lane_index]), "-U", "vane", database,
                    ],
                    cwd=ROOT,
                )
                database_url = (
                    f"postgres://vane@127.0.0.1:{postgres_ports[lane_index]}/"
                    f"{database}?sslmode=disable"
                )
                assert_disposable_database(
                    data_dir=postgres_clusters[lane_index], database_url=database_url,
                    owner_root=runtime, expected_database=database,
                )
                profile = (
                    artifacts / f"non-store-database-{package_index}.coverage.out"
                )
                database_env = {
                    **env,
                    "DATABASE_URL": database_url,
                    "VANE_TEST_DATABASE_URL": database_url,
                }
                run_go_tests_no_skips(
                    [
                        str(go), "test", "-json", "-race", "-p=1", "-parallel=1",
                        "-count=1", "-timeout=40m", "-covermode=atomic",
                        f"-coverprofile={profile}", package,
                    ],
                    cwd=SERVER,
                    env=database_env,
                )
                profiles.append(profile)
            return profiles

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(database_lanes), thread_name_prefix="non-store-db"
        ) as executor:
            futures = [
                executor.submit(run_database_lane, index, lane)
                for index, lane in enumerate(database_lanes)
            ]
            for future in futures:
                non_store_profiles.extend(future.result())
        for package, test in (
            ("./internal/agentfirstaudit", "^TestRetentionClockEvidenceRealTemporalHistory$"),
            ("./periodicbrief", "^TestPeriodicWorkflowExternalTerminationReplaysAndRecoveryConverges$"),
        ):
            integration_env = {
                **env,
                "DATABASE_URL": urls[3],
                "VANE_TEST_DATABASE_URL": urls[3],
                "VANE_TEMPORAL_ADDRESS": temporal_address,
            }
            run_go_tests_no_skips(
                [str(go), "test", "-json", "-tags=integration", package, "-run", test, "-count=1"],
                cwd=SERVER,
                env=integration_env,
            )

        server_coverage = artifacts / "coverage.out"
        run_checked(
            [
                str(go), "run", "./cmd/storetestshard", "merge-coverage",
                "--output", str(server_coverage),
                str(store_artifacts / "store.coverage.out"),
                *(str(profile) for profile in non_store_profiles),
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
        marker = WEB / "dist/vane-release.json"
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

        remote_main = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/main^{commit}"],
            cwd=ROOT, text=True, capture_output=True, check=False,
        )
        if (
            generated_timing_ready
            and remote_main.returncode == 0
            and remote_main.stdout.strip() == head
        ):
            try:
                publish_store_timing_seed(
                    cache_root=gate_cache_root,
                    revision=head,
                    source=generated_timing_seed,
                )
                print(f"Published trusted Store timing seed for {head}", flush=True)
            except (OSError, PolicyError) as error:
                print(
                    f"Store timing cache update failed; Gate remains valid: {error}",
                    file=sys.stderr,
                    flush=True,
                )

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
                "rollback_base_revision": rollback_base,
                "rollback_compatibility_path": rollback_proof.resolve().relative_to(
                    work_root.resolve()
                ).as_posix(),
                "rollback_compatibility_sha256": file_sha256(rollback_proof),
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
