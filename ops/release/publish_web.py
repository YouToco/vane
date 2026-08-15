#!/usr/bin/env python3
"""Publish a verified Vite dist directly from the release Mac to OSS/CDN."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Callable
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
BUCKET = "zhuoqidev-vane-web"
REGION = "cn-shenzhen"
CDN_DOMAIN = "vane.zhuoqidev.com"
OWNER_PREVIEW = "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"
RELEASE_MARKER_PATH = "vane-release.json"
RECEIPT_SCHEMA = "vane.web.aliyun-release/v1"
CURRENT_SCHEMA = "vane.web-current/v1"
PROOF_SCHEMA = "vane.web-provider-proof/v1"
OSS_WORKERS = 8
CDN_WORKERS = 4
DIGEST_RE = re.compile(r"[0-9a-f]{64}")
CONTENT_HASH_RE = re.compile(
    r"^.+[._-][A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9]+)+$"
)


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    if path.is_symlink():
        raise RuntimeError(f"Web publication evidence path is a symlink: {path}")
    pending = path.with_name(f".{path.name}.{os.getpid()}")
    pending.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(pending, path)


def atomic_copy(source: Path, destination: Path) -> None:
    if destination.is_symlink():
        raise RuntimeError(f"Web publication evidence path is a symlink: {destination}")
    pending = destination.with_name(f".{destination.name}.{os.getpid()}")
    shutil.copy2(source, pending)
    os.replace(pending, destination)


def run(command: list[str], *, env: dict[str, str], capture: bool = False) -> str:
    result = subprocess.run(
        command, env=env, text=True, capture_output=capture, check=False
    )
    if result.returncode != 0:
        detail = result.stderr.strip() if capture else ""
        raise RuntimeError(
            f"Web publication command failed ({result.returncode}): {command[0]} {detail}"
        )
    return (result.stdout + result.stderr).strip() if capture else ""


def publication_runtime(tool_cache: Path) -> tuple[Path, Path, dict[str, str], dict[str, str]]:
    if not tool_cache.is_absolute() or tool_cache.is_symlink() or not tool_cache.is_dir():
        raise RuntimeError("Web publication tool cache is unsafe")
    lock = json.loads(
        (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
    )["tools"]
    aliyun = tool_cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun"
    ossutil = tool_cache / "ossutil" / lock["ossutil"]["version"] / "ossutil"
    for binary in (aliyun, ossutil):
        if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"locked Web publication executable is missing: {binary}")
    if lock["aliyun_cli"]["version"] not in run(
        [str(aliyun), "version"], env=os.environ.copy(), capture=True
    ):
        raise RuntimeError("Aliyun CLI version differs from the lock")
    if run(
        [str(ossutil), "version"], env=os.environ.copy(), capture=True
    ) != lock["ossutil"]["version"]:
        raise RuntimeError("ossutil version differs from the lock")
    access_id = os.environ.get("ALIYUN_ACCESS_KEY_ID", "")
    access_secret = os.environ.get("ALIYUN_ACCESS_KEY_SECRET", "")
    if not access_id or not access_secret:
        raise RuntimeError("local OSS publication credentials are unavailable")
    provider_env = {
        **os.environ,
        "OSS_ACCESS_KEY_ID": access_id,
        "OSS_ACCESS_KEY_SECRET": access_secret,
        "OSS_REGION": REGION,
    }
    aliyun_env = {
        **os.environ,
        "ALIBABA_CLOUD_IGNORE_PROFILE": "TRUE",
        "ALIBABA_CLOUD_ACCESS_KEY_ID": access_id,
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": access_secret,
        "ALIBABA_CLOUD_REGION_ID": REGION,
    }
    return aliyun, ossutil, provider_env, aliyun_env


def verify_dist_public(dist: Path, revision: str, origin: str) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("Web verification revision is not an exact SHA")
    if not dist.is_absolute() or dist.is_symlink() or not dist.is_dir():
        raise RuntimeError("Web verification dist is unsafe")
    marker = dist / RELEASE_MARKER_PATH
    index = dist / "index.html"
    for path in (marker, index):
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"Web verification artifact is missing or unsafe: {path.name}")
    return verify_public_release(
        origin,
        revision,
        expected_marker=marker.read_bytes(),
        expected_index_sha256=sha256(index),
    )


def preflight(tool_cache: Path, origin: str) -> dict:
    if origin != f"https://{CDN_DOMAIN}":
        raise RuntimeError("Web publication origin differs from the canonical production origin")
    aliyun, ossutil, provider_env, aliyun_env = publication_runtime(tool_cache)
    run(
        [str(ossutil), "stat", f"oss://{BUCKET}/{RELEASE_MARKER_PATH}"],
        env=provider_env,
        capture=True,
    )
    detail_raw = run(
        [
            str(aliyun), "cdn", "DescribeCdnDomainDetail",
            "--DomainName", CDN_DOMAIN,
        ],
        env=aliyun_env,
        capture=True,
    )
    try:
        detail = json.loads(detail_raw, object_pairs_hook=strict_pairs)
    except json.JSONDecodeError as error:
        raise RuntimeError("CDN credential preflight returned invalid JSON") from error
    model = detail.get("GetDomainDetailModel") if isinstance(detail, dict) else None
    if not isinstance(model, dict) or model.get("DomainName") != CDN_DOMAIN:
        raise RuntimeError("CDN credential preflight returned the wrong domain")
    probe = f"{time.time_ns()}"
    request = Request(
        origin + f"/{RELEASE_MARKER_PATH}?preflight={probe}",
        headers={"Cache-Control": "no-cache"},
    )
    with urlopen(request, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError(f"Web public preflight returned HTTP {response.status}")
        marker = response.read(64 * 1024 + 1)
    try:
        marker_value = json.loads(marker, object_pairs_hook=strict_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("Web public preflight marker is invalid JSON") from error
    if (
        not isinstance(marker_value, dict)
        or marker_value.get("schema") != "vane.web-release/v1"
        or not isinstance(marker_value.get("source_revision"), str)
        or re.fullmatch(r"[0-9a-f]{40}", marker_value["source_revision"]) is None
    ):
        raise RuntimeError("Web public preflight marker is not a deployed release")
    return {
        "schema": "vane.web-preflight/v1",
        "ok": True,
        "bucket": BUCKET,
        "cdn_domain": CDN_DOMAIN,
        "public_revision": marker_value["source_revision"],
    }


def lines(path: Path) -> list[str]:
    return [value for value in path.read_text(encoding="utf-8").splitlines() if value]


def parallel_apply(
    label: str,
    values: list[str],
    action: Callable[[str], None],
    *,
    workers: int,
) -> None:
    if workers < 1:
        raise RuntimeError(f"{label} worker count is invalid")
    if len(values) != len(set(values)):
        raise RuntimeError(f"{label} contains duplicate objects")
    if not values:
        return
    failures: dict[str, Exception] = {}
    with ThreadPoolExecutor(
        max_workers=min(workers, len(values)),
        thread_name_prefix="vane-web",
    ) as executor:
        futures = {executor.submit(action, value): value for value in values}
        for future in as_completed(futures):
            value = futures[future]
            try:
                future.result()
            except Exception as error:  # all running immutable writes finish before refusal
                failures[value] = error
    if failures:
        first = sorted(failures)[0]
        raise RuntimeError(f"{label} failed for {first}: {failures[first]}")


def validate_release_marker(expected_marker: bytes, revision: str) -> dict:
    try:
        marker_value = json.loads(expected_marker, object_pairs_hook=strict_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("local Web release marker is invalid") from error
    if (
        not isinstance(marker_value, dict)
        or set(marker_value)
        != {"schema", "source_revision", "source_dirty", "tree_sha256", "file_count"}
        or marker_value.get("schema") != "vane.web-release/v1"
        or marker_value.get("source_revision") != revision
        or marker_value.get("source_dirty") is not False
        or not isinstance(marker_value.get("tree_sha256"), str)
        or DIGEST_RE.fullmatch(marker_value["tree_sha256"]) is None
        or type(marker_value.get("file_count")) is not int
        or marker_value["file_count"] <= 0
    ):
        raise RuntimeError("local Web release marker is not exact clean evidence")
    return marker_value


def verify_public_release(
    origin: str,
    revision: str,
    *,
    expected_marker: bytes,
    expected_index_sha256: str,
    attempts: int = 6,
) -> dict:
    marker_value = validate_release_marker(expected_marker, revision)
    last_error: Exception | None = None
    if attempts < 1 or attempts > 6:
        raise RuntimeError("public Web verification attempt count is invalid")
    for attempt in range(1, attempts + 1):
        try:
            # A revision-only query can remain cached at the CDN after the
            # marker-last commit. Each verification attempt must force a
            # distinct edge lookup so a stale response cannot trigger an
            # unnecessary full republish or exhaust the convergence window.
            probe = f"{time.time_ns()}-{attempt}"
            marker_target = (
                origin.rstrip("/")
                + f"/{RELEASE_MARKER_PATH}?release={revision}&probe={probe}"
            )
            index_target = (
                origin.rstrip("/")
                + f"/index.html?release={revision}&probe={probe}"
            )
            request = Request(marker_target, headers={"Cache-Control": "no-cache"})
            with urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"release marker returned HTTP {response.status}")
                public_marker = response.read(64 * 1024 + 1)
            if public_marker != expected_marker:
                raise RuntimeError("public release marker differs from exact artifact bytes")
            request = Request(index_target, headers={"Cache-Control": "no-cache"})
            with urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"Web entrypoint returned HTTP {response.status}")
                public_index = response.read(8 * 1024 * 1024 + 1)
            if hashlib.sha256(public_index).hexdigest() != expected_index_sha256:
                raise RuntimeError("public Web entrypoint differs from exact artifact bytes")
            return marker_value
        except Exception as error:  # network convergence is bounded and evidenced
            last_error = error
            if attempt < attempts:
                time.sleep(attempt * 2)
    raise RuntimeError(f"public Web release did not converge: {last_error}")


def verify_oss_object(
    ossutil: Path,
    object_name: str,
    expected: Path,
    env: dict[str, str],
    readback_root: Path,
) -> None:
    destination = readback_root / object_name
    destination.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            str(ossutil),
            "cp",
            f"oss://{BUCKET}/{object_name}",
            str(destination),
            "--force",
        ],
        env=env,
    )
    if destination.is_symlink() or not destination.is_file():
        raise RuntimeError(f"OSS readback is unsafe or missing: {object_name}")
    if destination.stat().st_size != expected.stat().st_size or sha256(destination) != sha256(expected):
        raise RuntimeError(f"OSS object differs from exact artifact bytes: {object_name}")


def strict_pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in items:
        if key in value:
            raise RuntimeError(f"duplicate Web publication evidence key: {key}")
        value[key] = item
    return value


def load_strict_json(path: Path, subject: str) -> dict:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"{subject} is missing or unsafe")
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=strict_pairs
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{subject} is invalid JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"{subject} is not an object")
    return value


def validate_object_path(value: object) -> str:
    if not isinstance(value, str):
        raise RuntimeError("Web provider proof contains a non-string object path")
    path = PurePosixPath(value)
    if (
        any(ord(char) < 32 or ord(char) == 127 for char in value)
        or "\\" in value
        or path.is_absolute()
        or str(path) != value
        or not path.parts
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise RuntimeError(f"Web provider proof contains an unsafe path: {value!r}")
    return value


def validate_file_record(value: object, subject: str) -> dict:
    if not isinstance(value, dict) or set(value) != {"path", "sha256", "size"}:
        raise RuntimeError(f"{subject} file record has an invalid shape")
    path = validate_object_path(value["path"])
    digest = value["sha256"]
    size = value["size"]
    if not isinstance(digest, str) or DIGEST_RE.fullmatch(digest) is None:
        raise RuntimeError(f"{subject} file digest is invalid: {path}")
    if type(size) is not int or size < 0:
        raise RuntimeError(f"{subject} file size is invalid: {path}")
    return {"path": path, "sha256": digest, "size": size}


def validate_receipt(value: dict, revision: str) -> dict[str, dict]:
    if set(value) != {
        "schema", "bucket", "source_sha", "entry_path", "entry_sha256", "files"
    }:
        raise RuntimeError("Web release receipt has an invalid shape")
    if (
        value.get("schema") != RECEIPT_SCHEMA
        or value.get("bucket") != BUCKET
        or value.get("source_sha") != revision
        or value.get("entry_path") != "index.html"
        or not isinstance(value.get("entry_sha256"), str)
        or DIGEST_RE.fullmatch(value["entry_sha256"]) is None
        or not isinstance(value.get("files"), list)
        or not value["files"]
    ):
        raise RuntimeError("Web release receipt is not exact publication evidence")
    records = [validate_file_record(item, "Web release receipt") for item in value["files"]]
    paths = [record["path"] for record in records]
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise RuntimeError("Web release receipt files are not unique and sorted")
    by_path = {record["path"]: record for record in records}
    if (
        "index.html" not in by_path
        or by_path["index.html"]["sha256"] != value["entry_sha256"]
        or RELEASE_MARKER_PATH not in by_path
    ):
        raise RuntimeError("Web release receipt is missing its exact entry or marker")
    return by_path


def validate_current(value: dict) -> dict:
    if set(value) != {"schema", "revision", "receipt_sha256"}:
        raise RuntimeError("Web current state has an invalid shape")
    if (
        value.get("schema") != CURRENT_SCHEMA
        or not isinstance(value.get("revision"), str)
        or re.fullmatch(r"[0-9a-f]{40}", value["revision"]) is None
        or not isinstance(value.get("receipt_sha256"), str)
        or DIGEST_RE.fullmatch(value["receipt_sha256"]) is None
    ):
        raise RuntimeError("Web current state is invalid")
    return value


def proof_directory(state_root: Path, receipt_digest: str) -> Path:
    return state_root / "web-proofs" / receipt_digest


def validate_proof_destination(state_root: Path, receipt_digest: str) -> None:
    proof_root = state_root / "web-proofs"
    if proof_root.is_symlink() or (proof_root.exists() and not proof_root.is_dir()):
        raise RuntimeError("Web provider proof root is unsafe")
    destination = proof_directory(state_root, receipt_digest)
    if destination.is_symlink() or (
        destination.exists() and not destination.is_dir()
    ):
        raise RuntimeError("Web provider proof directory is unsafe")
    if destination.is_dir():
        for name in ("receipt.json", "marker.json", "proof.json"):
            evidence = destination / name
            if evidence.is_symlink() or (evidence.exists() and not evidence.is_file()):
                raise RuntimeError("Web provider proof evidence path is unsafe")


def persist_provider_proof(
    *,
    state_root: Path,
    revision: str,
    receipt: Path,
    receipt_digest: str,
    receipt_files: dict[str, dict],
    marker_path: Path,
    critical_objects: list[str],
) -> None:
    proof_root = state_root / "web-proofs"
    if proof_root.is_symlink():
        raise RuntimeError("Web provider proof root is a symlink")
    proof_root.mkdir(mode=0o700, exist_ok=True)
    destination = proof_directory(state_root, receipt_digest)
    if destination.is_symlink():
        raise RuntimeError("Web provider proof directory is a symlink")
    destination.mkdir(mode=0o700, exist_ok=True)
    records = [
        receipt_files[path]
        for path in sorted(critical_objects)
        if CONTENT_HASH_RE.fullmatch(PurePosixPath(path).name) is not None
    ]
    proof = {
        "schema": PROOF_SCHEMA,
        "revision": revision,
        "receipt_sha256": receipt_digest,
        "marker_sha256": sha256(marker_path),
        "index_sha256": receipt_files["index.html"]["sha256"],
        "verified_objects": records,
    }
    atomic_copy(receipt, destination / "receipt.json")
    atomic_copy(marker_path, destination / "marker.json")
    atomic_json(destination / "proof.json", proof)


def load_provider_proof(
    *, state_root: Path, current: dict, origin: str
) -> dict[str, dict]:
    destination = proof_directory(state_root, current["receipt_sha256"])
    if not destination.exists():
        return {}
    proof_root = destination.parent
    if proof_root.is_symlink() or destination.is_symlink() or not destination.is_dir():
        raise RuntimeError("Web provider proof path is unsafe")
    receipt_path = destination / "receipt.json"
    marker_path = destination / "marker.json"
    proof_path = destination / "proof.json"
    receipt = load_strict_json(receipt_path, "Web provider proof receipt")
    if sha256(receipt_path) != current["receipt_sha256"]:
        raise RuntimeError("Web provider proof receipt differs from current state")
    receipt_files = validate_receipt(receipt, current["revision"])
    proof = load_strict_json(proof_path, "Web provider proof")
    if set(proof) != {
        "schema", "revision", "receipt_sha256", "marker_sha256",
        "index_sha256", "verified_objects",
    }:
        raise RuntimeError("Web provider proof has an invalid shape")
    if (
        proof.get("schema") != PROOF_SCHEMA
        or proof.get("revision") != current["revision"]
        or proof.get("receipt_sha256") != current["receipt_sha256"]
        or not isinstance(proof.get("marker_sha256"), str)
        or DIGEST_RE.fullmatch(proof["marker_sha256"]) is None
        or proof.get("index_sha256") != receipt_files["index.html"]["sha256"]
        or not isinstance(proof.get("verified_objects"), list)
    ):
        raise RuntimeError("Web provider proof is inconsistent with current state")
    if marker_path.is_symlink() or not marker_path.is_file():
        raise RuntimeError("Web provider proof marker is missing or unsafe")
    marker = marker_path.read_bytes()
    if (
        hashlib.sha256(marker).hexdigest() != proof["marker_sha256"]
        or proof["marker_sha256"] != receipt_files[RELEASE_MARKER_PATH]["sha256"]
    ):
        raise RuntimeError("Web provider proof marker digest is invalid")
    records = [
        validate_file_record(item, "Web provider proof")
        for item in proof["verified_objects"]
    ]
    paths = [record["path"] for record in records]
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise RuntimeError("Web provider proof objects are not unique and sorted")
    for record in records:
        path = record["path"]
        if receipt_files.get(path) != record:
            raise RuntimeError(f"Web provider proof object differs from receipt: {path}")
        if CONTENT_HASH_RE.fullmatch(PurePosixPath(path).name) is None:
            raise RuntimeError(f"Web provider proof object is not content-addressed: {path}")
    verify_public_release(
        origin,
        current["revision"],
        expected_marker=marker,
        expected_index_sha256=proof["index_sha256"],
    )
    return {record["path"]: record for record in records}


def publish(
    *, dist: Path, revision: str, work_root: Path, state_root: Path,
    tool_cache: Path, origin: str, result_path: Path,
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("Web publication revision is not an exact SHA")
    for path, subject in (
        (dist, "dist"), (work_root, "work root"), (state_root, "state root"),
        (tool_cache, "tool cache"),
    ):
        if not path.is_absolute() or path.is_symlink() or not path.is_dir():
            raise RuntimeError(f"Web publication {subject} is unsafe")
    if result_path.is_symlink() or result_path.exists():
        raise RuntimeError("Web publication result path already exists or is unsafe")
    if not origin.startswith("https://"):
        raise RuntimeError("Web public origin must use HTTPS")
    state_file = state_root / "web-current.json"
    lock_file = state_root / "web-release.lock"
    if (
        state_file.is_symlink()
        or lock_file.is_symlink()
        or (state_file.exists() and not state_file.is_file())
        or (lock_file.exists() and not lock_file.is_file())
    ):
        raise RuntimeError("Web publication state paths are unsafe")

    aliyun, ossutil, provider_env, aliyun_env = publication_runtime(tool_cache)

    with lock_file.open("a+b") as state_lock:
        fcntl.flock(state_lock, fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(prefix="vane-web-plan-", dir=work_root) as temporary:
            plan = Path(temporary)
            run(
                [
                    str(ROOT / "ops/release/web-release.py"),
                    "--dist", str(dist), "--sha", revision, "--output", str(plan),
                ],
                env=os.environ.copy(),
                capture=True,
            )
            receipt = plan / "release.json"
            receipt_digest = sha256(receipt)
            validate_proof_destination(state_root, receipt_digest)
            receipt_value = load_strict_json(receipt, "Web release receipt")
            receipt_files = validate_receipt(receipt_value, revision)
            marker_path = dist / RELEASE_MARKER_PATH
            if marker_path.is_symlink() or not marker_path.is_file():
                raise RuntimeError("Web release marker is missing or unsafe")
            expected_marker = marker_path.read_bytes()
            validate_release_marker(expected_marker, revision)
            expected_index_sha256 = sha256(dist / "index.html")
            if expected_index_sha256 != receipt_files["index.html"]["sha256"]:
                raise RuntimeError("Web entrypoint differs from its release receipt")
            asset_objects = lines(plan / "assets.list")
            if len(asset_objects) != len(set(asset_objects)):
                raise RuntimeError("Web asset plan contains duplicate objects")
            critical_objects: list[str] = []
            for row in lines(plan / "critical-assets.list"):
                size, object_name = row.split("\t", 1)
                record = receipt_files.get(object_name)
                if record is None:
                    raise RuntimeError(
                        f"critical asset is absent from its receipt: {object_name}"
                    )
                local = dist / object_name
                if (
                    int(size) != record["size"]
                    or int(size) != local.stat().st_size
                    or sha256(local) != record["sha256"]
                ):
                    raise RuntimeError(f"local critical asset differs: {object_name}")
                critical_objects.append(object_name)
            if len(critical_objects) != len(set(critical_objects)):
                raise RuntimeError("Web critical asset plan contains duplicate objects")
            if any(object_name not in receipt_files for object_name in asset_objects):
                raise RuntimeError("Web asset plan contains an object absent from its receipt")
            current: dict | None = None
            if state_file.is_file() and not state_file.is_symlink():
                current = validate_current(
                    load_strict_json(state_file, "Web current state")
                )
                expected_current = {
                    "schema": CURRENT_SCHEMA,
                    "revision": revision,
                    "receipt_sha256": receipt_digest,
                }
                if current.get("revision") == revision and current != expected_current:
                    raise RuntimeError(
                        "same Web revision has a different immutable release receipt"
                    )
                if current == expected_current:
                    marker = verify_public_release(
                        origin,
                        revision,
                        expected_marker=expected_marker,
                        expected_index_sha256=expected_index_sha256,
                    )
                    persist_provider_proof(
                        state_root=state_root,
                        revision=revision,
                        receipt=receipt,
                        receipt_digest=receipt_digest,
                        receipt_files=receipt_files,
                        marker_path=marker_path,
                        critical_objects=critical_objects,
                    )
                    result = {
                        "schema": "vane.web-publication/v1", "revision": revision,
                        "receipt_sha256": receipt_digest, "marker": marker,
                        "status": "already-current",
                    }
                    atomic_copy(
                        receipt, result_path.with_name("web-release-receipt.json")
                    )
                    atomic_json(result_path, result)
                    return result

            # If the provider commit and public entrypoint already match this
            # exact receipt, a prior process crashed only before local state
            # settlement. Marker-last makes this a safe, zero-mutation resume.
            try:
                marker = verify_public_release(
                    origin,
                    revision,
                    expected_marker=expected_marker,
                    expected_index_sha256=expected_index_sha256,
                    attempts=1,
                )
            except RuntimeError:
                marker = None
            if marker is not None:
                state = {
                    "schema": CURRENT_SCHEMA,
                    "revision": revision,
                    "receipt_sha256": receipt_digest,
                }
                result = {
                    "schema": "vane.web-publication/v1", "revision": revision,
                    "receipt_sha256": receipt_digest, "marker": marker,
                    "status": "provider-already-current",
                }
                persist_provider_proof(
                    state_root=state_root,
                    revision=revision,
                    receipt=receipt,
                    receipt_digest=receipt_digest,
                    receipt_files=receipt_files,
                    marker_path=marker_path,
                    critical_objects=critical_objects,
                )
                atomic_copy(receipt, result_path.with_name("web-release-receipt.json"))
                atomic_json(state_file, state)
                atomic_json(result_path, result)
                return result

            prior_verified: dict[str, dict] = {}
            if current is not None:
                prior_verified = load_provider_proof(
                    state_root=state_root,
                    current=current,
                    origin=origin,
                )
            reusable = {
                object_name
                for object_name in critical_objects
                if (
                    CONTENT_HASH_RE.fullmatch(PurePosixPath(object_name).name)
                    is not None
                    and prior_verified.get(object_name) == receipt_files[object_name]
                )
            }
            upload_objects = [
                object_name for object_name in asset_objects
                if object_name not in reusable
            ]
            readback_objects = [
                object_name for object_name in critical_objects
                if object_name not in reusable
            ]

            publication_started = time.monotonic()
            timings: dict[str, float | int] = {
                "asset_total": len(asset_objects),
                "reused_immutable": len(reusable),
                "uploaded": len(upload_objects),
                "readback": len(readback_objects),
            }

            def upload_asset(object_name: str) -> None:
                run(
                    [
                        str(ossutil), "cp", str(dist / object_name),
                        f"oss://{BUCKET}/{object_name}", "--force",
                    ],
                    env=provider_env,
                )

            phase_started = time.monotonic()
            parallel_apply(
                "OSS immutable upload",
                upload_objects,
                upload_asset,
                workers=OSS_WORKERS,
            )
            timings["immutable_upload"] = round(
                time.monotonic() - phase_started, 3
            )
            readback_root = plan / "provider-readback"

            def readback_asset(object_name: str) -> None:
                verify_oss_object(
                    ossutil,
                    object_name,
                    dist / object_name,
                    provider_env,
                    readback_root,
                )

            phase_started = time.monotonic()
            parallel_apply(
                "OSS critical readback",
                readback_objects,
                readback_asset,
                workers=OSS_WORKERS,
            )
            timings["critical_readback"] = round(
                time.monotonic() - phase_started, 3
            )
            phase_started = time.monotonic()
            for object_name in lines(plan / "html-before-entry.list"):
                run(
                    [
                        str(ossutil), "cp", str(dist / object_name),
                        f"oss://{BUCKET}/{object_name}", "--force",
                    ],
                    env=provider_env,
                )
            if (dist / OWNER_PREVIEW).is_file():
                run(
                    [
                        str(ossutil), "set-props",
                        f"oss://{BUCKET}/{OWNER_PREVIEW}",
                        "--cache-control", "no-store",
                        "--metadata-directive", "update", "--force",
                    ],
                    env=provider_env,
                )
            timings["html_before_entry"] = round(
                time.monotonic() - phase_started, 3
            )
            phase_started = time.monotonic()
            run(
                [
                    str(ossutil), "cp", str(dist / "index.html"),
                    f"oss://{BUCKET}/index.html", "--force",
                ],
                env=provider_env,
            )
            verify_oss_object(
                ossutil,
                "index.html",
                dist / "index.html",
                provider_env,
                readback_root,
            )
            timings["entry_commit"] = round(
                time.monotonic() - phase_started, 3
            )
            # Commit the public revision only after every earlier provider
            # object and the entrypoint have exact-byte readback evidence.
            phase_started = time.monotonic()
            run(
                [
                    str(ossutil),
                    "cp",
                    str(marker_path),
                    f"oss://{BUCKET}/{RELEASE_MARKER_PATH}",
                    "--force",
                ],
                env=provider_env,
            )
            verify_oss_object(
                ossutil,
                RELEASE_MARKER_PATH,
                marker_path,
                provider_env,
                readback_root,
            )
            timings["marker_commit"] = round(
                time.monotonic() - phase_started, 3
            )

            def refresh(refresh_path: str) -> None:
                url = origin.rstrip("/") + refresh_path
                for attempt in range(1, 4):
                    refreshed = subprocess.run(
                        [
                            str(aliyun), "cdn", "RefreshObjectCaches",
                            "--ObjectPath", url, "--ObjectType", "File",
                        ],
                        env=aliyun_env,
                        check=False,
                    )
                    if refreshed.returncode == 0:
                        break
                    if attempt == 3:
                        raise RuntimeError(f"CDN refresh failed after three attempts: {url}")
                    time.sleep(attempt * 5)
            phase_started = time.monotonic()
            parallel_apply(
                "CDN refresh",
                lines(plan / "cdn-refresh-paths.list"),
                refresh,
                workers=CDN_WORKERS,
            )
            timings["cdn_refresh"] = round(time.monotonic() - phase_started, 3)
            phase_started = time.monotonic()
            marker = verify_public_release(
                origin,
                revision,
                expected_marker=expected_marker,
                expected_index_sha256=expected_index_sha256,
            )
            timings["public_verify"] = round(
                time.monotonic() - phase_started, 3
            )
            timings["total"] = round(time.monotonic() - publication_started, 3)
            state = {
                "schema": CURRENT_SCHEMA,
                "revision": revision,
                "receipt_sha256": receipt_digest,
            }
            result = {
                "schema": "vane.web-publication/v1", "revision": revision,
                "receipt_sha256": receipt_digest, "marker": marker,
                "status": "published",
            }
            persist_provider_proof(
                state_root=state_root,
                revision=revision,
                receipt=receipt,
                receipt_digest=receipt_digest,
                receipt_files=receipt_files,
                marker_path=marker_path,
                critical_objects=critical_objects,
            )
            atomic_copy(receipt, result_path.with_name("web-release-receipt.json"))
            atomic_json(state_file, state)
            atomic_json(result_path, result)
            print(
                "Web publication timings: "
                + json.dumps(timings, sort_keys=True, separators=(",", ":")),
                file=sys.stderr,
            )
            return result


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    parser.add_argument("--dist", type=Path)
    parser.add_argument("--sha")
    parser.add_argument("--work-root", type=Path)
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--tool-cache", type=Path, required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--result", type=Path)
    args = parser.parse_args()
    if args.preflight:
        if any(value is not None for value in (args.dist, args.sha, args.work_root, args.state_root, args.result)):
            parser.error("--preflight accepts only --tool-cache and --origin")
        result = preflight(args.tool_cache, args.origin)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    if args.verify_only:
        if args.dist is None or args.sha is None:
            parser.error("--verify-only requires --dist and --sha")
        if any(value is not None for value in (args.work_root, args.state_root, args.result)):
            parser.error("--verify-only does not accept publication state paths")
        result = verify_dist_public(args.dist, args.sha, args.origin)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    if any(
        value is None
        for value in (args.dist, args.sha, args.work_root, args.state_root, args.result)
    ):
        parser.error(
            "publication requires --dist, --sha, --work-root, --state-root, and --result"
        )
    result = publish(
        dist=args.dist, revision=args.sha, work_root=args.work_root,
        state_root=args.state_root, tool_cache=args.tool_cache,
        origin=args.origin, result_path=args.result,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"Web publication refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
