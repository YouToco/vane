#!/usr/bin/env python3
"""Publish a verified Vite dist directly from the release Mac to OSS/CDN."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import fcntl
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
from typing import Callable
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
BUCKET = "zhuoqidev-vane-web"
REGION = "cn-shenzhen"
OWNER_PREVIEW = "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"
RELEASE_MARKER_PATH = "vane-release.json"
OSS_WORKERS = 8
CDN_WORKERS = 4


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


def verify_public_release(
    origin: str,
    revision: str,
    *,
    expected_marker: bytes,
    expected_index_sha256: str,
    attempts: int = 6,
) -> dict:
    def strict_pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                raise RuntimeError(f"duplicate local Web release marker key: {key}")
            value[key] = item
        return value

    try:
        marker_value = json.loads(expected_marker, object_pairs_hook=strict_pairs)
    except json.JSONDecodeError as error:
        raise RuntimeError("local Web release marker is invalid") from error
    if (
        not isinstance(marker_value, dict)
        or set(marker_value)
        != {"schema", "source_revision", "source_dirty", "tree_sha256", "file_count"}
        or marker_value.get("schema") != "vane.web-release/v1"
        or marker_value.get("source_revision") != revision
        or marker_value.get("source_dirty") is not False
        or not re.fullmatch(r"[0-9a-f]{64}", marker_value.get("tree_sha256", ""))
        or type(marker_value.get("file_count")) is not int
        or marker_value["file_count"] <= 0
    ):
        raise RuntimeError("local Web release marker is not exact clean evidence")
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
    if state_file.is_symlink() or lock_file.is_symlink():
        raise RuntimeError("Web publication state paths must not be symlinks")

    lock = json.loads(
        (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
    )["tools"]
    aliyun = tool_cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun"
    ossutil = tool_cache / "ossutil" / lock["ossutil"]["version"] / "ossutil"
    for binary in (aliyun, ossutil):
        if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"locked Web publication executable is missing: {binary}")
    if lock["aliyun_cli"]["version"] not in run([str(aliyun), "version"], env=os.environ.copy(), capture=True):
        raise RuntimeError("Aliyun CLI version differs from the lock")
    if run([str(ossutil), "version"], env=os.environ.copy(), capture=True) != lock["ossutil"]["version"]:
        raise RuntimeError("ossutil version differs from the lock")
    access_id = os.environ.get("ALIYUN_ACCESS_KEY_ID", "")
    access_secret = os.environ.get("ALIYUN_ACCESS_KEY_SECRET", "")
    if not access_id or not access_secret:
        raise RuntimeError("local OSS publication credentials are unavailable")

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
            marker_path = dist / RELEASE_MARKER_PATH
            if marker_path.is_symlink() or not marker_path.is_file():
                raise RuntimeError("Web release marker is missing or unsafe")
            expected_marker = marker_path.read_bytes()
            expected_index_sha256 = sha256(dist / "index.html")
            if state_file.is_file() and not state_file.is_symlink():
                current = json.loads(state_file.read_text(encoding="utf-8"))
                expected_current = {
                    "schema": "vane.web-current/v1",
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
                    "schema": "vane.web-current/v1",
                    "revision": revision,
                    "receipt_sha256": receipt_digest,
                }
                result = {
                    "schema": "vane.web-publication/v1", "revision": revision,
                    "receipt_sha256": receipt_digest, "marker": marker,
                    "status": "provider-already-current",
                }
                atomic_copy(receipt, result_path.with_name("web-release-receipt.json"))
                atomic_json(state_file, state)
                atomic_json(result_path, result)
                return result

            provider_env = {
                **os.environ,
                "OSS_ACCESS_KEY_ID": access_id,
                "OSS_ACCESS_KEY_SECRET": access_secret,
                "OSS_REGION": REGION,
            }
            publication_started = time.monotonic()
            timings: dict[str, float] = {}
            asset_objects = lines(plan / "assets.list")

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
                asset_objects,
                upload_asset,
                workers=OSS_WORKERS,
            )
            timings["immutable_upload"] = round(
                time.monotonic() - phase_started, 3
            )
            readback_root = plan / "provider-readback"
            critical_objects: list[str] = []
            for row in lines(plan / "critical-assets.list"):
                size, object_name = row.split("\t", 1)
                if int(size) != (dist / object_name).stat().st_size:
                    raise RuntimeError(f"local critical asset size differs: {object_name}")
                critical_objects.append(object_name)

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
                critical_objects,
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

            aliyun_env = {
                **os.environ,
                "ALIBABA_CLOUD_IGNORE_PROFILE": "TRUE",
                "ALIBABA_CLOUD_ACCESS_KEY_ID": access_id,
                "ALIBABA_CLOUD_ACCESS_KEY_SECRET": access_secret,
                "ALIBABA_CLOUD_REGION_ID": REGION,
            }
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
                "schema": "vane.web-current/v1",
                "revision": revision,
                "receipt_sha256": receipt_digest,
            }
            result = {
                "schema": "vane.web-publication/v1", "revision": revision,
                "receipt_sha256": receipt_digest, "marker": marker,
                "status": "published",
            }
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
    parser.add_argument("--dist", type=Path, required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--tool-cache", type=Path, required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
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
