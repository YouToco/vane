#!/usr/bin/env python3
"""Publish one verified Vite dist to Cloudflare Pages and Aliyun OSS/CDN."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import fcntl
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from typing import Callable
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
BUCKET = "zhuoqidev-vane-web"
REGION = "cn-shenzhen"
OSS_PUBLIC_ORIGIN = f"https://{BUCKET}.oss-{REGION}.aliyuncs.com"
CDN_DOMAIN = "vane.zhuoqidev.com"
DNS_DOMAIN = "zhuoqidev.com"
DNS_RR = "vane"
ALIYUN_CDN_CNAME = "vane.zhuoqidev.com.w.kunlunaq.com"
CLOUDFLARE_PROJECT = "vane-web"
CLOUDFLARE_BRANCH = "main"
CLOUDFLARE_ORIGIN = "https://vane-web.pages.dev"
CLOUDFLARE_CUSTOM_ORIGIN = f"https://{CDN_DOMAIN}"
OWNER_PREVIEW = "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"
RELEASE_MARKER_PATH = "vane-release.json"
RECEIPT_SCHEMA = "vane.web-artifact/v1"
CURRENT_SCHEMA = "vane.web-current/v2"
PROOF_SCHEMA = "vane.web-provider-proof/v1"
ALIYUN_RECEIPT_SCHEMA = "vane.web-provider.aliyun/v1"
CLOUDFLARE_RECEIPT_SCHEMA = "vane.web-provider.cloudflare-pages/v2"
PUBLICATION_SCHEMA = "vane.web-publication/v2"
PENDING_SCHEMA = "vane.web-pending/v2"
OSS_WORKERS = 8
CDN_WORKERS = 4
PUBLIC_OBJECT_ATTEMPTS = 3
PUBLIC_USER_AGENT = "vane-release-controller/1"
OWNER_PREVIEW_CSP = (
    "default-src 'self'; connect-src 'none'; img-src 'self' data:; "
    "font-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; "
    "object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
)
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


def cloudflare_public_path(object_name: str) -> str:
    """Map a built file to the canonical Pages URL that serves its bytes."""
    if object_name == "index.html":
        return "/"
    if object_name.endswith("/index.html"):
        return "/" + quote(object_name[:-len("index.html")], safe="/-._~")
    return "/" + quote(object_name, safe="/-._~")


def directory_tree_sha256(root: Path) -> str:
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError(f"Web artifact tree is missing or unsafe: {root}")
    records: list[dict] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise RuntimeError(f"Web artifact tree contains an unsafe member: {path}")
        if path.is_dir():
            continue
        records.append({
            "path": path.relative_to(root).as_posix(),
            "sha256": sha256(path),
            "size": path.stat().st_size,
            "mode": path.stat().st_mode & 0o777,
        })
    if not records:
        raise RuntimeError("Web artifact tree is empty")
    payload = json.dumps(
        {"schema": "vane.directory-tree/v1", "files": records},
        sort_keys=True, separators=(",", ":"),
    ).encode() + b"\n"
    return hashlib.sha256(payload).hexdigest()


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


def runtime_environment() -> dict[str, str]:
    allowed = {
        "HOME", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR",
        "TMPDIR", "TMP", "TEMP", "WEB_CALL_LOG", "OSS_REMOTE",
        "CLOUDFLARE_REMOTE", "CLOUDFLARE_REMOTE_SHA", "FAIL_DOWNLOAD", "CORRUPT_UPLOAD",
        "FAIL_CLOUDFLARE_DEPLOY", "FAIL_STAT",
    }
    return {name: value for name, value in os.environ.items() if name in allowed}


def machine_arch() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "darwin" or machine not in {"arm64", "aarch64"}:
        raise RuntimeError(
            "Web publication mutation authority is exactly darwin-arm64"
        )
    return "darwin-arm64"


def installed_tree_sha256(root: Path) -> str:
    """Hash installed bytes and internal symlink targets, independent of owner."""
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise RuntimeError("locked installed tool tree is unsafe")
    resolved_root = root.resolve(strict=True)
    records: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            target_text = os.readlink(path)
            try:
                path.resolve(strict=True).relative_to(resolved_root)
            except (OSError, ValueError) as error:
                raise RuntimeError(
                    f"installed tool symlink escapes its tree: {relative}"
                ) from error
            records.append({"path": relative, "type": "symlink", "target": target_text})
        elif path.is_file():
            records.append({
                "path": relative,
                "type": "file",
                "sha256": sha256(path),
                "size": path.stat().st_size,
            })
        elif not path.is_dir():
            raise RuntimeError(f"installed tool tree has an unsafe member: {relative}")
    if not records:
        raise RuntimeError("locked installed tool tree is empty")
    payload = json.dumps(
        {"schema": "vane.installed-tool-tree/v1", "entries": records},
        sort_keys=True,
        separators=(",", ":"),
    ).encode() + b"\n"
    return hashlib.sha256(payload).hexdigest()


def validate_locked_publication_tools(
    lock: dict, *, aliyun: Path, ossutil: Path, node: Path,
    wrangler_js: Path, wrangler_root: Path,
) -> dict[str, str]:
    arch = machine_arch()
    paths = {
        "aliyun_cli": (aliyun, lock["aliyun_cli"]),
        "ossutil": (ossutil, lock["ossutil"]),
        "node": (node, lock["node"]),
        "wrangler": (wrangler_js, lock["wrangler"]),
    }
    observed: dict[str, str] = {}
    for name, (path, definition) in paths.items():
        expected = definition.get("entry_sha256", {}).get(arch)
        if not isinstance(expected, str) or not DIGEST_RE.fullmatch(expected):
            raise RuntimeError(f"locked {name} entry digest is unavailable for {arch}")
        actual = sha256(path)
        if actual != expected:
            raise RuntimeError(f"locked {name} entry bytes differ for {arch}")
        observed[name] = actual
    expected_tree = lock["wrangler"].get("installed_tree_sha256", {}).get(arch)
    if not isinstance(expected_tree, str) or not DIGEST_RE.fullmatch(expected_tree):
        raise RuntimeError(f"locked Wrangler tree digest is unavailable for {arch}")
    actual_tree = installed_tree_sha256(wrangler_root)
    if actual_tree != expected_tree:
        raise RuntimeError(f"locked Wrangler installed tree differs for {arch}")
    observed["wrangler_tree"] = actual_tree
    return observed


def publication_toolchain(
    tool_cache: Path,
) -> tuple[Path, Path, list[str], dict[str, str], dict[str, str]]:
    if machine_arch() != "darwin-arm64":
        raise RuntimeError("Web publication mutation is locked to darwin-arm64")
    if not tool_cache.is_absolute() or tool_cache.is_symlink() or not tool_cache.is_dir():
        raise RuntimeError("Web publication tool cache is unsafe")
    lock = json.loads(
        (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
    )["tools"]
    aliyun = tool_cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun"
    ossutil = tool_cache / "ossutil" / lock["ossutil"]["version"] / "ossutil"
    node = tool_cache / "node" / lock["node"]["version"] / "bin" / "node"
    wrangler_js = (
        tool_cache / "wrangler" / lock["wrangler"]["version"]
        / "node_modules" / "wrangler" / "bin" / "wrangler.js"
    )
    wrangler_root = tool_cache / "wrangler" / lock["wrangler"]["version"]
    for binary in (aliyun, ossutil, node, wrangler_js):
        if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"locked Web publication executable is missing: {binary}")
    validate_locked_publication_tools(
        lock,
        aliyun=aliyun,
        ossutil=ossutil,
        node=node,
        wrangler_js=wrangler_js,
        wrangler_root=wrangler_root,
    )
    base_env = runtime_environment()
    if lock["aliyun_cli"]["version"] not in run(
        [str(aliyun), "version"], env=base_env, capture=True
    ):
        raise RuntimeError("Aliyun CLI version differs from the lock")
    if run(
        [str(ossutil), "version"], env=base_env, capture=True
    ) != lock["ossutil"]["version"]:
        raise RuntimeError("ossutil version differs from the lock")
    if run([str(node), "--version"], env=base_env, capture=True) != f"v{lock['node']['version']}":
        raise RuntimeError("Node version differs from the lock")
    wrangler_command = [str(node), str(wrangler_js)]
    if run(
        [*wrangler_command, "--version"], env=base_env, capture=True
    ) != lock["wrangler"]["version"]:
        raise RuntimeError("Wrangler version differs from the lock")
    return aliyun, ossutil, wrangler_command, base_env, {
        "aliyun_cli": sha256(aliyun),
        "ossutil": sha256(ossutil),
        "node": sha256(node),
        "wrangler": sha256(wrangler_js),
        "wrangler_tree": installed_tree_sha256(wrangler_root),
    }


def publication_runtime(
    tool_cache: Path,
) -> tuple[Path, Path, list[str], dict[str, str], dict[str, str], dict[str, str]]:
    aliyun, ossutil, wrangler_command, base_env, _ = publication_toolchain(
        tool_cache
    )
    access_id = os.environ.get("ALIYUN_ACCESS_KEY_ID", "")
    access_secret = os.environ.get("ALIYUN_ACCESS_KEY_SECRET", "")
    if not access_id or not access_secret:
        raise RuntimeError("local OSS publication credentials are unavailable")
    cloudflare_token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    cloudflare_account = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
    if not cloudflare_token or not cloudflare_account:
        raise RuntimeError("local Cloudflare publication credentials are unavailable")
    provider_env = {
        **base_env,
        "OSS_ACCESS_KEY_ID": access_id,
        "OSS_ACCESS_KEY_SECRET": access_secret,
        "OSS_REGION": REGION,
    }
    aliyun_env = {
        **base_env,
        "ALIBABA_CLOUD_IGNORE_PROFILE": "TRUE",
        "ALIBABA_CLOUD_ACCESS_KEY_ID": access_id,
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": access_secret,
        "ALIBABA_CLOUD_REGION_ID": REGION,
    }
    cloudflare_env = {
        **base_env,
        "CLOUDFLARE_API_TOKEN": cloudflare_token,
        "CLOUDFLARE_ACCOUNT_ID": cloudflare_account,
    }
    return aliyun, ossutil, wrangler_command, provider_env, aliyun_env, cloudflare_env


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


def verify_finalized_publication(
    dist: Path, revision: str, result_path: Path
) -> dict:
    result = load_strict_json(result_path, "Web combined publication result")
    if (
        set(result) != {
            "schema", "revision", "artifact_receipt_sha256", "marker",
            "providers", "status",
        }
        or result.get("schema") != PUBLICATION_SCHEMA
        or result.get("revision") != revision
        or result.get("status") not in {
            "published", "recovered", "provider-already-current", "already-current",
        }
        or not isinstance(result.get("providers"), dict)
        or set(result["providers"]) != {"aliyun", "cloudflare_pages"}
    ):
        raise RuntimeError("Web combined publication result is invalid")
    artifact = result_path.with_name("web-release-receipt.json")
    artifact_digest = result["artifact_receipt_sha256"]
    if sha256(artifact) != artifact_digest:
        raise RuntimeError("Web artifact receipt differs from combined result")
    receipt_files = validate_receipt(
        load_strict_json(artifact, "Web artifact receipt"), revision
    )
    for object_name, record in receipt_files.items():
        local = dist / object_name
        if (
            local.is_symlink()
            or not local.is_file()
            or local.stat().st_size != record["size"]
            or sha256(local) != record["sha256"]
        ):
            raise RuntimeError(
                f"Web dist differs from finalized artifact receipt: {object_name}"
            )
    expected_marker = (dist / RELEASE_MARKER_PATH).read_bytes()
    expected_index_sha256 = sha256(dist / "index.html")
    if (
        receipt_files["index.html"]["sha256"] != expected_index_sha256
        or receipt_files[RELEASE_MARKER_PATH]["sha256"]
        != hashlib.sha256(expected_marker).hexdigest()
    ):
        raise RuntimeError("Web dist differs from finalized artifact receipt")
    cf_path = result_path.with_name("web-cloudflare-receipt.json")
    ali_path = result_path.with_name("web-aliyun-receipt.json")
    cf = validate_cloudflare_receipt(
        load_strict_json(cf_path, "Cloudflare provider receipt"),
        revision, artifact_digest,
    )
    ali = validate_aliyun_receipt(
        load_strict_json(ali_path, "Aliyun provider receipt"),
        revision, artifact_digest,
    )
    cf_verified = validate_serving_file_evidence(
        cf, "Cloudflare provider receipt"
    )
    ali_verified = validate_serving_file_evidence(ali, "Aliyun provider receipt")
    control_files = {
        item["path"]: item for item in cf["control_files"]
    }
    expected_cf = {
        path: record for path, record in receipt_files.items()
        if path not in control_files
    }
    if cf_verified != expected_cf or ali_verified != receipt_files or any(
        receipt_files.get(path) != record for path, record in control_files.items()
    ):
        raise RuntimeError("Web provider serving evidence differs from artifact")
    serving_files = {
        path: record for path, record in receipt_files.items()
        if path in {"index.html", RELEASE_MARKER_PATH}
        or CONTENT_HASH_RE.fullmatch(PurePosixPath(path).name) is not None
    }
    _, serving_digest = serving_file_evidence(cf_verified)
    if cf["custom_origin_smoke"]["verified_files_sha256"] != serving_digest:
        raise RuntimeError("Cloudflare custom serving evidence differs from artifact")
    if (
        result.get("marker") != validate_release_marker(expected_marker, revision)
        or result["providers"]["cloudflare_pages"]
        != {**cf, "receipt_sha256": sha256(cf_path)}
        or result["providers"]["aliyun"]
        != {**ali, "receipt_sha256": sha256(ali_path)}
        or
        sha256(cf_path) != result["providers"]["cloudflare_pages"]["receipt_sha256"]
        or sha256(ali_path) != result["providers"]["aliyun"]["receipt_sha256"]
    ):
        raise RuntimeError("Web provider receipt digest differs from combined result")
    for cf_origin in (cf["deployment_url"], CLOUDFLARE_ORIGIN):
        verify_public_release(
            cf_origin, revision, expected_marker=expected_marker,
            expected_index_sha256=expected_index_sha256,
            expected_files=cf_verified,
            index_path="/",
            directory_indexes=True,
        )
    verify_cloudflare_custom_edge(
        revision, expected_marker, expected_index_sha256, cf_verified,
        control_files,
    )
    verify_cloudflare_controls(
        cf["deployment_url"], revision, control_files, cf_verified
    )
    verify_public_release(
        OSS_PUBLIC_ORIGIN, revision, expected_marker=expected_marker,
        expected_index_sha256=expected_index_sha256,
        expected_files=ali_verified,
    )
    verify_aliyun_edge(
        ALIYUN_CDN_CNAME, revision, expected_marker, expected_index_sha256,
        ali_verified,
    )
    return result


def parse_json_output(raw: str, subject: str) -> object:
    try:
        return json.loads(raw, object_pairs_hook=strict_pairs)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{subject} returned invalid JSON") from error


def cloudflare_api(account: str, path: str, cloudflare_env: dict[str, str]) -> object:
    token = cloudflare_env["CLOUDFLARE_API_TOKEN"]
    request = Request(
        f"https://api.cloudflare.com/client/v4/accounts/{account}/{path.lstrip('/')}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": PUBLIC_USER_AGENT,
        },
    )
    with urlopen(request, timeout=20) as response:
        if response.status != 200:
            raise RuntimeError(f"Cloudflare API returned HTTP {response.status}")
        raw = response.read(1024 * 1024 + 1)
    try:
        envelope = json.loads(raw, object_pairs_hook=strict_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("Cloudflare API returned invalid JSON") from error
    if (
        not isinstance(envelope, dict)
        or envelope.get("success") is not True
        or "result" not in envelope
    ):
        raise RuntimeError("Cloudflare API did not return a successful result")
    return envelope["result"]


def validate_provider_routes(
    project: object, domains: object, cdn: object, dns: object
) -> None:
    refusal = RuntimeError("Web provider route contract is not exact")
    if (
        not isinstance(project, dict)
        or project.get("name") != CLOUDFLARE_PROJECT
        or project.get("production_branch") != CLOUDFLARE_BRANCH
        or project.get("source") is not None
        or not isinstance(project.get("domains"), list)
        or set(project["domains"])
        != {"vane-web.pages.dev", CDN_DOMAIN}
    ):
        raise refusal
    if (
        not isinstance(domains, list)
        or not any(
            isinstance(item, dict)
            and item.get("name") == CDN_DOMAIN
            and item.get("status") == "active"
            and isinstance(item.get("validation_data"), dict)
            and item["validation_data"].get("status") == "active"
            and isinstance(item.get("verification_data"), dict)
            and item["verification_data"].get("status") == "active"
            for item in domains
        )
    ):
        raise refusal
    model = cdn.get("GetDomainDetailModel") if isinstance(cdn, dict) else None
    if (
        not isinstance(model, dict)
        or model.get("DomainName") != CDN_DOMAIN
        or model.get("DomainStatus") != "online"
        or not isinstance(model.get("Cname"), str)
        or model["Cname"] != ALIYUN_CDN_CNAME
    ):
        raise refusal
    records_model = dns.get("DomainRecords") if isinstance(dns, dict) else None
    records = records_model.get("Record") if isinstance(records_model, dict) else None
    if not isinstance(records, list):
        raise refusal
    vane_records = [
        item for item in records
        if isinstance(item, dict) and item.get("RR") == DNS_RR
    ]
    if (
        len(vane_records) != 2
        or any(
            item.get("Type") != "CNAME"
            or item.get("Status") != "ENABLE"
            or item.get("Line") not in {"default", "oversea"}
            for item in vane_records
        )
    ):
        raise refusal
    matching = {item["Line"]: item for item in vane_records}
    if (
        set(matching) != {"default", "oversea"}
        or matching["default"].get("Value") != model["Cname"]
        or matching["oversea"].get("Value") != "vane-web.pages.dev"
    ):
        raise refusal


def cloudflare_project_contract(
    wrangler_command: list[str], cloudflare_env: dict[str, str]
) -> tuple[dict, list[dict]]:
    account = cloudflare_env["CLOUDFLARE_ACCOUNT_ID"]
    project = cloudflare_api(
        account, f"pages/projects/{CLOUDFLARE_PROJECT}", cloudflare_env
    )
    domains = cloudflare_api(
        account,
        f"pages/projects/{CLOUDFLARE_PROJECT}/domains",
        cloudflare_env,
    )
    return project, domains


def provider_route_authority(
    aliyun: Path, aliyun_env: dict[str, str],
    wrangler_command: list[str], cloudflare_env: dict[str, str],
    *, expected_cloudflare_deployment_id: str | None = None,
    revision: str | None = None,
) -> dict:
    detail = parse_json_output(
        run(
            [str(aliyun), "cdn", "DescribeCdnDomainDetail",
             "--DomainName", CDN_DOMAIN],
            env=aliyun_env, capture=True,
        ),
        "CDN route authority",
    )
    dns = parse_json_output(
        run(
            [str(aliyun), "alidns", "DescribeDomainRecords",
             "--DomainName", DNS_DOMAIN, "--RRKeyWord", DNS_RR],
            env=aliyun_env, capture=True,
        ),
        "DNS route authority",
    )
    project, domains = cloudflare_project_contract(
        wrangler_command, cloudflare_env
    )
    validate_provider_routes(project, domains, detail, dns)
    if (expected_cloudflare_deployment_id is None) != (revision is None):
        raise RuntimeError("Cloudflare canonical authority expectation is incomplete")
    if expected_cloudflare_deployment_id is not None and revision is not None:
        deployment = validate_cloudflare_deployment(
            project.get("canonical_deployment"), revision
        )
        if deployment["id"] != expected_cloudflare_deployment_id:
            raise RuntimeError("Cloudflare canonical deployment changed before finalize")
    return detail


def read_public_marker(origin: str) -> dict:
    probe = f"{time.time_ns()}"
    request = Request(
        origin.rstrip("/") + f"/{RELEASE_MARKER_PATH}?preflight={probe}",
        headers={
            "Cache-Control": "no-cache", "User-Agent": PUBLIC_USER_AGENT,
        },
    )
    with urlopen(request, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError(f"Web public preflight returned HTTP {response.status}")
        marker = response.read(64 * 1024 + 1)
    try:
        value = json.loads(marker, object_pairs_hook=strict_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("Web public preflight marker is invalid JSON") from error
    if (
        not isinstance(value, dict)
        or value.get("schema") != "vane.web-release/v1"
        or not isinstance(value.get("source_revision"), str)
        or re.fullmatch(r"[0-9a-f]{40}", value["source_revision"]) is None
    ):
        raise RuntimeError("Web public preflight marker is not a deployed release")
    return value


def preflight(tool_cache: Path, origin: str) -> dict:
    if origin != f"https://{CDN_DOMAIN}":
        raise RuntimeError("Web publication origin differs from the canonical production origin")
    (
        aliyun, ossutil, wrangler_command, provider_env, aliyun_env,
        cloudflare_env,
    ) = publication_runtime(tool_cache)
    run(
        [str(ossutil), "stat", f"oss://{BUCKET}/{RELEASE_MARKER_PATH}"],
        env=provider_env,
        capture=True,
    )
    detail = provider_route_authority(
        aliyun, aliyun_env, wrangler_command, cloudflare_env
    )
    model = detail.get("GetDomainDetailModel") if isinstance(detail, dict) else None
    if not isinstance(model, dict) or not isinstance(model.get("Cname"), str):
        raise RuntimeError("Aliyun CDN preflight authority is invalid")
    with tempfile.TemporaryDirectory(prefix="vane-web-preflight-") as temporary:
        snapshot = Path(temporary)
        for object_name in (RELEASE_MARKER_PATH, "index.html"):
            run(
                [
                    str(ossutil), "cp", f"oss://{BUCKET}/{object_name}",
                    str(snapshot / object_name), "--force",
                ],
                env=provider_env,
            )
        marker_bytes = (snapshot / RELEASE_MARKER_PATH).read_bytes()
        try:
            marker_value = json.loads(marker_bytes, object_pairs_hook=strict_pairs)
            revision = marker_value.get("source_revision")
            if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
                raise RuntimeError("legacy marker")
        except (RuntimeError, UnicodeDecodeError, json.JSONDecodeError):
            # The first monorepo release may inherit a legacy marker shape;
            # bytes must still agree between OSS and a pinned Ali edge.
            revision = "0" * 40
        edge = verify_aliyun_edge(
            model["Cname"], revision, marker_bytes,
            sha256(snapshot / "index.html"),
            {
                name: {
                    "path": name,
                    "sha256": sha256(snapshot / name),
                    "size": (snapshot / name).stat().st_size,
                }
                for name in ("index.html", RELEASE_MARKER_PATH)
            },
        )
    pages_revision: str | None = None
    try:
        pages_revision = read_public_marker(CLOUDFLARE_ORIGIN)["source_revision"]
    except RuntimeError:
        # A direct-upload project can legitimately still serve the legacy SPA
        # fallback at the marker path before its first receipt-backed release.
        request = Request(
            CLOUDFLARE_ORIGIN + "/?preflight=" + str(time.time_ns()),
            headers={
                "Cache-Control": "no-cache", "User-Agent": PUBLIC_USER_AGENT,
            },
        )
        with urlopen(request, timeout=15) as response:
            if response.status != 200 or not response.read(8 * 1024 * 1024 + 1):
                raise RuntimeError("Cloudflare Pages public endpoint is unavailable")
    page_bodies: dict[str, bytes] = {}
    for object_name, limit in (
        (RELEASE_MARKER_PATH, 64 * 1024 + 1),
        ("index.html", 8 * 1024 * 1024 + 1),
    ):
        public_path = "" if object_name == "index.html" else "/" + object_name
        request = Request(
            CLOUDFLARE_ORIGIN + public_path
            + "?preflight-custom=" + str(time.time_ns()),
            headers={
                "Cache-Control": "no-cache", "User-Agent": PUBLIC_USER_AGENT,
            },
        )
        with urlopen(request, timeout=15) as response:
            if response.status != 200:
                raise RuntimeError("Cloudflare Pages preflight object is unavailable")
            page_bodies[object_name] = response.read(limit)
    custom_edge = verify_cloudflare_custom_edge(
        pages_revision or "0" * 40,
        page_bodies[RELEASE_MARKER_PATH],
        hashlib.sha256(page_bodies["index.html"]).hexdigest(),
        {
            name: {
                "path": name,
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
            for name, payload in page_bodies.items()
        },
    )
    return {
        "schema": "vane.web-preflight/v2",
        "ok": True,
        "bucket": BUCKET,
        "cdn_domain": CDN_DOMAIN,
        "providers": {
            "aliyun": {
                "cdn_cname": detail["GetDomainDetailModel"]["Cname"],
                "edge_ip": edge["ip"],
            },
            "cloudflare_pages": {
                "project": CLOUDFLARE_PROJECT,
                "public_revision": pages_revision,
                "custom_edge_ip": custom_edge["edge_ip"],
            },
        },
        "public_revision": pages_revision,
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
    expected_files: dict[str, dict] | None = None,
    index_path: str = "/index.html",
    directory_indexes: bool = False,
    attempts: int = 6,
) -> dict:
    marker_value = validate_release_marker(expected_marker, revision)
    last_error: Exception | None = None
    if attempts < 1 or attempts > 6:
        raise RuntimeError("public Web verification attempt count is invalid")
    if index_path not in {"/", "/index.html"}:
        raise RuntimeError("public Web entrypoint path is invalid")
    if type(directory_indexes) is not bool:
        raise RuntimeError("public Web directory-index mode is invalid")
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
                + f"{index_path}?release={revision}&probe={probe}"
            )
            request = Request(marker_target, headers={
                "Cache-Control": "no-cache", "User-Agent": PUBLIC_USER_AGENT,
            })
            with urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"release marker returned HTTP {response.status}")
                public_marker = response.read(64 * 1024 + 1)
            if public_marker != expected_marker:
                raise RuntimeError("public release marker differs from exact artifact bytes")
            request = Request(index_target, headers={
                "Cache-Control": "no-cache", "User-Agent": PUBLIC_USER_AGENT,
            })
            with urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"Web entrypoint returned HTTP {response.status}")
                public_index = response.read(8 * 1024 * 1024 + 1)
            if hashlib.sha256(public_index).hexdigest() != expected_index_sha256:
                raise RuntimeError("public Web entrypoint differs from exact artifact bytes")
            object_names = [
                name for name in sorted(expected_files or {})
                if name not in {"index.html", RELEASE_MARKER_PATH}
            ]

            def verify_object(object_name: str) -> None:
                record = (expected_files or {})[object_name]
                public_path = (
                    cloudflare_public_path(object_name)
                    if directory_indexes
                    else "/" + quote(object_name, safe="/-._~")
                )
                target = (
                    origin.rstrip("/") + public_path
                    + f"?release={revision}&probe={probe}"
                )
                last_error: Exception | None = None
                for object_attempt in range(1, PUBLIC_OBJECT_ATTEMPTS + 1):
                    try:
                        request = Request(target + f"&object-attempt={object_attempt}", headers={
                            "Cache-Control": "no-cache",
                            "User-Agent": PUBLIC_USER_AGENT,
                        })
                        with urlopen(request, timeout=15) as response:
                            if response.status != 200:
                                raise RuntimeError(
                                    "public Web object returned HTTP "
                                    f"{response.status}: {object_name}"
                                )
                            payload = response.read(record["size"] + 1)
                        if (
                            len(payload) != record["size"]
                            or hashlib.sha256(payload).hexdigest() != record["sha256"]
                        ):
                            raise RuntimeError(
                                f"public Web object differs from artifact: {object_name}"
                            )
                        return
                    except Exception as error:
                        last_error = error
                        if object_attempt < PUBLIC_OBJECT_ATTEMPTS:
                            time.sleep(0.25 * object_attempt)
                raise RuntimeError(
                    f"public Web object did not converge: {object_name}: {last_error}"
                )
            parallel_apply(
                "public Web object verification", object_names,
                verify_object, workers=CDN_WORKERS,
            )
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


def serving_file_evidence(files: dict[str, dict]) -> tuple[list[dict], str]:
    records = [files[path] for path in sorted(files)]
    payload = json.dumps(records, sort_keys=True, separators=(",", ":")).encode()
    return records, hashlib.sha256(payload).hexdigest()


def validate_serving_file_evidence(value: dict, subject: str) -> dict[str, dict]:
    records_value = value.get("verified_files")
    if not isinstance(records_value, list) or not records_value:
        raise RuntimeError(f"{subject} verified files are invalid")
    records = [validate_file_record(item, subject) for item in records_value]
    paths = [item["path"] for item in records]
    if (
        paths != sorted(paths)
        or len(paths) != len(set(paths))
        or "index.html" not in paths
        or RELEASE_MARKER_PATH not in paths
    ):
        raise RuntimeError(f"{subject} verified files are not exact")
    payload = json.dumps(records, sort_keys=True, separators=(",", ":")).encode()
    if value.get("verified_files_sha256") != hashlib.sha256(payload).hexdigest():
        raise RuntimeError(f"{subject} verified file digest differs")
    return {record["path"]: record for record in records}


def validate_receipt(value: dict, revision: str) -> dict[str, dict]:
    if set(value) != {
        "schema", "source_sha", "entry_path", "entry_sha256", "files"
    }:
        raise RuntimeError("Web release receipt has an invalid shape")
    if (
        value.get("schema") != RECEIPT_SCHEMA
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
    if set(value) == {"schema", "revision", "receipt_sha256"}:
        if (
            value.get("schema") != "vane.web-current/v1"
            or not isinstance(value.get("revision"), str)
            or re.fullmatch(r"[0-9a-f]{40}", value["revision"]) is None
            or not isinstance(value.get("receipt_sha256"), str)
            or DIGEST_RE.fullmatch(value["receipt_sha256"]) is None
        ):
            raise RuntimeError("legacy Web current state is invalid")
        return {
            "schema": "vane.web-current/v1",
            "revision": value["revision"],
            "artifact_receipt_sha256": value["receipt_sha256"],
            "providers": {},
        }
    if set(value) != {
        "schema", "revision", "artifact_receipt_sha256", "providers"
    }:
        raise RuntimeError("Web current state has an invalid shape")
    if (
        value.get("schema") != CURRENT_SCHEMA
        or not isinstance(value.get("revision"), str)
        or re.fullmatch(r"[0-9a-f]{40}", value["revision"]) is None
        or not isinstance(value.get("artifact_receipt_sha256"), str)
        or DIGEST_RE.fullmatch(value["artifact_receipt_sha256"]) is None
        or not isinstance(value.get("providers"), dict)
        or set(value["providers"]) != {"aliyun", "cloudflare_pages"}
        or any(
            not isinstance(digest, str) or DIGEST_RE.fullmatch(digest) is None
            for digest in value["providers"].values()
        )
    ):
        raise RuntimeError("Web current state is invalid")
    return value


def validate_legacy_artifact_receipt(
    state_root: Path, current: dict, revision: str
) -> dict[str, dict]:
    digest = current["artifact_receipt_sha256"]
    path = state_root / "web-proofs" / digest / "receipt.json"
    if path.is_symlink() or not path.is_file() or sha256(path) != digest:
        raise RuntimeError("legacy Web artifact receipt is missing or changed")
    value = load_strict_json(path, "legacy Web artifact receipt")
    if (
        not isinstance(value, dict)
        or set(value) != {
            "schema", "source_sha", "entry_path", "entry_sha256", "files",
            "bucket",
        }
        or value.get("schema") != "vane.web.aliyun-release/v1"
        or value.get("bucket") != BUCKET
    ):
        raise RuntimeError("legacy Web artifact receipt shape is invalid")
    neutral = {key: item for key, item in value.items() if key != "bucket"}
    neutral["schema"] = RECEIPT_SCHEMA
    return validate_receipt(neutral, revision)


def validate_cloudflare_receipt(value: dict, revision: str, artifact_digest: str) -> dict:
    if set(value) != {
        "schema", "project", "deployment_id", "deployment_url",
        "production_origin", "source_sha", "commit_dirty",
        "artifact_receipt_sha256", "index_sha256", "marker_sha256",
        "previous_canonical_deployment_id",
        "verified_files", "verified_files_sha256", "custom_alias",
        "custom_origin_smoke", "control_files", "control_files_sha256",
        "control_smoke", "status",
    }:
        raise RuntimeError("Cloudflare provider receipt shape is invalid")
    if (
        value.get("schema") != CLOUDFLARE_RECEIPT_SCHEMA
        or value.get("project") != CLOUDFLARE_PROJECT
        or value.get("source_sha") != revision
        or value.get("commit_dirty") is not False
        or value.get("artifact_receipt_sha256") != artifact_digest
        or value.get("status") != "verified"
        or value.get("custom_alias") != f"https://{CDN_DOMAIN}"
        or (
            value.get("previous_canonical_deployment_id") is not None
            and (
                not isinstance(value["previous_canonical_deployment_id"], str)
                or not value["previous_canonical_deployment_id"].isascii()
                or not value["previous_canonical_deployment_id"]
                or value["previous_canonical_deployment_id"]
                == value.get("deployment_id")
            )
        )
        or not isinstance(value.get("custom_origin_smoke"), dict)
        or set(value["custom_origin_smoke"]) != {
            "host", "via", "edge_ip", "verified_files_sha256"
        }
        or value["custom_origin_smoke"].get("host") != CDN_DOMAIN
        or value["custom_origin_smoke"].get("via") != "vane-web.pages.dev"
        or not isinstance(value["custom_origin_smoke"].get("edge_ip"), str)
        or any(
            not isinstance(value.get(name), str)
            or DIGEST_RE.fullmatch(value[name]) is None
            for name in ("index_sha256", "marker_sha256")
        )
    ):
        raise RuntimeError("Cloudflare provider receipt is not exact")
    verified = validate_serving_file_evidence(value, "Cloudflare provider receipt")
    try:
        custom_ip = ipaddress.ip_address(value["custom_origin_smoke"]["edge_ip"])
    except ValueError as error:
        raise RuntimeError("Cloudflare custom edge IP is invalid") from error
    if not custom_ip.is_global:
        raise RuntimeError("Cloudflare custom edge IP is not public")
    if (
        verified.get("index.html", {}).get("sha256") != value["index_sha256"]
        or verified.get(RELEASE_MARKER_PATH, {}).get("sha256")
        != value["marker_sha256"]
        or value["custom_origin_smoke"]["verified_files_sha256"]
        != value["verified_files_sha256"]
    ):
        raise RuntimeError("Cloudflare provider receipt digests are not self-consistent")
    controls = value.get("control_files")
    if not isinstance(controls, list) or any(
        not isinstance(item, dict) or item.get("path") not in {"_headers", "_redirects"}
        for item in controls
    ):
        raise RuntimeError("Cloudflare control file receipt is invalid")
    control_records = [validate_file_record(item, "Cloudflare control file") for item in controls]
    control_paths = [item["path"] for item in control_records]
    if control_paths != sorted(control_paths) or len(control_paths) != len(set(control_paths)):
        raise RuntimeError("Cloudflare control files are not sorted")
    payload = json.dumps(control_records, sort_keys=True, separators=(",", ":")).encode()
    if value.get("control_files_sha256") != hashlib.sha256(payload).hexdigest():
        raise RuntimeError("Cloudflare control file digest differs")
    if value.get("control_smoke") != {
        "headers": "verified" if any(item["path"] == "_headers" for item in controls) else "not-present",
        "redirects": "verified" if any(item["path"] == "_redirects" for item in controls) else "not-present",
    }:
        raise RuntimeError("Cloudflare control behavior smoke is invalid")
    validate_cloudflare_deployment({
        "id": value["deployment_id"], "url": value["deployment_url"],
        "environment": "production", "latest_stage": {"name": "deploy", "status": "success"},
        "aliases": [value["custom_alias"]],
        "deployment_trigger": {"metadata": {"branch": CLOUDFLARE_BRANCH,
            "commit_hash": revision, "commit_dirty": False}},
    }, revision)
    return value


def validate_aliyun_receipt(value: dict, revision: str, artifact_digest: str) -> dict:
    if set(value) != {
        "schema", "source_sha", "bucket", "cdn_domain",
        "artifact_receipt_sha256", "index_sha256", "marker_sha256",
        "verified_files", "verified_files_sha256", "refresh_tasks",
        "edge_ip", "control_smoke", "mode", "status",
    }:
        raise RuntimeError("Aliyun provider receipt shape is invalid")
    if (
        value.get("schema") != ALIYUN_RECEIPT_SCHEMA
        or value.get("source_sha") != revision
        or value.get("bucket") != BUCKET
        or value.get("cdn_domain") != CDN_DOMAIN
        or value.get("artifact_receipt_sha256") != artifact_digest
        or value.get("status") != "verified"
        or not isinstance(value.get("edge_ip"), str)
        or not isinstance(value.get("refresh_tasks"), list)
        or value.get("mode") not in {"published", "adopted"}
        or value.get("control_smoke") != {
            "redirects": (
                "verified"
                if any(
                    isinstance(item, dict) and item.get("path") == "_redirects"
                    for item in value.get("verified_files", [])
                )
                else "not-present"
            )
        }
        or (value.get("mode") == "published" and not value["refresh_tasks"])
        or (value.get("mode") == "adopted" and value["refresh_tasks"])
    ):
        raise RuntimeError("Aliyun provider receipt is not exact")
    verified = validate_serving_file_evidence(value, "Aliyun provider receipt")
    try:
        edge_ip = ipaddress.ip_address(value["edge_ip"])
    except ValueError as error:
        raise RuntimeError("Aliyun provider edge IP is invalid") from error
    if not edge_ip.is_global:
        raise RuntimeError("Aliyun provider edge IP is not public")
    if (
        verified.get("index.html", {}).get("sha256") != value["index_sha256"]
        or verified.get(RELEASE_MARKER_PATH, {}).get("sha256")
        != value["marker_sha256"]
    ):
        raise RuntimeError("Aliyun provider receipt digests are not self-consistent")
    tasks = value["refresh_tasks"]
    normalized_tasks: list[tuple[str, str]] = []
    for task in tasks:
        if (
            not isinstance(task, dict)
            or set(task) != {"path", "task_id"}
            or not isinstance(task.get("path"), str)
            or not task["path"].startswith("/")
            or not task["path"].isascii()
            or urlsplit(task["path"]).path != task["path"]
            or not isinstance(task.get("task_id"), str)
            or not task["task_id"].isdigit()
        ):
            raise RuntimeError("Aliyun refresh task receipt is invalid")
        normalized_tasks.append((task["path"], task["task_id"]))
    task_paths = [path for path, _ in normalized_tasks]
    if task_paths != sorted(task_paths) or len(task_paths) != len(set(task_paths)):
        raise RuntimeError("Aliyun refresh task paths are not unique and sorted")
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
    *, state_root: Path, current: dict
) -> dict[str, dict]:
    receipt_digest = current["artifact_receipt_sha256"]
    destination = proof_directory(state_root, receipt_digest)
    if not destination.exists():
        return {}
    proof_root = destination.parent
    if proof_root.is_symlink() or destination.is_symlink() or not destination.is_dir():
        raise RuntimeError("Web provider proof path is unsafe")
    receipt_path = destination / "receipt.json"
    marker_path = destination / "marker.json"
    proof_path = destination / "proof.json"
    receipt = load_strict_json(receipt_path, "Web provider proof receipt")
    if sha256(receipt_path) != receipt_digest:
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
        or proof.get("receipt_sha256") != receipt_digest
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
    return {record["path"]: record for record in records}


def cloudflare_deployments(
    wrangler_command: list[str], cloudflare_env: dict[str, str]
) -> list[dict]:
    value = parse_json_output(
        run(
            [
                *wrangler_command, "pages", "deployment", "list",
                "--project-name", CLOUDFLARE_PROJECT,
                "--environment", "production", "--json",
            ],
            env=cloudflare_env,
            capture=True,
        ),
        "Cloudflare deployment metadata",
    )
    if not isinstance(value, list):
        raise RuntimeError("Cloudflare deployment metadata is not a list")
    return value


def validate_cloudflare_deployment(value: object, revision: str) -> dict:
    if not isinstance(value, dict):
        raise RuntimeError("Cloudflare deployment metadata is invalid")
    trigger = value.get("deployment_trigger")
    metadata = trigger.get("metadata") if isinstance(trigger, dict) else None
    stage = value.get("latest_stage")
    deployment_id = value.get("id")
    deployment_url = value.get("url")
    if (
        not isinstance(deployment_id, str)
        or not deployment_id.isascii()
        or not deployment_id
        or not isinstance(deployment_url, str)
        or re.fullmatch(
            r"https://[a-z0-9-]+\.vane-web\.pages\.dev", deployment_url
        ) is None
        or value.get("environment") != "production"
        or not isinstance(stage, dict)
        or stage.get("name") != "deploy"
        or stage.get("status") != "success"
        or not isinstance(metadata, dict)
        or metadata.get("branch") != CLOUDFLARE_BRANCH
        or metadata.get("commit_hash") != revision
        or metadata.get("commit_dirty") is not False
        or not isinstance(value.get("aliases"), list)
        or f"https://{CDN_DOMAIN}" not in value["aliases"]
    ):
        raise RuntimeError("Cloudflare deployment metadata is not exact production success")
    return {
        "id": deployment_id,
        "url": deployment_url,
        "environment": "production",
        "branch": CLOUDFLARE_BRANCH,
        "commit_hash": revision,
        "commit_dirty": False,
    }


def verify_cloudflare_controls(
    origin: str, revision: str, control_files: dict[str, dict],
    public_files: dict[str, dict], *, edge_ip: str | None = None,
) -> dict:
    parsed = urlsplit(origin)
    if parsed.scheme != "https" or not parsed.hostname or parsed.path not in {"", "/"}:
        raise RuntimeError("Cloudflare control smoke origin is invalid")

    def request(path: str) -> tuple[int, dict[str, str]]:
        connection = (
            PinnedEdgeHTTPSConnection(edge_ip)
            if edge_ip is not None
            else http.client.HTTPSConnection(
                parsed.hostname, 443, timeout=15,
                context=ssl.create_default_context(),
            )
        )
        try:
            connection.request(
                "GET", path, headers={
                    "Cache-Control": "no-cache",
                    "User-Agent": PUBLIC_USER_AGENT,
                    **({"Host": CDN_DOMAIN} if edge_ip is not None else {}),
                }
            )
            response = connection.getresponse()
            response.read(1024 * 1024 + 1)
            return response.status, {
                name.lower(): value for name, value in response.getheaders()
            }
        finally:
            connection.close()

    headers_status = "not-present"
    if "_headers" in control_files:
        status, headers = request(f"/?release={revision}&control=1")
        root_cache = headers.get("cache-control", "").lower().replace(" ", "")
        asset_path = next((
            path for path in sorted(public_files)
            if CONTENT_HASH_RE.fullmatch(PurePosixPath(path).name) is not None
        ), None)
        if asset_path is None:
            raise RuntimeError("Cloudflare header smoke has no hashed asset")
        asset_status, asset_headers = request(
            "/" + quote(asset_path, safe="/-._~")
            + f"?release={revision}&control=1"
        )
        asset_cache = asset_headers.get("cache-control", "").lower().replace(" ", "")
        if (
            status != 200 or "no-cache" not in root_cache
            or "must-revalidate" not in root_cache or asset_status != 200
            or "public" not in asset_cache or "max-age=31536000" not in asset_cache
            or "immutable" not in asset_cache
        ):
            raise RuntimeError("Cloudflare _headers behavior differs")
        if OWNER_PREVIEW in public_files:
            preview_status, preview_headers = request(
                cloudflare_public_path(OWNER_PREVIEW)
            )
            preview_cache = preview_headers.get("cache-control", "").lower().replace(" ", "")
            preview_robots = preview_headers.get("x-robots-tag", "").lower().replace(" ", "")
            preview_csp = preview_headers.get("content-security-policy", "")
            if (
                preview_status != 200 or "no-store" not in preview_cache
                or preview_robots != "noindex,nofollow,noarchive"
                or preview_headers.get("referrer-policy", "").lower() != "no-referrer"
                or "default-src 'self'" not in preview_csp
                or "connect-src 'none'" not in preview_csp
                or "frame-ancestors 'none'" not in preview_csp
            ):
                raise RuntimeError("Cloudflare preview header behavior differs")
        headers_status = "verified"

    redirects_status = "not-present"
    if "_redirects" in control_files:
        status, headers = request("/.well-known/agent-card.json")
        if (
            status != 302
            or headers.get("location")
            != "https://api.vane.zhuoqidev.com/.well-known/agent-card.json"
        ):
            raise RuntimeError("Cloudflare _redirects behavior differs")
        probe_status, probe_headers = request("/.well-known/probe.json")
        if (
            probe_status != 302
            or probe_headers.get("location")
            != "https://api.vane.zhuoqidev.com/.well-known/probe.json"
        ):
            raise RuntimeError("Cloudflare _redirects splat behavior differs")
        redirects_status = "verified"
    return {"headers": headers_status, "redirects": redirects_status}


def cloudflare_previous_canonical_id(
    wrangler_command: list[str], cloudflare_env: dict[str, str]
) -> str | None:
    """Capture the canonical deployment before any direct-upload mutation.

    Missing historical metadata is represented as null; no other deployment
    is guessed as a substitute. Project/custom-domain authority drift remains
    a hard refusal because a new deployment cannot repair that binding.
    """
    project, domains = cloudflare_project_contract(
        wrangler_command, cloudflare_env
    )
    if (
        not isinstance(project, dict)
        or project.get("name") != CLOUDFLARE_PROJECT
        or project.get("production_branch") != CLOUDFLARE_BRANCH
        or project.get("source") is not None
        or not isinstance(project.get("domains"), list)
        or set(project["domains"]) != {"vane-web.pages.dev", CDN_DOMAIN}
        or not isinstance(domains, list)
        or not any(
            isinstance(item, dict)
            and item.get("name") == CDN_DOMAIN
            and item.get("status") == "active"
            and isinstance(item.get("validation_data"), dict)
            and item["validation_data"].get("status") == "active"
            and isinstance(item.get("verification_data"), dict)
            and item["verification_data"].get("status") == "active"
            for item in domains
        )
    ):
        raise RuntimeError("Cloudflare Pages project authority is not direct-upload")
    canonical = project.get("canonical_deployment")
    if canonical is None:
        return None
    if (
        not isinstance(canonical, dict)
        or not isinstance(canonical.get("aliases"), list)
        or f"https://{CDN_DOMAIN}" not in canonical["aliases"]
    ):
        raise RuntimeError("Cloudflare canonical custom-domain authority differs")
    stage = canonical.get("latest_stage")
    trigger = canonical.get("deployment_trigger")
    metadata = trigger.get("metadata") if isinstance(trigger, dict) else None
    deployment_id = canonical.get("id")
    if (
        not isinstance(deployment_id, str)
        or not deployment_id.isascii()
        or not deployment_id
        or canonical.get("environment") != "production"
        or not isinstance(stage, dict)
        or stage.get("name") != "deploy"
        or stage.get("status") != "success"
        or not isinstance(metadata, dict)
        or metadata.get("branch") != CLOUDFLARE_BRANCH
        or metadata.get("commit_dirty") is not False
        or not isinstance(metadata.get("commit_hash"), str)
        or re.fullmatch(r"[0-9a-f]{40}", metadata["commit_hash"]) is None
    ):
        return None
    return deployment_id


def adopt_cloudflare_deployment(
    *, wrangler_command: list[str], cloudflare_env: dict[str, str], revision: str,
    expected_marker: bytes, expected_index_sha256: str, custom_origin: str,
    expected_files: dict[str, dict], serving_files: dict[str, dict],
    control_files: dict[str, dict],
    previous_canonical_deployment_id: str | None,
) -> dict | None:
    account = cloudflare_env["CLOUDFLARE_ACCOUNT_ID"]
    project = cloudflare_api(
        account, f"pages/projects/{CLOUDFLARE_PROJECT}", cloudflare_env
    )
    domains = cloudflare_api(
        account, f"pages/projects/{CLOUDFLARE_PROJECT}/domains", cloudflare_env
    )
    if (
        custom_origin != f"https://{CDN_DOMAIN}"
        or
        not isinstance(project, dict)
        or project.get("name") != CLOUDFLARE_PROJECT
        or project.get("production_branch") != CLOUDFLARE_BRANCH
        or project.get("source") is not None
        or not isinstance(project.get("domains"), list)
        or set(project["domains"]) != {"vane-web.pages.dev", CDN_DOMAIN}
        or not isinstance(domains, list)
        or not any(
            isinstance(item, dict)
            and item.get("name") == CDN_DOMAIN
            and item.get("status") == "active"
            and isinstance(item.get("validation_data"), dict)
            and item["validation_data"].get("status") == "active"
            and isinstance(item.get("verification_data"), dict)
            and item["verification_data"].get("status") == "active"
            for item in domains
        )
    ):
        raise RuntimeError("Cloudflare Pages project authority is not direct-upload")
    canonical = project.get("canonical_deployment")
    if canonical is not None and (
        not isinstance(canonical, dict)
        or not isinstance(canonical.get("aliases"), list)
        or f"https://{CDN_DOMAIN}" not in canonical["aliases"]
    ):
        # A new deployment can repair stale bytes or an old revision, but it
        # cannot repair a project/custom-domain authority mismatch.
        raise RuntimeError("Cloudflare canonical custom-domain authority differs")
    try:
        deployment = validate_cloudflare_deployment(
            canonical, revision
        )
        for cloudflare_origin in (deployment["url"], CLOUDFLARE_ORIGIN):
            verify_public_release(
                cloudflare_origin,
                revision,
                expected_marker=expected_marker,
                expected_index_sha256=expected_index_sha256,
                expected_files=expected_files,
                index_path="/",
                directory_indexes=True,
                attempts=1,
            )
        custom_smoke = verify_cloudflare_custom_edge(
            revision, expected_marker, expected_index_sha256, expected_files,
            control_files,
        )
        _, custom_digest = serving_file_evidence(expected_files)
        custom_smoke["verified_files_sha256"] = custom_digest
        control_smoke = verify_cloudflare_controls(
            deployment["url"], revision, control_files, expected_files
        )
        verified_files, verified_files_sha256 = serving_file_evidence(expected_files)
        control_records, control_files_sha256 = serving_file_evidence(control_files)
        return {
            "schema": CLOUDFLARE_RECEIPT_SCHEMA,
            "project": CLOUDFLARE_PROJECT,
            "deployment_id": deployment["id"],
            "deployment_url": deployment["url"],
            "production_origin": CLOUDFLARE_ORIGIN,
            "source_sha": revision,
            "commit_dirty": False,
            "artifact_receipt_sha256": "",
            "previous_canonical_deployment_id": (
                previous_canonical_deployment_id
                if previous_canonical_deployment_id != deployment["id"]
                else None
            ),
            "index_sha256": expected_index_sha256,
            "marker_sha256": hashlib.sha256(expected_marker).hexdigest(),
            "verified_files": verified_files,
            "verified_files_sha256": verified_files_sha256,
            "custom_alias": f"https://{CDN_DOMAIN}",
            "custom_origin_smoke": custom_smoke,
            "control_files": control_records,
            "control_files_sha256": control_files_sha256,
            "control_smoke": control_smoke,
            "status": "verified",
        }
    except RuntimeError:
        return None


def publish_cloudflare(
    *, wrangler_command: list[str], cloudflare_env: dict[str, str], dist: Path,
    revision: str, expected_marker: bytes, expected_index_sha256: str,
    custom_origin: str,
    expected_files: dict[str, dict], serving_files: dict[str, dict],
    control_files: dict[str, dict],
    previous_canonical_deployment_id: str | None,
) -> tuple[dict, bool]:
    adopted = adopt_cloudflare_deployment(
        wrangler_command=wrangler_command,
        cloudflare_env=cloudflare_env,
        revision=revision,
        expected_marker=expected_marker,
        expected_index_sha256=expected_index_sha256,
        custom_origin=custom_origin,
        expected_files=expected_files,
        serving_files=serving_files,
        control_files=control_files,
        previous_canonical_deployment_id=previous_canonical_deployment_id,
    )
    if adopted is not None:
        return adopted, True
    current_canonical_id = cloudflare_previous_canonical_id(
        wrangler_command, cloudflare_env
    )
    if (
        previous_canonical_deployment_id is not None
        and current_canonical_id != previous_canonical_deployment_id
    ):
        raise RuntimeError(
            "Cloudflare canonical deployment changed after pending evidence"
        )
    try:
        run(
            [
                *wrangler_command, "pages", "deploy", str(dist),
                "--project-name", CLOUDFLARE_PROJECT,
                "--branch", CLOUDFLARE_BRANCH,
                "--commit-hash", revision,
                "--commit-dirty=false",
            ],
            env=cloudflare_env,
            capture=True,
        )
    except RuntimeError as error:
        raise RuntimeError(f"Cloudflare Pages publication failed: {error}") from error
    receipt = adopt_cloudflare_deployment(
        wrangler_command=wrangler_command,
        cloudflare_env=cloudflare_env,
        revision=revision,
        expected_marker=expected_marker,
        expected_index_sha256=expected_index_sha256,
        custom_origin=custom_origin,
        expected_files=expected_files,
        serving_files=serving_files,
        control_files=control_files,
        previous_canonical_deployment_id=previous_canonical_deployment_id,
    )
    if receipt is None:
        raise RuntimeError(
            "Cloudflare Pages deployment did not become exact production success"
        )
    return receipt, False


def aliyun_exact(
    *, ossutil: Path, provider_env: dict[str, str], dist: Path,
    readback_root: Path, expected_files: dict[str, dict],
) -> bool:
    try:
        for object_name in sorted(expected_files):
            verify_oss_object(
                ossutil, object_name, dist / object_name, provider_env, readback_root
            )
        if OWNER_PREVIEW in expected_files:
            metadata = run(
                [str(ossutil), "stat", f"oss://{BUCKET}/{OWNER_PREVIEW}"],
                env=provider_env,
                capture=True,
            )
            if re.search(
                r"(?im)^cache-control\s*:\s*no-store\s*$", metadata
            ) is None:
                return False
        return True
    except RuntimeError:
        return False


class PinnedEdgeHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, ip: str) -> None:
        super().__init__(CDN_DOMAIN, timeout=5, context=ssl.create_default_context())
        self._edge_ip = ip

    def connect(self) -> None:
        raw = socket.create_connection((self._edge_ip, 443), self.timeout)
        self.sock = self._context.wrap_socket(raw, server_hostname=CDN_DOMAIN)


def aliyun_doh_addresses(cname: str) -> list[str]:
    resolvers = (
        ("AliDNS", "https://dns.alidns.com/resolve", "object"),
        ("Cloudflare", "https://cloudflare-dns.com/dns-query", "array"),
        ("Google", "https://dns.google/resolve", "array"),
    )
    failures: list[str] = []
    resolver_groups: list[list[str]] = []
    for resolver, endpoint, question_shape in resolvers:
        addresses: set[str] = set()
        try:
            for record_type, type_code in (("A", 1), ("AAAA", 28)):
                target = endpoint + "?" + urlencode({
                    "name": cname, "type": record_type,
                })
                request = Request(
                    target, headers={
                        "Accept": "application/dns-json",
                        "User-Agent": PUBLIC_USER_AGENT,
                    }
                )
                with urlopen(request, timeout=10) as response:
                    if response.status != 200:
                        raise RuntimeError(
                            f"{resolver} DoH returned HTTP {response.status}"
                        )
                    raw = response.read(1024 * 1024 + 1)
                value = json.loads(raw, object_pairs_hook=strict_pairs)
                question = value.get("Question") if isinstance(value, dict) else None
                answers = value.get("Answer") if isinstance(value, dict) else None
                if question_shape == "object":
                    question_ok = (
                        isinstance(question, dict)
                        and question.get("name", "").rstrip(".") == cname
                        and question.get("type") == type_code
                    )
                else:
                    question_ok = (
                        isinstance(question, list) and len(question) == 1
                        and isinstance(question[0], dict)
                        and question[0].get("name", "").rstrip(".") == cname
                        and question[0].get("type") == type_code
                    )
                if (
                    not isinstance(value, dict) or value.get("Status") != 0
                    or not question_ok
                    or (answers is not None and not isinstance(answers, list))
                ):
                    raise RuntimeError(f"{resolver} DoH response authority is invalid")
                for answer in answers or []:
                    if (
                        not isinstance(answer, dict)
                        or answer.get("name", "").rstrip(".") != cname
                        or answer.get("type") != type_code
                        or not isinstance(answer.get("data"), str)
                    ):
                        raise RuntimeError(f"{resolver} DoH answer is not exact")
                    address = ipaddress.ip_address(answer["data"])
                    if not address.is_global:
                        raise RuntimeError(
                            f"{resolver} DoH returned a non-public edge"
                        )
                    addresses.add(str(address))
            if addresses:
                resolver_groups.append(sorted(addresses))
            else:
                raise RuntimeError(f"{resolver} DoH returned no public edge")
        except Exception as error:
            failures.append(str(error))
    if resolver_groups:
        # Preserve resolver independence at the connection layer.  A global
        # sort would put every currently unreachable AliDNS 103.* candidate
        # ahead of a healthy Cloudflare/Google answer and multiply the pinned
        # TLS timeout by the size of that first answer set.  Round-robin keeps
        # every strictly validated answer available while reaching the next
        # resolver after at most one failed candidate from the previous one.
        ordered: list[str] = []
        seen: set[str] = set()
        for offset in range(max(len(group) for group in resolver_groups)):
            for group in resolver_groups:
                if offset < len(group) and group[offset] not in seen:
                    seen.add(group[offset])
                    ordered.append(group[offset])
        return ordered
    raise RuntimeError("trusted DoH resolvers returned no public edge: " + "; ".join(failures))


def verify_cloudflare_custom_edge(
    revision: str, expected_marker: bytes, expected_index_sha256: str,
    expected_files: dict[str, dict],
    control_files: dict[str, dict] | None = None,
) -> dict:
    """Verify the overseas custom hostname through a pinned CF Pages edge.

    The project/domain APIs and GeoDNS contract establish that the custom alias
    belongs to this Pages project.  Resolving pages.dev (not the GeoDNS custom
    hostname) and sending SNI/Host for the custom hostname makes this an
    independent overseas-route smoke without treating local GeoDNS as authority.
    """
    cname = "vane-web.pages.dev"
    objects = {
        path: record for path, record in expected_files.items()
        if path not in {"index.html", RELEASE_MARKER_PATH}
    }
    last_error: Exception | None = None
    for address in aliyun_doh_addresses(cname):
        try:
            def read_exact_object(
                object_name: str, *, limit: int,
                expected_bytes: bytes | None = None,
                expected_size: int | None = None,
                expected_sha256: str | None = None,
            ) -> bytes:
                object_error: Exception | None = None
                for object_attempt in range(1, PUBLIC_OBJECT_ATTEMPTS + 1):
                    connection = PinnedEdgeHTTPSConnection(address)
                    try:
                        connection.request(
                            "GET",
                            cloudflare_public_path(object_name)
                            + f"?release={revision}&cf-edge=1"
                            + f"&object-attempt={object_attempt}",
                            headers={
                                "Host": CDN_DOMAIN,
                                "Cache-Control": "no-cache",
                                "User-Agent": PUBLIC_USER_AGENT,
                            },
                        )
                        response = connection.getresponse()
                        if response.status != 200:
                            raise RuntimeError(
                                "Cloudflare pinned custom edge returned HTTP "
                                f"{response.status}: {object_name}"
                            )
                        payload = response.read(limit)
                    except Exception as error:
                        object_error = error
                    else:
                        if expected_bytes is not None and payload != expected_bytes:
                            object_error = RuntimeError(
                                "Cloudflare custom edge object differs from artifact: "
                                f"{object_name}"
                            )
                        elif (
                            expected_size is not None
                            and expected_sha256 is not None
                            and (
                                len(payload) != expected_size
                                or hashlib.sha256(payload).hexdigest()
                                != expected_sha256
                            )
                        ):
                            object_error = RuntimeError(
                                "Cloudflare custom edge object differs from artifact: "
                                f"{object_name}"
                            )
                        else:
                            return payload
                    finally:
                        connection.close()
                    if object_attempt < PUBLIC_OBJECT_ATTEMPTS:
                        time.sleep(0.25 * object_attempt)
                raise RuntimeError(
                    "Cloudflare custom edge object did not converge: "
                    f"{object_name}: {object_error}"
                )

            bodies: dict[str, bytes] = {}
            bodies[RELEASE_MARKER_PATH] = read_exact_object(
                RELEASE_MARKER_PATH, limit=64 * 1024 + 1,
                expected_bytes=expected_marker,
            )
            index_record = expected_files.get("index.html")
            if not isinstance(index_record, dict):
                raise RuntimeError("Cloudflare custom edge index evidence is missing")
            bodies["index.html"] = read_exact_object(
                "index.html", limit=8 * 1024 * 1024 + 1,
                expected_size=index_record["size"],
                expected_sha256=expected_index_sha256,
            )
            def verify_object(path: str) -> None:
                record = objects[path]
                read_exact_object(
                    path, limit=record["size"] + 1,
                    expected_size=record["size"],
                    expected_sha256=record["sha256"],
                )
            parallel_apply(
                "Cloudflare pinned custom edge verification",
                sorted(objects), verify_object, workers=CDN_WORKERS,
            )
            if control_files is not None:
                verify_cloudflare_controls(
                    f"https://{CDN_DOMAIN}", revision, control_files,
                    expected_files, edge_ip=address,
                )
            return {"host": CDN_DOMAIN, "via": cname, "edge_ip": address}
        except Exception as error:
            last_error = error
    raise RuntimeError(
        f"Cloudflare pinned custom edge verification failed: {last_error}"
    )


def verify_aliyun_edge(
    cname: str, revision: str, expected_marker: bytes, expected_index_sha256: str,
    expected_files: dict[str, dict],
) -> dict:
    if cname != ALIYUN_CDN_CNAME:
        raise RuntimeError("Aliyun CDN CNAME is outside the exact authority")
    addresses = aliyun_doh_addresses(cname)
    edge_errors: list[str] = []
    for address in addresses:
        try:
            parsed = ipaddress.ip_address(address)
            if not parsed.is_global:
                raise RuntimeError("Aliyun CDN resolved to a non-public edge")
            bodies: list[bytes] = []
            for path, limit in (
                (f"/{RELEASE_MARKER_PATH}?release={revision}&edge=1", 64 * 1024 + 1),
                (f"/index.html?release={revision}&edge=1", 8 * 1024 * 1024 + 1),
            ):
                connection = PinnedEdgeHTTPSConnection(address)
                try:
                    connection.request(
                        "GET", path,
                        headers={
                            "Host": CDN_DOMAIN, "Cache-Control": "no-cache",
                            "User-Agent": PUBLIC_USER_AGENT,
                        },
                    )
                    response = connection.getresponse()
                    if response.status != 200:
                        raise RuntimeError(
                            f"Aliyun pinned edge returned HTTP {response.status}"
                        )
                    bodies.append(response.read(limit))
                finally:
                    connection.close()
            if bodies[0] != expected_marker:
                raise RuntimeError("Aliyun pinned edge marker differs from artifact")
            if hashlib.sha256(bodies[1]).hexdigest() != expected_index_sha256:
                raise RuntimeError("Aliyun pinned edge index differs from artifact")
            object_names = [
                name for name in sorted(expected_files)
                if name not in {"index.html", RELEASE_MARKER_PATH}
            ]
            preview_headers: dict[str, str] = {}

            def verify_object(object_name: str) -> None:
                record = expected_files[object_name]
                connection = PinnedEdgeHTTPSConnection(address)
                try:
                    connection.request(
                        "GET",
                        "/" + quote(object_name, safe="/-._~")
                        + f"?release={revision}&edge=1",
                        headers={
                            "Host": CDN_DOMAIN, "Cache-Control": "no-cache",
                            "User-Agent": PUBLIC_USER_AGENT,
                        },
                    )
                    response = connection.getresponse()
                    if response.status != 200:
                        raise RuntimeError(
                            f"Aliyun pinned edge object returned HTTP {response.status}"
                        )
                    if object_name == OWNER_PREVIEW:
                        preview_headers.update({
                            name.lower(): value
                            for name, value in response.getheaders()
                        })
                    payload = response.read(record["size"] + 1)
                finally:
                    connection.close()
                if (
                    len(payload) != record["size"]
                    or hashlib.sha256(payload).hexdigest() != record["sha256"]
                ):
                    raise RuntimeError(
                        f"Aliyun pinned edge object differs from artifact: {object_name}"
                    )
            parallel_apply(
                "Aliyun pinned edge object verification", object_names,
                verify_object, workers=CDN_WORKERS,
            )
            if OWNER_PREVIEW in expected_files:
                cache = preview_headers.get("cache-control", "").lower().replace(" ", "")
                if "no-store" not in cache:
                    raise RuntimeError(
                        "Aliyun pinned owner preview cache header differs"
                    )
            redirect_status = "not-present"
            if "_redirects" in expected_files:
                for source, destination in (
                    ("/.well-known/agent-card.json", "https://api.vane.zhuoqidev.com/.well-known/agent-card.json"),
                    ("/.well-known/probe.json", "https://api.vane.zhuoqidev.com/.well-known/probe.json"),
                ):
                    connection = PinnedEdgeHTTPSConnection(address)
                    try:
                        connection.request(
                            "GET", source,
                            headers={
                                "Host": CDN_DOMAIN, "Cache-Control": "no-cache",
                                "User-Agent": PUBLIC_USER_AGENT,
                            },
                        )
                        response = connection.getresponse()
                        headers = {
                            name.lower(): value for name, value in response.getheaders()
                        }
                        response.read(1024 * 1024 + 1)
                    finally:
                        connection.close()
                    if response.status != 302 or headers.get("location") != destination:
                        raise RuntimeError("Aliyun CDN RoutingRules behavior differs")
                redirect_status = "verified"
            return {
                "ip": address,
                "cname": cname,
                "control_smoke": {"redirects": redirect_status},
            }
        except Exception as error:
            edge_errors.append(f"{address}: {error}")
    raise RuntimeError(
        "Aliyun pinned edge exact verification failed: " + "; ".join(edge_errors)
    )


def verify_final_provider_bytes(
    *,
    revision: str,
    expected_marker: bytes,
    expected_index_sha256: str,
    cloudflare_receipt: dict,
    cloudflare_files: dict[str, dict],
    control_files: dict[str, dict],
    ossutil: Path,
    provider_env: dict[str, str],
    dist: Path,
    readback_root: Path,
    receipt_files: dict[str, dict],
) -> dict:
    """Re-prove both provider data planes immediately before final authority.

    Every successful publication path uses this same byte gate.  Provider
    metadata is intentionally checked by the caller *after* this function so
    the final route/canonical check remains the last network authority before
    writing current state or a publication result.
    """
    for cloudflare_origin in (
        cloudflare_receipt["deployment_url"],
        CLOUDFLARE_ORIGIN,
    ):
        verify_public_release(
            cloudflare_origin,
            revision,
            expected_marker=expected_marker,
            expected_index_sha256=expected_index_sha256,
            expected_files=cloudflare_files,
            index_path="/",
            directory_indexes=True,
        )
    custom_smoke = verify_cloudflare_custom_edge(
        revision,
        expected_marker,
        expected_index_sha256,
        cloudflare_files,
        control_files,
    )
    _, cloudflare_files_sha256 = serving_file_evidence(cloudflare_files)
    custom_smoke["verified_files_sha256"] = cloudflare_files_sha256
    control_smoke = verify_cloudflare_controls(
        cloudflare_receipt["deployment_url"],
        revision,
        control_files,
        cloudflare_files,
    )
    if not aliyun_exact(
        ossutil=ossutil,
        provider_env=provider_env,
        dist=dist,
        readback_root=readback_root,
        expected_files=receipt_files,
    ):
        raise RuntimeError("Aliyun exact final verification failed")
    aliyun_edge = verify_aliyun_edge(
        ALIYUN_CDN_CNAME,
        revision,
        expected_marker,
        expected_index_sha256,
        receipt_files,
    )
    return {
        "cloudflare_custom": custom_smoke,
        "cloudflare_controls": control_smoke,
        "aliyun_edge": aliyun_edge,
    }


def persist_provider_receipt(state_root: Path, receipt: dict) -> str:
    payload = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    directory = state_root / "web-provider-receipts"
    if directory.is_symlink():
        raise RuntimeError("Web provider receipt root is unsafe")
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / f"{digest}.json"
    if path.exists():
        if path.is_symlink() or path.read_bytes() != payload:
            raise RuntimeError("Web provider receipt digest collision")
    else:
        atomic_json(path, receipt)
    return digest


def load_provider_receipt(state_root: Path, digest: str, subject: str) -> dict:
    path = state_root / "web-provider-receipts" / f"{digest}.json"
    value = load_strict_json(path, subject)
    if sha256(path) != digest:
        raise RuntimeError(f"{subject} digest differs from current state")
    return value


def write_pending(
    path: Path, revision: str, artifact_digest: str,
    cloudflare_status: str, aliyun_status: str,
    previous_canonical_deployment_id: str | None,
) -> None:
    atomic_json(path, {
        "schema": PENDING_SCHEMA,
        "revision": revision,
        "artifact_receipt_sha256": artifact_digest,
        "previous_canonical_deployment_id": previous_canonical_deployment_id,
        "providers": {
            "cloudflare_pages": cloudflare_status,
            "aliyun": aliyun_status,
        },
    })


def validate_pending(value: dict) -> dict:
    statuses = {"not_started", "verified", "failed"}
    if (
        not isinstance(value, dict)
        or set(value) != {
            "schema", "revision", "artifact_receipt_sha256",
            "previous_canonical_deployment_id", "providers",
        }
        or value.get("schema") != PENDING_SCHEMA
        or not isinstance(value.get("revision"), str)
        or re.fullmatch(r"[0-9a-f]{40}", value["revision"]) is None
        or not isinstance(value.get("artifact_receipt_sha256"), str)
        or DIGEST_RE.fullmatch(value["artifact_receipt_sha256"]) is None
        or (
            value.get("previous_canonical_deployment_id") is not None
            and (
                not isinstance(value["previous_canonical_deployment_id"], str)
                or not value["previous_canonical_deployment_id"].isascii()
                or not value["previous_canonical_deployment_id"]
            )
        )
        or not isinstance(value.get("providers"), dict)
        or set(value["providers"]) != {"cloudflare_pages", "aliyun"}
        or any(status not in statuses for status in value["providers"].values())
    ):
        raise RuntimeError("Web pending publication state is invalid")
    return value


def publish(
    *, dist: Path, revision: str, work_root: Path, state_root: Path,
    tool_cache: Path, origin: str, result_path: Path,
    expected_web_tree_sha256: str,
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("Web publication revision is not an exact SHA")
    if DIGEST_RE.fullmatch(expected_web_tree_sha256) is None:
        raise RuntimeError("Web publication Gate tree digest is invalid")
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

    (
        aliyun, ossutil, wrangler_command, provider_env, aliyun_env,
        cloudflare_env,
    ) = publication_runtime(tool_cache)
    pending_file = state_root / "web-pending.json"
    if pending_file.is_symlink() or (
        pending_file.exists() and not pending_file.is_file()
    ):
        raise RuntimeError("Web publication pending state path is unsafe")

    with lock_file.open("a+b") as state_lock:
        fcntl.flock(state_lock, fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(prefix="vane-web-plan-", dir=work_root) as temporary:
            temporary_root = Path(temporary)
            source_dist = dist
            snapshot = temporary_root / "dist-snapshot"
            shutil.copytree(source_dist, snapshot, copy_function=shutil.copy2)
            if directory_tree_sha256(snapshot) != expected_web_tree_sha256:
                raise RuntimeError("Web private snapshot differs from signed Gate tree")
            dist = snapshot
            plan = temporary_root / "plan"
            plan.mkdir()
            run(
                [
                    str(ROOT / "ops/release/web-release.py"),
                    "--dist", str(dist), "--sha", revision, "--output", str(plan),
                ],
                env=runtime_environment(),
                capture=True,
            )
            receipt = plan / "release.json"
            receipt_digest = sha256(receipt)
            validate_proof_destination(state_root, receipt_digest)
            receipt_value = load_strict_json(receipt, "Web artifact receipt")
            receipt_files = validate_receipt(receipt_value, revision)
            marker_path = dist / RELEASE_MARKER_PATH
            if marker_path.is_symlink() or not marker_path.is_file():
                raise RuntimeError("Web release marker is missing or unsafe")
            expected_marker = marker_path.read_bytes()
            validate_release_marker(expected_marker, revision)
            expected_index_sha256 = sha256(dist / "index.html")
            if expected_index_sha256 != receipt_files["index.html"]["sha256"]:
                raise RuntimeError("Web entrypoint differs from its artifact receipt")
            asset_objects = lines(plan / "assets.list")
            if len(asset_objects) != len(set(asset_objects)):
                raise RuntimeError("Web asset plan contains duplicate objects")
            critical_objects: list[str] = []
            for row in lines(plan / "critical-assets.list"):
                size, object_name = row.split("\t", 1)
                record = receipt_files.get(object_name)
                local = dist / object_name
                if (
                    record is None
                    or int(size) != record["size"]
                    or int(size) != local.stat().st_size
                    or sha256(local) != record["sha256"]
                ):
                    raise RuntimeError(f"local critical asset differs: {object_name}")
                critical_objects.append(object_name)
            if len(critical_objects) != len(set(critical_objects)):
                raise RuntimeError("Web critical asset plan contains duplicate objects")
            serving_files = {
                object_name: receipt_files[object_name]
                for object_name in sorted(receipt_files)
                if object_name in {"index.html", RELEASE_MARKER_PATH}
                or CONTENT_HASH_RE.fullmatch(PurePosixPath(object_name).name)
                is not None
            }
            verified_files, verified_files_sha256 = serving_file_evidence(
                receipt_files
            )
            control_files = {
                path: receipt_files[path]
                for path in ("_headers", "_redirects") if path in receipt_files
            }
            cloudflare_files = {
                path: record for path, record in receipt_files.items()
                if path not in control_files
            }
            if any(object_name not in receipt_files for object_name in asset_objects):
                raise RuntimeError("Web asset plan contains an object absent from its receipt")

            current: dict | None = None
            if state_file.is_file() and not state_file.is_symlink():
                current = validate_current(
                    load_strict_json(state_file, "Web current state")
                )
                if (
                    current["schema"] == CURRENT_SCHEMA
                    and current["revision"] == revision
                    and current["artifact_receipt_sha256"] != receipt_digest
                ):
                    raise RuntimeError(
                        "same Web revision has a different immutable artifact receipt"
                    )
                if (
                    current["schema"] == "vane.web-current/v1"
                    and current["revision"] == revision
                    and validate_legacy_artifact_receipt(
                        state_root, current, revision
                    ) != receipt_files
                ):
                    raise RuntimeError(
                        "same legacy Web revision has a different immutable artifact receipt"
                    )
            pending: dict | None = None
            if pending_file.is_file():
                pending = validate_pending(
                    load_strict_json(pending_file, "Web pending publication")
                )
                if (
                    pending["revision"] == revision
                    and pending["artifact_receipt_sha256"] != receipt_digest
                ):
                    raise RuntimeError(
                        "same pending Web revision has a different artifact receipt"
                    )
            had_pending = bool(
                pending is not None
                and pending["revision"] == revision
                and pending["artifact_receipt_sha256"] == receipt_digest
            )
            if had_pending:
                previous_canonical_deployment_id = pending[
                    "previous_canonical_deployment_id"
                ]
            elif (
                current is not None
                and current["schema"] == CURRENT_SCHEMA
                and current["revision"] == revision
            ):
                current_cloudflare = validate_cloudflare_receipt(
                    load_provider_receipt(
                        state_root,
                        current["providers"]["cloudflare_pages"],
                        "Cloudflare provider receipt",
                    ),
                    revision,
                    receipt_digest,
                )
                previous_canonical_deployment_id = current_cloudflare[
                    "previous_canonical_deployment_id"
                ]
            else:
                previous_canonical_deployment_id = cloudflare_previous_canonical_id(
                    wrangler_command, cloudflare_env
                )
            write_pending(
                pending_file, revision, receipt_digest, "not_started", "not_started",
                previous_canonical_deployment_id,
            )

            def assert_artifact_unchanged() -> None:
                if directory_tree_sha256(dist) != expected_web_tree_sha256:
                    raise RuntimeError("Web private snapshot changed during publication")
                if directory_tree_sha256(source_dist) != expected_web_tree_sha256:
                    raise RuntimeError("Gate Web dist changed during publication")

            try:
                assert_artifact_unchanged()
                cloudflare_receipt, cloudflare_adopted = publish_cloudflare(
                    wrangler_command=wrangler_command,
                    cloudflare_env=cloudflare_env,
                    dist=dist,
                    revision=revision,
                    expected_marker=expected_marker,
                    expected_index_sha256=expected_index_sha256,
                    custom_origin=origin,
                    expected_files=cloudflare_files,
                    serving_files=serving_files,
                    control_files=control_files,
                    previous_canonical_deployment_id=(
                        previous_canonical_deployment_id
                    ),
                )
                cloudflare_receipt["artifact_receipt_sha256"] = receipt_digest
                assert_artifact_unchanged()
            except RuntimeError:
                write_pending(
                    pending_file, revision, receipt_digest, "failed", "not_started",
                    previous_canonical_deployment_id,
                )
                raise
            write_pending(
                pending_file, revision, receipt_digest, "verified", "not_started",
                previous_canonical_deployment_id,
            )

            prior_verified: dict[str, dict] = {}
            if (
                current is not None
                and current["schema"] == CURRENT_SCHEMA
                and current["revision"] != revision
            ):
                prior_verified = load_provider_proof(
                    state_root=state_root, current=current
                )
            reusable_candidates = {
                object_name
                for object_name in critical_objects
                if (
                    CONTENT_HASH_RE.fullmatch(PurePosixPath(object_name).name)
                    is not None
                    and prior_verified.get(object_name) == receipt_files[object_name]
                )
            }
            readback_root = plan / "provider-readback"
            reusable: set[str] = set()
            for object_name in sorted(reusable_candidates):
                try:
                    verify_oss_object(
                        ossutil, object_name, dist / object_name,
                        provider_env, readback_root,
                    )
                    reusable.add(object_name)
                except RuntimeError:
                    # Historical proof is not current OSS authority. Repair
                    # the missing/corrupt immutable object in this release.
                    pass
            aliyun_was_exact = aliyun_exact(
                ossutil=ossutil,
                provider_env=provider_env,
                dist=dist,
                readback_root=readback_root,
                expected_files=receipt_files,
            )
            if (
                current is not None
                and current["schema"] == CURRENT_SCHEMA
                and current["revision"] == revision
                and cloudflare_adopted
                and aliyun_was_exact
            ):
                cloudflare_digest = current["providers"]["cloudflare_pages"]
                aliyun_digest = current["providers"]["aliyun"]
                stored_cloudflare = validate_cloudflare_receipt(
                    load_provider_receipt(
                        state_root, cloudflare_digest,
                        "Cloudflare provider receipt",
                    ),
                    revision,
                    receipt_digest,
                )
                stored_aliyun = validate_aliyun_receipt(
                    load_provider_receipt(
                        state_root, aliyun_digest, "Aliyun provider receipt"
                    ),
                    revision,
                    receipt_digest,
                )
                assert_artifact_unchanged()
                verify_final_provider_bytes(
                    revision=revision,
                    expected_marker=expected_marker,
                    expected_index_sha256=expected_index_sha256,
                    cloudflare_receipt=stored_cloudflare,
                    cloudflare_files=cloudflare_files,
                    control_files=control_files,
                    ossutil=ossutil,
                    provider_env=provider_env,
                    dist=dist,
                    readback_root=readback_root,
                    receipt_files=receipt_files,
                )
                assert_artifact_unchanged()
                provider_route_authority(
                    aliyun, aliyun_env, wrangler_command, cloudflare_env,
                    expected_cloudflare_deployment_id=stored_cloudflare["deployment_id"],
                    revision=revision,
                )
                assert_artifact_unchanged()
                result = {
                    "schema": PUBLICATION_SCHEMA,
                    "revision": revision,
                    "artifact_receipt_sha256": receipt_digest,
                    "marker": validate_release_marker(expected_marker, revision),
                    "providers": {
                        "aliyun": {**stored_aliyun, "receipt_sha256": aliyun_digest},
                        "cloudflare_pages": {
                            **stored_cloudflare,
                            "receipt_sha256": cloudflare_digest,
                        },
                    },
                    "status": "already-current",
                }
                atomic_copy(
                    receipt, result_path.with_name("web-release-receipt.json")
                )
                atomic_json(
                    result_path.with_name("web-aliyun-receipt.json"), stored_aliyun
                )
                atomic_json(
                    result_path.with_name("web-cloudflare-receipt.json"),
                    stored_cloudflare,
                )
                atomic_json(result_path, result)
                pending_file.unlink(missing_ok=True)
                return result
            if cloudflare_adopted and aliyun_was_exact:
                assert_artifact_unchanged()
                final_evidence = verify_final_provider_bytes(
                    revision=revision,
                    expected_marker=expected_marker,
                    expected_index_sha256=expected_index_sha256,
                    cloudflare_receipt=cloudflare_receipt,
                    cloudflare_files=cloudflare_files,
                    control_files=control_files,
                    ossutil=ossutil,
                    provider_env=provider_env,
                    dist=dist,
                    readback_root=readback_root,
                    receipt_files=receipt_files,
                )
                assert_artifact_unchanged()
                cloudflare_receipt["custom_origin_smoke"] = final_evidence[
                    "cloudflare_custom"
                ]
                cloudflare_receipt["control_smoke"] = final_evidence[
                    "cloudflare_controls"
                ]
                edge = final_evidence["aliyun_edge"]
                provider_route_authority(
                    aliyun, aliyun_env, wrangler_command, cloudflare_env,
                    expected_cloudflare_deployment_id=cloudflare_receipt["deployment_id"],
                    revision=revision,
                )
                assert_artifact_unchanged()
                aliyun_receipt = {
                    "schema": ALIYUN_RECEIPT_SCHEMA,
                    "source_sha": revision,
                    "bucket": BUCKET,
                    "cdn_domain": CDN_DOMAIN,
                    "artifact_receipt_sha256": receipt_digest,
                    "index_sha256": expected_index_sha256,
                    "marker_sha256": hashlib.sha256(expected_marker).hexdigest(),
                    "verified_files": verified_files,
                    "verified_files_sha256": verified_files_sha256,
                    "refresh_tasks": [],
                    "edge_ip": edge["ip"],
                    "control_smoke": edge["control_smoke"],
                    "mode": "adopted",
                    "status": "verified",
                }
                validate_cloudflare_receipt(
                    cloudflare_receipt, revision, receipt_digest
                )
                validate_aliyun_receipt(
                    aliyun_receipt, revision, receipt_digest
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
                cloudflare_digest = persist_provider_receipt(
                    state_root, cloudflare_receipt
                )
                aliyun_digest = persist_provider_receipt(
                    state_root, aliyun_receipt
                )
                state = {
                    "schema": CURRENT_SCHEMA,
                    "revision": revision,
                    "artifact_receipt_sha256": receipt_digest,
                    "providers": {
                        "aliyun": aliyun_digest,
                        "cloudflare_pages": cloudflare_digest,
                    },
                }
                result = {
                    "schema": PUBLICATION_SCHEMA,
                    "revision": revision,
                    "artifact_receipt_sha256": receipt_digest,
                    "marker": validate_release_marker(expected_marker, revision),
                    "providers": {
                        "aliyun": {**aliyun_receipt, "receipt_sha256": aliyun_digest},
                        "cloudflare_pages": {
                            **cloudflare_receipt,
                            "receipt_sha256": cloudflare_digest,
                        },
                    },
                    "status": "recovered" if had_pending else "provider-already-current",
                }
                atomic_copy(
                    receipt, result_path.with_name("web-release-receipt.json")
                )
                atomic_json(
                    result_path.with_name("web-aliyun-receipt.json"), aliyun_receipt
                )
                atomic_json(
                    result_path.with_name("web-cloudflare-receipt.json"),
                    cloudflare_receipt,
                )
                atomic_json(state_file, state)
                atomic_json(result_path, result)
                pending_file.unlink(missing_ok=True)
                return result
            publication_started = time.monotonic()
            timings: dict[str, float | int] = {
                "asset_total": len(asset_objects),
                "reused_immutable": len(reusable),
                "uploaded": 0,
                "readback": 0,
            }
            refresh_receipts: list[dict] = []
            try:
                assert_artifact_unchanged()
                if not aliyun_was_exact:
                    upload_objects = [
                        value for value in asset_objects if value not in reusable
                    ]
                    readback_objects = [
                        value for value in critical_objects if value not in reusable
                    ]
                    timings["uploaded"] = len(upload_objects)
                    timings["readback"] = len(readback_objects)

                    def upload_asset(object_name: str) -> None:
                        run(
                            [
                                str(ossutil), "cp", str(dist / object_name),
                                f"oss://{BUCKET}/{object_name}", "--force",
                            ],
                            env=provider_env,
                        )

                    parallel_apply(
                        "OSS immutable upload", upload_objects, upload_asset,
                        workers=OSS_WORKERS,
                    )

                    def readback_asset(object_name: str) -> None:
                        verify_oss_object(
                            ossutil, object_name, dist / object_name,
                            provider_env, readback_root,
                        )

                    parallel_apply(
                        "OSS critical readback", readback_objects, readback_asset,
                        workers=OSS_WORKERS,
                    )
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
                    run(
                        [
                            str(ossutil), "cp", str(dist / "index.html"),
                            f"oss://{BUCKET}/index.html", "--force",
                        ],
                        env=provider_env,
                    )
                    verify_oss_object(
                        ossutil, "index.html", dist / "index.html",
                        provider_env, readback_root,
                    )
                    run(
                        [
                            str(ossutil), "cp", str(marker_path),
                            f"oss://{BUCKET}/{RELEASE_MARKER_PATH}", "--force",
                        ],
                        env=provider_env,
                    )
                    verify_oss_object(
                        ossutil, RELEASE_MARKER_PATH, marker_path,
                        provider_env, readback_root,
                    )

                for refresh_path in lines(plan / "cdn-refresh-paths.list"):
                    url = origin.rstrip("/") + refresh_path
                    raw = ""
                    for attempt in range(1, 4):
                        try:
                            raw = run(
                                [
                                    str(aliyun), "cdn", "RefreshObjectCaches",
                                    "--ObjectPath", url, "--ObjectType", "File",
                                ],
                                env=aliyun_env,
                                capture=True,
                            )
                            break
                        except RuntimeError:
                            if attempt == 3:
                                raise
                            time.sleep(attempt * 5)
                    refresh = parse_json_output(raw, "Aliyun CDN refresh")
                    task_id = (
                        refresh.get("RefreshTaskId")
                        if isinstance(refresh, dict) else None
                    )
                    if not isinstance(task_id, str) or not task_id.isdigit():
                        raise RuntimeError(
                            "Aliyun CDN refresh returned no exact task receipt"
                        )
                    refresh_receipts.append(
                        {"path": refresh_path, "task_id": task_id}
                    )
                if not aliyun_exact(
                    ossutil=ossutil,
                    provider_env=provider_env,
                    dist=dist,
                    readback_root=readback_root,
                    expected_files=receipt_files,
                    ):
                    raise RuntimeError(
                        "OSS exact readback failed after Aliyun publication"
                    )
                assert_artifact_unchanged()
            except RuntimeError as error:
                write_pending(
                    pending_file, revision, receipt_digest, "verified", "failed",
                    previous_canonical_deployment_id,
                )
                raise RuntimeError(f"Aliyun Web publication failed: {error}") from error

            detail = provider_route_authority(
                aliyun, aliyun_env, wrangler_command, cloudflare_env,
                expected_cloudflare_deployment_id=cloudflare_receipt["deployment_id"],
                revision=revision,
            )
            model = detail.get("GetDomainDetailModel") if isinstance(detail, dict) else None
            if (
                not isinstance(model, dict)
                or model.get("DomainName") != CDN_DOMAIN
                or model.get("DomainStatus") != "online"
                or not isinstance(model.get("Cname"), str)
            ):
                raise RuntimeError("Aliyun CDN final authority is invalid")
            edge = verify_aliyun_edge(
                model["Cname"], revision, expected_marker, expected_index_sha256,
                receipt_files,
            )
            aliyun_receipt = {
                "schema": ALIYUN_RECEIPT_SCHEMA,
                "source_sha": revision,
                "bucket": BUCKET,
                "cdn_domain": CDN_DOMAIN,
                "artifact_receipt_sha256": receipt_digest,
                "index_sha256": expected_index_sha256,
                "marker_sha256": hashlib.sha256(expected_marker).hexdigest(),
                "verified_files": verified_files,
                "verified_files_sha256": verified_files_sha256,
                "refresh_tasks": refresh_receipts,
                "edge_ip": edge["ip"],
                "control_smoke": edge["control_smoke"],
                "mode": "published",
                "status": "verified",
            }
            validate_cloudflare_receipt(
                cloudflare_receipt, revision, receipt_digest
            )
            validate_aliyun_receipt(aliyun_receipt, revision, receipt_digest)

            # Combined finalize is allowed only after a second independent
            # exact-byte verification of both provider authorities.  This is
            # the same gate used by the adopt/already-current fast paths.
            assert_artifact_unchanged()
            try:
                final_evidence = verify_final_provider_bytes(
                    revision=revision,
                    expected_marker=expected_marker,
                    expected_index_sha256=expected_index_sha256,
                    cloudflare_receipt=cloudflare_receipt,
                    cloudflare_files=cloudflare_files,
                    control_files=control_files,
                    ossutil=ossutil,
                    provider_env=provider_env,
                    dist=dist,
                    readback_root=readback_root,
                    receipt_files=receipt_files,
                )
            except RuntimeError:
                write_pending(
                    pending_file, revision, receipt_digest, "verified", "failed",
                    previous_canonical_deployment_id,
                )
                raise
            assert_artifact_unchanged()
            cloudflare_receipt["custom_origin_smoke"] = final_evidence[
                "cloudflare_custom"
            ]
            cloudflare_receipt["control_smoke"] = final_evidence[
                "cloudflare_controls"
            ]
            edge = final_evidence["aliyun_edge"]
            aliyun_receipt["edge_ip"] = edge["ip"]
            aliyun_receipt["control_smoke"] = edge["control_smoke"]
            validate_cloudflare_receipt(
                cloudflare_receipt, revision, receipt_digest
            )
            validate_aliyun_receipt(aliyun_receipt, revision, receipt_digest)
            provider_route_authority(
                aliyun, aliyun_env, wrangler_command, cloudflare_env,
                expected_cloudflare_deployment_id=cloudflare_receipt["deployment_id"],
                revision=revision,
            )
            assert_artifact_unchanged()

            persist_provider_proof(
                state_root=state_root,
                revision=revision,
                receipt=receipt,
                receipt_digest=receipt_digest,
                receipt_files=receipt_files,
                marker_path=marker_path,
                critical_objects=critical_objects,
            )
            cloudflare_digest = persist_provider_receipt(
                state_root, cloudflare_receipt
            )
            aliyun_digest = persist_provider_receipt(state_root, aliyun_receipt)
            state = {
                "schema": CURRENT_SCHEMA,
                "revision": revision,
                "artifact_receipt_sha256": receipt_digest,
                "providers": {
                    "aliyun": aliyun_digest,
                    "cloudflare_pages": cloudflare_digest,
                },
            }
            if (
                current is not None
                and current["schema"] == CURRENT_SCHEMA
                and current["revision"] == revision
            ):
                status = "already-current"
            elif had_pending:
                status = "recovered"
            elif cloudflare_adopted and aliyun_was_exact:
                status = "provider-already-current"
            else:
                status = "published"
            result = {
                "schema": PUBLICATION_SCHEMA,
                "revision": revision,
                "artifact_receipt_sha256": receipt_digest,
                "marker": validate_release_marker(expected_marker, revision),
                "providers": {
                    "aliyun": {
                        **aliyun_receipt, "receipt_sha256": aliyun_digest,
                    },
                    "cloudflare_pages": {
                        **cloudflare_receipt,
                        "receipt_sha256": cloudflare_digest,
                    },
                },
                "status": status,
            }
            atomic_copy(
                receipt, result_path.with_name("web-release-receipt.json")
            )
            atomic_json(
                result_path.with_name("web-aliyun-receipt.json"),
                aliyun_receipt,
            )
            atomic_json(
                result_path.with_name("web-cloudflare-receipt.json"),
                cloudflare_receipt,
            )
            atomic_json(state_file, state)
            atomic_json(result_path, result)
            pending_file.unlink(missing_ok=True)
            timings["total"] = round(
                time.monotonic() - publication_started, 3
            )
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
    mode.add_argument("--toolchain-check", action="store_true")
    parser.add_argument("--dist", type=Path)
    parser.add_argument("--sha")
    parser.add_argument("--work-root", type=Path)
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--tool-cache", type=Path)
    parser.add_argument("--origin")
    parser.add_argument("--result", type=Path)
    parser.add_argument("--publication-result", type=Path)
    parser.add_argument("--expected-web-tree-sha256")
    args = parser.parse_args()
    if args.toolchain_check:
        if args.tool_cache is None or any(
            value is not None for value in (
                args.dist, args.sha, args.work_root, args.state_root, args.origin,
                args.result, args.publication_result,
                args.expected_web_tree_sha256,
            )
        ):
            parser.error("--toolchain-check accepts only --tool-cache")
        _, _, _, _, evidence = publication_toolchain(args.tool_cache)
        print(json.dumps({
            "schema": "vane.web-toolchain-evidence/v1",
            "ok": True,
            "machine": machine_arch(),
            "digests": evidence,
        }, sort_keys=True, separators=(",", ":")))
        return 0
    if args.preflight:
        if any(value is not None for value in (args.dist, args.sha, args.work_root, args.state_root, args.result, args.publication_result, args.expected_web_tree_sha256)):
            parser.error("--preflight accepts only --tool-cache and --origin")
        if args.tool_cache is None or args.origin is None:
            parser.error("--preflight requires --tool-cache and --origin")
        result = preflight(args.tool_cache, args.origin)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    if args.verify_only:
        if (
            args.dist is None or args.sha is None or args.publication_result is None
            or args.origin is None
        ):
            parser.error("--verify-only requires --dist, --sha, --origin, and --publication-result")
        if any(value is not None for value in (args.work_root, args.state_root, args.result, args.expected_web_tree_sha256)):
            parser.error("--verify-only does not accept publication state paths")
        result = verify_finalized_publication(
            args.dist, args.sha, args.publication_result
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    if any(
        value is None
        for value in (
            args.dist, args.sha, args.work_root, args.state_root,
            args.tool_cache, args.origin, args.result,
            args.expected_web_tree_sha256,
        )
    ):
        parser.error(
            "publication requires --dist, --sha, --work-root, --state-root, and --result"
        )
    if args.publication_result is not None:
        parser.error("publication does not accept --publication-result")
    result = publish(
        dist=args.dist, revision=args.sha, work_root=args.work_root,
        state_root=args.state_root, tool_cache=args.tool_cache,
        origin=args.origin, result_path=args.result,
        expected_web_tree_sha256=args.expected_web_tree_sha256,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"Web publication refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
