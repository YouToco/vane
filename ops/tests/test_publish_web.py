from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "publish_web", ROOT / "ops/release/publish_web.py"
)
assert SPEC and SPEC.loader
publish_web = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish_web)
REAL_TOOLCHAIN_VALIDATOR = publish_web.validate_locked_publication_tools
SHA = "0123456789abcdef0123456789abcdef01234567"
SHA2 = "89abcdef0123456789abcdef0123456789abcdef"
PREVIEW = "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"


class PublicResponse:
    def __init__(self, payload: bytes, status: int = 200) -> None:
        self.payload = payload
        self.status = status

    def __enter__(self) -> "PublicResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        if len(self.payload) > limit:
            raise AssertionError("fixture exceeded response limit")
        return self.payload


class PublishWebTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.dist = self.root / "dist"
        (self.dist / "assets").mkdir(parents=True)
        (self.dist / "assets/app-AbCdEf12.js").write_text("app", encoding="utf-8")
        (self.dist / "index.html").write_text(
            '<script type="module" src="/assets/app-AbCdEf12.js"></script>',
            encoding="utf-8",
        )
        (self.dist / "vane-release.json").write_text(
            json.dumps({
                "schema": "vane.web-release/v1", "source_revision": SHA,
                "source_dirty": False, "tree_sha256": "a" * 64, "file_count": 2,
            }),
            encoding="utf-8",
        )
        self.work = self.root / "work"
        self.state = self.root / "state"
        self.cache = self.root / "cache"
        self.remote = self.root / "remote"
        self.cloudflare_remote = self.root / "cloudflare-remote"
        self.bin = self.root / "bin"
        for path in (
            self.work, self.state, self.cache, self.remote,
            self.cloudflare_remote, self.bin,
        ):
            path.mkdir()
        lock = json.loads((ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8"))["tools"]
        self.log = self.root / "calls.log"
        self.cloudflare_domain_reads = 0
        self.cloudflare_project_reads = 0
        self.aliyun_edge_reads = 0
        self.cloudflare_control_reads = 0
        self.aliyun_edge_overrides: dict[str, bytes] = {}
        aliyun = self.cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun"
        ossutil = self.cache / "ossutil" / lock["ossutil"]["version"] / "ossutil"
        node = self.cache / "node" / "22.23.2" / "bin/node"
        wrangler = (
            self.cache / "wrangler" / "4.115.0"
            / "node_modules/wrangler/bin/wrangler.js"
        )
        aliyun.parent.mkdir(parents=True)
        ossutil.parent.mkdir(parents=True)
        wrangler.parent.mkdir(parents=True)
        node.parent.mkdir(parents=True)
        aliyun.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = version ]; then echo 3.4.10; exit 0; fi\n"
            "printf 'aliyun %s\\n' \"$*\" >>\"$WEB_CALL_LOG\"\n"
            "if [ \"$1 $2\" = 'cdn DescribeCdnDomainDetail' ]; then printf '%s\\n' '{\"GetDomainDetailModel\":{\"DomainName\":\"vane.zhuoqidev.com\",\"DomainStatus\":\"online\",\"Cname\":\"vane.zhuoqidev.com.w.kunlunaq.com\"}}'; exit 0; fi\n"
            "if [ \"$1 $2\" = 'alidns DescribeDomainRecords' ]; then printf '%s\\n' '{\"DomainRecords\":{\"Record\":[{\"RR\":\"vane\",\"Type\":\"CNAME\",\"Line\":\"default\",\"Value\":\"vane.zhuoqidev.com.w.kunlunaq.com\",\"Status\":\"ENABLE\"},{\"RR\":\"vane\",\"Type\":\"CNAME\",\"Line\":\"oversea\",\"Value\":\"vane-web.pages.dev\",\"Status\":\"ENABLE\"}]}}'; exit 0; fi\n"
            "if [ \"$1 $2\" = 'cdn RefreshObjectCaches' ]; then printf '%s\\n' '{\"RefreshTaskId\":\"12345\"}'; exit 0; fi\n",
            encoding="utf-8",
        )
        ossutil.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = version ]; then echo 2.3.0; exit 0; fi\n"
            "printf 'ossutil %s\\n' \"$*\" >>\"$WEB_CALL_LOG\"\n"
            "case \"$1\" in\n"
            " cp) case \"$2\" in\n"
            "       oss://*) object=${2#oss://zhuoqidev-vane-web/}; [ \"${FAIL_DOWNLOAD-}\" != \"$object\" ] || exit 71; mkdir -p \"$(dirname \"$3\")\"; cp \"$OSS_REMOTE/$object\" \"$3\";;\n"
            "       *) object=${3#oss://zhuoqidev-vane-web/}; mkdir -p \"$OSS_REMOTE/$(dirname \"$object\")\"; cp \"$2\" \"$OSS_REMOTE/$object\"; [ \"${CORRUPT_UPLOAD-}\" != \"$object\" ] || printf bad >\"$OSS_REMOTE/$object\";;\n"
            "     esac;;\n"
            " stat) object=${2#oss://zhuoqidev-vane-web/}; [ \"${FAIL_STAT-}\" != \"$object\" ] || exit 71; size=$(wc -c <\"$OSS_REMOTE/$object\"); printf 'Content-Length : %s\\n' \"$size\"; [ \"$object\" != '_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html' ] || [ ! -f \"$OSS_REMOTE/.preview-no-store\" ] || printf 'Cache-Control : no-store\\n';;\n"
            " set-props) [ \"$3\" = --cache-control ] && [ \"$4\" = no-store ] || exit 72; touch \"$OSS_REMOTE/.preview-no-store\";;\n"
            " sync|rm) exit 73;;\n"
            "esac\n",
            encoding="utf-8",
        )
        aliyun.chmod(0o755)
        ossutil.chmod(0o755)
        node.write_text(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo v22.23.2; exit 0; fi\nexec \"$@\"\n",
            encoding="utf-8",
        )
        node.chmod(0o755)
        wrangler.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = --version ]; then echo '4.115.0'; exit 0; fi\n"
            "printf 'wrangler %s\\n' \"$*\" >>\"$WEB_CALL_LOG\"\n"
            "if [ \"$1 $2 $3\" = 'pages project list' ]; then printf '%s\\n' '[{\"name\":\"vane-web\",\"production_branch\":\"main\",\"domains\":[\"vane-web.pages.dev\",\"vane.zhuoqidev.com\"],\"source\":null}]'; exit 0; fi\n"
            "if [ \"$1 $2 $3\" = 'pages deployment list' ]; then if [ -f \"$CLOUDFLARE_REMOTE_SHA\" ]; then sha=$(cat \"$CLOUDFLARE_REMOTE_SHA\"); printf '[{\"id\":\"fixture\",\"url\":\"https://fixture.vane-web.pages.dev\",\"environment\":\"production\",\"latest_stage\":{\"name\":\"deploy\",\"status\":\"success\"},\"deployment_trigger\":{\"metadata\":{\"branch\":\"main\",\"commit_hash\":\"%s\"}}}]\\n' \"$sha\"; else printf '[]\\n'; fi; exit 0; fi\n"
            "if [ \"$1 $2\" = 'pages deploy' ]; then [ -z \"${FAIL_CLOUDFLARE_DEPLOY-}\" ] || exit 73; rm -rf \"$CLOUDFLARE_REMOTE\"/*; cp -R \"$3\"/. \"$CLOUDFLARE_REMOTE\"/; while [ $# -gt 0 ]; do if [ \"$1\" = --commit-hash ]; then printf '%s' \"$2\" >\"$CLOUDFLARE_REMOTE_SHA\"; break; fi; shift; done; echo 'https://fixture.vane-web.pages.dev'; exit 0; fi\n"
            "exit 74\n",
            encoding="utf-8",
        )
        wrangler.chmod(0o755)
        def fixture_lock(lock_value: dict, paths: dict[str, Path]) -> dict:
            value = json.loads(json.dumps(lock_value))
            arch = publish_web.machine_arch()
            for name, key in (
                ("aliyun_cli", "aliyun"),
                ("ossutil", "ossutil"),
                ("node", "node"),
                ("wrangler", "wrangler_js"),
            ):
                value[name]["entry_sha256"] = {
                    arch: publish_web.sha256(paths[key])
                }
            value["wrangler"]["installed_tree_sha256"] = {
                arch: publish_web.installed_tree_sha256(paths["wrangler_root"])
            }
            return value

        self.fixture_integrity_lock = fixture_lock
        self.toolchain_authority = mock.patch.object(
            publish_web,
            "validate_locked_publication_tools",
            side_effect=lambda lock_value, **paths: REAL_TOOLCHAIN_VALIDATOR(
                fixture_lock(lock_value, paths), **paths
            ),
        )
        self.toolchain_authority.start()
        self.addCleanup(self.toolchain_authority.stop)
        self.environment = mock.patch.dict(os.environ, {
            "ALIYUN_ACCESS_KEY_ID": "fixture-id",
            "ALIYUN_ACCESS_KEY_SECRET": "fixture-secret",
            "OSS_REMOTE": str(self.remote),
            "CLOUDFLARE_API_TOKEN": "fixture-cloudflare-token",
            "CLOUDFLARE_ACCOUNT_ID": "fixture-cloudflare-account",
            "CLOUDFLARE_REMOTE": str(self.cloudflare_remote),
            "CLOUDFLARE_REMOTE_SHA": str(self.root / "cloudflare-sha"),
            "WEB_CALL_LOG": str(self.log),
        })

    def test_same_version_replaced_tool_bytes_are_rejected(self) -> None:
        lock = json.loads(
            (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
        )["tools"]
        wrangler_root = self.cache / "wrangler" / lock["wrangler"]["version"]
        paths = {
            "aliyun": self.cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun",
            "ossutil": self.cache / "ossutil" / lock["ossutil"]["version"] / "ossutil",
            "node": self.cache / "node" / lock["node"]["version"] / "bin/node",
            "wrangler_js": wrangler_root / "node_modules/wrangler/bin/wrangler.js",
            "wrangler_root": wrangler_root,
        }
        fixture_lock = self.fixture_integrity_lock(lock, paths)
        version = subprocess.run(
            [str(paths["node"]), str(paths["wrangler_js"]), "--version"],
            text=True, capture_output=True, check=True,
        )
        self.assertEqual(version.stdout.strip(), lock["wrangler"]["version"])
        with paths["wrangler_js"].open("a", encoding="utf-8") as handle:
            handle.write("# same-version replacement\n")
        with self.assertRaisesRegex(RuntimeError, "wrangler entry bytes differ"):
            REAL_TOOLCHAIN_VALIDATOR(fixture_lock, **paths)

    def test_canonical_installed_tree_accepts_internal_npm_bin_symlink(self) -> None:
        lock = json.loads(
            (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
        )["tools"]
        wrangler_root = self.cache / "wrangler" / lock["wrangler"]["version"]
        dot_bin = wrangler_root / "node_modules/.bin"
        dot_bin.mkdir()
        (dot_bin / "wrangler").symlink_to("../wrangler/bin/wrangler.js")
        first = publish_web.installed_tree_sha256(wrangler_root)
        second = publish_web.installed_tree_sha256(wrangler_root)
        self.assertEqual(first, second)

    def test_same_entry_with_replaced_wrangler_dependency_is_rejected(self) -> None:
        lock = json.loads(
            (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
        )["tools"]
        wrangler_root = self.cache / "wrangler" / lock["wrangler"]["version"]
        paths = {
            "aliyun": self.cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun",
            "ossutil": self.cache / "ossutil" / lock["ossutil"]["version"] / "ossutil",
            "node": self.cache / "node" / lock["node"]["version"] / "bin/node",
            "wrangler_js": wrangler_root / "node_modules/wrangler/bin/wrangler.js",
            "wrangler_root": wrangler_root,
        }
        fixture_lock = self.fixture_integrity_lock(lock, paths)
        dependency = wrangler_root / "node_modules/dependency.js"
        dependency.write_text("malicious transitive", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "installed tree differs"):
            REAL_TOOLCHAIN_VALIDATOR(fixture_lock, **paths)

    def set_revision(self, revision: str) -> None:
        marker = json.loads(
            (self.dist / "vane-release.json").read_text(encoding="utf-8")
        )
        marker["source_revision"] = revision
        (self.dist / "vane-release.json").write_text(
            json.dumps(marker), encoding="utf-8"
        )

    def publish(
        self, result_name: str = "result.json", revision: str = SHA
    ) -> dict:
        result_path = self.root / result_name

        def verify(
            _origin: str,
            _revision: str,
            *,
            expected_marker: bytes,
            expected_index_sha256: str,
            expected_files: dict[str, dict] | None = None,
            index_path: str = "/index.html",
            directory_indexes: bool = False,
            attempts: int = 6,
        ) -> dict:
            provider_root = (
                self.cloudflare_remote
                if "pages.dev" in _origin or _origin == "https://vane.zhuoqidev.com"
                else self.remote
            )
            marker = provider_root / "vane-release.json"
            index = provider_root / "index.html"
            if (
                not marker.is_file()
                or marker.read_bytes() != expected_marker
                or not index.is_file()
                or hashlib.sha256(index.read_bytes()).hexdigest()
                != expected_index_sha256
            ):
                raise RuntimeError("provider is not current")
            for object_name, record in sorted((expected_files or {}).items()):
                object_path = provider_root / object_name
                if (
                    not object_path.is_file()
                    or object_path.stat().st_size != record["size"]
                    or hashlib.sha256(object_path.read_bytes()).hexdigest()
                    != record["sha256"]
                ):
                    raise RuntimeError(
                        f"provider object is not current: {object_name}"
                    )
            return json.loads(expected_marker)

        with self.environment, mock.patch.object(
            publish_web, "verify_public_release", side_effect=verify,
        ), mock.patch.object(
            publish_web, "cloudflare_api", side_effect=self.cloudflare_api
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge",
            side_effect=self.verify_aliyun_edge,
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge",
            return_value={
                "host": "vane.zhuoqidev.com", "via": "vane-web.pages.dev",
                "edge_ip": "104.18.1.20",
            },
        ), mock.patch.object(
            publish_web, "verify_cloudflare_controls",
            side_effect=self.verify_cloudflare_controls,
        ):
            return publish_web.publish(
                dist=self.dist, revision=revision, work_root=self.work,
                state_root=self.state, tool_cache=self.cache,
                origin="https://vane.zhuoqidev.com", result_path=result_path,
                expected_web_tree_sha256=publish_web.directory_tree_sha256(self.dist),
            )

    def verify_aliyun_edge(
        self, cname: str, _revision: str, _marker: bytes, _index: str,
        expected_files: dict[str, dict],
    ) -> dict:
        self.aliyun_edge_reads += 1
        for object_name, record in expected_files.items():
            path = self.remote / object_name
            payload = self.aliyun_edge_overrides.get(
                object_name, path.read_bytes() if path.is_file() else b""
            )
            if (
                not path.is_file()
                or len(payload) != record["size"]
                or hashlib.sha256(payload).hexdigest() != record["sha256"]
            ):
                raise RuntimeError(f"Ali edge stale object: {object_name}")
        return {
            "ip": "183.95.252.39",
            "cname": cname,
            "control_smoke": {
                "redirects": "verified" if "_redirects" in expected_files else "not-present"
            },
        }

    def verify_cloudflare_controls(
        self, _origin: str, _revision: str, controls: dict, _public: dict,
    ) -> dict:
        self.cloudflare_control_reads += 1
        if (
            os.environ.get("STALE_ALI_EDGE_AFTER_FIRST")
            and self.cloudflare_control_reads >= 2
        ):
            self.aliyun_edge_overrides[
                os.environ["STALE_ALI_EDGE_AFTER_FIRST"]
            ] = b"stale-edge"
        return {
            "headers": "verified" if "_headers" in controls else "not-present",
            "redirects": "verified" if "_redirects" in controls else "not-present",
        }

    def cloudflare_api(self, _account: str, path: str, _env: dict) -> object:
        if path.endswith("/domains"):
            self.cloudflare_domain_reads += 1
            active = not (
                (
                    os.environ.get("DRIFT_ROUTES_AFTER_CF")
                    and self.cloudflare_domain_reads >= 5
                )
                or (
                    os.environ.get("DRIFT_ROUTES_AT_FINAL")
                    and self.cloudflare_domain_reads >= 6
                )
            )
            return [{
                "name": "vane.zhuoqidev.com",
                "status": "active" if active else "inactive",
                "validation_data": {"status": "active" if active else "pending"},
                "verification_data": {"status": "active" if active else "pending"},
            }]
        self.cloudflare_project_reads += 1
        sha_path = self.root / "cloudflare-sha"
        canonical = None
        if sha_path.is_file():
            canonical_sha = sha_path.read_text()
            canonical = {
                "id": "fixture-" + canonical_sha[:8],
                "url": "https://fixture.vane-web.pages.dev",
                "environment": "production",
                "latest_stage": {"name": "deploy", "status": "success"},
                "aliases": ["https://vane.zhuoqidev.com"],
                "deployment_trigger": {"metadata": {
                    "branch": "main", "commit_hash": canonical_sha,
                    "commit_dirty": False,
                }},
            }
            if (
                os.environ.get("DRIFT_CF_CANONICAL_AFTER_ALI")
                and self.cloudflare_project_reads >= 6
            ):
                canonical = {
                    **canonical,
                    "id": "other-deployment",
                    "deployment_trigger": {"metadata": {
                        "branch": "main", "commit_hash": sha_path.read_text(),
                        "commit_dirty": True,
                    }},
                }
        return {
            "name": "vane-web", "production_branch": "main",
            "source": (
                {"type": "github"}
                if os.environ.get("DRIFT_CF_AUTHORITY_BEFORE_ADOPT") else None
            ),
            "domains": ["vane-web.pages.dev", "vane.zhuoqidev.com"],
            "canonical_deployment": canonical,
        }

    def test_preflight_is_read_only_and_proves_both_provider_credentials(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        shutil.copytree(self.dist, self.remote, dirs_exist_ok=True)
        def preflight_response(request, timeout):
            self.assertEqual(
                request.get_header("User-agent"), publish_web.PUBLIC_USER_AGENT
            )
            if "api.cloudflare.com" in request.full_url:
                result = (
                    [{
                        "name": "vane.zhuoqidev.com", "status": "active",
                        "validation_data": {"status": "active"},
                        "verification_data": {"status": "active"},
                    }]
                    if request.full_url.endswith("/domains")
                    else {
                        "name": "vane-web", "production_branch": "main",
                        "source": None,
                        "domains": ["vane-web.pages.dev", "vane.zhuoqidev.com"],
                        "canonical_deployment": None,
                    }
                )
                return PublicResponse(json.dumps({
                    "success": True, "result": result,
                }).encode())
            return PublicResponse(marker)
        with self.environment, mock.patch.object(
            publish_web, "urlopen", side_effect=preflight_response
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge", side_effect=self.verify_aliyun_edge
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge",
            return_value={"edge_ip": "104.18.1.20"},
        ):
            result = publish_web.preflight(
                self.cache, "https://vane.zhuoqidev.com"
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["public_revision"], SHA)
        calls = self.log.read_text(encoding="utf-8")
        self.assertIn(
            "ossutil stat oss://zhuoqidev-vane-web/vane-release.json", calls
        )
        self.assertIn("aliyun cdn DescribeCdnDomainDetail", calls)
        self.assertIn("aliyun alidns DescribeDomainRecords", calls)
        self.assertFalse([
            line for line in calls.splitlines()
            if line.startswith("ossutil cp /")
        ])

    def test_preflight_allows_legacy_pages_spa_without_release_marker(self) -> None:
        spa = b"<!doctype html><html><body>legacy</body></html>"
        shutil.copytree(self.dist, self.remote, dirs_exist_ok=True)

        def response(request, timeout):
            if "api.cloudflare.com" in request.full_url:
                result = (
                    [{
                        "name": "vane.zhuoqidev.com", "status": "active",
                        "validation_data": {"status": "active"},
                        "verification_data": {"status": "active"},
                    }]
                    if request.full_url.endswith("/domains")
                    else {
                        "name": "vane-web", "production_branch": "main",
                        "source": None,
                        "domains": ["vane-web.pages.dev", "vane.zhuoqidev.com"],
                        "canonical_deployment": None,
                    }
                )
                return PublicResponse(json.dumps({
                    "success": True, "result": result,
                }).encode())
            return PublicResponse(spa)

        with self.environment, mock.patch.object(
            publish_web, "urlopen", side_effect=response
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge", side_effect=self.verify_aliyun_edge
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge",
            return_value={"edge_ip": "104.18.1.20"},
        ):
            result = publish_web.preflight(
                self.cache, "https://vane.zhuoqidev.com"
            )
        self.assertTrue(result["ok"])
        self.assertIsNone(result["public_revision"])
        self.assertIsNone(
            result["providers"]["cloudflare_pages"]["public_revision"]
        )

    def test_preflight_rejects_unreachable_aliyun_pinned_edge(self) -> None:
        shutil.copytree(self.dist, self.remote, dirs_exist_ok=True)
        detail = {"GetDomainDetailModel": {
            "DomainName": "vane.zhuoqidev.com", "DomainStatus": "online",
            "Cname": publish_web.ALIYUN_CDN_CNAME,
        }}
        with self.environment, mock.patch.object(
            publish_web, "provider_route_authority", return_value=detail
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge",
            side_effect=RuntimeError("all Ali edges unreachable"),
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge"
        ) as cloudflare_edge:
            with self.assertRaisesRegex(RuntimeError, "all Ali edges unreachable"):
                publish_web.preflight(
                    self.cache, "https://vane.zhuoqidev.com"
                )
        cloudflare_edge.assert_not_called()

    def test_preflight_rejects_unreachable_cloudflare_custom_edge(self) -> None:
        shutil.copytree(self.dist, self.remote, dirs_exist_ok=True)
        marker = (self.dist / "vane-release.json").read_bytes()
        detail = {"GetDomainDetailModel": {
            "DomainName": "vane.zhuoqidev.com", "DomainStatus": "online",
            "Cname": publish_web.ALIYUN_CDN_CNAME,
        }}
        with self.environment, mock.patch.object(
            publish_web, "provider_route_authority", return_value=detail
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge", side_effect=self.verify_aliyun_edge
        ), mock.patch.object(
            publish_web, "read_public_marker",
            return_value={"source_revision": SHA},
        ), mock.patch.object(
            publish_web, "urlopen", return_value=PublicResponse(marker)
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge",
            side_effect=RuntimeError("Cloudflare custom edge unreachable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "custom edge unreachable"):
                publish_web.preflight(
                    self.cache, "https://vane.zhuoqidev.com"
                )

    def test_preflight_rejects_missing_credentials_before_provider_calls(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "ALIYUN_ACCESS_KEY_ID": "",
                "ALIYUN_ACCESS_KEY_SECRET": "",
                "CLOUDFLARE_API_TOKEN": "",
                "CLOUDFLARE_ACCOUNT_ID": "",
                "WEB_CALL_LOG": str(self.log),
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "credentials are unavailable"):
                publish_web.preflight(
                    self.cache, "https://vane.zhuoqidev.com"
                )
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil stat", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_provider_route_contract_rejects_wrong_or_inactive_authorities(self) -> None:
        valid_project = {
            "name": "vane-web", "production_branch": "main", "source": None,
            "domains": ["vane-web.pages.dev", "vane.zhuoqidev.com"],
        }
        valid_domains = [{
            "name": "vane.zhuoqidev.com", "status": "active",
            "validation_data": {"status": "active"},
            "verification_data": {"status": "active"},
        }]
        valid_cdn = {
            "GetDomainDetailModel": {
                "DomainName": "vane.zhuoqidev.com", "DomainStatus": "online",
                "Cname": "vane.zhuoqidev.com.w.kunlunaq.com",
            }
        }
        valid_dns = {"DomainRecords": {"Record": [
            {"RR": "vane", "Type": "CNAME", "Line": "default",
             "Value": "vane.zhuoqidev.com.w.kunlunaq.com", "Status": "ENABLE"},
            {"RR": "vane", "Type": "CNAME", "Line": "oversea",
             "Value": "vane-web.pages.dev", "Status": "ENABLE"},
        ]}}
        publish_web.validate_provider_routes(
            valid_project, valid_domains, valid_cdn, valid_dns
        )
        variants = [
            ({**valid_project, "source": {"type": "github"}}, valid_domains, valid_cdn, valid_dns),
            (valid_project, [{"name": "vane.zhuoqidev.com", "status": "pending"}], valid_cdn, valid_dns),
            (valid_project, [{**valid_domains[0], "validation_data": {"status": "pending"}}], valid_cdn, valid_dns),
            (valid_project, [{**valid_domains[0], "verification_data": {"status": "pending"}}], valid_cdn, valid_dns),
            (valid_project, valid_domains, valid_cdn, {"DomainRecords": {"Record": valid_dns["DomainRecords"]["Record"][:1]}}),
            (valid_project, valid_domains, valid_cdn, {"DomainRecords": {"Record": [
                valid_dns["DomainRecords"]["Record"][0],
                {**valid_dns["DomainRecords"]["Record"][1], "Value": "attacker.example"},
            ]}}),
            (valid_project, valid_domains, valid_cdn, {"DomainRecords": {"Record": [
                *valid_dns["DomainRecords"]["Record"],
                valid_dns["DomainRecords"]["Record"][0],
            ]}}),
            (valid_project, valid_domains, valid_cdn, {"DomainRecords": {"Record": [
                *valid_dns["DomainRecords"]["Record"],
                {"RR": "vane", "Type": "A", "Line": "default",
                 "Value": "192.0.2.1", "Status": "ENABLE"},
            ]}}),
        ]
        for project, domains, cdn, dns in variants:
            with self.subTest(variant=variants.index((project, domains, cdn, dns))):
                with self.assertRaisesRegex(RuntimeError, "route contract"):
                    publish_web.validate_provider_routes(project, domains, cdn, dns)

    def test_verify_only_uses_exact_dist_bytes_without_provider_credentials(self) -> None:
        marker = json.loads(
            (self.dist / "vane-release.json").read_text(encoding="utf-8")
        )
        with mock.patch.object(
            publish_web, "verify_public_release", return_value=marker
        ) as verify:
            self.assertEqual(
                publish_web.verify_dist_public(
                    self.dist, SHA, "https://vane.example"
                ),
                marker,
            )
        verify.assert_called_once_with(
            "https://vane.example",
            SHA,
            expected_marker=(self.dist / "vane-release.json").read_bytes(),
            expected_index_sha256=hashlib.sha256(
                (self.dist / "index.html").read_bytes()
            ).hexdigest(),
        )

    def test_assets_precede_entry_and_cdn_refresh(self) -> None:
        result = self.publish()
        self.assertEqual(result["status"], "published")
        calls = self.log.read_text(encoding="utf-8")
        asset = calls.index("assets/app-AbCdEf12.js")
        entry = calls.index("index.html", asset)
        marker = calls.index("vane-release.json", entry)
        refresh = calls.index("aliyun cdn RefreshObjectCaches", entry)
        self.assertLess(asset, entry)
        self.assertLess(entry, marker)
        self.assertLess(marker, refresh)
        self.assertIn(
            "wrangler pages deploy ", calls,
        )
        self.assertIn("/dist-snapshot --project-name vane-web ", calls)
        self.assertIn(
            f"--branch main --commit-hash {SHA} --commit-dirty=false", calls
        )
        self.assertNotIn("ossutil sync", calls)
        self.assertNotIn("ossutil rm", calls)
        self.assertNotIn("ossutil stat", calls)
        self.assertEqual(
            json.loads((self.state / "web-current.json").read_text(encoding="utf-8"))["revision"],
            SHA,
        )

    def test_cloudflare_failure_cannot_mutate_aliyun_or_settle_overall_success(self) -> None:
        old_marker = b'{"source_revision":"old"}\n'
        (self.remote / "vane-release.json").write_bytes(old_marker)
        with mock.patch.dict(os.environ, {"FAIL_CLOUDFLARE_DEPLOY": "1"}):
            with self.assertRaisesRegex(RuntimeError, "Cloudflare"):
                self.publish("failed.json")
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())
        pending = json.loads(
            (self.state / "web-pending.json").read_text(encoding="utf-8")
        )
        self.assertEqual(pending["revision"], SHA)
        self.assertEqual(pending["providers"]["aliyun"], "not_started")
        self.assertEqual(pending["providers"]["cloudflare_pages"], "failed")
        self.assertEqual((self.remote / "vane-release.json").read_bytes(), old_marker)
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("RefreshObjectCaches", calls)

    def test_cloudflare_authority_drift_cannot_trigger_redeploy_or_aliyun(self) -> None:
        with mock.patch.dict(
            os.environ, {"DRIFT_CF_AUTHORITY_BEFORE_ADOPT": "1"}
        ):
            with self.assertRaisesRegex(RuntimeError, "project authority"):
                self.publish("failed.json")
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("wrangler pages deploy", calls)
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertFalse((self.state / "web-current.json").exists())

    def test_aliyun_failure_resumes_by_adopting_exact_cloudflare_deployment(self) -> None:
        with mock.patch.dict(os.environ, {"FAIL_DOWNLOAD": "index.html"}):
            with self.assertRaisesRegex(RuntimeError, "Aliyun Web publication"):
                self.publish("failed.json")
        pending = json.loads(
            (self.state / "web-pending.json").read_text(encoding="utf-8")
        )
        self.assertEqual(pending["providers"]["cloudflare_pages"], "verified")
        self.assertEqual(pending["providers"]["aliyun"], "failed")
        self.log.write_text("", encoding="utf-8")
        result = self.publish("retry.json")
        self.assertEqual(result["status"], "recovered")
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("wrangler pages deploy " + str(self.dist), calls)
        self.assertIn("ossutil cp", calls)
        self.assertFalse((self.state / "web-pending.json").exists())

    def test_cloudflare_receipt_binds_exact_deployment_and_both_providers(self) -> None:
        result = self.publish()
        self.assertEqual(result["schema"], "vane.web-publication/v2")
        self.assertEqual(set(result["providers"]), {"aliyun", "cloudflare_pages"})
        self.assertEqual(
            result["providers"]["cloudflare_pages"]["deployment_url"],
            "https://fixture.vane-web.pages.dev",
        )
        self.assertEqual(
            result["providers"]["cloudflare_pages"]["project"], "vane-web"
        )
        self.assertEqual(
            result["providers"]["cloudflare_pages"]["artifact_receipt_sha256"],
            result["artifact_receipt_sha256"],
        )
        current = json.loads(
            (self.state / "web-current.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current["schema"], "vane.web-current/v2")
        self.assertEqual(
            set(current["providers"]), {"aliyun", "cloudflare_pages"}
        )

    def test_finalized_verify_is_credential_free_and_checks_all_public_authorities(self) -> None:
        self.publish()
        marker = json.loads(
            (self.dist / "vane-release.json").read_text(encoding="utf-8")
        )
        with mock.patch.dict(os.environ, {
            "ALIYUN_ACCESS_KEY_ID": "", "ALIYUN_ACCESS_KEY_SECRET": "",
            "CLOUDFLARE_API_TOKEN": "", "CLOUDFLARE_ACCOUNT_ID": "",
        }), mock.patch.object(
            publish_web, "verify_public_release", return_value=marker
        ) as public, mock.patch.object(
            publish_web, "verify_aliyun_edge",
            return_value={"ip": "183.95.252.39"},
        ) as edge, mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge",
            return_value={
                "host": "vane.zhuoqidev.com", "via": "vane-web.pages.dev",
                "edge_ip": "104.18.1.20",
            },
        ):
            result = publish_web.verify_finalized_publication(
                self.dist, SHA, self.root / "result.json"
            )
        self.assertEqual(result["schema"], "vane.web-publication/v2")
        self.assertEqual(public.call_count, 3)
        edge.assert_called_once()

    def test_verify_only_rejects_tampered_remote_critical_asset(self) -> None:
        self.publish()
        (self.cloudflare_remote / "assets/app-AbCdEf12.js").write_text(
            "tampered", encoding="utf-8"
        )

        def verify(
            origin: str, _revision: str, *, expected_marker: bytes,
            expected_index_sha256: str, expected_files=None,
            index_path: str = "/index.html", directory_indexes: bool = False,
            attempts: int = 6,
        ) -> dict:
            provider_root = (
                self.cloudflare_remote if "pages.dev" in origin else self.remote
            )
            for object_name, record in (expected_files or {}).items():
                path = provider_root / object_name
                if (
                    not path.is_file()
                    or path.stat().st_size != record["size"]
                    or hashlib.sha256(path.read_bytes()).hexdigest()
                    != record["sha256"]
                ):
                    raise RuntimeError(
                        f"provider object is not current: {object_name}"
                    )
            return json.loads(expected_marker)

        with mock.patch.object(
            publish_web, "verify_public_release", side_effect=verify
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge",
            return_value={
                "host": "vane.zhuoqidev.com", "via": "vane-web.pages.dev",
                "edge_ip": "104.18.1.20",
            },
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge", return_value={"ip": "183.95.252.39"}
        ):
            with self.assertRaisesRegex(
                RuntimeError, "provider object is not current"
            ):
                publish_web.verify_finalized_publication(
                    self.dist, SHA, self.root / "result.json"
                )

    def test_verify_only_rejects_stale_aliyun_nonhashed_json(self) -> None:
        stable = self.dist / "stable-config.json"
        stable.write_text('{"feature":"exact"}', encoding="utf-8")
        self.publish()
        (self.remote / "stable-config.json").write_text(
            '{"feature":"stale"}', encoding="utf-8"
        )
        marker = json.loads(
            (self.dist / "vane-release.json").read_text(encoding="utf-8")
        )
        with mock.patch.object(
            publish_web, "verify_public_release", return_value=marker
        ), mock.patch.object(
            publish_web, "verify_cloudflare_custom_edge", return_value={}
        ), mock.patch.object(
            publish_web, "verify_cloudflare_controls",
            return_value={"headers": "not-present", "redirects": "not-present"},
        ), mock.patch.object(
            publish_web, "verify_aliyun_edge", side_effect=self.verify_aliyun_edge
        ):
            with self.assertRaisesRegex(RuntimeError, "Ali edge stale object"):
                publish_web.verify_finalized_publication(
                    self.dist, SHA, self.root / "result.json"
                )

    def test_failed_asset_readback_never_cuts_over_index(self) -> None:
        old_marker = b'{"source_revision":"old"}\n'
        (self.remote / "vane-release.json").write_bytes(old_marker)
        with mock.patch.dict(os.environ, {"FAIL_DOWNLOAD": "assets/app-AbCdEf12.js"}):
            with self.assertRaisesRegex(RuntimeError, "critical readback failed"):
                self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("cp " + str(self.dist / "index.html"), calls)
        self.assertEqual((self.remote / "vane-release.json").read_bytes(), old_marker)
        self.assertFalse((self.state / "web-current.json").exists())

    def test_same_size_corrupt_critical_asset_fails_before_entry_and_marker(self) -> None:
        old_marker = b'{"source_revision":"old"}\n'
        (self.remote / "vane-release.json").write_bytes(old_marker)
        with mock.patch.dict(os.environ, {"CORRUPT_UPLOAD": "assets/app-AbCdEf12.js"}):
            with self.assertRaisesRegex(RuntimeError, "differs from exact artifact bytes"):
                self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("cp " + str(self.dist / "index.html"), calls)
        self.assertEqual((self.remote / "vane-release.json").read_bytes(), old_marker)

    def test_same_sha_with_changed_bytes_fails_before_remote_mutation(self) -> None:
        self.publish("first.json")
        self.log.write_text("", encoding="utf-8")
        (self.dist / "assets/app-AbCdEf12.js").write_text(
            "different", encoding="utf-8"
        )
        with self.assertRaisesRegex(RuntimeError, "different immutable artifact receipt"):
            self.publish("second.json")
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp " + str(self.dist), calls)
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_unhashed_runtime_fails_before_remote_mutation(self) -> None:
        (self.dist / "assets/runtime.js").write_text("runtime", encoding="utf-8")
        (self.dist / "index.html").write_text(
            '<script type="module" src="/assets/runtime.js"></script>',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(RuntimeError, "publication command failed"):
            self.publish()
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp " + str(self.dist), calls)
        self.assertNotIn("RefreshObjectCaches", calls)

    def test_missing_referenced_asset_fails_before_remote_mutation(self) -> None:
        (self.dist / "assets/app-AbCdEf12.js").unlink()
        with self.assertRaisesRegex(RuntimeError, "publication command failed"):
            self.publish()
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp " + str(self.dist), calls)
        self.assertNotIn("RefreshObjectCaches", calls)

    def test_owner_preview_is_published_with_no_store(self) -> None:
        preview = self.dist / PREVIEW
        preview.parent.mkdir(parents=True)
        preview.write_text(
            '<script type="module" src="/assets/app-AbCdEf12.js"></script>',
            encoding="utf-8",
        )
        self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertTrue(any(
            line.startswith("ossutil cp ")
            and "/dist-snapshot/" + PREVIEW in line
            and line.endswith(f" oss://zhuoqidev-vane-web/{PREVIEW} --force")
            for line in calls.splitlines()
        ))
        self.assertIn(
            f"ossutil set-props oss://zhuoqidev-vane-web/{PREVIEW} "
            "--cache-control no-store --metadata-directive update --force",
            calls,
        )

    def test_symlinked_lock_is_rejected_before_tools_or_network(self) -> None:
        outside = self.root / "outside.lock"
        outside.write_text("", encoding="utf-8")
        (self.state / "web-release.lock").symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "state paths are unsafe"):
            self.publish()
        self.assertFalse(self.log.exists())

    def test_source_dist_mutation_after_cloudflare_blocks_aliyun(self) -> None:
        original = publish_web.publish_cloudflare

        def publish_then_mutate(**kwargs):
            result = original(**kwargs)
            (self.dist / "assets/app-AbCdEf12.js").write_text(
                "tampered-after-cloudflare", encoding="utf-8"
            )
            return result

        with mock.patch.object(
            publish_web, "publish_cloudflare", side_effect=publish_then_mutate
        ):
            with self.assertRaisesRegex(RuntimeError, "Gate Web dist changed"):
                self.publish("failed.json")
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertFalse(any(
            line.startswith("ossutil cp /") for line in calls.splitlines()
        ))
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())

    def test_private_snapshot_mutation_after_cloudflare_blocks_aliyun(self) -> None:
        original = publish_web.publish_cloudflare

        def publish_then_mutate(**kwargs):
            result = original(**kwargs)
            (kwargs["dist"] / "assets/app-AbCdEf12.js").write_text(
                "tampered-private-snapshot", encoding="utf-8"
            )
            return result

        with mock.patch.object(
            publish_web, "publish_cloudflare", side_effect=publish_then_mutate
        ):
            with self.assertRaisesRegex(RuntimeError, "private snapshot changed"):
                self.publish("failed.json")
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertFalse(any(
            line.startswith("ossutil cp /") for line in calls.splitlines()
        ))
        self.assertFalse((self.state / "web-current.json").exists())

    def test_missing_cloudflare_critical_asset_is_not_adopted_and_is_repaired(self) -> None:
        shutil.copytree(self.dist, self.cloudflare_remote, dirs_exist_ok=True)
        (self.cloudflare_remote / "assets/app-AbCdEf12.js").unlink()
        (self.root / "cloudflare-sha").write_text(SHA, encoding="utf-8")
        self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertIn("wrangler pages deploy ", calls)
        self.assertEqual(
            (self.cloudflare_remote / "assets/app-AbCdEf12.js").read_bytes(),
            (self.dist / "assets/app-AbCdEf12.js").read_bytes(),
        )

    def test_missing_cloudflare_unreferenced_file_is_not_adopted_and_is_repaired(self) -> None:
        (self.dist / "unreferenced.txt").write_text("must-publish", encoding="utf-8")
        shutil.copytree(self.dist, self.cloudflare_remote, dirs_exist_ok=True)
        (self.cloudflare_remote / "unreferenced.txt").unlink()
        (self.root / "cloudflare-sha").write_text(SHA, encoding="utf-8")
        self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertIn("wrangler pages deploy ", calls)
        self.assertEqual(
            (self.cloudflare_remote / "unreferenced.txt").read_text(encoding="utf-8"),
            "must-publish",
        )

    def test_cloudflare_loses_unreferenced_file_before_finalize_and_cannot_settle(self) -> None:
        (self.dist / "unreferenced.txt").write_text("must-stay", encoding="utf-8")
        original = publish_web.provider_route_authority

        def route_then_delete(*args, **kwargs):
            result = original(*args, **kwargs)
            (self.cloudflare_remote / "unreferenced.txt").unlink()
            return result

        with mock.patch.object(
            publish_web, "provider_route_authority", side_effect=route_then_delete
        ):
            with self.assertRaisesRegex(
                RuntimeError, "provider object is not current: unreferenced.txt"
            ):
                self.publish("failed.json")
        self.assertTrue((self.remote / "unreferenced.txt").is_file())
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())

    def test_cloudflare_control_files_are_receipted_but_not_byte_probed(self) -> None:
        (self.dist / "_headers").write_text(
            "/\n  Cache-Control: no-cache, must-revalidate\n", encoding="utf-8"
        )
        (self.dist / "_redirects").write_text(
            "/.well-known/agent-card.json "
            "https://api.vane.zhuoqidev.com/.well-known/agent-card.json 302\n",
            encoding="utf-8",
        )
        result = self.publish()
        provider = result["providers"]["cloudflare_pages"]
        self.assertEqual(
            [item["path"] for item in provider["control_files"]],
            ["_headers", "_redirects"],
        )
        self.assertNotIn(
            "_headers", [item["path"] for item in provider["verified_files"]]
        )
        self.assertNotIn(
            "_redirects", [item["path"] for item in provider["verified_files"]]
        )
        self.assertEqual(
            provider["control_smoke"],
            {"headers": "verified", "redirects": "verified"},
        )

    def test_cloudflare_control_smoke_verifies_preview_headers_and_redirect_splat(self) -> None:
        asset = "assets/app-AbCdEf12.js"
        responses = {
            "/": (200, [("Cache-Control", "no-cache, must-revalidate")]),
            "/" + asset: (200, [("Cache-Control", "public, max-age=31536000, immutable")]),
            publish_web.cloudflare_public_path(PREVIEW): (200, [
                ("Cache-Control", "no-store"),
                ("X-Robots-Tag", "noindex, nofollow, noarchive"),
                ("Referrer-Policy", "no-referrer"),
                ("Content-Security-Policy", publish_web.OWNER_PREVIEW_CSP),
            ]),
            "/.well-known/agent-card.json": (302, [
                ("Location", "https://api.vane.zhuoqidev.com/.well-known/agent-card.json")
            ]),
            "/.well-known/probe.json": (302, [
                ("Location", "https://api.vane.zhuoqidev.com/.well-known/probe.json")
            ]),
        }

        class Response:
            def __init__(self, status: int, headers: list[tuple[str, str]]) -> None:
                self.status = status
                self.headers = headers

            def read(self, limit: int) -> bytes:
                return b""

            def getheaders(self):
                return self.headers

        class Connection:
            def __init__(self, *args, **kwargs) -> None:
                self.path = ""

            def request(self, method: str, path: str, headers: dict) -> None:
                self.path = path.split("?", 1)[0]

            def getresponse(self):
                return Response(*responses[self.path])

            def close(self) -> None:
                pass

        controls = {
            name: {"path": name, "size": 1, "sha256": "0" * 64}
            for name in ("_headers", "_redirects")
        }
        public = {
            asset: {"path": asset, "size": 3, "sha256": "0" * 64},
            PREVIEW: {"path": PREVIEW, "size": 3, "sha256": "0" * 64},
        }
        with mock.patch.object(
            publish_web.http.client, "HTTPSConnection", Connection
        ):
            self.assertEqual(
                publish_web.verify_cloudflare_controls(
                    "https://fixture.vane-web.pages.dev", SHA, controls, public
                ),
                {"headers": "verified", "redirects": "verified"},
            )
        pinned_hosts: list[str | None] = []

        class PinnedConnection(Connection):
            def request(self, method: str, path: str, headers: dict) -> None:
                pinned_hosts.append(headers.get("Host"))
                super().request(method, path, headers)

        with mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", PinnedConnection
        ):
            self.assertEqual(
                publish_web.verify_cloudflare_controls(
                    "https://vane.zhuoqidev.com", SHA, controls, public,
                    edge_ip="104.18.1.20",
                ),
                {"headers": "verified", "redirects": "verified"},
            )
        self.assertTrue(pinned_hosts)
        self.assertEqual(set(pinned_hosts), {"vane.zhuoqidev.com"})
        responses["/.well-known/probe.json"] = (302, [("Location", "https://attacker.example")])
        with mock.patch.object(
            publish_web.http.client, "HTTPSConnection", Connection
        ):
            with self.assertRaisesRegex(RuntimeError, "splat behavior differs"):
                publish_web.verify_cloudflare_controls(
                    "https://fixture.vane-web.pages.dev", SHA, controls, public
                )
        with mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", PinnedConnection
        ):
            with self.assertRaisesRegex(RuntimeError, "splat behavior differs"):
                publish_web.verify_cloudflare_controls(
                    "https://vane.zhuoqidev.com", SHA, controls, public,
                    edge_ip="104.18.1.20",
                )

    def test_route_drift_after_provider_commits_blocks_combined_finalize(self) -> None:
        with mock.patch.dict(os.environ, {"DRIFT_ROUTES_AFTER_CF": "1"}):
            with self.assertRaisesRegex(RuntimeError, "route contract"):
                self.publish("failed.json")
        self.assertTrue((self.cloudflare_remote / "vane-release.json").is_file())
        self.assertTrue((self.remote / "vane-release.json").is_file())
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())

    def test_cloudflare_canonical_switch_after_aliyun_blocks_finalize(self) -> None:
        with mock.patch.dict(os.environ, {"DRIFT_CF_CANONICAL_AFTER_ALI": "1"}):
            with self.assertRaisesRegex(
                RuntimeError, "canonical deployment|not exact production"
            ):
                self.publish("failed.json")
        self.assertTrue((self.remote / "vane-release.json").is_file())
        self.assertGreaterEqual(self.aliyun_edge_reads, 2)
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())

    def test_geodns_drift_after_final_aliyun_edge_blocks_finalize(self) -> None:
        with mock.patch.dict(os.environ, {"DRIFT_ROUTES_AT_FINAL": "1"}):
            with self.assertRaisesRegex(RuntimeError, "route contract"):
                self.publish("failed.json")
        self.assertGreaterEqual(self.aliyun_edge_reads, 2)
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())

    def test_stale_aliyun_nonhashed_json_after_first_edge_blocks_finalize(self) -> None:
        (self.dist / "stable-config.json").write_text(
            '{"feature":"exact"}', encoding="utf-8"
        )
        with mock.patch.dict(
            os.environ, {"STALE_ALI_EDGE_AFTER_FIRST": "stable-config.json"}
        ):
            with self.assertRaisesRegex(RuntimeError, "Ali edge stale object"):
                self.publish("failed.json")
        self.assertGreaterEqual(self.aliyun_edge_reads, 2)
        self.assertFalse((self.state / "web-current.json").exists())
        self.assertFalse((self.root / "failed.json").exists())

    def test_stale_aliyun_preview_after_first_edge_blocks_finalize(self) -> None:
        preview = self.dist / PREVIEW
        preview.parent.mkdir(parents=True)
        preview.write_text("preview-exact", encoding="utf-8")
        with mock.patch.dict(os.environ, {"STALE_ALI_EDGE_AFTER_FIRST": PREVIEW}):
            with self.assertRaisesRegex(RuntimeError, "Ali edge stale object"):
                self.publish("failed.json")
        self.assertFalse((self.state / "web-current.json").exists())

    def test_symlinked_proof_root_is_rejected_before_remote_mutation(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        (self.state / "web-proofs").symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "proof root is unsafe"):
            self.publish()
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_invalid_local_marker_is_rejected_before_remote_mutation(self) -> None:
        (self.dist / "vane-release.json").write_text(
            json.dumps({"source_revision": SHA}), encoding="utf-8"
        )
        with self.assertRaisesRegex(RuntimeError, "not exact clean evidence"):
            self.publish()
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_already_current_restores_the_deterministic_receipt(self) -> None:
        self.publish("first.json")
        receipt = self.root / "web-release-receipt.json"
        expected = receipt.read_bytes()
        receipt.unlink()
        self.log.write_text("", encoding="utf-8")
        result = self.publish("second.json")
        self.assertEqual(result["status"], "already-current")
        self.assertEqual(receipt.read_bytes(), expected)
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("wrangler pages deploy " + str(self.dist), calls)
        self.assertNotIn("ossutil cp " + str(self.dist), calls)
        self.assertNotIn("RefreshObjectCaches", calls)

    def test_already_current_rechecks_cloudflare_after_aliyun_readback(self) -> None:
        stable = self.dist / "stable-config.json"
        stable.write_text('{"feature":"exact"}', encoding="utf-8")
        self.publish("first.json")
        current_before = (self.state / "web-current.json").read_bytes()
        real_aliyun_exact = publish_web.aliyun_exact
        mutated = False

        def mutate_cloudflare_after_readback(**kwargs) -> bool:
            nonlocal mutated
            exact = real_aliyun_exact(**kwargs)
            if not mutated:
                mutated = True
                (self.cloudflare_remote / "stable-config.json").write_text(
                    '{"feature":"drifted"}', encoding="utf-8"
                )
            return exact

        with mock.patch.object(
            publish_web, "aliyun_exact",
            side_effect=mutate_cloudflare_after_readback,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "provider object is not current: stable-config.json"
            ):
                self.publish("second.json")
        self.assertTrue(mutated)
        self.assertEqual(
            (self.state / "web-current.json").read_bytes(), current_before
        )
        self.assertFalse((self.root / "second.json").exists())

    def test_legacy_v1_current_state_is_strictly_upgraded_by_remote_adoption(self) -> None:
        shutil.copytree(self.dist, self.remote, dirs_exist_ok=True)
        shutil.copytree(self.dist, self.cloudflare_remote, dirs_exist_ok=True)
        (self.root / "cloudflare-sha").write_text(SHA, encoding="utf-8")
        with tempfile.TemporaryDirectory(dir=self.work) as temporary:
            plan = Path(temporary)
            publish_web.run(
                [str(ROOT / "ops/release/web-release.py"), "--dist", str(self.dist),
                 "--sha", SHA, "--output", str(plan)],
                env=os.environ.copy(), capture=True,
            )
            neutral = json.loads((plan / "release.json").read_text(encoding="utf-8"))
            legacy = {
                **neutral,
                "schema": "vane.web.aliyun-release/v1",
                "bucket": "zhuoqidev-vane-web",
            }
            legacy_bytes = (
                json.dumps(legacy, sort_keys=True, indent=2) + "\n"
            ).encode()
            artifact_digest = hashlib.sha256(legacy_bytes).hexdigest()
        legacy_proof = self.state / "web-proofs" / artifact_digest
        legacy_proof.mkdir(parents=True)
        (legacy_proof / "receipt.json").write_bytes(legacy_bytes)
        (self.state / "web-current.json").write_text(json.dumps({
            "schema": "vane.web-current/v1", "revision": SHA,
            "receipt_sha256": artifact_digest,
        }), encoding="utf-8")
        result = self.publish()
        self.assertEqual(result["status"], "provider-already-current")
        current = json.loads(
            (self.state / "web-current.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current["schema"], "vane.web-current/v2")

    def test_legacy_same_revision_changed_artifact_refuses_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory(dir=self.work) as temporary:
            plan = Path(temporary)
            publish_web.run(
                [str(ROOT / "ops/release/web-release.py"), "--dist", str(self.dist),
                 "--sha", SHA, "--output", str(plan)],
                env=os.environ.copy(), capture=True,
            )
            neutral = json.loads((plan / "release.json").read_text(encoding="utf-8"))
            legacy = {
                **neutral,
                "schema": "vane.web.aliyun-release/v1",
                "bucket": "zhuoqidev-vane-web",
            }
            legacy_bytes = (
                json.dumps(legacy, sort_keys=True, indent=2) + "\n"
            ).encode()
        legacy_digest = hashlib.sha256(legacy_bytes).hexdigest()
        legacy_proof = self.state / "web-proofs" / legacy_digest
        legacy_proof.mkdir(parents=True)
        (legacy_proof / "receipt.json").write_bytes(legacy_bytes)
        current_path = self.state / "web-current.json"
        current_path.write_text(json.dumps({
            "schema": "vane.web-current/v1",
            "revision": SHA,
            "receipt_sha256": legacy_digest,
        }), encoding="utf-8")
        current_before = current_path.read_bytes()
        (self.dist / "stable-config.json").write_text(
            '{"feature":"changed"}', encoding="utf-8"
        )

        with self.assertRaisesRegex(
            RuntimeError, "same legacy Web revision has a different"
        ):
            self.publish("changed.json")
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("wrangler pages deploy", calls)
        self.assertFalse([
            line for line in calls.splitlines()
            if line.startswith("ossutil cp /")
        ])
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertEqual(current_path.read_bytes(), current_before)
        self.assertFalse((self.root / "changed.json").exists())

    def test_provider_commit_resumes_without_remote_mutation(self) -> None:
        shutil.copytree(self.dist, self.remote, dirs_exist_ok=True)
        shutil.copytree(self.dist, self.cloudflare_remote, dirs_exist_ok=True)
        (self.root / "cloudflare-sha").write_text(SHA, encoding="utf-8")
        result = self.publish()
        self.assertEqual(result["status"], "provider-already-current")
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp " + str(self.dist), calls)
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertEqual(
            json.loads((self.state / "web-current.json").read_text(encoding="utf-8"))["revision"],
            SHA,
        )

    def test_crash_after_cloudflare_commit_preserves_real_previous_canonical(self) -> None:
        shutil.copytree(self.dist, self.cloudflare_remote, dirs_exist_ok=True)
        old_marker = json.loads(
            (self.cloudflare_remote / "vane-release.json").read_text(encoding="utf-8")
        )
        old_marker["source_revision"] = SHA2
        (self.cloudflare_remote / "vane-release.json").write_text(
            json.dumps(old_marker), encoding="utf-8"
        )
        (self.root / "cloudflare-sha").write_text(SHA2, encoding="utf-8")
        old_id = "fixture-" + SHA2[:8]
        original = publish_web.publish_cloudflare

        def crash_after_commit(**kwargs):
            original(**kwargs)
            raise RuntimeError("simulated crash after Cloudflare commit")

        with mock.patch.object(
            publish_web, "publish_cloudflare", side_effect=crash_after_commit
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                self.publish("crashed.json")
        pending = json.loads(
            (self.state / "web-pending.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            pending["previous_canonical_deployment_id"], old_id
        )
        self.assertEqual((self.root / "cloudflare-sha").read_text(), SHA)
        self.assertFalse((self.remote / "vane-release.json").exists())

        result = self.publish("recovered.json")
        cloudflare = result["providers"]["cloudflare_pages"]
        self.assertEqual(cloudflare["previous_canonical_deployment_id"], old_id)
        self.assertNotEqual(
            cloudflare["previous_canonical_deployment_id"],
            cloudflare["deployment_id"],
        )
        self.assertEqual(result["status"], "recovered")

    def test_resume_refuses_when_previous_canonical_changed_before_deploy(self) -> None:
        shutil.copytree(self.dist, self.cloudflare_remote, dirs_exist_ok=True)
        old_marker = json.loads(
            (self.cloudflare_remote / "vane-release.json").read_text(encoding="utf-8")
        )
        old_marker["source_revision"] = SHA2
        (self.cloudflare_remote / "vane-release.json").write_text(
            json.dumps(old_marker), encoding="utf-8"
        )
        canonical_path = self.root / "cloudflare-sha"
        canonical_path.write_text(SHA2, encoding="utf-8")
        old_id = "fixture-" + SHA2[:8]

        with mock.patch.object(
            publish_web, "publish_cloudflare",
            side_effect=RuntimeError("failed before Cloudflare mutation"),
        ):
            with self.assertRaisesRegex(RuntimeError, "before Cloudflare mutation"):
                self.publish("first-failed.json")
        pending_path = self.state / "web-pending.json"
        pending_before = json.loads(pending_path.read_text(encoding="utf-8"))
        self.assertEqual(
            pending_before["previous_canonical_deployment_id"], old_id
        )

        intervening_revision = "f" * 40
        canonical_path.write_text(intervening_revision, encoding="utf-8")
        self.log.write_text("", encoding="utf-8")
        with self.assertRaisesRegex(
            RuntimeError, "canonical deployment changed after pending evidence"
        ):
            self.publish("resume-refused.json")
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("wrangler pages deploy", calls)
        self.assertFalse([
            line for line in calls.splitlines()
            if line.startswith("ossutil cp /")
        ])
        self.assertNotIn("RefreshObjectCaches", calls)
        self.assertFalse((self.remote / "vane-release.json").exists())
        self.assertFalse((self.root / "resume-refused.json").exists())
        pending_after = json.loads(pending_path.read_text(encoding="utf-8"))
        self.assertEqual(
            pending_after["previous_canonical_deployment_id"], old_id
        )

    def test_next_revision_reuses_exact_content_addressed_asset(self) -> None:
        self.publish("first.json")
        self.log.write_text("", encoding="utf-8")
        self.set_revision(SHA2)
        result = self.publish("second.json", SHA2)
        self.assertEqual(result["status"], "published")
        calls = self.log.read_text(encoding="utf-8")
        self.assertFalse([
            line for line in calls.splitlines()
            if line.startswith("ossutil cp /")
            and "assets/app-AbCdEf12.js" in line
        ])
        self.assertIn("index.html", calls)
        self.assertIn("vane-release.json", calls)
        current = json.loads(
            (self.state / "web-current.json").read_text(encoding="utf-8")
        )
        proof = self.state / "web-proofs" / current["artifact_receipt_sha256"] / "proof.json"
        self.assertTrue(proof.is_file())
        self.assertEqual(
            json.loads(proof.read_text(encoding="utf-8"))["verified_objects"][0]["path"],
            "assets/app-AbCdEf12.js",
        )

    def test_prior_proof_does_not_reuse_missing_oss_asset(self) -> None:
        self.publish("first.json")
        (self.remote / "assets/app-AbCdEf12.js").unlink()
        self.set_revision(SHA2)
        self.log.write_text("", encoding="utf-8")
        result = self.publish("second.json", SHA2)
        self.assertEqual(result["status"], "published")
        self.assertEqual(
            (self.remote / "assets/app-AbCdEf12.js").read_text(encoding="utf-8"),
            "app",
        )
        calls = self.log.read_text(encoding="utf-8")
        self.assertEqual(
            sum(
                line.startswith("ossutil cp /")
                and "assets/app-AbCdEf12.js" in line
                for line in calls.splitlines()
            ),
            1,
        )

    def test_exact_preview_bytes_with_drifted_metadata_are_repaired_once(self) -> None:
        preview = self.dist / PREVIEW
        preview.parent.mkdir(parents=True)
        preview.write_text("preview-exact", encoding="utf-8")
        self.publish("first.json")
        (self.remote / ".preview-no-store").unlink()
        self.set_revision(SHA2)
        self.log.write_text("", encoding="utf-8")
        self.publish("second.json", SHA2)
        repaired = self.log.read_text(encoding="utf-8")
        self.assertIn("ossutil set-props", repaired)
        self.assertIn("RefreshObjectCaches", repaired)
        self.log.write_text("", encoding="utf-8")
        result = self.publish("third.json", SHA2)
        self.assertEqual(result["status"], "already-current")
        stable = self.log.read_text(encoding="utf-8")
        self.assertNotIn("ossutil set-props", stable)
        self.assertNotIn("RefreshObjectCaches", stable)

    def test_same_named_changed_asset_is_uploaded_and_read_back(self) -> None:
        self.publish("first.json")
        self.log.write_text("", encoding="utf-8")
        self.set_revision(SHA2)
        (self.dist / "assets/app-AbCdEf12.js").write_text(
            "changed", encoding="utf-8"
        )
        result = self.publish("second.json", SHA2)
        self.assertEqual(result["status"], "published")
        calls = self.log.read_text(encoding="utf-8")
        self.assertEqual(
            sum(
                line.startswith("ossutil cp /")
                and "assets/app-AbCdEf12.js" in line
                for line in calls.splitlines()
            ), 1
        )

    def test_tampered_provider_proof_refuses_before_remote_mutation(self) -> None:
        self.publish("first.json")
        current = json.loads(
            (self.state / "web-current.json").read_text(encoding="utf-8")
        )
        proof_path = (
            self.state / "web-proofs" / current["artifact_receipt_sha256"] / "proof.json"
        )
        proof = json.loads(proof_path.read_text(encoding="utf-8"))
        proof["verified_objects"][0]["sha256"] = "0" * 64
        proof_path.write_text(json.dumps(proof), encoding="utf-8")
        self.log.write_text("", encoding="utf-8")
        self.set_revision(SHA2)
        with self.assertRaisesRegex(RuntimeError, "differs from receipt"):
            self.publish("second.json", SHA2)
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_provider_receipts_reject_internally_inconsistent_evidence(self) -> None:
        (self.dist / "_headers").write_text(
            "/\n  Cache-Control: no-cache, must-revalidate\n",
            encoding="utf-8",
        )
        result = self.publish("first.json")
        artifact_digest = result["artifact_receipt_sha256"]
        cloudflare = json.loads(
            (self.root / "web-cloudflare-receipt.json").read_text(encoding="utf-8")
        )
        aliyun = json.loads(
            (self.root / "web-aliyun-receipt.json").read_text(encoding="utf-8")
        )
        publish_web.validate_cloudflare_receipt(cloudflare, SHA, artifact_digest)
        publish_web.validate_aliyun_receipt(aliyun, SHA, artifact_digest)

        cf_variants = []
        changed = json.loads(json.dumps(cloudflare))
        changed["index_sha256"] = "0" * 64
        cf_variants.append(changed)
        changed = json.loads(json.dumps(cloudflare))
        changed["custom_origin_smoke"]["verified_files_sha256"] = "0" * 64
        cf_variants.append(changed)
        changed = json.loads(json.dumps(cloudflare))
        changed["control_files"].append(changed["control_files"][0])
        cf_variants.append(changed)
        changed = json.loads(json.dumps(cloudflare))
        changed["previous_canonical_deployment_id"] = changed["deployment_id"]
        cf_variants.append(changed)
        changed = json.loads(json.dumps(cloudflare))
        changed["previous_canonical_deployment_id"] = "previous\N{SNOWMAN}"
        cf_variants.append(changed)
        for changed in cf_variants:
            with self.subTest(provider="cloudflare", changed=changed):
                with self.assertRaises(RuntimeError):
                    publish_web.validate_cloudflare_receipt(
                        changed, SHA, artifact_digest
                    )

        ali_variants = []
        changed = json.loads(json.dumps(aliyun))
        changed["marker_sha256"] = "0" * 64
        ali_variants.append(changed)
        changed = json.loads(json.dumps(aliyun))
        changed["edge_ip"] = "198.18.1.1"
        ali_variants.append(changed)
        changed = json.loads(json.dumps(aliyun))
        changed["refresh_tasks"].append(changed["refresh_tasks"][0])
        ali_variants.append(changed)
        changed = json.loads(json.dumps(aliyun))
        changed["refresh_tasks"][0]["path"] = "https://attacker.invalid/"
        ali_variants.append(changed)
        for changed in ali_variants:
            with self.subTest(provider="aliyun", changed=changed):
                with self.assertRaises(RuntimeError):
                    publish_web.validate_aliyun_receipt(
                        changed, SHA, artifact_digest
                    )

    def test_legacy_state_without_proof_falls_back_to_full_upload(self) -> None:
        self.publish("first.json")
        shutil.rmtree(self.state / "web-proofs")
        self.log.write_text("", encoding="utf-8")
        self.set_revision(SHA2)
        result = self.publish("second.json", SHA2)
        self.assertEqual(result["status"], "published")
        calls = self.log.read_text(encoding="utf-8")
        self.assertEqual(
            sum(
                line.startswith("ossutil cp /")
                and "assets/app-AbCdEf12.js" in line
                for line in calls.splitlines()
            ), 1
        )

    def test_parallel_apply_uses_a_bounded_worker_pool(self) -> None:
        lock = threading.Lock()
        active = 0
        peak = 0

        def action(_: str) -> None:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.03)
            with lock:
                active -= 1

        publish_web.parallel_apply(
            "fixture", [str(index) for index in range(12)], action, workers=4
        )
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 4)

    def test_public_verification_uses_fixed_root_marker_and_exact_bytes(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        requests = []

        class Response:
            status = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self, limit: int) -> bytes:
                return self.payload[:limit]

        def open_request(request, timeout):
            requests.append((request.full_url, timeout))
            return Response(marker if len(requests) == 1 else index)

        with mock.patch.object(
            publish_web, "urlopen", side_effect=open_request
        ), mock.patch.object(publish_web.time, "time_ns", return_value=123456):
            value = publish_web.verify_public_release(
                "https://vane.example",
                SHA,
                expected_marker=marker,
                expected_index_sha256=hashlib.sha256(index).hexdigest(),
            )
        self.assertEqual(value["schema"], "vane.web-release/v1")
        self.assertEqual(
            [url for url, _ in requests],
            [
                f"https://vane.example/vane-release.json?release={SHA}&probe=123456-1",
                f"https://vane.example/index.html?release={SHA}&probe=123456-1",
            ],
        )

    def test_public_verification_changes_probe_after_stale_edge_response(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        requests: list[str] = []

        class Response:
            status = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self, limit: int) -> bytes:
                return self.payload[:limit]

        def open_request(request, timeout):
            requests.append(request.full_url)
            if len(requests) == 1:
                return Response(b'{"source_revision":"stale"}')
            return Response(marker if len(requests) == 2 else index)

        with mock.patch.object(
            publish_web, "urlopen", side_effect=open_request
        ), mock.patch.object(
            publish_web.time, "time_ns", side_effect=[111, 222]
        ), mock.patch.object(publish_web.time, "sleep"):
            value = publish_web.verify_public_release(
                "https://vane.example",
                SHA,
                expected_marker=marker,
                expected_index_sha256=hashlib.sha256(index).hexdigest(),
                attempts=2,
            )
        self.assertEqual(value["source_revision"], SHA)
        self.assertEqual(
            requests,
            [
                f"https://vane.example/vane-release.json?release={SHA}&probe=111-1",
                f"https://vane.example/vane-release.json?release={SHA}&probe=222-2",
                f"https://vane.example/index.html?release={SHA}&probe=222-2",
            ],
        )

    def test_cloudflare_public_verification_reads_entrypoint_from_root(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        requests: list[str] = []

        def open_request(request, timeout):
            requests.append(request.full_url)
            return PublicResponse(marker if len(requests) == 1 else index)

        with mock.patch.object(
            publish_web, "urlopen", side_effect=open_request
        ), mock.patch.object(publish_web.time, "time_ns", return_value=123456):
            publish_web.verify_public_release(
                "https://vane-web.pages.dev",
                SHA,
                expected_marker=marker,
                expected_index_sha256=hashlib.sha256(index).hexdigest(),
                index_path="/",
                directory_indexes=True,
            )
        self.assertEqual(
            requests,
            [
                f"https://vane-web.pages.dev/vane-release.json?release={SHA}&probe=123456-1",
                f"https://vane-web.pages.dev/?release={SHA}&probe=123456-1",
            ],
        )
        self.assertEqual(
            publish_web.cloudflare_public_path(PREVIEW),
            "/_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/",
        )

    def test_public_object_verification_retries_one_transient_edge_body(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        stable = b'exact-stable-object'
        object_reads = 0

        def open_request(request, timeout):
            nonlocal object_reads
            if "/vane-release.json?" in request.full_url:
                return PublicResponse(marker)
            if "/stable-config.json?" in request.full_url:
                object_reads += 1
                return PublicResponse(b"transient" if object_reads == 1 else stable)
            return PublicResponse(index)

        expected = {
            "index.html": {"path": "index.html", "size": len(index),
                           "sha256": hashlib.sha256(index).hexdigest()},
            "vane-release.json": {"path": "vane-release.json", "size": len(marker),
                                  "sha256": hashlib.sha256(marker).hexdigest()},
            "stable-config.json": {"path": "stable-config.json", "size": len(stable),
                                   "sha256": hashlib.sha256(stable).hexdigest()},
        }
        with mock.patch.object(
            publish_web, "urlopen", side_effect=open_request
        ), mock.patch.object(publish_web.time, "sleep"):
            publish_web.verify_public_release(
                "https://vane-web.pages.dev", SHA,
                expected_marker=marker,
                expected_index_sha256=hashlib.sha256(index).hexdigest(),
                expected_files=expected, index_path="/", directory_indexes=True,
                attempts=1,
            )
        self.assertEqual(object_reads, 2)

    def test_public_verification_rejects_same_revision_forged_marker(self) -> None:
        expected = (self.dist / "vane-release.json").read_bytes()
        forged = json.dumps({
            "schema": "attacker/v9",
            "source_revision": SHA,
            "source_dirty": False,
            "tree_sha256": "0" * 64,
            "file_count": 2,
        }).encode()
        response = mock.MagicMock()
        response.status = 200
        response.read.return_value = forged
        response.__enter__.return_value = response
        with mock.patch.object(publish_web, "urlopen", return_value=response), mock.patch.object(
            publish_web.time, "sleep"
        ):
            with self.assertRaisesRegex(RuntimeError, "did not converge"):
                publish_web.verify_public_release(
                    "https://vane.example",
                    SHA,
                    expected_marker=expected,
                    expected_index_sha256="0" * 64,
                )

    def test_aliyun_doh_rejects_fake_or_polluted_edge_answers(self) -> None:
        def response(payload: dict) -> PublicResponse:
            return PublicResponse(json.dumps(payload).encode())

        valid_aaaa = {
            "Status": 0,
            "Question": {"name": publish_web.ALIYUN_CDN_CNAME, "type": 28},
            "Answer": [],
        }
        variants = [
            {
                "Status": 0,
                "Question": {"name": publish_web.ALIYUN_CDN_CNAME, "type": 1},
                "Answer": [{"name": publish_web.ALIYUN_CDN_CNAME,
                            "type": 1, "data": "198.18.1.131"}],
            },
            {
                "Status": 0,
                "Question": {"name": "attacker.example", "type": 1},
                "Answer": [{"name": "attacker.example",
                            "type": 1, "data": "8.8.8.8"}],
            },
            {
                "Status": 0,
                "Question": {"name": publish_web.ALIYUN_CDN_CNAME, "type": 1},
                "Answer": [],
            },
        ]
        for payload in variants:
            with self.subTest(payload=payload):
                with mock.patch.object(
                    publish_web, "urlopen",
                    side_effect=[response(payload), response(valid_aaaa)],
                ):
                    with self.assertRaises(RuntimeError):
                        publish_web.aliyun_doh_addresses(
                            publish_web.ALIYUN_CDN_CNAME
                        )

        valid_a = {
            "Status": 0,
            "Question": {"name": publish_web.ALIYUN_CDN_CNAME, "type": 1},
            "Answer": [{"name": publish_web.ALIYUN_CDN_CNAME,
                        "type": 1, "data": "183.95.252.39"}],
        }
        with mock.patch.object(
            publish_web, "urlopen",
            side_effect=[response(valid_a), response(valid_aaaa)],
        ):
            self.assertEqual(
                publish_web.aliyun_doh_addresses(publish_web.ALIYUN_CDN_CNAME),
                ["183.95.252.39"],
            )

    def test_trusted_doh_fails_over_when_alidns_has_no_usable_edge(self) -> None:
        cname = publish_web.ALIYUN_CDN_CNAME
        alidns_addresses = [f"103.78.127.{value}" for value in range(140, 148)]
        alidns_a = {
            "Status": 0, "Question": {"name": cname, "type": 1},
            "Answer": [
                {"name": cname, "type": 1, "data": address}
                for address in alidns_addresses
            ],
        }
        alidns_aaaa = {
            "Status": 0, "Question": {"name": cname, "type": 28}, "Answer": [],
        }
        cloudflare_a = {
            "Status": 0, "Question": [{"name": cname, "type": 1}],
            "Answer": [{"name": cname, "type": 1, "data": "211.144.72.167"}],
        }
        cloudflare_aaaa = {
            "Status": 0, "Question": [{"name": cname, "type": 28}], "Answer": [],
        }
        responses = [
            PublicResponse(json.dumps(value).encode())
            for value in (alidns_a, alidns_aaaa, cloudflare_a, cloudflare_aaaa)
        ]
        with mock.patch.object(publish_web, "urlopen", side_effect=responses):
            ordered = publish_web.aliyun_doh_addresses(cname)
        self.assertEqual(ordered[:2], ["103.78.127.140", "211.144.72.167"])
        self.assertEqual(set(ordered), set(alidns_addresses + ["211.144.72.167"]))

    def test_trusted_doh_rejects_when_every_resolver_fails(self) -> None:
        polluted = {
            "Status": 0,
            "Question": {"name": "attacker.example", "type": 1},
            "Answer": [],
        }
        with mock.patch.object(
            publish_web, "urlopen",
            side_effect=[
                PublicResponse(json.dumps(polluted).encode()) for _ in range(3)
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "trusted DoH resolvers"):
                publish_web.aliyun_doh_addresses(publish_web.ALIYUN_CDN_CNAME)

    def test_aliyun_edge_falls_through_unreachable_alidns_candidate(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()

        class Response:
            status = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self, limit: int) -> bytes:
                return self.payload[:limit]

            def getheaders(self):
                return []

        attempts: list[str] = []

        class Connection:
            def __init__(self, ip: str) -> None:
                attempts.append(ip)
                if ip.startswith("103."):
                    raise TimeoutError("AliDNS candidate is unreachable")
                self.path = ""

            def request(self, method: str, path: str, headers: dict) -> None:
                self.path = path

            def getresponse(self):
                return Response(
                    marker if "vane-release.json" in self.path else index
                )

            def close(self) -> None:
                pass

        expected = {
            "index.html": {"path": "index.html", "size": len(index),
                           "sha256": hashlib.sha256(index).hexdigest()},
            "vane-release.json": {"path": "vane-release.json", "size": len(marker),
                                  "sha256": hashlib.sha256(marker).hexdigest()},
        }
        with mock.patch.object(
            publish_web, "aliyun_doh_addresses",
            return_value=[
                "103.78.127.140", "211.144.72.167",
                "103.78.127.141", "103.78.127.142", "103.78.127.143",
                "103.78.127.144", "103.78.127.145", "103.78.127.146",
                "103.78.127.147",
            ],
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ):
            result = publish_web.verify_aliyun_edge(
                publish_web.ALIYUN_CDN_CNAME, SHA, marker,
                hashlib.sha256(index).hexdigest(), expected,
            )
        self.assertEqual(result["ip"], "211.144.72.167")
        self.assertEqual(attempts[:2], ["103.78.127.140", "211.144.72.167"])

    def test_cloudflare_custom_edge_uses_exact_host_and_rejects_tampered_body(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        asset = (self.dist / "assets/app-AbCdEf12.js").read_bytes()
        stable = b'{"stable":true}'
        expected = {
            "index.html": {"path": "index.html", "size": len(index),
                           "sha256": hashlib.sha256(index).hexdigest()},
            "vane-release.json": {"path": "vane-release.json", "size": len(marker),
                                  "sha256": hashlib.sha256(marker).hexdigest()},
            "assets/app-AbCdEf12.js": {
                "path": "assets/app-AbCdEf12.js", "size": len(asset),
                "sha256": hashlib.sha256(asset).hexdigest(),
            },
            "stable-config.json": {
                "path": "stable-config.json", "size": len(stable),
                "sha256": hashlib.sha256(stable).hexdigest(),
            },
        }
        requests: list[tuple[str, dict]] = []
        bodies = {
            "/vane-release.json": marker,
            "/": index,
            "/assets/app-AbCdEf12.js": asset,
            "/stable-config.json": stable,
        }

        class Response:
            status = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self, limit: int) -> bytes:
                return self.payload[:limit]

        class Connection:
            def __init__(self, ip: str) -> None:
                self.path = ""

            def request(self, method: str, path: str, headers: dict) -> None:
                self.path = path.split("?", 1)[0]
                requests.append((self.path, headers))

            def getresponse(self):
                return Response(bodies[self.path])

            def close(self) -> None:
                pass

        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["104.18.1.1"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ):
            smoke = publish_web.verify_cloudflare_custom_edge(
                SHA, marker, hashlib.sha256(index).hexdigest(), expected
            )
        self.assertEqual(smoke["host"], "vane.zhuoqidev.com")
        self.assertTrue(requests)
        self.assertTrue(all(
            headers["Host"] == "vane.zhuoqidev.com"
            for _, headers in requests
        ))

        bodies["/stable-config.json"] = b"bad"
        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["104.18.1.1"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ):
            with self.assertRaisesRegex(RuntimeError, "custom edge verification failed"):
                publish_web.verify_cloudflare_custom_edge(
                    SHA, marker, hashlib.sha256(index).hexdigest(), expected
                )

    def test_cloudflare_custom_edge_retries_one_transient_object(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        stable = b'{"stable":true}'
        expected = {
            "index.html": {"path": "index.html", "size": len(index),
                           "sha256": hashlib.sha256(index).hexdigest()},
            "vane-release.json": {
                "path": "vane-release.json", "size": len(marker),
                "sha256": hashlib.sha256(marker).hexdigest(),
            },
            "stable-config.json": {
                "path": "stable-config.json", "size": len(stable),
                "sha256": hashlib.sha256(stable).hexdigest(),
            },
        }
        attempts: dict[str, int] = {}

        class Response:
            status = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self, limit: int) -> bytes:
                return self.payload[:limit]

        class Connection:
            def __init__(self, ip: str) -> None:
                self.path = ""

            def request(self, method: str, path: str, headers: dict) -> None:
                self.path = path.split("?", 1)[0]
                attempts[self.path] = attempts.get(self.path, 0) + 1

            def getresponse(self):
                if self.path == "/stable-config.json" and attempts[self.path] == 1:
                    return Response(b"transient")
                return Response({
                    "/vane-release.json": marker,
                    "/": index,
                    "/stable-config.json": stable,
                }[self.path])

            def close(self) -> None:
                pass

        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["104.18.1.1"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ), mock.patch.object(publish_web.time, "sleep"):
            smoke = publish_web.verify_cloudflare_custom_edge(
                SHA, marker, hashlib.sha256(index).hexdigest(), expected
            )
        self.assertEqual(smoke["edge_ip"], "104.18.1.1")
        self.assertEqual(attempts["/stable-config.json"], 2)

    def test_aliyun_edge_requires_exact_redirect_and_splat_behavior(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        redirects = b"/.well-known/* https://api.vane.zhuoqidev.com/.well-known/:splat 302\n"
        expected = {
            name: {"path": name, "size": len(payload),
                   "sha256": hashlib.sha256(payload).hexdigest()}
            for name, payload in {
                "index.html": index,
                "vane-release.json": marker,
                "_redirects": redirects,
            }.items()
        }
        wrong_probe = False

        class Response:
            def __init__(self, path: str) -> None:
                self.path = path
                self.status = 302 if path.startswith("/.well-known/") else 200

            def read(self, limit: int) -> bytes:
                bodies = {
                    "/vane-release.json": marker,
                    "/index.html": index,
                    "/_redirects": redirects,
                }
                return bodies.get(self.path, b"")[:limit]

            def getheaders(self):
                if not self.path.startswith("/.well-known/"):
                    return []
                suffix = self.path.removeprefix("/.well-known/")
                location = "https://api.vane.zhuoqidev.com/.well-known/" + suffix
                if wrong_probe and suffix == "probe.json":
                    location = "https://attacker.example/probe.json"
                return [("Location", location)]

        class Connection:
            def __init__(self, ip: str) -> None:
                self.path = ""

            def request(self, method: str, path: str, headers: dict) -> None:
                self.path = path.split("?", 1)[0]

            def getresponse(self):
                return Response(self.path)

            def close(self) -> None:
                pass

        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["183.95.252.39"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ):
            result = publish_web.verify_aliyun_edge(
                publish_web.ALIYUN_CDN_CNAME, SHA, marker,
                hashlib.sha256(index).hexdigest(), expected,
            )
            self.assertEqual(result["control_smoke"], {"redirects": "verified"})
            wrong_probe = True
            with self.assertRaisesRegex(RuntimeError, "RoutingRules"):
                publish_web.verify_aliyun_edge(
                    publish_web.ALIYUN_CDN_CNAME, SHA, marker,
                    hashlib.sha256(index).hexdigest(), expected,
                )

    def test_cloudflare_custom_edge_rejects_non_cloudflare_tls_authority(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        expected = {
            "index.html": {"path": "index.html", "size": len(index),
                           "sha256": hashlib.sha256(index).hexdigest()},
            "vane-release.json": {"path": "vane-release.json", "size": len(marker),
                                  "sha256": hashlib.sha256(marker).hexdigest()},
        }
        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["8.8.8.8"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection",
            side_effect=RuntimeError("TLS certificate is not valid for custom host"),
        ):
            with self.assertRaisesRegex(RuntimeError, "custom edge verification failed"):
                publish_web.verify_cloudflare_custom_edge(
                    SHA, marker, hashlib.sha256(index).hexdigest(), expected
                )

    def test_aliyun_owner_preview_requires_exact_security_headers(self) -> None:
        marker = (self.dist / "vane-release.json").read_bytes()
        index = (self.dist / "index.html").read_bytes()
        preview = b"private-preview"
        expected = {
            "index.html": {"path": "index.html", "size": len(index),
                           "sha256": hashlib.sha256(index).hexdigest()},
            "vane-release.json": {"path": "vane-release.json", "size": len(marker),
                                  "sha256": hashlib.sha256(marker).hexdigest()},
            PREVIEW: {"path": PREVIEW, "size": len(preview),
                      "sha256": hashlib.sha256(preview).hexdigest()},
        }
        bodies = {
            "/vane-release.json": marker,
            "/index.html": index,
            "/" + PREVIEW: preview,
        }
        preview_headers = [
            ("Cache-Control", "no-store"),
            ("X-Robots-Tag", "noindex, nofollow, noarchive"),
            ("Referrer-Policy", "no-referrer"),
            ("Content-Security-Policy", publish_web.OWNER_PREVIEW_CSP),
        ]

        class Response:
            status = 200

            def __init__(self, path: str) -> None:
                self.path = path

            def read(self, limit: int) -> bytes:
                return bodies[self.path][:limit]

            def getheaders(self):
                return preview_headers if self.path == "/" + PREVIEW else []

        class Connection:
            def __init__(self, ip: str) -> None:
                self.path = ""

            def request(self, method: str, path: str, headers: dict) -> None:
                self.path = path.split("?", 1)[0]

            def getresponse(self):
                return Response(self.path)

            def close(self) -> None:
                pass

        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["183.95.252.39"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ), mock.patch.object(
            publish_web, "parallel_apply", wraps=publish_web.parallel_apply
        ) as parallel:
            publish_web.verify_aliyun_edge(
                publish_web.ALIYUN_CDN_CNAME, SHA, marker,
                hashlib.sha256(index).hexdigest(), expected,
            )
        self.assertEqual(parallel.call_args.kwargs["workers"], publish_web.CDN_WORKERS)
        preview_headers[:] = [
            item for item in preview_headers if item[0] != "Cache-Control"
        ]
        with mock.patch.object(
            publish_web, "aliyun_doh_addresses", return_value=["183.95.252.39"]
        ), mock.patch.object(
            publish_web, "PinnedEdgeHTTPSConnection", Connection
        ):
            with self.assertRaisesRegex(RuntimeError, "cache header differs"):
                publish_web.verify_aliyun_edge(
                    publish_web.ALIYUN_CDN_CNAME, SHA, marker,
                    hashlib.sha256(index).hexdigest(), expected,
                )


if __name__ == "__main__":
    unittest.main()
