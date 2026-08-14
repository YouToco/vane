from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "publish_web", ROOT / "ops/release/publish_web.py"
)
assert SPEC and SPEC.loader
publish_web = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish_web)
SHA = "0123456789abcdef0123456789abcdef01234567"
PREVIEW = "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"


class PublishWebTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
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
        for path in (self.work, self.state, self.cache, self.remote):
            path.mkdir()
        lock = json.loads((ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8"))["tools"]
        self.log = self.root / "calls.log"
        aliyun = self.cache / "aliyun_cli" / lock["aliyun_cli"]["version"] / "aliyun"
        ossutil = self.cache / "ossutil" / lock["ossutil"]["version"] / "ossutil"
        aliyun.parent.mkdir(parents=True)
        ossutil.parent.mkdir(parents=True)
        aliyun.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = version ]; then echo 3.4.10; exit 0; fi\n"
            "printf 'aliyun %s\\n' \"$*\" >>\"$WEB_CALL_LOG\"\n",
            encoding="utf-8",
        )
        ossutil.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = version ]; then echo 2.3.0; exit 0; fi\n"
            "printf 'ossutil %s\\n' \"$*\" >>\"$WEB_CALL_LOG\"\n"
            "case \"$1\" in\n"
            " cp) case \"$2\" in\n"
            "       oss://*) object=${2#oss://zhuoqidev-vane-web/}; mkdir -p \"$(dirname \"$3\")\"; cp \"$OSS_REMOTE/$object\" \"$3\";;\n"
            "       *) object=${3#oss://zhuoqidev-vane-web/}; mkdir -p \"$OSS_REMOTE/$(dirname \"$object\")\"; cp \"$2\" \"$OSS_REMOTE/$object\"; [ \"${CORRUPT_UPLOAD-}\" != \"$object\" ] || printf bad >\"$OSS_REMOTE/$object\";;\n"
            "     esac;;\n"
            " stat) object=${2#oss://zhuoqidev-vane-web/}; [ \"${FAIL_STAT-}\" != \"$object\" ] || exit 71; size=$(wc -c <\"$OSS_REMOTE/$object\"); printf 'Content-Length : %s\\n' \"$size\";;\n"
            " set-props) [ \"$3\" = --cache-control ] && [ \"$4\" = no-store ] || exit 72;;\n"
            " sync|rm) exit 73;;\n"
            "esac\n",
            encoding="utf-8",
        )
        aliyun.chmod(0o755)
        ossutil.chmod(0o755)
        self.environment = mock.patch.dict(os.environ, {
            "ALIYUN_ACCESS_KEY_ID": "fixture-id",
            "ALIYUN_ACCESS_KEY_SECRET": "fixture-secret",
            "OSS_REMOTE": str(self.remote),
            "WEB_CALL_LOG": str(self.log),
        })

    def publish(self, result_name: str = "result.json") -> dict:
        result_path = self.root / result_name
        with self.environment, mock.patch.object(
            publish_web, "verify_public_release",
            return_value={"source_revision": SHA, "source_dirty": False},
        ):
            return publish_web.publish(
                dist=self.dist, revision=SHA, work_root=self.work,
                state_root=self.state, tool_cache=self.cache,
                origin="https://vane.example", result_path=result_path,
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
        self.assertNotIn("ossutil sync", calls)
        self.assertNotIn("ossutil rm", calls)
        self.assertEqual(
            json.loads((self.state / "web-current.json").read_text(encoding="utf-8"))["revision"],
            SHA,
        )

    def test_failed_asset_stat_never_cuts_over_index(self) -> None:
        old_marker = b'{"source_revision":"old"}\n'
        (self.remote / "vane-release.json").write_bytes(old_marker)
        with mock.patch.dict(os.environ, {"FAIL_STAT": "assets/app-AbCdEf12.js"}):
            with self.assertRaisesRegex(RuntimeError, "publication command failed"):
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
        with self.assertRaisesRegex(RuntimeError, "different immutable release receipt"):
            self.publish("second.json")
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp", calls)
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
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_missing_referenced_asset_fails_before_remote_mutation(self) -> None:
        (self.dist / "assets/app-AbCdEf12.js").unlink()
        with self.assertRaisesRegex(RuntimeError, "publication command failed"):
            self.publish()
        calls = self.log.read_text(encoding="utf-8") if self.log.exists() else ""
        self.assertNotIn("ossutil cp", calls)
        self.assertNotIn("aliyun cdn", calls)

    def test_owner_preview_is_published_with_no_store(self) -> None:
        preview = self.dist / PREVIEW
        preview.parent.mkdir(parents=True)
        preview.write_text(
            '<script type="module" src="/assets/app-AbCdEf12.js"></script>',
            encoding="utf-8",
        )
        self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertIn(f"ossutil cp {preview} oss://zhuoqidev-vane-web/{PREVIEW}", calls)
        self.assertIn(
            f"ossutil set-props oss://zhuoqidev-vane-web/{PREVIEW} "
            "--cache-control no-store --metadata-directive update --force",
            calls,
        )

    def test_symlinked_lock_is_rejected_before_tools_or_network(self) -> None:
        outside = self.root / "outside.lock"
        outside.write_text("", encoding="utf-8")
        (self.state / "web-release.lock").symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "must not be symlinks"):
            self.publish()
        self.assertFalse(self.log.exists())

    def test_already_current_restores_the_deterministic_receipt(self) -> None:
        self.publish("first.json")
        receipt = self.root / "web-release-receipt.json"
        expected = receipt.read_bytes()
        receipt.unlink()
        result = self.publish("second.json")
        self.assertEqual(result["status"], "already-current")
        self.assertEqual(receipt.read_bytes(), expected)

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

        with mock.patch.object(publish_web, "urlopen", side_effect=open_request):
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
                f"https://vane.example/vane-release.json?release={SHA}",
                f"https://vane.example/index.html?release={SHA}",
            ],
        )

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


if __name__ == "__main__":
    unittest.main()
