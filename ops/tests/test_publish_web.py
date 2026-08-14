from __future__ import annotations

import importlib.util
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
        (self.dist / ".well-known").mkdir()
        (self.dist / "assets/app-AbCdEf12.js").write_text("app", encoding="utf-8")
        (self.dist / "index.html").write_text(
            '<script type="module" src="/assets/app-AbCdEf12.js"></script>',
            encoding="utf-8",
        )
        (self.dist / ".well-known/vane-release.json").write_text(
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
            " cp) object=${3#oss://zhuoqidev-vane-web/}; mkdir -p \"$OSS_REMOTE/$(dirname \"$object\")\"; cp \"$2\" \"$OSS_REMOTE/$object\";;\n"
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
            publish_web, "verify_marker",
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
        refresh = calls.index("aliyun cdn RefreshObjectCaches", entry)
        self.assertLess(asset, entry)
        self.assertLess(entry, refresh)
        self.assertNotIn("ossutil sync", calls)
        self.assertNotIn("ossutil rm", calls)
        self.assertEqual(
            json.loads((self.state / "web-current.json").read_text(encoding="utf-8"))["revision"],
            SHA,
        )

    def test_failed_asset_stat_never_cuts_over_index(self) -> None:
        with mock.patch.dict(os.environ, {"FAIL_STAT": "assets/app-AbCdEf12.js"}):
            with self.assertRaisesRegex(RuntimeError, "publication command failed"):
                self.publish()
        calls = self.log.read_text(encoding="utf-8")
        self.assertNotIn("cp " + str(self.dist / "index.html"), calls)
        self.assertFalse((self.state / "web-current.json").exists())

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


if __name__ == "__main__":
    unittest.main()
