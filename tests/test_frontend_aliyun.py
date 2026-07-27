import contextlib
import json
import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "scripts" / "deploy.sh"
SHA = "190bf2d8264d393116f8a87da461e58f65784843"
PREVIEW_OBJECT = (
    "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"
)
JS_OBJECT = "assets/app-AbCdEf12.js"
CSS_OBJECT = "assets/app-ZyXwVu98.css"
SHARED_JS_OBJECT = "assets/shared-QwErTy12.js"
ICON_OBJECT = "brand icon.png"


class FrontendAliyunTests(unittest.TestCase):
    @contextlib.contextmanager
    def publication_case(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            home = root / "home"
            runner_temp = root / "runner-temp"
            payload = root / "payload"
            dist = payload / "dist"
            receipts = root / "receipts"
            fake_bin = root / "fake-bin"
            remote = root / "oss"
            for directory in (
                home,
                runner_temp,
                dist / "assets",
                dist / ".vite",
                dist / pathlib.Path(PREVIEW_OBJECT).parent,
                fake_bin,
                remote / "assets",
            ):
                directory.mkdir(parents=True)

            (dist / "index.html").write_text(
                "<!doctype html>"
                '<link rel="manifest" href="/manifest.webmanifest">'
                f'<link rel="stylesheet" href="/{CSS_OBJECT}">'
                f'<link rel="modulepreload" href="/{SHARED_JS_OBJECT}">'
                f'<script type="module" src="/{JS_OBJECT}"></script>'
                '<a href="/settings">Settings</a>',
                encoding="utf-8",
            )
            (dist / PREVIEW_OBJECT).write_text(
                f'<script src="/{JS_OBJECT}"></script>', encoding="utf-8"
            )
            (dist / JS_OBJECT).write_text("new-app", encoding="utf-8")
            (dist / CSS_OBJECT).write_text("new-css", encoding="utf-8")
            (dist / SHARED_JS_OBJECT).write_text(
                "new-shared", encoding="utf-8"
            )
            (dist / ICON_OBJECT).write_bytes(b"new-icon")
            (dist / "manifest.webmanifest").write_text(
                json.dumps({"icons": [{"src": f"/{ICON_OBJECT}"}]}),
                encoding="utf-8",
            )
            (dist / ".vite" / "manifest.json").write_text(
                json.dumps(
                    {
                        "src/main.ts": {
                            "file": JS_OBJECT,
                            "css": [CSS_OBJECT],
                            "imports": ["_shared"],
                            "dynamicImports": ["_shared"],
                        },
                        "_shared": {"file": SHARED_JS_OBJECT},
                    }
                ),
                encoding="utf-8",
            )
            (remote / "index.html").write_text("old-index", encoding="utf-8")
            (remote / "assets" / "old-hash.js").write_text(
                "old-asset", encoding="utf-8"
            )

            flock = fake_bin / "flock"
            flock.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            flock.chmod(0o755)

            aliyun_call_log = root / "aliyun-calls"
            aliyun = fake_bin / "aliyun"
            aliyun.write_text(
                "#!/bin/sh\n"
                "{\n"
                "  printf 'argv'\n"
                "  printf '\\t%s' \"$@\"\n"
                "  printf '\\tALI_ID=%s\\tALI_SECRET=%s\\tALI_REGION=%s"
                "\\tIGNORE_PROFILE=%s\\n' "
                "\"${ALIBABA_CLOUD_ACCESS_KEY_ID-}\" "
                "\"${ALIBABA_CLOUD_ACCESS_KEY_SECRET-}\" "
                "\"${ALIBABA_CLOUD_REGION_ID-}\" "
                "\"${ALIBABA_CLOUD_IGNORE_PROFILE-}\"\n"
                "} >>\"$ALIYUN_CALL_LOG\"\n",
                encoding="utf-8",
            )
            aliyun.chmod(0o755)

            ossutil_call_log = root / "ossutil-calls"
            ossutil = fake_bin / "ossutil"
            ossutil.write_text(
                "#!/bin/sh\n"
                "{\n"
                "  printf 'argv'\n"
                "  printf '\\t%s' \"$@\"\n"
                "  printf '\\tOSS_ID=%s\\tOSS_SECRET=%s\\tOSS_REGION=%s\\n' "
                "\"${OSS_ACCESS_KEY_ID-}\" "
                "\"${OSS_ACCESS_KEY_SECRET-}\" "
                "\"${OSS_REGION-}\"\n"
                "} >>\"$OSSUTIL_CALL_LOG\"\n"
                "if [ \"$1\" = version ]; then\n"
                "  printf '2.3.0\\n'\n"
                "  exit 0\n"
                "fi\n"
                "if [ -z \"${OSS_ACCESS_KEY_ID-}\" ] || "
                "[ -z \"${OSS_ACCESS_KEY_SECRET-}\" ] || "
                "[ -z \"${OSS_REGION-}\" ]; then\n"
                "  printf 'missing OSS credentials\\n' >&2\n"
                "  exit 9\n"
                "fi\n"
                "case \"$1\" in\n"
                "  cp)\n"
                "    object=${3#oss://zhuoqidev-vane-web/}\n"
                "    mkdir -p \"$OSS_REMOTE/$(dirname \"$object\")\"\n"
                "    cp \"$2\" \"$OSS_REMOTE/$object\"\n"
                "    ;;\n"
                "  stat)\n"
                "    object=${2#oss://zhuoqidev-vane-web/}\n"
                "    if [ \"${FAIL_STAT_OBJECT-}\" = \"$object\" ]; then\n"
                "      printf 'forced stat failure: %s\\n' \"$object\" >&2\n"
                "      exit 71\n"
                "    fi\n"
                "    [ -f \"$OSS_REMOTE/$object\" ] || exit 72\n"
                "    object_size=$(wc -c <\"$OSS_REMOTE/$object\")\n"
                "    if [ \"${FAIL_SIZE_OBJECT-}\" = \"$object\" ]; then\n"
                "      object_size=$((object_size + 1))\n"
                "    fi\n"
                "    printf 'Content-Length        : %s\\n' \"$object_size\"\n"
                "    case \"$object\" in\n"
                "      _preview/*/index.html) printf 'Cache-Control: no-store\\n' ;;\n"
                "    esac\n"
                "    ;;\n"
                "  set-props)\n"
                "    [ \"$3\" = --cache-control ] || exit 65\n"
                "    [ \"$4\" = no-store ] || exit 66\n"
                "    [ \"$5\" = --metadata-directive ] || exit 67\n"
                "    [ \"$6\" = update ] || exit 68\n"
                "    [ \"$7\" = --force ] || exit 69\n"
                "    ;;\n"
                "  sync|rm)\n"
                "    printf 'unsafe bulk mutation invoked\\n' >&2\n"
                "    exit 73\n"
                "    ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            ossutil.chmod(0o755)

            access_id = "TESTACCESSID"
            access_secret = "TESTACCESSSECRET"
            env = os.environ.copy()
            env.update(
                {
                    "ALIYUN_ACCESS_KEY_ID": access_id,
                    "ALIYUN_ACCESS_KEY_SECRET": access_secret,
                    "ALIYUN_BIN": str(aliyun),
                    "ALIYUN_CALL_LOG": str(aliyun_call_log),
                    "EXPECTED_DEPLOYED_SHA": "",
                    "FRONTEND_RECEIPT_DIR": str(receipts),
                    "HOME": str(home),
                    "OSSUTIL_BIN": str(ossutil),
                    "OSSUTIL_CALL_LOG": str(ossutil_call_log),
                    "OSS_REMOTE": str(remote),
                    "PATH": f"{fake_bin}:/usr/bin:/bin",
                    "RUNNER_TEMP": str(runner_temp),
                }
            )
            yield {
                "access_id": access_id,
                "access_secret": access_secret,
                "aliyun_log": aliyun_call_log,
                "dist": dist,
                "env": env,
                "home": home,
                "ossutil_log": ossutil_call_log,
                "payload": payload,
                "receipts": receipts,
                "remote": remote,
            }

    def run_deploy(self, case, **extra_env):
        env = case["env"].copy()
        env.update(extra_env)
        return subprocess.run(
            [str(DEPLOY), "frontend-aliyun", str(case["payload"]), SHA],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_assets_are_verified_before_entry_cutover_without_deletion(self) -> None:
        with self.publication_case() as case:
            result = self.run_deploy(case)
            self.assertEqual(result.returncode, 0, result.stderr)

            calls = case["ossutil_log"].read_text(encoding="utf-8").splitlines()
            self.assertTrue(all("\tsync\t" not in call for call in calls))
            self.assertTrue(all("\trm\t" not in call for call in calls))
            self.assertTrue(all("--delete" not in call for call in calls))
            entry_cp = next(
                index
                for index, call in enumerate(calls)
                if "\tcp\t" in call
                and "\toss://zhuoqidev-vane-web/index.html\t" in call
            )
            critical_stats = [
                index
                for index, call in enumerate(calls)
                if "\tstat\t" in call and "\toss://zhuoqidev-vane-web/assets/" in call
            ]
            self.assertGreaterEqual(len(critical_stats), 3, calls)
            self.assertTrue(all(index < entry_cp for index in critical_stats), calls)
            entry_stat = next(
                index
                for index, call in enumerate(calls)
                if "\tstat\toss://zhuoqidev-vane-web/index.html\t" in call
            )
            self.assertGreater(entry_stat, entry_cp, calls)
            self.assertTrue(
                all(
                    "\tcp\t" not in call
                    for call in calls[entry_cp + 1 :]
                ),
                calls,
            )
            self.assertEqual(
                (case["remote"] / "index.html").read_text(encoding="utf-8"),
                (case["dist"] / "index.html").read_text(encoding="utf-8"),
            )
            self.assertEqual(
                (case["remote"] / "assets" / "old-hash.js").read_text(
                    encoding="utf-8"
                ),
                "old-asset",
            )

            for call in calls:
                argv = call.split("\tOSS_ID=", 1)[0]
                self.assertNotIn(case["access_id"], argv)
                self.assertNotIn(case["access_secret"], argv)
            aliyun_calls = case["aliyun_log"].read_text(encoding="utf-8").splitlines()
            self.assertGreaterEqual(len(aliyun_calls), 2)
            for call in aliyun_calls:
                self.assertIn("\tcdn\tRefreshObjectCaches\t", call)
                self.assertIn("\t--ObjectType\tFile\t", call)
                self.assertNotIn("\tDirectory\t", call)
                argv = call.split("\tALI_ID=", 1)[0]
                self.assertNotIn(case["access_id"], argv)
                self.assertNotIn(case["access_secret"], argv)
            joined_aliyun_calls = "\n".join(aliyun_calls)
            self.assertIn(
                "\t--ObjectPath\t"
                "https://vane.zhuoqidev.com/manifest.webmanifest\t",
                joined_aliyun_calls,
            )
            self.assertIn(
                "\t--ObjectPath\t"
                "https://vane.zhuoqidev.com/brand%20icon.png\t",
                joined_aliyun_calls,
            )

            self.assertEqual(
                (case["receipts"] / "aliyun.sha").read_text(encoding="utf-8"),
                f"{SHA}\n",
            )
            release_path = (
                case["home"]
                / ".local"
                / "state"
                / "vane-deploy"
                / "releases"
                / SHA
                / "frontend-aliyun.json"
            )
            receipt = json.loads(release_path.read_text(encoding="utf-8"))
            self.assertEqual(receipt["source_sha"], SHA)
            self.assertEqual(receipt["entry_path"], "index.html")
            self.assertEqual(len(receipt["entry_sha256"]), 64)
            self.assertEqual(
                {item["path"] for item in receipt["files"]},
                {
                    ".vite/manifest.json",
                    PREVIEW_OBJECT,
                    CSS_OBJECT,
                    ICON_OBJECT,
                    "index.html",
                    JS_OBJECT,
                    "manifest.webmanifest",
                    SHARED_JS_OBJECT,
                },
            )

    def test_failed_asset_verification_does_not_cut_over_entry(self) -> None:
        with self.publication_case() as case:
            result = self.run_deploy(case, FAIL_SIZE_OBJECT=JS_OBJECT)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Content-Length does not match", result.stderr)
            self.assertEqual(
                (case["remote"] / "index.html").read_text(encoding="utf-8"),
                "old-index",
            )
            calls = case["ossutil_log"].read_text(encoding="utf-8")
            self.assertNotIn(
                "\tcp\t"
                f"{case['dist'] / 'index.html'}\t"
                "oss://zhuoqidev-vane-web/index.html\t",
                calls,
            )
            self.assertTrue((case["remote"] / "assets" / "old-hash.js").exists())
            self.assertFalse((case["receipts"] / "aliyun.sha").exists())
            self.assertFalse(
                (
                    case["home"]
                    / ".local"
                    / "state"
                    / "vane-deploy"
                    / "releases"
                    / SHA
                    / "frontend-aliyun.json"
                ).exists()
            )

    def test_symlinked_release_directory_fails_before_remote_calls(self) -> None:
        with self.publication_case() as case:
            state_dir = (
                case["home"] / ".local" / "state" / "vane-deploy"
            )
            state_dir.mkdir(parents=True)
            outside_releases = case["home"] / "outside-releases"
            outside_releases.mkdir()
            (state_dir / "releases").symlink_to(
                outside_releases, target_is_directory=True
            )

            result = self.run_deploy(case)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(
                "durable frontend release directory must not be a symlink",
                result.stderr,
            )
            self.assertFalse(case["ossutil_log"].exists())
            self.assertFalse(case["aliyun_log"].exists())
            self.assertEqual(
                (case["remote"] / "index.html").read_text(encoding="utf-8"),
                "old-index",
            )

    def test_same_sha_retry_is_idempotent(self) -> None:
        with self.publication_case() as case:
            first = self.run_deploy(case)
            self.assertEqual(first.returncode, 0, first.stderr)
            release_path = (
                case["home"]
                / ".local"
                / "state"
                / "vane-deploy"
                / "releases"
                / SHA
                / "frontend-aliyun.json"
            )
            first_receipt = release_path.read_bytes()
            first_entry = (case["remote"] / "index.html").read_bytes()

            second = self.run_deploy(case)
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(release_path.read_bytes(), first_receipt)
            self.assertEqual((case["remote"] / "index.html").read_bytes(), first_entry)
            self.assertTrue((case["remote"] / "assets" / "old-hash.js").exists())
            self.assertEqual(
                (case["receipts"] / "aliyun.sha").read_text(encoding="utf-8"),
                f"{SHA}\n",
            )

    def test_same_sha_payload_change_conflicts_before_remote_calls(self) -> None:
        with self.publication_case() as case:
            first = self.run_deploy(case)
            self.assertEqual(first.returncode, 0, first.stderr)
            ossutil_calls = case["ossutil_log"].read_bytes()
            aliyun_calls = case["aliyun_log"].read_bytes()
            remote_entry = (case["remote"] / "index.html").read_bytes()

            (case["dist"] / JS_OBJECT).write_text(
                "different-bytes-for-same-source-sha", encoding="utf-8"
            )
            second = self.run_deploy(case)
            self.assertNotEqual(second.returncode, 0)
            self.assertIn(
                "durable frontend release receipt conflicts", second.stderr
            )
            self.assertEqual(case["ossutil_log"].read_bytes(), ossutil_calls)
            self.assertEqual(case["aliyun_log"].read_bytes(), aliyun_calls)
            self.assertEqual(
                (case["remote"] / "index.html").read_bytes(), remote_entry
            )

    def test_missing_referenced_asset_fails_before_any_remote_mutation(self) -> None:
        with self.publication_case() as case:
            (case["dist"] / JS_OBJECT).unlink()
            result = self.run_deploy(case)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("referenced frontend file is missing", result.stderr)
            self.assertFalse(case["ossutil_log"].exists())
            self.assertEqual(
                (case["remote"] / "index.html").read_text(encoding="utf-8"),
                "old-index",
            )

    def test_vite_dependency_keys_must_exist_before_remote_calls(self) -> None:
        for field in ("imports", "dynamicImports"):
            with self.subTest(field=field), self.publication_case() as case:
                manifest_path = case["dist"] / ".vite" / "manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["src/main.ts"][field] = ["missing-manifest-key"]
                manifest_path.write_text(
                    json.dumps(manifest), encoding="utf-8"
                )

                result = self.run_deploy(case)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"Vite manifest {field} references missing keys",
                    result.stderr,
                )
                self.assertFalse(case["ossutil_log"].exists())
                self.assertFalse(case["aliyun_log"].exists())
                self.assertEqual(
                    (case["remote"] / "index.html").read_text(
                        encoding="utf-8"
                    ),
                    "old-index",
                )

    def test_unhashed_runtime_files_fail_before_remote_calls(self) -> None:
        for hashed_object, unhashed_object in (
            (JS_OBJECT, "assets/runtime.js"),
            (CSS_OBJECT, "assets/runtime.css"),
        ):
            with (
                self.subTest(unhashed_object=unhashed_object),
                self.publication_case() as case,
            ):
                (case["dist"] / unhashed_object).write_text(
                    "unsafe-stable-runtime-key", encoding="utf-8"
                )
                index_path = case["dist"] / "index.html"
                index_path.write_text(
                    index_path.read_text(encoding="utf-8").replace(
                        hashed_object, unhashed_object
                    ),
                    encoding="utf-8",
                )

                result = self.run_deploy(case)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "runtime JavaScript/CSS is not content-addressed",
                    result.stderr,
                )
                self.assertFalse(case["ossutil_log"].exists())
                self.assertFalse(case["aliyun_log"].exists())
                self.assertEqual(
                    (case["remote"] / "index.html").read_text(
                        encoding="utf-8"
                    ),
                    "old-index",
                )

    def test_unhashed_vite_runtime_files_fail_before_remote_calls(self) -> None:
        for field, unhashed_object in (
            ("file", "assets/vite-runtime.js"),
            ("css", "assets/vite-runtime.css"),
        ):
            with (
                self.subTest(field=field),
                self.publication_case() as case,
            ):
                (case["dist"] / unhashed_object).write_text(
                    "unsafe-stable-vite-runtime-key", encoding="utf-8"
                )
                manifest_path = case["dist"] / ".vite" / "manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["src/main.ts"][field] = (
                    [unhashed_object] if field == "css" else unhashed_object
                )
                manifest_path.write_text(
                    json.dumps(manifest), encoding="utf-8"
                )

                result = self.run_deploy(case)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "runtime JavaScript/CSS is not content-addressed",
                    result.stderr,
                )
                self.assertFalse(case["ossutil_log"].exists())
                self.assertFalse(case["aliyun_log"].exists())

    def test_missing_fixed_owner_preview_is_left_for_versioned_gc(self) -> None:
        with self.publication_case() as case:
            (case["dist"] / PREVIEW_OBJECT).unlink()
            stale_preview = case["remote"] / PREVIEW_OBJECT
            stale_preview.parent.mkdir(parents=True)
            stale_preview.write_text("stale-preview", encoding="utf-8")

            result = self.run_deploy(case)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                stale_preview.read_text(encoding="utf-8"), "stale-preview"
            )
            self.assertEqual(
                (case["remote"] / "index.html").read_text(encoding="utf-8"),
                (case["dist"] / "index.html").read_text(encoding="utf-8"),
            )

    def test_index_size_mismatch_blocks_cdn_and_receipts(self) -> None:
        with self.publication_case() as case:
            result = self.run_deploy(case, FAIL_SIZE_OBJECT="index.html")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Content-Length does not match", result.stderr)
            self.assertFalse(case["aliyun_log"].exists())
            self.assertFalse((case["receipts"] / "aliyun.sha").exists())
            self.assertFalse(
                (
                    case["home"]
                    / ".local"
                    / "state"
                    / "vane-deploy"
                    / "releases"
                    / SHA
                    / "frontend-aliyun.json"
                ).exists()
            )


if __name__ == "__main__":
    unittest.main()
