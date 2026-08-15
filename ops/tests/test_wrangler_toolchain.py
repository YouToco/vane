from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SPEC = importlib.util.spec_from_file_location(
    "publish_web_toolchain_test", ROOT / "ops/release/publish_web.py"
)
assert PUBLISH_SPEC and PUBLISH_SPEC.loader
publish_web = importlib.util.module_from_spec(PUBLISH_SPEC)
PUBLISH_SPEC.loader.exec_module(publish_web)


class WranglerToolchainTest(unittest.TestCase):
    def test_web_mutation_rejects_linux_and_darwin_amd64(self) -> None:
        for system, machine in (("Linux", "arm64"), ("Darwin", "x86_64")):
            with self.subTest(system=system, machine=machine), mock.patch.object(
                publish_web.platform, "system", return_value=system
            ), mock.patch.object(
                publish_web.platform, "machine", return_value=machine
            ):
                with self.assertRaisesRegex(RuntimeError, "exactly darwin-arm64"):
                    publish_web.machine_arch()

    def test_web_mutation_is_darwin_arm64_only_with_installed_byte_pins(self) -> None:
        lock = json.loads(
            (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
        )["tools"]
        for tool in ("node", "aliyun_cli", "ossutil", "wrangler"):
            self.assertEqual(set(lock[tool]["entry_sha256"]), {"darwin-arm64"})
        self.assertEqual(
            set(lock["wrangler"]["installed_tree_sha256"]), {"darwin-arm64"}
        )
        policy = json.loads(
            (ROOT / "ops/policy/release-policy.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            policy["web_publication_mutation_platform"], "darwin-arm64"
        )

    def test_current_release_mac_install_matches_entry_and_tree_pins(self) -> None:
        cache = ROOT / ".vane/tool-cache"
        if not (cache / "wrangler/4.115.0").is_dir():
            self.skipTest("release Mac tool cache is not provisioned")
        result = subprocess.run(
            [
                "python3", str(ROOT / "ops/release/publish_web.py"),
                "--toolchain-check", "--tool-cache", str(cache),
            ],
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence["machine"], "darwin-arm64")
        self.assertEqual(
            evidence["digests"]["wrangler_tree"],
            json.loads(
                (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
            )["tools"]["wrangler"]["installed_tree_sha256"]["darwin-arm64"],
        )

    def test_full_npm_graph_and_root_tarball_are_integrity_locked(self) -> None:
        toolchain = json.loads(
            (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
        )["tools"]["wrangler"]
        lock_path = ROOT / toolchain["package_lock"]
        self.assertEqual(
            hashlib.sha256(lock_path.read_bytes()).hexdigest(),
            toolchain["package_lock_sha256"],
        )
        package_lock = json.loads(lock_path.read_text(encoding="utf-8"))
        packages = package_lock["packages"]
        self.assertEqual(
            packages["node_modules/wrangler"]["version"], toolchain["version"]
        )
        self.assertEqual(
            packages["node_modules/wrangler"]["integrity"],
            toolchain["package_integrity"],
        )
        self.assertFalse([
            name for name, value in packages.items()
            if name and not value.get("link") and "integrity" not in value
        ])

    def test_installer_uses_locked_node_and_npm_ci_without_scripts(self) -> None:
        source = (
            ROOT / "ops/bootstrap/install-wrangler.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('node_version=22.23.2', source)
        self.assertIn('"$node" "$npm_cli" ci --ignore-scripts', source)
        self.assertNotIn("/opt/homebrew", source)
        self.assertNotIn("npx", source)

    def test_installer_accepts_real_npm_symlink_shape_and_is_idempotent(self) -> None:
        actual_node = shutil.which("node")
        if actual_node is None:
            self.skipTest("Node runtime is unavailable for installer smoke")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / "cache"
            work = root / "work"
            node_root = cache / "node/22.23.2"
            npm_cli = node_root / "lib/node_modules/npm/bin/npm-cli.js"
            node = node_root / "bin/node"
            npm_cli.parent.mkdir(parents=True)
            node.parent.mkdir(parents=True)
            work.mkdir()
            node.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = --version ]; then echo v22.23.2; exit 0; fi\n"
                f"exec {json.dumps(actual_node)} \"$@\"\n",
                encoding="utf-8",
            )
            node.chmod(0o755)
            counter = root / "npm-count"
            npm_cli.write_text(
                "const fs=require('fs'), p=require('path');\n"
                "const root=process.cwd(), counter=process.env.NPM_SMOKE_COUNTER;\n"
                "let n=fs.existsSync(counter)?Number(fs.readFileSync(counter,'utf8')):0;\n"
                "fs.writeFileSync(counter,String(n+1));\n"
                "const bin=p.join(root,'node_modules/wrangler/bin');\n"
                "const dot=p.join(root,'node_modules/.bin');\n"
                "fs.mkdirSync(bin,{recursive:true}); fs.mkdirSync(dot,{recursive:true});\n"
                "fs.writeFileSync(p.join(bin,'wrangler.js'),'console.log(\"4.115.0\")\\n');\n"
                "fs.symlinkSync('../wrangler/bin/wrangler.js',p.join(dot,'wrangler'));\n",
                encoding="utf-8",
            )
            environment = {
                **os.environ,
                "VANE_TOOL_CACHE": str(cache),
                "VANE_WORK_ROOT": str(work),
                "NPM_SMOKE_COUNTER": str(counter),
            }
            installer = ROOT / "ops/bootstrap/install-wrangler.sh"
            subprocess.run(["bash", str(installer)], env=environment, check=True)
            subprocess.run(["bash", str(installer)], env=environment, check=True)
            self.assertEqual(counter.read_text(encoding="utf-8"), "1")
            link = cache / "wrangler/4.115.0/node_modules/.bin/wrangler"
            self.assertTrue(link.is_symlink())
            self.assertEqual(os.readlink(link), "../wrangler/bin/wrangler.js")

            bad_cache = root / "bad-cache"
            shutil.copytree(cache / "node", bad_cache / "node")
            outside = root / "outside"
            outside.mkdir()
            (bad_cache / "wrangler").symlink_to(outside, target_is_directory=True)
            bad_work = root / "bad-work"
            bad_work.mkdir()
            rejected = subprocess.run(
                ["bash", str(installer)],
                env={
                    **environment,
                    "VANE_TOOL_CACHE": str(bad_cache),
                    "VANE_WORK_ROOT": str(bad_work),
                },
                capture_output=True, text=True,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("install parent is unsafe", rejected.stderr)
            self.assertFalse((outside / "4.115.0").exists())

    def test_installer_rejects_symlinked_install_parent(self) -> None:
        source = (
            ROOT / "ops/bootstrap/install-wrangler.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("[[ -d $install_parent && ! -L $install_parent ]]", source)


if __name__ == "__main__":
    unittest.main()
