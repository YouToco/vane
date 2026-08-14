import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]


class WranglerLockTest(unittest.TestCase):
    def test_complete_registry_tree_is_integrity_locked(self) -> None:
        package = json.loads(
            (ROOT / "tools/wrangler/package.json").read_text(encoding="utf-8")
        )
        lock = json.loads(
            (ROOT / "tools/wrangler/package-lock.json").read_text(encoding="utf-8")
        )
        self.assertEqual(package["dependencies"], {"wrangler": "4.115.0"})
        self.assertEqual(package["overrides"], {"undici": "7.29.0"})
        self.assertEqual(lock["lockfileVersion"], 3)
        self.assertEqual(
            lock["packages"]["node_modules/wrangler"]["version"], "4.115.0"
        )
        self.assertEqual(
            lock["packages"]["node_modules/undici"]["version"], "7.29.0"
        )
        for path, entry in lock["packages"].items():
            if not path:
                continue
            resolved = entry.get("resolved", "")
            self.assertTrue(
                resolved.startswith("https://registry.npmjs.org/"),
                f"{path} has non-registry source {resolved!r}",
            )
            self.assertRegex(entry.get("integrity", ""), r"^sha512-[A-Za-z0-9+/]+=*$")

    def test_deploy_materializes_wrangler_without_runner_preinstall(self) -> None:
        workflow = (ROOT / ".github/workflows/deploy.yml").read_text(
            encoding="utf-8"
        )
        installer = (ROOT / "scripts/install-wrangler.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("Set up Node for pinned deployment tools", workflow)
        self.assertIn("scripts/install-wrangler.sh", workflow)
        self.assertNotIn("/opt/vane-deploy-tools/wrangler", workflow)
        self.assertIn('npm_bin" ci --ignore-scripts --no-audit --no-fund', installer)
        self.assertRegex(installer, re.escape('== "v22.23.1"'))
        self.assertIn("wrangler_version=4.115.0", installer)


if __name__ == "__main__":
    unittest.main()
