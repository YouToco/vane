import json
from pathlib import Path
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
        self.assertEqual(lock["lockfileVersion"], 3)
        self.assertEqual(
            lock["packages"]["node_modules/wrangler"]["version"], "4.115.0"
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


if __name__ == "__main__":
    unittest.main()
