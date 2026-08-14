from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "locked_tool", ROOT / "tools/install/locked_tool.py"
)
assert SPEC and SPEC.loader
locked_tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(locked_tool)


class LockedToolAcquisitionTest(unittest.TestCase):
    def test_cached_archive_checksum_mismatch_fails_before_network(self) -> None:
        lock = json.loads((ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tempdir:
            cache = Path(tempdir)
            artifact = lock["tools"]["go"]["artifacts"][locked_tool.arch()]
            downloads = cache / "downloads"
            downloads.mkdir()
            (downloads / artifact["filename"]).write_bytes(b"tampered")
            with mock.patch.object(locked_tool.urllib.request, "urlopen") as network:
                with self.assertRaisesRegex(RuntimeError, "cached artifact checksum mismatch"):
                    locked_tool.install("go", ROOT / "tools/toolchain.lock.json", cache)
                network.assert_not_called()

    def test_govuln_wrong_module_sum_is_rejected_before_install(self) -> None:
        lock_path = ROOT / "tools/toolchain.lock.json"
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tempdir:
            cache = Path(tempdir)
            go = cache / "go" / lock["tools"]["go"]["version"] / "bin/go"
            go.parent.mkdir(parents=True)
            go.write_text("fixture", encoding="utf-8")
            metadata = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=json.dumps(
                    {
                        "Sum": "h1:wrong",
                        "Origin": {"Hash": lock["tools"]["govulncheck"]["source_commit"]},
                    }
                ),
                stderr="",
            )
            with mock.patch.object(locked_tool.subprocess, "run", return_value=metadata) as run:
                with self.assertRaisesRegex(RuntimeError, "checksum or source commit mismatch"):
                    locked_tool.install("govulncheck", lock_path, cache)
                self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
