from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


OPS = Path(__file__).resolve().parents[1]
BROKER = OPS / "broker" / "forced_command.py"


class ForcedCommandBrokerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.requests = self.root / "requests"
        self.state = self.root / "state"
        self.repo = self.root / "repo"
        self.requests.mkdir()
        self.state.mkdir()
        (self.repo / "ops/bin").mkdir(parents=True)
        cli = self.repo / "ops/bin/vane"
        cli.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        cli.chmod(0o755)
        self.current = self.state / "current-release.json"
        self.current.write_text('{"fixture":"N"}\n', encoding="utf-8")

    def run_broker(self, command: str, request: dict) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "SSH_ORIGINAL_COMMAND": command,
            "VANE_BROKER_REQUEST_ROOT": str(self.requests),
            "VANE_BROKER_STATE_ROOT": str(self.state),
            "VANE_BROKER_REPO_ROOT": str(self.repo),
        }
        return subprocess.run(
            [str(BROKER)],
            input=json.dumps(request),
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )

    def test_shell_and_unknown_commands_are_rejected(self) -> None:
        for command in ("", "sh", "vane-broker release; id", "vane-broker unknown"):
            with self.subTest(command=command):
                result = self.run_broker(command, {})
                self.assertEqual(result.returncode, 78, result)
                self.assertIn("not allowlisted", result.stderr)

    def test_release_is_locked_and_cas_checked_but_mutation_is_disabled(self) -> None:
        for name in ("finalize.json", "current.json", "candidate.json", "receipt.json"):
            (self.requests / name).write_text("{}\n", encoding="utf-8")
        digest = hashlib.sha256(self.current.read_bytes()).hexdigest()
        request = {
            "manifest": "finalize.json",
            "current_release": "current.json",
            "candidate_release": "candidate.json",
            "release_receipt": "receipt.json",
            "expected_current_digest": digest,
        }
        # The request copy must bind the same CAS state as the broker authority.
        (self.requests / "current.json").write_bytes(self.current.read_bytes())
        result = self.run_broker("vane-broker release", request)
        self.assertEqual(result.returncode, 78, result)
        report = json.loads(result.stdout)
        self.assertTrue(report["admitted"])
        self.assertEqual(report["mutation"], "not-installed")
        self.assertTrue((self.state / "release.lock").is_file())
        self.assertEqual(self.current.read_text(encoding="utf-8"), '{"fixture":"N"}\n')

    def test_stale_cas_fails_before_admission(self) -> None:
        for name in ("finalize.json", "current.json", "candidate.json", "receipt.json"):
            (self.requests / name).write_text("{}\n", encoding="utf-8")
        result = self.run_broker(
            "vane-broker release",
            {
                "manifest": "finalize.json",
                "current_release": "current.json",
                "candidate_release": "candidate.json",
                "release_receipt": "receipt.json",
                "expected_current_digest": "f" * 64,
            },
        )
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("CAS mismatch", result.stderr)

    def test_legacy_import_cannot_return(self) -> None:
        self.assertFalse((OPS / "legacy-import").exists())


if __name__ == "__main__":
    unittest.main()
