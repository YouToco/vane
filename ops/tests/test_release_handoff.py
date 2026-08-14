from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


OPS = Path(__file__).resolve().parents[1]
ROOT = OPS.parent
CLI = OPS / "bin" / "vane"
REVISION = "0123456789abcdef0123456789abcdef01234567"


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ReleaseManifestChainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)

    def write_stage(self, stage: str, parent: Path | None) -> Path:
        path = self.root / f"{stage}.json"
        parent_value = None
        if parent is not None:
            parent_value = {
                "path": parent.name,
                "sha256": digest(parent),
                "stage": json.loads(parent.read_text(encoding="utf-8"))["stage"],
            }
        value = {
            "schema": "vane.ops-manifest/v1",
            "stage": stage,
            "revision": REVISION,
            "created_at": "2026-08-13T12:00:00Z",
            "parent": parent_value,
            "signer": "test-validator",
            "evidence": [{"name": f"{stage}-result", "sha256": "a" * 64}],
        }
        path.write_bytes(canonical(value))
        return path

    def run_audit(self, path: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(CLI), "audit", "--structural-only", "--manifest", str(path)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_complete_plan_gate_artifact_chain_is_accepted_structurally(self) -> None:
        plan = self.write_stage("plan", None)
        gate = self.write_stage("gate", plan)
        artifact = self.write_stage("artifact", gate)
        result = self.run_audit(artifact)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["stage"], "artifact")
        self.assertEqual(len(report["chain"]), 3)
        self.assertFalse(report["signatures_verified"])

    def test_parent_digest_mismatch_fails_closed(self) -> None:
        plan = self.write_stage("plan", None)
        gate = self.write_stage("gate", plan)
        plan.write_text("{}\n", encoding="utf-8")
        result = self.run_audit(gate)
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("parent digest mismatch", result.stderr)

    def test_skipped_stage_is_rejected(self) -> None:
        plan = self.write_stage("plan", None)
        artifact = self.write_stage("artifact", plan)
        result = self.run_audit(artifact)
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("skips or reverses", result.stderr)

    def test_unsigned_chain_is_rejected_by_non_structural_audit(self) -> None:
        plan = self.write_stage("plan", None)
        result = subprocess.run(
            [str(CLI), "audit", "--manifest", str(plan)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("signature is missing", result.stderr)


if __name__ == "__main__":
    unittest.main()
