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
CURRENT_REVISION = "1" * 40
CANDIDATE_REVISION = "2" * 40
CONTROL_REVISION = "3" * 40


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ReleaseStateContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receipt = self.root / "release-receipt.json"
        self.receipt_value = {
            "schema_version": "vane.release-receipt/v1",
            "source_revision": CANDIDATE_REVISION,
            "control_plane_revision": CONTROL_REVISION,
            "deploy_run_id": "12345",
            "build_run_attempt": 1,
            "backend_archive_sha256": "a" * 64,
            "backend_manifest_sha256": "b" * 64,
            "server_release_contract_sha256": "c" * 64,
            "vane_sha256": "d" * 64,
            "agentfirstretention_sha256": "e" * 64,
        }
        self.receipt.write_bytes(canonical(self.receipt_value))
        self.current = self.root / "current-release.json"
        self.current.write_bytes(canonical(self.release_state(CURRENT_REVISION, "4")))
        self.candidate = self.root / "candidate-release.json"
        candidate = self.release_state(CANDIDATE_REVISION, "5")
        candidate["controller_revision"] = CONTROL_REVISION
        candidate["server"]["artifact_digest"] = "a" * 64
        self.candidate.write_bytes(canonical(candidate))
        self.manifests: dict[str, Path] = {}
        parent: Path | None = None
        for stage in ("plan", "gate", "artifact", "deploy", "verify", "finalize"):
            parent_value = None
            if parent is not None:
                parent_value = {
                    "path": parent.name,
                    "sha256": sha256(parent),
                    "stage": json.loads(parent.read_text(encoding="utf-8"))["stage"],
                }
            evidence = [{"name": f"{stage}-result", "sha256": stage_digest(stage)}]
            if stage == "artifact":
                evidence.append(
                    {"name": "release-receipt.json", "sha256": sha256(self.receipt)}
                )
            manifest = {
                "schema": "vane.ops-manifest/v1",
                "stage": stage,
                "revision": CANDIDATE_REVISION,
                "created_at": "2026-08-13T12:00:00Z",
                "parent": parent_value,
                "signer": "fixture-validator",
                "evidence": evidence,
            }
            path = self.root / f"{stage}.json"
            path.write_bytes(canonical(manifest))
            self.manifests[stage] = path
            parent = path

    @staticmethod
    def release_state(revision: str, fill: str) -> dict:
        return {
            "schema": "vane.current-release/v2",
            "monorepo_revision": revision,
            "server": {
                "tree_digest": fill * 64,
                "artifact_digest": fill * 64,
                "deployed_revision": revision,
            },
            "infra_manifest_digest": fill * 64,
            "controller_revision": revision,
        }

    def run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(CLI), *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def activation_args(self, stage: str) -> tuple[str, ...]:
        return (
            "audit",
            "--structural-only",
            "--manifest",
            str(self.manifests[stage]),
            "--current-release",
            str(self.current),
            "--candidate-release",
            str(self.candidate),
            "--expected-current-digest",
            sha256(self.current),
            "--release-receipt",
            str(self.receipt),
        )

    def test_strict_receipt_consumer_accepts_exact_ten_fields(self) -> None:
        result = self.run_cli(
            "status", "--release-receipt", str(self.receipt), "--sha", CANDIDATE_REVISION
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["sha256"], sha256(self.receipt))

    def test_receipt_rejects_unknown_null_and_boolean_integer(self) -> None:
        cases = []
        unknown = dict(self.receipt_value, unexpected="x")
        cases.append(unknown)
        null_digest = dict(self.receipt_value, vane_sha256=None)
        cases.append(null_digest)
        boolean_attempt = dict(self.receipt_value, build_run_attempt=True)
        cases.append(boolean_attempt)
        for index, value in enumerate(cases):
            with self.subTest(index=index):
                path = self.root / f"bad-receipt-{index}.json"
                path.write_bytes(canonical(value))
                result = self.run_cli("status", "--release-receipt", str(path))
                self.assertEqual(result.returncode, 78, result)

    def test_receipt_rejects_duplicate_keys(self) -> None:
        raw = self.receipt.read_text(encoding="utf-8")
        duplicate = self.root / "duplicate-receipt.json"
        duplicate.write_text('{"schema_version":"duplicate",' + raw[1:], encoding="utf-8")
        result = self.run_cli("status", "--release-receipt", str(duplicate))
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("duplicate JSON key", result.stderr)

    def test_current_release_rejects_unknown_and_null(self) -> None:
        for name, mutate in (
            ("unknown", lambda value: value.update({"unexpected": "x"})),
            ("null", lambda value: value["server"].update({"tree_digest": None})),
        ):
            with self.subTest(name=name):
                value = self.release_state(CURRENT_REVISION, "4")
                mutate(value)
                path = self.root / f"bad-current-{name}.json"
                path.write_bytes(canonical(value))
                result = self.run_cli("status", "--current-release", str(path))
                self.assertEqual(result.returncode, 78, result)

    def test_current_release_cas_mismatch_fails_closed(self) -> None:
        args = list(self.activation_args("finalize"))
        digest_index = args.index("--expected-current-digest") + 1
        args[digest_index] = "f" * 64
        result = self.run_cli(*args)
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("CAS mismatch", result.stderr)

    def test_n_plus_one_activation_is_delayed_until_finalize(self) -> None:
        premature = self.run_cli(*self.activation_args("artifact"))
        self.assertEqual(premature.returncode, 78, premature)
        self.assertIn("requires the finalized N+1 chain", premature.stderr)

        finalized = self.run_cli(*self.activation_args("finalize"))
        self.assertEqual(finalized.returncode, 0, finalized.stderr)
        report = json.loads(finalized.stdout)
        self.assertEqual(report["activation"]["current_revision"], CURRENT_REVISION)
        self.assertEqual(report["activation"]["candidate_revision"], CANDIDATE_REVISION)


def stage_digest(stage: str) -> str:
    return hashlib.sha256(stage.encode()).hexdigest()


if __name__ == "__main__":
    unittest.main()
