from __future__ import annotations

import json
import os
import argparse
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from ops.cli import controller
from ops.audit import full_gate


ROOT = Path(__file__).resolve().parents[2]


class FullGatePolicyTest(unittest.TestCase):
    def run_fake(self, stdout: str, returncode: int = 0) -> None:
        completed = subprocess.CompletedProcess(
            args=["go", "test"], returncode=returncode, stdout=stdout, stderr=""
        )
        with mock.patch.object(controller.subprocess, "run", return_value=completed):
            controller.run_go_tests_no_skips(["go", "test", "-json"], cwd=ROOT)

    def test_terminal_test_skip_is_rejected(self) -> None:
        event = {"Action": "skip", "Package": "example/pkg", "Test": "TestHidden"}
        with self.assertRaisesRegex(controller.PolicyError, "TestHidden"):
            self.run_fake(json.dumps(event) + "\n")

    def test_package_level_skip_is_rejected(self) -> None:
        event = {"Action": "skip", "Package": "example/pkg"}
        with self.assertRaisesRegex(controller.PolicyError, "package-level"):
            self.run_fake(json.dumps(event) + "\n")

    def test_nonzero_exit_is_reported_before_malformed_json(self) -> None:
        with self.assertRaisesRegex(controller.PolicyError, "exit 17") as raised:
            self.run_fake("not-json\n", returncode=17)
        self.assertNotIn("invalid JSON", str(raised.exception))

    def test_full_calls_static_scanner_before_dynamic_go_gate(self) -> None:
        source = (ROOT / "ops/cli/controller.py").read_text(encoding="utf-8")
        body = source[source.index("def command_full"):source.index("def default_manifest")]
        self.assertLess(
            body.index("check-go-skips.sh"), body.index("ops.audit.full_gate")
        )

    def test_full_allows_clean_exact_non_main_rehearsal(self) -> None:
        revision = "b" * 40
        clean = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with mock.patch.object(controller, "git_revision", return_value=revision), \
             mock.patch.object(controller.subprocess, "run", return_value=clean), \
             mock.patch.object(controller, "run_checked") as checked:
            self.assertEqual(controller.command_full(argparse.Namespace(sha=revision)), 0)
        self.assertGreaterEqual(checked.call_count, 4)

    def test_full_rejects_wrong_or_dirty_exact_checkout(self) -> None:
        with mock.patch.object(controller, "git_revision", return_value="c" * 40):
            with self.assertRaisesRegex(controller.PolicyError, "differs"):
                controller.command_full(argparse.Namespace(sha="d" * 40))
        dirty = subprocess.CompletedProcess([], 0, stdout=" M server/main.go\n", stderr="")
        with mock.patch.object(controller, "git_revision", return_value="e" * 40), \
             mock.patch.object(controller.subprocess, "run", return_value=dirty):
            with self.assertRaisesRegex(controller.PolicyError, "clean"):
                controller.command_full(argparse.Namespace(sha="e" * 40))

    def test_full_gate_names_every_required_integration_and_cleanup(self) -> None:
        source = (ROOT / "ops/audit/full_gate.py").read_text(encoding="utf-8")
        for required in (
            "storetestshard",
            "-race",
            "merge-coverage",
            '"tool", "cover"',
            "server merged coverage is missing or zero",
            "server_coverage.py",
            "server-coverage-baseline.json",
            "govuln",
            "agenttoolinventory",
            "TestRetentionClockEvidenceRealTemporalHistory",
            "TestPeriodicWorkflowExternalTerminationReplaysAndRecoveryConverges",
            "server\", \"start-dev",
            "TestCanonicalTemporalServerPostgreSQLRoundTrip",
            "server-binaries.json",
            "check-go-build-info.sh",
            "audit-level=high",
            "require_native_postgres",
            "pg_ctl",
            "createdb",
            'runtime / f"postgres-{index}"',
            'f"postgres-{index}.log"',
            '"GOWORK": "off"',
            '"VANE_FULL_GATE": "1"',
            '"VANE_REQUIRE_CLEAN_RELEASE": "1"',
            "VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST",
            "assert_disposable_database",
            "test:coverage",
            '"VANE_COVERAGE_HEAD_SHA": head',
            '"VANE_COVERAGE_BASE_SHA": coverage_base',
            "legacy/vane-web/final",
            "prototype:p0a:build",
            "vane-release.json",
        ):
            self.assertIn(required, source)

    def test_destructive_role_test_rejects_production_looking_database(self) -> None:
        with self.assertRaisesRegex(controller.PolicyError, "proven disposable"):
            full_gate.assert_disposable_database(
                data_dir=Path("/var/lib/postgresql/18/main"),
                database_url="postgres://vane@db.production.internal:5432/vane",
                owner_root=Path("/tmp/vane-full-fixture"),
                expected_database="vane_full_0",
            )
        self.assertNotEqual(
            os.environ.get("VANE_RUN_DESTRUCTIVE_MIGRATION101_ROLE_TEST"), "1"
        )

    def test_destructive_role_test_accepts_bound_native_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "postgres"
            data.mkdir()
            (data / "PG_VERSION").write_text("18\n", encoding="ascii")
            full_gate.assert_disposable_database(
                data_dir=data,
                database_url=(
                    "postgres://vane@127.0.0.1:49152/"
                    "vane_full_0?sslmode=disable"
                ),
                owner_root=root,
                expected_database="vane_full_0",
            )

    def test_native_postgres_discovers_keg_only_homebrew_install(self) -> None:
        fake = Path("/opt/homebrew/opt/postgresql@18/bin")
        with mock.patch.object(full_gate.shutil, "which", return_value=None), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch.object(full_gate.os, "access", return_value=True), \
             mock.patch.object(full_gate, "output", return_value="postgres (PostgreSQL) 18.4"):
            binaries = full_gate.require_native_postgres()
        self.assertEqual(binaries["postgres"], fake / "postgres")

    def test_verified_artifact_tree_digest_detects_post_gate_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            artifact = root / "vane"
            artifact.write_bytes(b"verified")
            artifact.chmod(0o755)
            before = controller.directory_tree_sha256(root)
            artifact.write_bytes(b"substituted")
            self.assertNotEqual(controller.directory_tree_sha256(root), before)
            link = root / "escape"
            link.symlink_to(artifact)
            with self.assertRaisesRegex(controller.PolicyError, "unsafe member"):
                controller.directory_tree_sha256(root)


if __name__ == "__main__":
    unittest.main()
