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

    def test_go_test_gate_uses_explicit_environment(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["go", "test"], returncode=0,
            stdout=json.dumps({"Action": "pass", "Package": "example/pkg"}) + "\n",
            stderr="",
        )
        isolated = {"VANE_FULL_GATE": "1"}
        with mock.patch.object(
            controller.subprocess, "run", return_value=completed
        ) as invoked:
            controller.run_go_tests_no_skips(
                ["go", "test", "-json"], cwd=ROOT, env=isolated
            )
        self.assertEqual(invoked.call_args.kwargs["env"], isolated)

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

    def test_full_ignores_caller_timing_cache_override_and_restores_it(self) -> None:
        revision = "b" * 40
        clean = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        observed: list[tuple[list[str], str | None]] = []

        def capture_environment(command: list[str], **_kwargs: object) -> None:
            observed.append((command, os.environ.get("VANE_GATE_CACHE_ROOT")))

        with mock.patch.dict(
            os.environ, {"VANE_GATE_CACHE_ROOT": "/tmp/candidate-cache"}
        ), mock.patch.object(controller, "git_revision", return_value=revision), \
             mock.patch.object(controller.subprocess, "run", return_value=clean), \
             mock.patch.object(controller, "run_checked", side_effect=capture_environment):
            self.assertEqual(controller.command_full(argparse.Namespace(sha=revision)), 0)
            full_gate_call = next(
                value for command, value in observed
                if "ops.audit.full_gate" in command
            )
            self.assertEqual(full_gate_call, str(controller.GATE_CACHE_ROOT))
            self.assertEqual(
                os.environ["VANE_GATE_CACHE_ROOT"], "/tmp/candidate-cache"
            )

    def test_full_runs_cheap_policy_gates_before_expensive_product_gate(self) -> None:
        revision = "b" * 40
        clean = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with mock.patch.object(controller, "git_revision", return_value=revision), \
             mock.patch.object(controller.subprocess, "run", return_value=clean), \
             mock.patch.object(controller, "run_checked") as checked:
            self.assertEqual(controller.command_full(argparse.Namespace(sha=revision)), 0)
        commands = [call.args[0] for call in checked.call_args_list]
        scanner_index = next(
            index for index, command in enumerate(commands)
            if "check-go-skips.sh" in command[0]
        )
        contract_index = next(
            index for index, command in enumerate(commands)
            if "tests/contract" in command
        )
        ops_index = next(
            index for index, command in enumerate(commands)
            if "ops/tests" in command
        )
        product_index = next(
            index for index, command in enumerate(commands)
            if "ops.audit.full_gate" in command
        )
        self.assertLess(scanner_index, contract_index)
        self.assertLess(contract_index, ops_index)
        self.assertLess(ops_index, product_index)

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

    def test_non_store_database_inventory_matches_repository(self) -> None:
        full_gate.validate_database_package_inventory(
            server_root=ROOT / "server",
            declared=full_gate.NON_STORE_DATABASE_PACKAGE_DIRS,
        )

    def test_non_store_database_inventory_rejects_unregistered_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            server = Path(temporary)
            package = server / "newdomain"
            package.mkdir()
            (package / "database_test.go").write_text(
                'package newdomain\n// DATABASE_URL is required.\n', encoding="utf-8"
            )
            with self.assertRaisesRegex(controller.PolicyError, "newdomain"):
                full_gate.validate_database_package_inventory(
                    server_root=server, declared=()
                )

    def test_non_store_packages_are_partitioned_without_overlap(self) -> None:
        module = "github.com/YouToco/vane/server"
        database = [
            f"{module}/{relative}"
            for relative in full_gate.NON_STORE_DATABASE_PACKAGE_DIRS
        ]
        pure, detected_database = full_gate.partition_non_store_packages(
            packages=[module, *database], module_path=module
        )
        self.assertEqual(pure, [module])
        self.assertEqual(detected_database, database)
        self.assertFalse(set(pure) & set(detected_database))

    def test_non_store_partition_rejects_stale_declared_package(self) -> None:
        with self.assertRaisesRegex(controller.PolicyError, "missing"):
            full_gate.partition_non_store_packages(
                packages=["github.com/YouToco/vane/server"],
                module_path="github.com/YouToco/vane/server",
            )

    def test_database_packages_are_assigned_once_across_isolated_lanes(self) -> None:
        packages = [f"example/package-{index}" for index in range(9)]
        lanes = full_gate.database_package_lanes(packages, lane_count=3)
        self.assertEqual([len(lane) for lane in lanes], [3, 3, 3])
        flattened = sorted(item for lane in lanes for item in lane)
        self.assertEqual(flattened, list(enumerate(packages)))
        self.assertEqual(
            [[index for index, _ in lane] for lane in lanes],
            [[0, 3, 6], [1, 4, 7], [2, 5, 8]],
        )

    def test_database_lane_count_is_bounded(self) -> None:
        for lane_count in (0, 3):
            with self.subTest(lane_count=lane_count):
                with self.assertRaisesRegex(controller.PolicyError, "lane count"):
                    full_gate.database_package_lanes(
                        ["example/a", "example/b"], lane_count=lane_count
                    )

    def test_store_timing_cache_prefers_exact_head_then_base(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cache = Path(temporary) / "gate-cache"
            head = "a" * 40
            base = "b" * 40
            for revision, payload in ((base, b"base\n"), (head, b"head\n")):
                source = Path(temporary) / f"{revision}.jsonl"
                source.write_bytes(payload)
                full_gate.publish_store_timing_seed(
                    cache_root=cache, revision=revision, source=source
                )
            selected = full_gate.select_store_timing_seed(
                cache_root=cache, revisions=(head, base)
            )
            self.assertIsNotNone(selected)
            assert selected is not None
            self.assertEqual(selected[1], head)
            self.assertEqual(
                (selected[0] / full_gate.STORE_TIMING_SEED_NAME).read_bytes(),
                b"head\n",
            )

    def test_store_timing_cache_rejects_writable_or_linked_seed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cache = Path(temporary) / "gate-cache"
            revision = "c" * 40
            source = Path(temporary) / "source.jsonl"
            source.write_bytes(b"seed\n")
            full_gate.publish_store_timing_seed(
                cache_root=cache, revision=revision, source=source
            )
            seed = (
                cache / full_gate.STORE_TIMING_CACHE_VERSION / revision
                / full_gate.STORE_TIMING_SEED_NAME
            )
            seed.chmod(0o622)
            with self.assertRaisesRegex(controller.PolicyError, "unsafe"):
                full_gate.select_store_timing_seed(
                    cache_root=cache, revisions=(revision,)
                )
            seed.unlink()
            seed.symlink_to(source)
            with self.assertRaisesRegex(controller.PolicyError, "unsafe"):
                full_gate.select_store_timing_seed(
                    cache_root=cache, revisions=(revision,)
                )

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
        self.assertEqual(binaries["psql"], fake / "psql")

    def test_migration_delta_accepts_only_forward_additions(self) -> None:
        base = {
            "130_retained.sql": "a" * 40,
            "131_projection.sql": "b" * 40,
        }
        current = {**base, "132_fence.sql": "c" * 40}
        self.assertEqual(
            full_gate.additive_migration_delta(base=base, current=current),
            {"132_fence.sql": "c" * 40},
        )

    def test_migration_delta_rejects_mutation_deletion_and_backfill(self) -> None:
        base = {
            "130_retained.sql": "a" * 40,
            "131_projection.sql": "b" * 40,
        }
        cases = (
            {"130_retained.sql": "c" * 40, "131_projection.sql": "b" * 40},
            {"131_projection.sql": "b" * 40},
            {**base, "129_late_backfill.sql": "d" * 40},
        )
        for current in cases:
            with self.subTest(current=current), self.assertRaises(controller.PolicyError):
                full_gate.additive_migration_delta(base=base, current=current)

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

    def test_rollback_proof_is_exact_and_digest_bound(self) -> None:
        revision = "a" * 40
        base = "b" * 40
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            gate_evidence = root / "full-gate.json"
            gate_evidence.write_text("{}\n", encoding="utf-8")
            proof = root / "proof.json"
            value = {
                "schema": "vane.server-rollback-compatibility/v1",
                "base_revision": base,
                "current_revision": revision,
                "mode": "previous-binary-on-upgraded-schema",
                "added_migrations": [{
                    "path": "132_fence.sql",
                    "git_blob": "c" * 40,
                    "sha256": "d" * 64,
                }],
                "previous_gate_sha256": "e" * 64,
                "previous_gate_output_sha256": "f" * 64,
                "status": "passed",
            }
            proof.write_bytes(controller.canonical_json(value))
            gate = {
                "server_rollback_safe": True,
                "rollback_base_revision": base,
                "rollback_compatibility_path": proof.name,
                "rollback_compatibility_sha256": controller.sha256_file(proof),
            }
            self.assertEqual(
                controller.validate_rollback_compatibility_proof(
                    gate=gate, gate_evidence=gate_evidence, revision=revision
                ),
                proof,
            )
            proof.write_bytes(controller.canonical_json({**value, "status": "failed"}))
            with self.assertRaisesRegex(controller.PolicyError, "changed"):
                controller.validate_rollback_compatibility_proof(
                    gate=gate, gate_evidence=gate_evidence, revision=revision
                )

    def test_identical_migration_proof_rejects_fake_gate_digests(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            gate_evidence = root / "full-gate.json"
            gate_evidence.write_text("{}\n", encoding="utf-8")
            proof = root / "proof.json"
            value = {
                "schema": "vane.server-rollback-compatibility/v1",
                "base_revision": "b" * 40,
                "current_revision": revision,
                "mode": "identical-migration-history",
                "added_migrations": [],
                "previous_gate_sha256": "e" * 64,
                "previous_gate_output_sha256": None,
                "status": "passed",
            }
            proof.write_bytes(controller.canonical_json(value))
            gate = {
                "server_rollback_safe": True,
                "rollback_base_revision": "b" * 40,
                "rollback_compatibility_path": proof.name,
                "rollback_compatibility_sha256": controller.sha256_file(proof),
            }
            with self.assertRaisesRegex(controller.PolicyError, "contradictory"):
                controller.validate_rollback_compatibility_proof(
                    gate=gate, gate_evidence=gate_evidence, revision=revision
                )


if __name__ == "__main__":
    unittest.main()
