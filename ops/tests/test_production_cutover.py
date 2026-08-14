from __future__ import annotations

import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

from ops.bootstrap import production_cutover
from ops.cli.controller import validate_current_release


ROOT = Path(__file__).resolve().parents[2]


class ProductionCutoverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.layout = production_cutover.Layout(self.root)
        self.baseline = copy.deepcopy(
            production_cutover.validate_baseline(
                production_cutover.strict_json(production_cutover.BASELINE_PATH)
            )
        )
        for section in ("binaries", "infra"):
            for name, entry in self.baseline[section].items():
                path = self.layout.path(entry["source"])
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = f"{section}:{name}\n".encode()
                path.write_bytes(payload)
                path.chmod(0o755 if section == "binaries" else 0o644)
                entry["sha256"] = hashlib.sha256(payload).hexdigest()
        receipt = {
            "schema_version": "vane.release-receipt/v1",
            "source_revision": self.baseline["server_revision"],
            "control_plane_revision": "cf9dcb997beb3091704b9a70f8294c76221d04a2",
            "deploy_run_id": self.baseline["deploy_run_id"],
            "build_run_attempt": 3,
            "backend_archive_sha256": self.baseline["backend_archive_sha256"],
            "backend_manifest_sha256": "1" * 64,
            "server_release_contract_sha256": "2" * 64,
            "vane_sha256": self.baseline["binaries"]["vane"]["sha256"],
            "agentfirstretention_sha256": self.baseline["binaries"]["agentfirstretention"]["sha256"],
        }
        receipt_path = self.layout.path(self.baseline["receipt"]["source"])
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_bytes(production_cutover.canonical(receipt))
        self.baseline["receipt"]["sha256"] = production_cutover.sha256(receipt_path)
        for name, revision in (
            ("deployed-vane.sha", self.baseline["server_revision"]),
            ("deployed-vane-web.sha", self.baseline["web_revision"]),
        ):
            path = self.layout.path(
                f"/var/lib/vane-deploy-runner/.local/state/vane-deploy/{name}"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(revision + "\n", encoding="ascii")

    def test_committed_baseline_binds_exact_legacy_git_archive(self) -> None:
        baseline = production_cutover.validate_baseline(
            production_cutover.strict_json(production_cutover.BASELINE_PATH)
        )
        archive = subprocess.run(
            ["git", "archive", "--format=tar", baseline["server_revision"]],
            cwd=ROOT,
            capture_output=True,
            check=True,
        ).stdout
        self.assertEqual(
            hashlib.sha256(archive).hexdigest(),
            baseline["server_tree_archive_sha256"],
        )

    def test_file_inventory_detects_any_legacy_byte_drift(self) -> None:
        production_cutover.verify_file_inventory(self.baseline, self.layout)
        path = self.layout.path(self.baseline["binaries"]["gate"]["source"])
        path.write_bytes(b"changed\n")
        with self.assertRaisesRegex(production_cutover.BootstrapError, "binaries.*gate"):
            production_cutover.verify_file_inventory(self.baseline, self.layout)

    def test_legacy_release_is_complete_traversable_and_manifest_bound(self) -> None:
        target, infra_digest = production_cutover.build_legacy_release(
            self.baseline, self.layout
        )
        self.assertEqual(target.stat().st_mode & 0o777, 0o755)
        self.assertEqual((target / "bin").stat().st_mode & 0o777, 0o755)
        self.assertEqual(
            {path.name for path in (target / "bin").iterdir()},
            set(self.baseline["binaries"]),
        )
        self.assertEqual(
            infra_digest,
            production_cutover.sha256(target / "infra-manifest.sha256"),
        )
        bound = (target / "infra-bound-files.sha256").read_text(encoding="ascii")
        for name in self.baseline["binaries"]:
            self.assertIn(f"  bin/{name}\n", bound)
        self.assertNotIn(str(self.root), bound)

    def test_initial_current_release_is_strict_and_broker_readable(self) -> None:
        target, infra_digest = production_cutover.build_legacy_release(
            self.baseline, self.layout
        )
        del target
        (self.layout.path("/var/lib/vane-broker/state/broker-work")).mkdir(
            parents=True
        )
        current = production_cutover.write_current_release(
            baseline=self.baseline,
            controller_revision="0" * 40,
            infra_digest=infra_digest,
            layout=self.layout,
            broker_gid=os.getgid(),
        )
        value = validate_current_release(current)
        self.assertEqual(value["server"]["deployed_revision"], self.baseline["server_revision"])
        self.assertEqual(value["controller_revision"], "0" * 40)
        self.assertEqual(current.stat().st_mode & 0o777, 0o640)

    def test_production_config_uses_ephemeral_uat_identity(self) -> None:
        config = production_cutover.write_production_config(self.baseline, self.layout)
        value = json.loads(config.read_text(encoding="utf-8"))
        self.assertEqual(value["uat_identity"], self.baseline["uat_identity"])
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        self.assertNotIn("uat_session_cookie", config.read_text(encoding="utf-8"))

    def test_plan_rejects_unknown_keys_and_baseline_rebinding(self) -> None:
        digest = production_cutover.sha256(production_cutover.BASELINE_PATH)
        plan = {
            "schema": production_cutover.PLAN_SCHEMA,
            "controller_revision": "0" * 40,
            "controller_archive_sha256": "1" * 64,
            "baseline_sha256": digest,
            "release_signer": "vane-release-local",
            "broker_signer": "vane-production-broker",
            "broker_signer_public_key": "ssh-ed25519 AAAA",
            "transport_public_key": "ssh-ed25519 AAAA",
        }
        production_cutover.validate_plan(plan, digest)
        plan["extra"] = True
        with self.assertRaisesRegex(production_cutover.BootstrapError, "keys"):
            production_cutover.validate_plan(plan, digest)
        plan.pop("extra")
        plan["baseline_sha256"] = "f" * 64
        with self.assertRaisesRegex(production_cutover.BootstrapError, "different"):
            production_cutover.validate_plan(plan, digest)

    def test_broker_key_comparison_ignores_only_public_comment(self) -> None:
        private = self.layout.path(
            "/etc/vane-broker/credentials/broker_signing_key"
        )
        private.parent.mkdir(parents=True)
        private.write_text("private fixture\n", encoding="ascii")
        plan = {"broker_signer_public_key": "ssh-ed25519 AAAA"}
        with mock.patch.object(
            production_cutover,
            "command_output",
            return_value="ssh-ed25519 AAAA vane-production-broker",
        ):
            production_cutover.verify_broker_key(plan, self.layout)
        with mock.patch.object(
            production_cutover,
            "command_output",
            return_value="ssh-ed25519 BBBB vane-production-broker",
        ), self.assertRaisesRegex(production_cutover.BootstrapError, "differs"):
            production_cutover.verify_broker_key(plan, self.layout)

    def test_live_baseline_refuses_preexisting_new_authority(self) -> None:
        production_cutover.verify_live_baseline(self.baseline, self.layout)
        current = self.layout.path("/opt/vane/current")
        current.parent.mkdir(parents=True, exist_ok=True)
        current.symlink_to("/tmp/untrusted")
        with self.assertRaisesRegex(production_cutover.BootstrapError, "already exists"):
            production_cutover.verify_live_baseline(self.baseline, self.layout)

    def test_apply_builds_both_authorities_without_persistent_uat_cookie(self) -> None:
        revision = "0" * 40
        archive = self.root / "controller.tar.gz"
        members = {
            "ops/bin/vane": b"#!/bin/sh\n",
            "ops/broker/forced_command.py": b"pass\n",
            "ops/broker/production_handler.py": b"pass\n",
            "ops/release/artifact.py": b"pass\n",
            "tools/toolchain.lock.json": b"{}\n",
            "server/go.mod": b"module example.invalid/control\n",
            "server/internal/testgate/cmd/testpolicyscan/main.go": b"package main\n",
        }
        with tarfile.open(archive, mode="w:gz") as bundle:
            for name, payload in members.items():
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                info.mode = 0o755 if name.endswith(("/vane", ".py")) else 0o644
                bundle.addfile(info, io.BytesIO(payload))
        plan = {
            "controller_revision": revision,
            "transport_public_key": "ssh-ed25519 AAAA",
        }
        with mock.patch.object(
            production_cutover,
            "audit",
            return_value=(plan, self.baseline),
        ):
            result = production_cutover.apply(
                self.root / "unused-plan.json", archive, self.layout
            )

        self.assertTrue(result["ok"])
        self.assertTrue(self.layout.path("/opt/vane/current").is_symlink())
        self.assertTrue(self.layout.path("/opt/vane-control/current").is_symlink())
        self.assertTrue(
            self.layout.path("/var/lib/vane-broker/state/current-release.json").is_file()
        )
        self.assertFalse(
            self.layout.path("/etc/vane/credentials/uat_session_cookie").exists()
        )


if __name__ == "__main__":
    unittest.main()
