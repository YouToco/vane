from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

from ops.bootstrap import controller_upgrade


OLD = "1" * 40
NEW = "2" * 40


class ControllerUpgradeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control = self.root / "control"
        self.releases = self.control / "releases"
        self.old = self.releases / OLD
        self.old.mkdir(parents=True)
        (self.control / "current").symlink_to(self.old)
        self.state = self.root / "current-release.json"
        self.state.write_bytes(controller_upgrade.canonical({
            "schema": "vane.current-release/v2",
            "monorepo_revision": OLD,
            "server": {
                "tree_digest": "3" * 64,
                "artifact_digest": "4" * 64,
                "deployed_revision": OLD,
            },
            "infra_manifest_digest": "5" * 64,
            "controller_revision": OLD,
        }))
        self.lock = self.root / "release.lock"
        self.lock.write_bytes(b"")
        self.evidence = self.root / "evidence"
        self.allowed = self.root / "allowed_signers"
        self.allowed.write_text("fixture\n", encoding="ascii")
        self.stable_allowed = self.root / "stable-allowed-signers"
        self.archive = self.root / "controller.tar.gz"
        self.make_archive()
        self.plan = self.root / "plan.json"
        self.plan.write_bytes(controller_upgrade.canonical({
            "schema": controller_upgrade.SCHEMA,
            "controller_revision": NEW,
            "controller_archive_sha256": controller_upgrade.sha256(self.archive),
            "expected_current_release_sha256": controller_upgrade.sha256(self.state),
            "expected_updated_current_release_sha256": hashlib.sha256(
                controller_upgrade.canonical({
                    **json.loads(self.state.read_text()),
                    "controller_revision": NEW,
                })
            ).hexdigest(),
            "expected_product_revision": OLD,
            "expected_controller_revision": OLD,
            "expected_active_controller_revision": OLD,
            "expected_allowed_signers_sha256": controller_upgrade.sha256(self.allowed),
            "release_signer": "vane-release-local",
            "transport_public_key": "ssh-ed25519 AAAA",
        }))
        self.plan.with_name("plan.json.sig").write_text("fixture\n", encoding="ascii")

    def make_archive(self) -> None:
        members = {
            "ops/bin/vane": b"#!/bin/sh\n",
            "ops/broker/forced_command.py": b"#!/usr/bin/env python3\n",
            "ops/broker/production_handler.py": b"#!/usr/bin/env python3\n",
            "ops/broker/promote_finalized_controller.py": b"#!/usr/bin/env python3\n",
            "ops/broker/run-production-handler.sh": b"#!/bin/sh\n",
            "ops/audit/production-uat.py": b"#!/usr/bin/env python3\n",
            "ops/audit/workspace-memory-runtime-uat.py": b"#!/usr/bin/env python3\n",
            "ops/release/artifact.py": b"#!/usr/bin/env python3\n",
            "ops/release/remote-atomic-release.sh": b"#!/bin/sh\n",
            "ops/rollback/switch-server-release.sh": b"#!/bin/sh\n",
            "tools/toolchain.lock.json": b"{}\n",
            "server/go.mod": b"module example.invalid/control\n",
            "server/internal/testgate/cmd/testpolicyscan/main.go": b"package main\n",
        }
        with tarfile.open(self.archive, "w:gz") as bundle:
            for name, payload in members.items():
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                info.mode = 0o755 if name.endswith(("/vane", ".py", ".sh")) else 0o644
                bundle.addfile(info, io.BytesIO(payload))

    def apply(self) -> dict:
        with mock.patch.object(controller_upgrade, "verify_signature"):
            return controller_upgrade.apply(
                plan_path=self.plan,
                archive=self.archive,
                state=self.state,
                control_root=self.control,
                evidence_root=self.evidence,
                lock_path=self.lock,
                current_allowed_signers=self.allowed,
                stable_allowed_signers=self.stable_allowed,
                testing=True,
            )

    def test_upgrade_changes_only_controller_authority_and_is_idempotent(self) -> None:
        original_server = json.loads(self.state.read_text())["server"]
        result = self.apply()
        current = json.loads(self.state.read_text())
        self.assertTrue(result["ok"])
        self.assertEqual(current["monorepo_revision"], OLD)
        self.assertEqual(current["server"], original_server)
        self.assertEqual(current["controller_revision"], NEW)
        self.assertEqual((self.control / "current").resolve(), (self.releases / NEW).resolve())
        evidence = json.loads((self.evidence / "controller-bootstrap" / f"{NEW}.json").read_text())
        self.assertEqual(evidence["product_revision"], OLD)
        self.assertTrue(self.apply()["already_current"])

    def test_retry_recovers_link_switched_before_state_cas(self) -> None:
        with mock.patch.object(controller_upgrade, "verify_signature"):
            from ops.broker.production_handler import stage_controller
            target = stage_controller(archive=self.archive, revision=NEW, controller_root=self.control)
        (self.control / "current").unlink()
        (self.control / "current").symlink_to(target)
        self.apply()
        self.assertEqual(json.loads(self.state.read_text())["controller_revision"], NEW)

    def test_retry_recovers_state_cas_persisted_before_link(self) -> None:
        value = json.loads(self.state.read_text())
        value["controller_revision"] = NEW
        self.state.write_bytes(controller_upgrade.canonical(value))
        self.apply()
        self.assertEqual((self.control / "current").resolve(), (self.releases / NEW).resolve())

    def test_retry_removes_only_its_stale_evidence_temp(self) -> None:
        directory = self.evidence / "controller-bootstrap"
        directory.mkdir(parents=True)
        (directory / f".{NEW}.json.12345").write_text("partial", encoding="ascii")
        self.apply()
        self.assertEqual([path.name for path in directory.iterdir()], [f"{NEW}.json"])

    def test_state_cas_drift_fails_before_activation(self) -> None:
        value = json.loads(self.state.read_text())
        value["infra_manifest_digest"] = "6" * 64
        self.state.write_bytes(controller_upgrade.canonical(value))
        with self.assertRaisesRegex(controller_upgrade.UpgradeError, "CAS mismatch"):
            self.apply()
        self.assertEqual((self.control / "current").resolve(), self.old.resolve())
        self.assertFalse(self.stable_allowed.exists())

    def test_signer_authority_digest_is_plan_bound(self) -> None:
        self.allowed.write_text("changed\n", encoding="ascii")
        with self.assertRaisesRegex(controller_upgrade.UpgradeError, "signer authority differs"):
            self.apply()
        self.assertFalse(self.stable_allowed.exists())

    def test_bootstrap_is_one_time_not_a_general_controller_deployer(self) -> None:
        consumed = self.evidence / "controller-bootstrap" / f"{'9' * 40}.json"
        consumed.parent.mkdir(parents=True)
        consumed.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(controller_upgrade.UpgradeError, "already consumed"):
            self.apply()
        self.assertFalse((self.releases / NEW).exists())
        self.assertFalse(self.stable_allowed.exists())
        self.assertEqual((self.control / "current").resolve(), self.old.resolve())
        self.assertEqual(json.loads(self.state.read_text())["controller_revision"], OLD)

    def test_plan_supports_distinct_product_durable_and_active_authorities(self) -> None:
        plan = json.loads(self.plan.read_text())
        plan["expected_controller_revision"] = "8" * 40
        plan["expected_active_controller_revision"] = "9" * 40
        self.assertEqual(
            controller_upgrade.validate_plan(plan)["expected_product_revision"], OLD
        )

    def test_updated_state_recovery_rejects_unrelated_drift(self) -> None:
        value = json.loads(self.state.read_text())
        value["controller_revision"] = NEW
        value["infra_manifest_digest"] = "7" * 64
        self.state.write_bytes(controller_upgrade.canonical(value))
        with self.assertRaisesRegex(controller_upgrade.UpgradeError, "updated current-release CAS"):
            self.apply()
        self.assertEqual((self.control / "current").resolve(), self.old.resolve())

    def test_controller_target_is_fsynced_before_link_and_state(self) -> None:
        from ops.broker import production_handler

        events: list[str] = []
        original_tree = production_handler.make_tree_durable
        original_directory = production_handler.fsync_directory
        original_switch = controller_upgrade.switch_link
        original_atomic = controller_upgrade.atomic_json

        def durable(path: Path) -> None:
            events.append("target-tree")
            original_tree(path)

        def directory(path: Path) -> None:
            if path == self.releases:
                events.append("releases-dir")
            original_directory(path)

        def switch(link: Path, target: Path) -> None:
            events.append("current-link")
            original_switch(link, target)

        def atomic(path: Path, value: dict, *, mode: int, gid: int | None = None) -> None:
            if path == self.state:
                events.append("current-state")
            original_atomic(path, value, mode=mode, gid=gid)

        with mock.patch.object(production_handler, "make_tree_durable", side_effect=durable), mock.patch.object(
            production_handler, "fsync_directory", side_effect=directory
        ), mock.patch.object(controller_upgrade, "switch_link", side_effect=switch), mock.patch.object(
            controller_upgrade, "atomic_json", side_effect=atomic
        ):
            self.apply()
        self.assertLess(events.index("target-tree"), events.index("releases-dir"))
        self.assertLess(events.index("releases-dir"), events.index("current-link"))
        self.assertLess(events.index("current-link"), events.index("current-state"))


if __name__ == "__main__":
    unittest.main()
