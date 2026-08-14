from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "promote_finalized_controller",
    ROOT / "ops/broker/promote_finalized_controller.py",
)
assert SPEC and SPEC.loader
promotion = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(promotion)
OLD = "1" * 40
FINALIZED = "2" * 40
UNKNOWN = "3" * 40


class ControllerPromotionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control = self.root / "control"
        self.releases = self.control / "releases"
        self.evidence = self.root / "evidence"
        self.lock = self.root / "release.lock"
        self.lock.write_bytes(b"")
        self.releases.mkdir(parents=True)
        self.state = self.root / "current-release.json"
        self.make_controller(OLD)
        self.make_controller(FINALIZED)
        (self.control / "current").symlink_to(self.releases / OLD)
        self.write_state(monorepo=FINALIZED, controller=OLD)
        finalize = self.evidence / "releases" / FINALIZED / "manifests/finalize.json"
        finalize.parent.mkdir(parents=True)
        finalize.write_text("{}\n", encoding="utf-8")

    def make_controller(self, revision: str) -> Path:
        target = self.releases / revision
        launcher = target / "ops/broker/run-production-handler.sh"
        launcher.parent.mkdir(parents=True)
        launcher.write_text("#!/bin/sh\n", encoding="utf-8")
        launcher.chmod(0o755)
        (target / ".controller-archive.sha256").write_text("a" * 64 + "\n", encoding="ascii")
        return target

    def write_state(self, *, monorepo: str, controller: str) -> None:
        self.state.write_text(
            json.dumps({"monorepo_revision": monorepo, "controller_revision": controller}) + "\n",
            encoding="utf-8",
        )

    def promote(self) -> Path:
        return promotion.promote(
            state=self.state,
            control_root=self.control,
            evidence_root=self.evidence,
        )

    def test_promotes_only_the_previously_finalized_product_controller(self) -> None:
        launcher = self.promote()
        self.assertEqual(launcher, (self.releases / FINALIZED / "ops/broker/run-production-handler.sh").resolve())
        self.assertEqual((self.control / "current").resolve(), (self.releases / FINALIZED).resolve())
        self.assertEqual(json.loads(self.state.read_text())["controller_revision"], OLD)

    def test_replay_after_kill_immediately_after_link_switch_is_idempotent(self) -> None:
        first = self.promote()
        second = self.promote()
        self.assertEqual(first, second)
        self.assertEqual((self.control / "current").resolve(), (self.releases / FINALIZED).resolve())

    def test_missing_finalize_evidence_cannot_move_the_link(self) -> None:
        (self.evidence / "releases" / FINALIZED / "manifests/finalize.json").unlink()
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            self.promote()
        self.assertEqual((self.control / "current").resolve(), (self.releases / OLD).resolve())

    def test_unknown_active_controller_fails_closed(self) -> None:
        self.make_controller(UNKNOWN)
        (self.control / "current").unlink()
        (self.control / "current").symlink_to(self.releases / UNKNOWN)
        with self.assertRaisesRegex(RuntimeError, "inconsistent"):
            self.promote()

    def test_one_time_bootstrap_controller_handles_first_normal_release(self) -> None:
        legacy = "4" * 40
        self.write_state(monorepo=legacy, controller=OLD)
        (self.evidence / "releases" / FINALIZED / "manifests/finalize.json").unlink()
        launcher = self.promote()
        self.assertEqual(launcher, (self.releases / OLD / "ops/broker/run-production-handler.sh").resolve())
        self.assertEqual((self.control / "current").resolve(), (self.releases / OLD).resolve())

    def test_promoter_refuses_to_create_a_missing_lock_as_root(self) -> None:
        self.lock.unlink()
        argv = [
            "promote_finalized_controller.py",
            "--state",
            str(self.state),
            "--control-root",
            str(self.control),
            "--evidence-root",
            str(self.evidence),
            "--lock",
            str(self.lock),
        ]
        with mock.patch("sys.argv", argv):
            with self.assertRaisesRegex(RuntimeError, "lock authority is unsafe"):
                promotion.main()
        self.assertFalse(self.lock.exists())


if __name__ == "__main__":
    unittest.main()
