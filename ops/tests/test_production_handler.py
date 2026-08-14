from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

from ops.broker import production_handler


REVISION = "0123456789abcdef0123456789abcdef01234567"


class ProductionHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def archive(self, members: dict[str, bytes]) -> Path:
        path = self.root / f"archive-{len(list(self.root.glob('archive-*')))}.tar.gz"
        with tarfile.open(path, mode="w:gz") as bundle:
            for name, data in members.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o755 if name.endswith(("/vane", ".py")) else 0o644
                bundle.addfile(info, io.BytesIO(data))
        return path

    def valid_members(self) -> dict[str, bytes]:
        return {
            "ops/bin/vane": b"#!/bin/sh\n",
            "ops/broker/forced_command.py": b"pass\n",
            "ops/broker/production_handler.py": b"pass\n",
            "ops/release/artifact.py": b"pass\n",
            "tools/toolchain.lock.json": b"{}\n",
            "server/go.mod": b"module example.invalid/control\n",
            "server/internal/testgate/cmd/testpolicyscan/main.go": b"package main\n",
        }

    def test_controller_archive_is_content_addressed_and_immutable(self) -> None:
        archive = self.archive(self.valid_members())
        control = self.root / "control"
        target = production_handler.stage_controller(
            archive=archive, revision=REVISION, controller_root=control
        )
        self.assertEqual(target, control / "releases" / REVISION)
        marker = target / ".controller-archive.sha256"
        self.assertEqual(
            marker.read_text(encoding="ascii").strip(),
            hashlib.sha256(archive.read_bytes()).hexdigest(),
        )
        self.assertEqual(control.stat().st_mode & 0o777, 0o755)
        self.assertEqual((control / "releases").stat().st_mode & 0o777, 0o755)
        self.assertEqual(target.stat().st_mode & 0o777, 0o755)
        self.assertEqual(marker.stat().st_mode & 0o777, 0o600)
        replay = production_handler.stage_controller(
            archive=archive, revision=REVISION, controller_root=control
        )
        self.assertEqual(replay, target)
        changed = self.valid_members()
        changed["ops/bin/vane"] = b"changed\n"
        with self.assertRaisesRegex(RuntimeError, "differs"):
            production_handler.stage_controller(
                archive=self.archive(changed),
                revision=REVISION,
                controller_root=control,
            )

    def test_controller_archive_rejects_traversal_and_links(self) -> None:
        traversal = self.archive({"../escape": b"x"})
        with self.assertRaisesRegex(RuntimeError, "unsafe"):
            production_handler.stage_controller(
                archive=traversal,
                revision=REVISION,
                controller_root=self.root / "traversal-control",
            )
        link = self.root / "link.tar.gz"
        with tarfile.open(link, mode="w:gz") as bundle:
            info = tarfile.TarInfo("ops/bin/vane")
            info.type = tarfile.SYMTYPE
            info.linkname = "/bin/sh"
            bundle.addfile(info)
        with self.assertRaisesRegex(RuntimeError, "unsafe"):
            production_handler.stage_controller(
                archive=link,
                revision=REVISION,
                controller_root=self.root / "link-control",
            )

    def test_controller_root_rejects_symlink_authority(self) -> None:
        outside = self.root / "outside-control"
        outside.mkdir()
        control = self.root / "control-link"
        control.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "root is unsafe"):
            production_handler.stage_controller(
                archive=self.archive(self.valid_members()),
                revision=REVISION,
                controller_root=control,
            )

    def test_atomic_current_release_refuses_stale_cas(self) -> None:
        current = self.root / "current-release.json"
        current.write_text('{"state":"N"}\n', encoding="utf-8")
        before = current.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "CAS"):
            production_handler.atomic_current_release(
                current, {"state": "N+1"}, "f" * 64
            )
        self.assertEqual(current.read_bytes(), before)

    def test_atomic_current_release_advances_exact_cas(self) -> None:
        current = self.root / "current-release.json"
        current.write_text('{"state":"N"}\n', encoding="utf-8")
        expected = hashlib.sha256(current.read_bytes()).hexdigest()
        production_handler.atomic_current_release(
            current, {"schema": "fixture", "state": "N+1"}, expected
        )
        self.assertEqual(
            json.loads(current.read_text(encoding="utf-8")),
            {"schema": "fixture", "state": "N+1"},
        )
        self.assertEqual(current.stat().st_mode & 0o777, 0o600)

    def test_atomic_current_release_sets_readable_group_before_replace(self) -> None:
        current = self.root / "current-release.json"
        current.write_text('{"state":"N"}\n', encoding="utf-8")
        expected = hashlib.sha256(current.read_bytes()).hexdigest()
        original = production_handler.grp.getgrnam
        self.addCleanup(setattr, production_handler.grp, "getgrnam", original)
        production_handler.grp.getgrnam = lambda _: type("Group", (), {"gr_gid": os.getgid()})()
        original_chown = production_handler.os.chown
        self.addCleanup(setattr, production_handler.os, "chown", original_chown)
        production_handler.os.chown = lambda path, uid, gid: None

        production_handler.atomic_current_release(
            current,
            {"state": "N+1"},
            expected,
            reader_group="vane-broker",
        )

        self.assertEqual(current.stat().st_mode & 0o777, 0o640)

    def test_active_controller_is_confined_to_release_authority(self) -> None:
        control = self.root / "control"
        target = control / "releases" / REVISION
        target.mkdir(parents=True)
        (control / "current").symlink_to(target)
        self.assertEqual(
            production_handler.active_controller_target(control), target.resolve()
        )
        (control / "current").unlink()
        outside = self.root / "outside"
        outside.mkdir()
        (control / "current").symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "escapes"):
            production_handler.active_controller_target(control)

    def test_failed_evidence_prefers_partial_durable_tree(self) -> None:
        evidence = self.root / "evidence"
        transaction = evidence / "inflight" / REVISION
        durable = evidence / "releases" / REVISION
        transaction.mkdir(parents=True)
        durable.mkdir(parents=True)
        (transaction / "early").write_text("early", encoding="utf-8")
        (durable / "late").write_text("late", encoding="utf-8")
        failed = production_handler.preserve_failed_evidence(
            revision=REVISION,
            evidence_root=evidence,
            durable=durable,
            transaction=transaction,
        )
        self.assertIsNotNone(failed)
        assert failed is not None
        self.assertEqual((failed / "late").read_text(encoding="utf-8"), "late")
        self.assertFalse(durable.exists())
        self.assertTrue(transaction.exists())

    def test_handler_is_server_only_and_finishes_uat_before_state(self) -> None:
        source = (Path(__file__).parents[1] / "broker/production_handler.py").read_text(
            encoding="utf-8"
        )
        server = source.index("remote-atomic-release.sh")
        uat = source.index("run_uat", server)
        state = source.index("atomic_current_release", uat)
        self.assertLess(server, uat)
        self.assertLess(uat, state)
        self.assertNotIn('"frontend-aliyun"', source)
        self.assertNotIn('"frontend-cloudflare"', source)
        self.assertNotIn("docker build", source)
        self.assertNotIn("docker push", source)

    def test_current_state_uses_the_activated_release_manifest_digest(self) -> None:
        source = (Path(__file__).parents[1] / "broker/production_handler.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'infra_manifest = Path(f"/opt/vane/releases/{revision}/infra-manifest.sha256")',
            source,
        )
        self.assertIn('"infra_manifest_digest": digest(infra_manifest)', source)
        self.assertNotIn('"infra_manifest_digest": gate["infra_tree_sha256"]', source)

    def test_retry_restarts_the_same_atomic_release_state_machine(self) -> None:
        source = (Path(__file__).parents[1] / "broker/production_handler.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('verb not in {"release", "retry"}', source)
        self.assertEqual(source.count("result = release("), 1)

    def test_handler_sandbox_can_install_canonical_systemd_units(self) -> None:
        launcher = (Path(__file__).parents[1] / "broker/run-production-handler.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("--property=ProtectSystem=strict", launcher)
        self.assertEqual(
            launcher.count("--property=ReadWritePaths=/etc/systemd/system"), 1
        )
        controller = (Path(__file__).parents[1] / "broker/controller.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('["/usr/bin/sudo", "--non-interactive", "--", *command]', controller)

    def test_uat_uses_a_temporary_session_and_always_revokes_it(self) -> None:
        executable = self.root / "uat.py"
        executable.write_text(
            """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
credential = Path(os.environ["CREDENTIALS_DIRECTORY"]) / "uat_session_cookie"
token = credential.read_text(encoding="ascii").strip()
assert len(token) == 43
assert credential.stat().st_mode & 0o777 == 0o600
print(json.dumps({"schema":"vane.production-uat/v1","revision":sys.argv[-1],"ok":True}))
""",
            encoding="utf-8",
        )
        executable.chmod(0o755)
        statements: list[str] = []

        def query(sql: str) -> str:
            statements.append(sql)
            return "1"

        output = self.root / "uat.json"
        with mock.patch.object(production_handler, "postgres_query", side_effect=query):
            value = production_handler.run_uat(
                [str(executable)], REVISION, output, {"user_id": 1, "tenant_id": 1}
            )

        self.assertTrue(value["ok"])
        self.assertEqual(len(statements), 2)
        self.assertIn("INSERT INTO user_sessions", statements[0])
        self.assertIn("interval '10 minutes'", statements[0])
        self.assertIn("DELETE FROM user_sessions", statements[1])
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)

    def test_uat_failure_still_revokes_the_temporary_session(self) -> None:
        executable = self.root / "fail.sh"
        executable.write_text("#!/bin/sh\nexit 23\n", encoding="utf-8")
        executable.chmod(0o755)
        statements: list[str] = []

        def query(sql: str) -> str:
            statements.append(sql)
            return "1"

        with mock.patch.object(production_handler, "postgres_query", side_effect=query):
            with self.assertRaisesRegex(RuntimeError, "exit 23"):
                production_handler.run_uat(
                    [str(executable)],
                    REVISION,
                    self.root / "unused.json",
                    {"user_id": 1, "tenant_id": 1},
                )
        self.assertEqual(len(statements), 2)
        self.assertIn("DELETE FROM user_sessions", statements[1])


if __name__ == "__main__":
    unittest.main()
