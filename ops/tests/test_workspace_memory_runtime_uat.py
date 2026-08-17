from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "workspace_memory_runtime_uat",
    ROOT / "audit" / "workspace-memory-runtime-uat.py",
)
assert SPEC is not None and SPEC.loader is not None
uat = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(uat)


class WorkspaceMemoryRuntimeUATTest(unittest.TestCase):
    revision = "a" * 40
    operation = "5d358bef-7093-4327-9f66-f5af9f194e51"

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        release_root = root / "releases"
        release = release_root / self.revision
        (release / "bin").mkdir(parents=True)
        executable = release / "bin" / "vane-migrate"
        executable.write_bytes(b"trusted-binary")
        executable.chmod(0o755)
        current = root / "current"
        current.symlink_to(release)
        server_env = root / "server.env"
        server_env.write_text("VANE_DB_URL=postgres://runtime\n", encoding="utf-8")
        server_env.chmod(0o640)
        credential = root / "migration_db_url"
        credential.write_text("postgres://owner\n", encoding="utf-8")
        credential.chmod(0o600)
        self.paths = mock.patch.multiple(
            uat,
            CURRENT_RELEASE=current,
            RELEASE_ROOT=release_root.resolve(),
            SERVER_ENV=server_env,
            MIGRATION_CREDENTIAL=credential,
            TRUSTED_UID=os.getuid(),
        )
        self.paths.start()
        self.addCleanup(self.paths.stop)

    def report(self) -> dict[str, object]:
        return {
            "schema": uat.SCHEMA,
            "revision": self.revision,
            "operation_id": self.operation,
            "runtime_boundary_verified": True,
            "personal_write_verified": True,
            "team_write_verified": True,
            "cross_member_recall_verified": True,
            "personal_excluded_from_team": True,
            "team_excluded_from_personal": True,
            "cleanup_verified": True,
            "personal_evidence_digest": "1" * 64,
            "team_evidence_digest": "2" * 64,
        }

    def invoke(self, completed: subprocess.CompletedProcess[str]) -> tuple[int, list[str]]:
        argv = [
            "workspace-memory-runtime-uat.py",
            "--sha",
            self.revision,
            "--operation-id",
            self.operation,
        ]
        captured: list[str] = []
        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            uat.subprocess, "run", return_value=completed
        ) as run, mock.patch("builtins.print", side_effect=lambda value: captured.append(value)):
            code = uat.main()
        command = run.call_args.args[0]
        return code, command

    def test_exact_release_runs_hardened_transient_unit(self) -> None:
        completed = subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(self.report()), stderr=""
        )
        code, command = self.invoke(completed)
        self.assertEqual(code, 0)
        self.assertIn("--property=User=vane-migrate", command)
        self.assertIn("--property=NoNewPrivileges=yes", command)
        self.assertIn("--property=ProtectSystem=strict", command)
        self.assertIn("--property=TimeoutStartSec=4min", command)
        self.assertEqual(command[-7:], [
            "workspace-memory-uat", "--operation-id", self.operation,
            "--expected-revision", self.revision, "--confirm", uat.SCHEMA,
        ])

    def test_report_rejects_missing_false_duplicate_and_equal_digest(self) -> None:
        valid = self.report()
        for mutation in (
            {key: value for key, value in valid.items() if key != "cleanup_verified"},
            {**valid, "cleanup_verified": False},
            {**valid, "team_evidence_digest": valid["personal_evidence_digest"]},
        ):
            with self.subTest(mutation=mutation):
                with self.assertRaises(RuntimeError):
                    uat.validate_report(mutation, self.revision, self.operation)
        duplicate = '{"schema":"x","schema":"y"}'
        with self.assertRaises(RuntimeError):
            uat.strict_json(duplicate)

    def test_authority_files_and_subprocess_fail_closed(self) -> None:
        uat.SERVER_ENV.chmod(0o666)
        with self.assertRaises(RuntimeError):
            self.invoke(subprocess.CompletedProcess([], 0, stdout="{}", stderr=""))
        uat.SERVER_ENV.chmod(0o640)
        with self.assertRaises(RuntimeError):
            self.invoke(subprocess.CompletedProcess([], 1, stdout="", stderr="failed"))
        with self.assertRaises(RuntimeError):
            self.invoke(subprocess.CompletedProcess([], 0, stdout=json.dumps(self.report()), stderr="warning"))


if __name__ == "__main__":
    unittest.main()
