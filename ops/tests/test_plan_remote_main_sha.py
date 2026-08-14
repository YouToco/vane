from pathlib import Path
import argparse
import subprocess
import tempfile
import unittest
from unittest import mock

from ops.cli import controller


OPS = Path(__file__).resolve().parents[1]
ROOT = OPS.parent
CLI = OPS / "bin" / "vane"


class ExactRevisionCLITest(unittest.TestCase):
    def run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(CLI), *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_release_rejects_short_sha_at_argument_boundary(self) -> None:
        result = self.run_cli("release", "--sha", "abc")
        self.assertEqual(result.returncode, 2, result)
        self.assertIn("exact lowercase 40-character SHA", result.stderr)

    def test_release_rejects_uppercase_sha(self) -> None:
        result = self.run_cli("release", "--sha", "A" * 40)
        self.assertEqual(result.returncode, 2, result)

    def test_release_rejects_exact_non_main_sha(self) -> None:
        result = self.run_cli("release", "--sha", "0" * 40)
        self.assertEqual(result.returncode, 78, result)
        self.assertIn("not exact origin/main", result.stderr)

    def test_quick_resolves_symbolic_refs_to_exact_commits(self) -> None:
        result = self.run_cli(
            "quick", "--risk", "B", "--base", "HEAD", "--head", "HEAD"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"status":"passed"', result.stdout)
        self.assertIn('"paths":[]', result.stdout)

    def test_release_sha_is_the_only_user_supplied_release_input(self) -> None:
        args = controller.parser().parse_args(["release", "--sha", "a" * 40])
        self.assertEqual(args.sha, "a" * 40)
        for retired in (
            "manifest", "release_receipt", "current_release",
            "candidate_release", "expected_current_digest",
        ):
            self.assertFalse(hasattr(args, retired))

    def test_one_command_release_runs_gate_build_and_broker_submission(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            key = work / "signing-key"
            broker = work / "broker-submit"
            supervisor = work / "build-supervisor"
            key.write_text("fixture", encoding="utf-8")
            broker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            broker.chmod(0o755)
            supervisor.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            supervisor.chmod(0o755)
            args = argparse.Namespace(
                sha=revision,
                lock=controller.DEFAULT_LOCK,
                policy=controller.DEFAULT_POLICY,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            completed = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            with (
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=revision),
                mock.patch.object(controller, "require_release_runtime", return_value=(work, key, "release-test", broker, supervisor)),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(controller.shutil, "copy2"),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "build_release_submission", side_effect=lambda **values: values["release_root"]) as build,
                mock.patch.object(controller.shutil, "rmtree"),
                mock.patch.object(controller.subprocess, "run", return_value=completed) as run,
            ):
                self.assertEqual(controller.command_release(args), 0)
            build.assert_called_once()
            self.assertEqual(run.call_args_list[-1].args[0], [str(broker), str(work / f"release-{revision}")])


if __name__ == "__main__":
    unittest.main()
