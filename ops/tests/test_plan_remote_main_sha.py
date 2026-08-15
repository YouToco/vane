import argparse
import json
import os
from pathlib import Path
import pwd
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
        self.assertEqual(args.lock, controller.DEFAULT_LOCK)
        self.assertEqual(args.policy, controller.DEFAULT_POLICY)
        self.assertEqual(args.allowed_signers, controller.DEFAULT_SIGNERS)

    def test_release_rejects_external_policy_argument(self) -> None:
        result = self.run_cli(
            "release", "--sha", "a" * 40, "--policy", "/tmp/fake-policy.json"
        )
        self.assertEqual(result.returncode, 2, result)
        self.assertIn("unrecognized arguments", result.stderr)

    def test_committed_export_ignores_post_gate_worktree_pollution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repository = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=repository, check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Vane Test"],
                cwd=repository, check=True,
            )
            infra = repository / "infra/production"
            infra.mkdir(parents=True)
            source = infra / "service.conf"
            source.write_text("tested\n", encoding="utf-8")
            subprocess.run(["git", "add", "infra"], cwd=repository, check=True)
            subprocess.run(["git", "commit", "-qm", "fixture"], cwd=repository, check=True)
            revision = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=repository, text=True
            ).strip()
            source.write_text("post-gate pollution\n", encoding="utf-8")
            files = controller.committed_files(
                revision, ("infra/production",), repository=repository
            )
            self.assertEqual(files, [(Path("infra/production/service.conf"), 0o644, b"tested\n")])

    def test_committed_build_source_is_not_nested_in_broker_handoff(self) -> None:
        source = (ROOT / "ops/cli/controller.py").read_text(encoding="utf-8")
        self.assertIn(
            'committed_source = release_root / "committed-source"', source
        )
        self.assertNotIn(
            'committed_source = handoff / "committed-source"', source
        )

    def test_default_broker_submitter_ignores_environment_home(self) -> None:
        expected = (
            Path(pwd.getpwuid(os.getuid()).pw_dir)
            / ".local"
            / "libexec"
            / "vane-broker-submit"
        )
        with mock.patch.dict(
            os.environ,
            {
                "HOME": "/tmp/candidate-home",
                "VANE_BROKER_SUBMIT": "/tmp/candidate-broker",
            },
        ):
            self.assertEqual(controller.default_broker_submit_path(), expected)

    def test_broker_submitter_rejects_symlink_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            home = base / "home"
            outside = base / "outside"
            home.mkdir(mode=0o700)
            outside.mkdir(mode=0o700)
            (home / ".local").symlink_to(outside, target_is_directory=True)
            libexec = outside / "libexec"
            libexec.mkdir(mode=0o700)
            broker = libexec / "vane-broker-submit"
            broker.write_bytes((ROOT / "ops/broker/submit.py").read_bytes())
            broker.chmod(0o700)
            through_link = home / ".local" / "libexec" / broker.name
            with self.assertRaisesRegex(controller.PolicyError, "chain is unsafe"):
                controller.validate_broker_submitter(
                    through_link, account_home=home
                )

    def test_independent_status_rejects_wrong_server_revision(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            response = subprocess.CompletedProcess(
                ["/usr/bin/ssh"],
                0,
                stdout=json.dumps({
                    "ok": True,
                    "verb": "status",
                    "current_digest": "f" * 64,
                    "server_revision": "b" * 40,
                }).encode(),
                stderr=b"",
            )
            with (
                mock.patch.object(controller.broker_client, "default_config_path", return_value=Path(temporary) / "config.json"),
                mock.patch.object(controller.broker_client, "validate_config_file"),
                mock.patch.object(controller.broker_client, "strict_json", return_value={"host": controller.BROKER_HOST, "port": controller.BROKER_PORT}),
                mock.patch.object(controller.broker_client, "fixed_ssh_command", return_value=["/usr/bin/ssh", "fixed"]),
                mock.patch.object(controller.subprocess, "run", return_value=response),
            ):
                with self.assertRaisesRegex(controller.PolicyError, "differs from released revision"):
                    controller.verify_production_revision(revision)

    def test_independent_status_rejects_client_endpoint_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch.object(controller.broker_client, "default_config_path", return_value=Path(temporary) / "config.json"),
                mock.patch.object(controller.broker_client, "validate_config_file"),
                mock.patch.object(controller.broker_client, "strict_json", return_value={"host": "192.0.2.11", "port": 10058}),
                mock.patch.object(controller.subprocess, "run") as remote_status,
            ):
                with self.assertRaisesRegex(controller.PolicyError, "endpoint differs"):
                    controller.verify_production_revision("a" * 40)
            remote_status.assert_not_called()

    def test_one_command_release_runs_gate_build_and_broker_submission(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            key = work / "signing-key"
            broker = work / "broker-submit"
            key.write_text("fixture", encoding="utf-8")
            broker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            broker.chmod(0o700)
            args = argparse.Namespace(
                sha=revision,
                lock=controller.DEFAULT_LOCK,
                policy=controller.DEFAULT_POLICY,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                if command == [str(broker), "--status"]:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps({
                            "current_digest": "f" * 64,
                            "server_revision": "b" * 40,
                        }),
                        stderr="",
                    )
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            observed_base: list[str] = []
            observed_gate_secrets: list[set[str]] = []

            def full(_: argparse.Namespace) -> int:
                observed_base.append(os.environ["VANE_ROLLBACK_BASE_SHA"])
                observed_gate_secrets.append({
                    name for name in (
                        "VANE_RELEASE_SIGNING_KEY", "ALIYUN_ACCESS_KEY_ID",
                        "ALIYUN_ACCESS_KEY_SECRET", "CLOUDFLARE_API_TOKEN",
                        "SSH_AUTH_SOCK", "VANE_BROKER_SUBMIT",
                    ) if name in os.environ
                })
                return 0

            with (
                mock.patch.dict(os.environ, {
                    "VANE_RELEASE_SIGNING_KEY": "/private/signing-key",
                    "ALIYUN_ACCESS_KEY_ID": "provider-id",
                    "ALIYUN_ACCESS_KEY_SECRET": "provider-secret",
                    "CLOUDFLARE_API_TOKEN": "cloudflare-secret",
                    "SSH_AUTH_SOCK": "/private/agent.sock",
                    "VANE_BROKER_SUBMIT": "/tmp/fake-broker",
                }),
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=revision),
                mock.patch.object(controller, "require_release_runtime", return_value=(work, key, "release-test", broker)),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(controller, "command_full", side_effect=full),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "build_release_submission", side_effect=lambda **values: values["release_root"]) as build,
                mock.patch.object(controller, "verify_production_revision", return_value="e" * 64) as verify_production,
                mock.patch.object(controller, "publish_web_after_server", return_value=work / "web.json"),
                mock.patch.object(controller.subprocess, "run", side_effect=run) as run_mock,
            ):
                self.assertEqual(controller.command_release(args), 0)
            build.assert_called_once()
            self.assertEqual(verify_production.call_count, 2)
            verify_production.assert_has_calls([mock.call(revision), mock.call(revision)])
            self.assertEqual(observed_base, ["b" * 40])
            self.assertEqual(observed_gate_secrets, [set()])
            self.assertEqual(run_mock.call_args_list[-1].args[0], [str(broker), str(work / f"release-{revision}")])

    def test_fake_submitter_cannot_publish_web_without_independent_status(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            key = work / "signing-key"
            broker = work / "broker-submit"
            key.write_text("fixture", encoding="utf-8")
            broker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            broker.chmod(0o700)
            args = argparse.Namespace(
                sha=revision,
                lock=controller.DEFAULT_LOCK,
                policy=controller.DEFAULT_POLICY,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )

            def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                if command == [str(broker), "--status"]:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps({
                            "current_digest": "f" * 64,
                            "server_revision": "b" * 40,
                        }),
                        stderr="",
                    )
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with (
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=revision),
                mock.patch.object(controller, "require_release_runtime", return_value=(work, key, "release-test", broker)),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(controller, "command_full", return_value=0),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "build_release_submission", side_effect=lambda **values: values["release_root"]),
                mock.patch.object(controller, "verify_production_revision", side_effect=controller.PolicyError("independent status failed")),
                mock.patch.object(controller, "publish_web_after_server") as publish_web,
                mock.patch.object(controller.subprocess, "run", side_effect=run),
            ):
                with self.assertRaisesRegex(controller.PolicyError, "independent status failed"):
                    controller.command_release(args)
            publish_web.assert_not_called()


if __name__ == "__main__":
    unittest.main()
