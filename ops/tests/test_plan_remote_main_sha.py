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

    def test_resume_web_accepts_only_exact_sha_and_release_root(self) -> None:
        revision = "a" * 40
        args = controller.parser().parse_args(
            ["resume-web", "--sha", revision, "--release-root", "/tmp/evidence"]
        )
        self.assertEqual(args.sha, revision)
        self.assertEqual(args.release_root, Path("/tmp/evidence"))
        self.assertEqual(args.allowed_signers, controller.DEFAULT_SIGNERS)
        result = self.run_cli(
            "resume-web", "--sha", revision, "--release-root", "/tmp/evidence",
            "--allowed-signers", "/tmp/attacker",
        )
        self.assertEqual(result.returncode, 2, result)

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

    def test_post_gate_authority_rejects_publisher_and_lock_mutation(self) -> None:
        for relative in (
            Path("ops/release/publish_web.py"),
            Path("tools/toolchain.lock.json"),
        ):
            with self.subTest(path=relative), tempfile.TemporaryDirectory() as temporary:
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
                protected = repository / relative
                protected.parent.mkdir(parents=True)
                protected.write_text("trusted\n", encoding="utf-8")
                subprocess.run(["git", "add", "."], cwd=repository, check=True)
                subprocess.run(
                    ["git", "commit", "-qm", "fixture"],
                    cwd=repository, check=True,
                )
                revision = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=repository, text=True
                ).strip()
                snapshot = controller.capture_release_authority(
                    revision, (protected,), repository=repository
                )

                # Model a credential-free Gate process changing an entry that
                # the restored controller would execute with provider secrets.
                protected.write_text("same-version malicious replacement\n", encoding="utf-8")
                with self.assertRaisesRegex(
                    controller.PolicyError, "not clean|changed while"
                ):
                    controller.validate_release_authority_after_gate(
                        revision,
                        snapshot,
                        signing_key=protected,
                        allowed_signers=protected,
                        broker_submit=protected,
                        repository=repository,
                        paths=(protected,),
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
                mock.patch.object(controller.subprocess, "run", return_value=response) as remote,
                mock.patch.dict(os.environ, {
                    "ALIYUN_ACCESS_KEY_ID": "secret",
                    "ALIYUN_ACCESS_KEY_SECRET": "secret",
                    "CLOUDFLARE_API_TOKEN": "secret",
                    "CLOUDFLARE_ACCOUNT_ID": "secret",
                }),
            ):
                with self.assertRaisesRegex(controller.PolicyError, "differs from released revision"):
                    controller.verify_production_revision(revision)
            self.assertFalse(
                set(remote.call_args.kwargs["env"]).intersection(
                    controller.WEB_PROVIDER_CREDENTIALS
                )
            )

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

    def test_gate_environment_uses_canonical_shared_public_caches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            gate_home = root / "gate-home"
            release_root = root / "release"
            gate_home.mkdir()
            release_root.mkdir()
            tool_cache = root / "tool-cache"
            locked_go = tool_cache / "go/1.26.6/bin/go"
            locked_go.parent.mkdir(parents=True)
            locked_go.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            locked_go.chmod(0o700)
            environment = controller.sanitized_gate_environment(
                {
                    "PATH": "/usr/bin",
                    "VANE_TOOL_CACHE": str(tool_cache),
                    "GOCACHE": "/private/candidate-build-cache",
                    "GOMODCACHE": "/private/candidate-module-cache",
                    "GOTOOLCHAIN": "auto",
                    "VANE_RELEASE_SIGNING_KEY": "/private/signing-key",
                    "ALIYUN_ACCESS_KEY_ID": "provider-id",
                    "ALIYUN_ACCESS_KEY_SECRET": "provider-secret",
                    "CLOUDFLARE_API_TOKEN": "cloudflare-token",
                    "CLOUDFLARE_ACCOUNT_ID": "cloudflare-account",
                },
                gate_home=gate_home,
                release_root=release_root,
                gate_evidence=release_root / "full-gate.json",
                rollback_base="a" * 40,
                cache_root=root / "shared-cache",
            )
            self.assertEqual(environment["GOCACHE"], str(root / "shared-cache/go-build"))
            self.assertEqual(environment["GOMODCACHE"], str(root / "shared-cache/go-mod"))
            self.assertEqual(environment["VANE_GATE_CACHE_ROOT"], str(root / "shared-cache"))
            self.assertEqual(environment["GOTOOLCHAIN"], "local")
            self.assertEqual(environment["GOSUMDB"], "sum.golang.org")
            self.assertTrue(environment["PATH"].startswith(str(locked_go.parent) + ":"))
            self.assertNotIn("VANE_RELEASE_SIGNING_KEY", environment)
            for secret in (
                "ALIYUN_ACCESS_KEY_ID", "ALIYUN_ACCESS_KEY_SECRET",
                "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID",
            ):
                self.assertNotIn(secret, environment)
            for name in ("shared-cache", "go-build", "go-mod"):
                path = root / (name if name == "shared-cache" else f"shared-cache/{name}")
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)

    def test_gate_environment_rejects_symlinked_shared_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.mkdir()
            cache = root / "shared-cache"
            cache.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(controller.PolicyError, "cache root is unsafe"):
                controller.sanitized_gate_environment(
                    {"PATH": "/usr/bin"},
                    gate_home=root,
                    release_root=root,
                    gate_evidence=root / "full-gate.json",
                    rollback_base="a" * 40,
                    cache_root=cache,
                )

    def test_gate_environment_rejects_symlinked_cache_parent_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.mkdir()
            linked_parent = root / "linked-parent"
            linked_parent.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(controller.PolicyError, "cache parent is unsafe"):
                controller.sanitized_gate_environment(
                    {"PATH": "/usr/bin"},
                    gate_home=root,
                    release_root=root,
                    gate_evidence=root / "full-gate.json",
                    rollback_base="a" * 40,
                    cache_root=linked_parent / "gate-cache",
                )
            self.assertFalse((outside / "gate-cache").exists())

    def test_release_refuses_failed_web_preflight_before_gate_or_broker(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            key = root / "signing-key"
            broker = root / "broker"
            key.write_text("fixture", encoding="utf-8")
            broker.write_text("fixture", encoding="utf-8")
            args = argparse.Namespace(
                sha=revision,
                lock=controller.DEFAULT_LOCK,
                policy=controller.DEFAULT_POLICY,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            clean = subprocess.CompletedProcess(
                ["git", "status"], 0, stdout="", stderr=""
            )
            with (
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=revision),
                mock.patch.object(controller, "require_release_runtime", return_value=(root, key, "release-test", broker)),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(controller, "preflight_web_publication", side_effect=controller.PolicyError("Web provider preflight failed")),
                mock.patch.object(controller, "command_full") as full,
                mock.patch.object(controller, "build_release_submission") as build,
                mock.patch.object(controller.subprocess, "run", return_value=clean) as run,
            ):
                with self.assertRaisesRegex(
                    controller.PolicyError, "Web provider preflight failed"
                ):
                    controller.command_release(args)
            full.assert_not_called()
            build.assert_not_called()
            self.assertFalse(
                any(call.args[0] == [str(broker), "--status"] for call in run.call_args_list)
            )

    def test_broker_environment_is_a_minimal_allowlist(self) -> None:
        source = {
            "PATH": "/usr/bin",
            "LANG": "en_US.UTF-8",
            "TMPDIR": "/tmp/fixture",
            "ALIYUN_ACCESS_KEY_ID": "secret",
            "ALIBABA_CLOUD_ACCESS_KEY_ID": "legacy-secret",
            "OSS_ACCESS_KEY_SECRET": "oss-secret",
            "CF_API_TOKEN": "alias-secret",
            "CLOUDFLARE_API_TOKEN": "secret",
            "VANE_RELEASE_SIGNING_KEY": "/private/key",
            "ARBITRARY_FUTURE_SECRET": "must-not-pass",
        }
        with mock.patch.dict(os.environ, source, clear=True):
            sanitized = controller.sanitized_broker_environment()
        self.assertEqual(sanitized, {
            "PATH": "/usr/bin",
            "LANG": "en_US.UTF-8",
            "TMPDIR": "/tmp/fixture",
        })

    def test_resume_web_publishes_from_signed_gate_without_gate_or_server_submit(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary) / f"release-{revision}"
            release_root.mkdir(mode=0o700)
            resolved_root = release_root.resolve()
            gate = release_root / "server-submission/full-gate.json"
            gate.parent.mkdir()
            gate.write_text("{}\n", encoding="utf-8")
            web_result = release_root / "web-publication.json"

            def publish_result(**_: object) -> Path:
                web_result.write_text("{}\n", encoding="utf-8")
                return web_result
            args = argparse.Namespace(
                sha=revision,
                release_root=release_root,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            with (
                mock.patch.object(controller, "assert_resume_source"),
                mock.patch.object(controller, "resume_web_authority_paths", return_value=(gate,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(gate): controller.sha256_file(gate)}),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "preflight_web_publication") as preflight,
                mock.patch.object(controller, "verify_production_revision", return_value="f" * 64) as verify_server,
                mock.patch.object(controller, "publish_web_after_server", side_effect=publish_result) as publish,
                mock.patch.object(controller, "verify_web_toolchain_integrity"),
                mock.patch.object(controller, "command_full") as full,
                mock.patch.object(controller.subprocess, "run") as run,
            ):
                self.assertEqual(controller.command_resume_web(args), 0)
            preflight.assert_called_once_with()
            publish.assert_called_once_with(
                revision=revision,
                release_root=resolved_root,
                gate_evidence=gate,
            )
            self.assertEqual(verify_server.call_count, 2)
            full.assert_not_called()
            run.assert_not_called()

    def test_verify_web_passes_combined_publication_result(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary)
            dist = release_root / "dist"
            dist.mkdir()
            gate = release_root / "full-gate.json"
            gate.write_text("{}\n", encoding="utf-8")
            cache = release_root / "tool-cache"
            cache.mkdir()
            with (
                mock.patch.object(
                    controller, "validated_web_dist", return_value=dist
                ),
                mock.patch.object(
                    controller, "web_publication_context",
                    return_value=(release_root / "state", cache, "https://vane.zhuoqidev.com"),
                ),
                mock.patch.object(controller, "run_checked") as run,
            ):
                controller.verify_web_after_server(
                    revision=revision,
                    release_root=release_root,
                    gate_evidence=gate,
                )
            command = run.call_args.args[0]
            self.assertEqual(
                command[command.index("--publication-result") + 1],
                str(release_root / "web-publication.json"),
            )
            self.assertIn("--verify-only", command)

    def test_resume_web_rejects_same_sha_server_digest_drift(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary) / f"release-{revision}"
            release_root.mkdir(mode=0o700)
            gate = release_root / "full-gate.json"
            gate.write_text("{}\n", encoding="utf-8")
            args = argparse.Namespace(
                sha=revision,
                release_root=release_root,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            with (
                mock.patch.object(controller, "assert_resume_source"),
                mock.patch.object(controller, "resume_web_authority_paths", return_value=(gate,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(gate): controller.sha256_file(gate)}),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "preflight_web_publication"),
                mock.patch.object(
                    controller, "verify_production_revision",
                    side_effect=["1" * 64, "2" * 64],
                ),
                mock.patch.object(
                    controller, "publish_web_after_server",
                    return_value=release_root / "web-publication.json",
                ),
                mock.patch.object(controller, "verify_web_toolchain_integrity"),
            ):
                with self.assertRaisesRegex(
                    controller.PolicyError,
                    "Server current digest changed during Web recovery",
                ):
                    controller.command_resume_web(args)

    def test_resume_web_revalidates_authority_immediately_before_publish(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary) / f"release-{revision}"
            release_root.mkdir(mode=0o700)
            gate = release_root / "full-gate.json"
            gate.write_text("{}\n", encoding="utf-8")
            args = argparse.Namespace(
                sha=revision,
                release_root=release_root,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            with (
                mock.patch.object(controller, "assert_resume_source"),
                mock.patch.object(controller, "resume_web_authority_paths", return_value=(gate,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(gate): controller.sha256_file(gate)}),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "preflight_web_publication"),
                mock.patch.object(controller, "verify_production_revision", return_value="f" * 64),
                mock.patch.object(
                    controller, "validate_release_authority_after_gate",
                    side_effect=controller.PolicyError("release authority changed"),
                ),
                mock.patch.object(controller, "verify_web_toolchain_integrity") as tools,
                mock.patch.object(controller, "publish_web_after_server") as publish,
            ):
                with self.assertRaisesRegex(controller.PolicyError, "authority changed"):
                    controller.command_resume_web(args)
            tools.assert_not_called()
            publish.assert_not_called()

    def test_resume_web_existing_result_is_verify_only_without_credentials(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary) / f"release-{revision}"
            release_root.mkdir(mode=0o700)
            resolved_root = release_root.resolve()
            result = release_root / "web-publication.json"
            result.write_text("{}\n", encoding="utf-8")
            gate = release_root / "signed-gate.json"
            gate.write_text("{}\n", encoding="utf-8")
            args = argparse.Namespace(
                sha=revision,
                release_root=release_root,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            with (
                mock.patch.object(controller, "assert_resume_source"),
                mock.patch.object(controller, "resume_web_authority_paths", return_value=(gate,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(gate): controller.sha256_file(gate)}),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "validate_existing_web_result") as validate_result,
                mock.patch.object(controller, "preflight_web_publication") as preflight,
                mock.patch.object(controller, "verify_production_revision", return_value="f" * 64),
                mock.patch.object(controller, "verify_web_after_server") as verify_web,
                mock.patch.object(controller, "publish_web_after_server") as publish,
            ):
                self.assertEqual(controller.command_resume_web(args), 0)
            validate_result.assert_called_once()
            preflight.assert_not_called()
            publish.assert_not_called()
            verify_web.assert_called_once_with(
                revision=revision,
                release_root=resolved_root,
                gate_evidence=gate,
            )

    def test_resume_web_rejects_gate_not_bound_by_signed_manifest(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary) / f"release-{revision}"
            handoff = release_root / "server-submission"
            manifests = handoff / "manifests"
            manifests.mkdir(parents=True)
            gate = handoff / "full-gate.json"
            artifact = manifests / "artifact.json"
            gate.write_text("trusted gate\n", encoding="utf-8")
            artifact.write_text("fixture\n", encoding="utf-8")
            chain = [
                (manifests / "plan.json", {"stage": "plan", "evidence": []}),
                (manifests / "gate.json", {
                    "stage": "gate",
                    "evidence": [{"name": "full-gate.json", "sha256": "0" * 64}],
                }),
                (artifact, {"stage": "artifact", "evidence": []}),
            ]
            with (
                mock.patch.object(controller, "require_release_chain", return_value=chain),
                mock.patch.object(controller, "validated_web_dist") as validate_dist,
            ):
                with self.assertRaisesRegex(controller.PolicyError, "not bound"):
                    controller.signed_web_gate(
                        revision=revision,
                        release_root=release_root,
                        allowed_signers=controller.DEFAULT_SIGNERS,
                    )
            validate_dist.assert_not_called()

    def test_resume_web_rejects_signed_gate_path_escape(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            release_root = root / f"release-{revision}"
            release_root.mkdir()
            gate = {key: None for key in controller.FULL_GATE_KEYS}
            gate.update({
                "schema": "vane.full-gate-evidence/v1",
                "revision": revision,
                "web_dist": "../outside",
                "web_tree_sha256": "b" * 64,
                "release_marker_sha256": "c" * 64,
            })
            gate_path = release_root / "full-gate.json"
            gate_path.write_text(json.dumps(gate), encoding="utf-8")
            with self.assertRaisesRegex(controller.PolicyError, "safe and relative"):
                controller.validated_web_dist(
                    revision=revision,
                    release_root=release_root,
                    gate_evidence=gate_path,
                )

    def test_publish_web_binds_private_snapshot_to_signed_gate_tree(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary) / f"release-{revision}"
            release_root.mkdir()
            dist = release_root / "web-dist"
            dist.mkdir()
            gate = release_root / "full-gate.json"
            gate.write_text(json.dumps({
                "web_tree_sha256": "b" * 64,
            }), encoding="utf-8")
            state = release_root / "state"
            cache = release_root / "cache"
            cache.mkdir()
            commands: list[list[str]] = []
            with (
                mock.patch.object(controller, "validated_web_dist", return_value=dist),
                mock.patch.object(
                    controller, "web_publication_context",
                    return_value=(state, cache, "https://vane.zhuoqidev.com"),
                ),
                mock.patch.object(
                    controller, "run_checked",
                    side_effect=lambda command, **_: commands.append(command),
                ),
            ):
                controller.publish_web_after_server(
                    revision=revision, release_root=release_root,
                    gate_evidence=gate,
                )
            command = commands[0]
            self.assertEqual(
                command[command.index("--expected-web-tree-sha256") + 1],
                "b" * 64,
            )

    def test_one_command_release_runs_gate_build_and_broker_submission(self) -> None:
        revision = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            key = work / "signing-key"
            broker = work / "broker-submit"
            key.write_text("fixture", encoding="utf-8")
            broker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            broker.chmod(0o700)
            web_result = work / "web.json"
            web_result.write_text("{}\n", encoding="utf-8")
            args = argparse.Namespace(
                sha=revision,
                lock=controller.DEFAULT_LOCK,
                policy=controller.DEFAULT_POLICY,
                allowed_signers=controller.DEFAULT_SIGNERS,
            )
            observed_broker_secrets: list[set[str]] = []

            def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                if command and command[0] == str(broker):
                    observed_broker_secrets.append(
                        set(kwargs.get("env", {})).intersection(
                            controller.BROKER_ENV_SECRETS
                        )
                    )
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
                        "CLOUDFLARE_ACCOUNT_ID",
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
                    "CLOUDFLARE_ACCOUNT_ID": "cloudflare-account",
                    "SSH_AUTH_SOCK": "/private/agent.sock",
                    "VANE_BROKER_SUBMIT": "/tmp/fake-broker",
                }),
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=revision),
                mock.patch.object(controller, "require_release_runtime", return_value=(work, key, "release-test", broker)),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(controller, "preflight_web_publication") as web_preflight,
                mock.patch.object(controller, "command_full", side_effect=full),
                mock.patch.object(
                    controller, "sanitized_gate_environment",
                    return_value={"VANE_ROLLBACK_BASE_SHA": "b" * 40},
                ),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "post_gate_authority_paths", return_value=(key,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(key): controller.sha256_file(key)}),
                mock.patch.object(controller, "validate_release_authority_after_gate") as post_gate,
                mock.patch.object(controller, "build_release_submission", side_effect=lambda **values: values["release_root"]) as build,
                mock.patch.object(controller, "verify_production_revision", return_value="e" * 64) as verify_production,
                mock.patch.object(controller, "publish_web_after_server", return_value=web_result),
                mock.patch.object(controller.subprocess, "run", side_effect=run) as run_mock,
                mock.patch("builtins.print") as output,
            ):
                self.assertEqual(controller.command_release(args), 0)
            build.assert_called_once()
            self.assertEqual(web_preflight.call_count, 2)
            post_gate.assert_called_once()
            self.assertEqual(verify_production.call_count, 2)
            verify_production.assert_has_calls([mock.call(revision), mock.call(revision)])
            self.assertEqual(observed_base, ["b" * 40])
            self.assertEqual(observed_gate_secrets, [set()])
            self.assertTrue(observed_broker_secrets)
            self.assertTrue(all(not value for value in observed_broker_secrets))
            release_output = json.loads(output.call_args.args[0])
            self.assertEqual(release_output["server_current_digest"], "e" * 64)
            self.assertEqual(
                release_output["web_publication_sha256"],
                controller.sha256_file(web_result),
            )
            self.assertEqual(run_mock.call_args_list[-1].args[0], [str(broker), str(work / f"release-{revision}")])

    def test_release_rejects_post_gate_web_drift_before_broker_submission(self) -> None:
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

            def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
                if command == [str(broker), "--status"]:
                    return subprocess.CompletedProcess(
                        command, 0,
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
                mock.patch.object(
                    controller, "require_release_runtime",
                    return_value=(work, key, "release-test", broker),
                ),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(
                    controller, "preflight_web_publication",
                    side_effect=[None, controller.PolicyError("post-Gate Web route drift")],
                ) as preflight,
                mock.patch.object(controller, "command_full", return_value=0),
                mock.patch.object(controller, "sanitized_gate_environment", return_value={}),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "post_gate_authority_paths", return_value=(key,)),
                mock.patch.object(
                    controller, "capture_release_authority",
                    return_value={str(key): controller.sha256_file(key)},
                ),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(controller, "verify_web_toolchain_integrity"),
                mock.patch.object(controller, "build_release_submission") as build,
                mock.patch.object(controller, "publish_web_after_server") as publish,
                mock.patch.object(controller.subprocess, "run", side_effect=run) as run_mock,
            ):
                with self.assertRaisesRegex(
                    controller.PolicyError, "post-Gate Web route drift"
                ):
                    controller.command_release(args)
            self.assertEqual(preflight.call_count, 2)
            build.assert_not_called()
            publish.assert_not_called()
            broker_calls = [
                call.args[0] for call in run_mock.call_args_list
                if call.args and call.args[0] and call.args[0][0] == str(broker)
            ]
            self.assertEqual(broker_calls, [[str(broker), "--status"]])

    def test_release_rejects_same_sha_server_digest_drift(self) -> None:
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

            def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
                if command == [str(broker), "--status"]:
                    return subprocess.CompletedProcess(
                        command, 0,
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
                mock.patch.object(
                    controller, "require_release_runtime",
                    return_value=(work, key, "release-test", broker),
                ),
                mock.patch.object(controller, "validate_toolchain", return_value=[]),
                mock.patch.object(controller, "signer_entries", return_value=["fixture"]),
                mock.patch.object(controller, "command_full", return_value=0),
                mock.patch.object(controller, "sanitized_gate_environment", return_value={}),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "post_gate_authority_paths", return_value=(key,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(key): controller.sha256_file(key)}),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(
                    controller, "build_release_submission",
                    side_effect=lambda **values: values["release_root"],
                ),
                mock.patch.object(
                    controller, "verify_production_revision",
                    side_effect=["1" * 64, "2" * 64],
                ),
                mock.patch.object(
                    controller, "publish_web_after_server",
                    return_value=work / "web.json",
                ),
                mock.patch.object(controller.subprocess, "run", side_effect=run),
            ):
                with self.assertRaisesRegex(
                    controller.PolicyError,
                    "Server current digest changed during Web publication",
                ):
                    controller.command_release(args)

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
                mock.patch.object(
                    controller, "sanitized_gate_environment", return_value={}
                ),
                mock.patch.object(Path, "is_file", return_value=True),
                mock.patch.object(controller, "post_gate_authority_paths", return_value=(key,)),
                mock.patch.object(controller, "capture_release_authority", return_value={str(key): controller.sha256_file(key)}),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
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
