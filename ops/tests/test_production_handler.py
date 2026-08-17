from __future__ import annotations

import base64
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
                info.mode = 0o755 if name.endswith(("/vane", ".py", ".sh")) else 0o644
                bundle.addfile(info, io.BytesIO(data))
        return path

    def valid_members(self) -> dict[str, bytes]:
        return {
            "ops/bin/vane": b"#!/bin/sh\n",
            "ops/broker/forced_command.py": b"pass\n",
            "ops/broker/production_handler.py": b"pass\n",
            "ops/broker/promote_finalized_controller.py": b"#!/usr/bin/env python3\n",
            "ops/broker/run-production-handler.sh": b"#!/bin/sh\n",
            "ops/audit/production-uat.py": b"#!/usr/bin/env python3\n",
            "ops/release/artifact.py": b"pass\n",
            "ops/release/remote-atomic-release.sh": b"#!/bin/sh\n",
            "ops/rollback/switch-server-release.sh": b"#!/bin/sh\n",
            "tools/toolchain.lock.json": b"{}\n",
            "server/go.mod": b"module example.invalid/control\n",
            "server/internal/testgate/cmd/testpolicyscan/main.go": b"package main\n",
        }

    def test_controller_archive_is_content_addressed_and_immutable(self) -> None:
        archive = self.archive(self.valid_members())
        control = self.root / "control"
        previous_umask = os.umask(0o077)
        try:
            target = production_handler.stage_controller(
                archive=archive, revision=REVISION, controller_root=control
            )
        finally:
            os.umask(previous_umask)
        self.assertEqual(target, control / "releases" / REVISION)
        marker = target / ".controller-archive.sha256"
        self.assertEqual(
            marker.read_text(encoding="ascii").strip(),
            hashlib.sha256(archive.read_bytes()).hexdigest(),
        )
        self.assertEqual(control.stat().st_mode & 0o777, 0o755)
        self.assertEqual((control / "releases").stat().st_mode & 0o777, 0o755)
        self.assertEqual(target.stat().st_mode & 0o777, 0o755)
        self.assertTrue(
            all(
                path.stat().st_mode & 0o777 == 0o755
                for path in target.rglob("*")
                if path.is_dir()
            )
        )
        self.assertEqual(marker.stat().st_mode & 0o777, 0o600)
        (target / "ops").chmod(0o700)
        replay = production_handler.stage_controller(
            archive=archive, revision=REVISION, controller_root=control
        )
        self.assertEqual(replay, target)
        self.assertEqual((target / "ops").stat().st_mode & 0o777, 0o755)
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

    def test_retry_restores_candidate_left_active_before_durable_cas(self) -> None:
        provider = self.root / "provider"
        state = provider / "vane-deploy"
        state.mkdir(parents=True)
        (state / "deployed-vane.sha").write_text(REVISION + "\n", encoding="ascii")
        previous_revision = "f" * 40
        current = {"server": {"deployed_revision": previous_revision}}

        def restored(**kwargs):
            production_handler.write_revision(
                provider / "vane-deploy/deployed-vane.sha", previous_revision
            )

        with mock.patch.object(
            production_handler, "active_server_revision", return_value=REVISION
        ), mock.patch.object(
            production_handler, "restore", side_effect=restored
        ) as restore:
            production_handler.reconcile_server_before_release(
                repo=self.root,
                current=current,
                candidate_revision=REVISION,
                provider_root=provider,
            )
        restore.assert_called_once()
        self.assertEqual(
            (provider / "vane-deploy/deployed-vane.sha").read_text().strip(),
            previous_revision,
        )

    def test_retry_refuses_unknown_active_server_revision(self) -> None:
        provider = self.root / "provider"
        state = provider / "vane-deploy"
        state.mkdir(parents=True)
        (state / "deployed-vane.sha").write_text("f" * 40 + "\n", encoding="ascii")
        with mock.patch.object(
            production_handler, "active_server_revision", return_value="e" * 40
        ):
            with self.assertRaisesRegex(RuntimeError, "differs from both"):
                production_handler.reconcile_server_before_release(
                    repo=self.root,
                    current={"server": {"deployed_revision": "f" * 40}},
                    candidate_revision=REVISION,
                    provider_root=provider,
                )

    def test_retry_restarts_and_verifies_server_when_provider_was_advanced(self) -> None:
        provider = self.root / "provider"
        state = provider / "vane-deploy"
        state.mkdir(parents=True)
        (state / "deployed-vane.sha").write_text(REVISION + "\n", encoding="ascii")
        previous_revision = "f" * 40
        with mock.patch.object(
            production_handler, "active_server_revision", return_value=previous_revision
        ), mock.patch.object(production_handler, "restore") as restore:
            production_handler.reconcile_server_before_release(
                repo=self.root,
                current={"server": {"deployed_revision": previous_revision}},
                candidate_revision=REVISION,
                provider_root=provider,
            )
        restore.assert_called_once()

    def test_recovery_requires_every_systemd_unit_to_be_active(self) -> None:
        calls: list[list[str]] = []

        def command(arguments, **_):
            calls.append(arguments)
            if arguments[-1] == "vane-research-gateway.socket":
                raise RuntimeError("fixture inactive unit")

        with mock.patch.object(
            production_handler, "active_server_revision", return_value=REVISION
        ), mock.patch.object(production_handler, "run", side_effect=command):
            with self.assertRaisesRegex(RuntimeError, "inactive unit"):
                production_handler.verify_active_server(REVISION)
        self.assertEqual(
            calls,
            [
                ["/usr/bin/systemctl", "is-active", "--quiet", "vane.service"],
                [
                    "/usr/bin/systemctl",
                    "is-active",
                    "--quiet",
                    "vane-research-gateway.socket",
                ],
            ],
        )

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

    def test_controller_bootstrap_can_authorize_only_its_bound_product_successor(self) -> None:
        product = "a" * 40
        controller = "b" * 40
        control = self.root / "bootstrap-control"
        for revision in (product, controller):
            target = control / "releases" / revision
            target.mkdir(parents=True)
            (target / ".controller-archive.sha256").write_text(
                "c" * 64 + "\n", encoding="ascii"
            )
        (control / "current").symlink_to(control / "releases" / controller)
        evidence = self.root / "bootstrap-evidence"
        marker = evidence / "controller-bootstrap" / f"{controller}.json"
        marker.parent.mkdir(parents=True)
        marker.write_text(
            json.dumps({
                "schema": "vane.controller-bootstrap-evidence/v1",
                "product_revision": product,
                "controller_revision": controller,
                "controller_archive_sha256": "c" * 64,
            }) + "\n",
            encoding="utf-8",
        )
        current = {
            "monorepo_revision": product,
            "controller_revision": controller,
        }
        self.assertEqual(
            production_handler.active_controller_revision_for_release(
                current=current, controller_root=control, evidence_root=evidence
            ),
            controller,
        )
        current["monorepo_revision"] = "d" * 40
        (control / "releases" / ("d" * 40)).mkdir()
        with self.assertRaisesRegex(RuntimeError, "eligible finalized"):
            production_handler.active_controller_revision_for_release(
                current=current, controller_root=control, evidence_root=evidence
            )

    def test_active_server_revision_is_confined_to_release_authority(self) -> None:
        release_root = self.root / "server-releases"
        target = release_root / REVISION
        target.mkdir(parents=True)
        current = self.root / "server-current"
        current.symlink_to(target)
        self.assertEqual(
            production_handler.active_server_revision(current, release_root),
            REVISION,
        )
        current.unlink()
        outside = self.root / "outside-server"
        outside.mkdir()
        current.symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "unsafe target"):
            production_handler.active_server_revision(current, release_root)

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

    def test_pre_cas_durable_and_transaction_are_both_archivable_for_retry(self) -> None:
        evidence = self.root / "evidence"
        transaction = evidence / "inflight" / REVISION
        durable = evidence / "releases" / REVISION
        transaction.mkdir(parents=True)
        durable.mkdir(parents=True)
        (transaction / "early").write_text("early", encoding="utf-8")
        (durable / "late").write_text("late", encoding="utf-8")
        first = production_handler.preserve_failed_evidence(
            revision=REVISION,
            evidence_root=evidence,
            durable=durable,
            transaction=transaction,
        )
        second = production_handler.preserve_failed_evidence(
            revision=REVISION,
            evidence_root=evidence,
            durable=transaction,
            transaction=transaction,
        )
        assert first is not None and second is not None
        self.assertEqual((first / "late").read_text(), "late")
        self.assertEqual((second / "early").read_text(), "early")
        self.assertFalse(durable.exists())
        self.assertFalse(transaction.exists())

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

    def test_production_commands_cannot_pollute_broker_json_stdout(self) -> None:
        executable = self.root / "verbose-success.sh"
        executable.write_text(
            "#!/bin/sh\nprintf 'child stdout\\n'\nprintf 'child stderr\\n' >&2\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("sys.stdout", stdout), mock.patch("sys.stderr", stderr):
            production_handler.run([str(executable)])
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")

    def test_production_command_failure_never_returns_child_output(self) -> None:
        executable = self.root / "verbose-failure.sh"
        executable.write_text(
            "#!/bin/sh\nprintf 'do-not-copy-stdout\\n'\nprintf 'useful stderr\\n' >&2\nexit 23\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
        with self.assertRaisesRegex(RuntimeError, "exit 23") as raised:
            production_handler.run([str(executable)])
        self.assertNotIn("do-not-copy-stdout", str(raised.exception))
        self.assertNotIn("useful stderr", str(raised.exception))

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
        stale = source.index("if transaction.exists():")
        archive = source.index("preserve_failed_evidence(", stale)
        recreate = source.index("transaction.mkdir(", archive)
        provider = source.index("provider_root = ensure_provider_state(", recreate)
        recovery = source.index("except BaseException as release_error:", provider)
        already_current = source.index('if revision == current["monorepo_revision"]:')
        remote = source.index("server_stage = stage_server", already_current)
        self.assertLess(stale, archive)
        self.assertLess(archive, recreate)
        self.assertLess(recreate, provider)
        self.assertLess(provider, recovery)
        self.assertLess(already_current, stale)
        self.assertLess(already_current, remote)

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

    def test_firecracker_gate_is_exact_release_bound_and_kvm_is_narrowly_exposed(self) -> None:
        source = (Path(__file__).parents[1] / "broker/production_handler.py").read_text(
            encoding="utf-8"
        )
        gate = source[
            source.index("def firecracker_gate_unit_arguments") : source.index("def postgres_query")
        ]
        self.assertIn('/opt/vane/releases/{revision}/bin/sandboxd', gate)
        self.assertIn('"--property=DevicePolicy=closed"', gate)
        self.assertIn('"--property=DeviceAllow=char-misc m"', gate)
        self.assertIn('"--property=DeviceAllow=/dev/kvm rw"', gate)
        self.assertIn('"--property=PrivateNetwork=yes"', gate)
        self.assertIn('"--property=RestrictAddressFamilies=AF_UNIX AF_NETLINK"', gate)
        self.assertNotIn('"--property=Delegate=yes"', gate)
        self.assertIn('"--property=RuntimeMaxSec=30s"', gate)
        self.assertIn('"--property=TimeoutStopSec=5s"', gate)
        self.assertIn('"--property=KillMode=control-group"', gate)
        self.assertIn('"--property=CPUQuota=100%"', gate)
        self.assertIn('"--property=CPUQuotaPeriodSec=100ms"', gate)
        self.assertIn('"--property=MemoryMax=256M"', gate)
        self.assertIn('"--property=TasksMax=64"', gate)
        self.assertIn("release-gate-reap --sha {revision}", gate)
        self.assertIn("timeout_seconds=45", gate)
        self.assertNotIn("TimeoutStartSec", gate)
        self.assertNotIn("shell=True", gate)
        preflight = gate.index('["release-gate-netns-preflight", "--sha", revision]')
        microvm = gate.index('["release-gate", "--sha", revision, "--receipt", str(receipt)]')
        self.assertLess(preflight, microvm)
        release = source.index("server_stage = stage_server")
        gate_call = source.index("run_firecracker_gate(revision", release)
        uat = source.index("run_uat(", gate_call)
        self.assertLess(release, gate_call)
        self.assertLess(gate_call, uat)

    def test_firecracker_gate_unit_arguments_are_the_single_address_family_authority(self) -> None:
        executable = Path("/opt/vane/releases") / REVISION / "bin/sandboxd"
        arguments = production_handler.firecracker_gate_unit_arguments(
            f"vane-firecracker-gate-{REVISION}",
            f"vane-firecracker-gate-{REVISION}",
            Path("/var/lib/vane-deploy/evidence"),
            executable,
            REVISION,
        )
        family = [value for value in arguments if value.startswith("--property=RestrictAddressFamilies=")]
        self.assertEqual(family, ["--property=RestrictAddressFamilies=AF_UNIX AF_NETLINK"])
        self.assertIn("--property=PrivateNetwork=yes", arguments)
        self.assertNotIn("AF_INET", " ".join(family).split())
        self.assertNotIn("AF_INET6", " ".join(family).split())

    def test_firecracker_gate_failure_path_always_proves_runtime_collection(self) -> None:
        original = RuntimeError("simulated systemd-run failure")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            missing_cgroup = root / "missing-cgroup"
            missing_runtime = root / "missing-runtime"
            with self.assertRaisesRegex(RuntimeError, "simulated systemd-run failure") as raised:
                production_handler.ensure_firecracker_gate_collected(
                    "vane-firecracker-gate-fixture",
                    missing_cgroup,
                    missing_runtime,
                    original,
                )
            self.assertIs(raised.exception, original)

            cgroup = root / "cgroup"
            runtime = root / "runtime"
            cgroup.mkdir()
            runtime.mkdir()
            with mock.patch.object(production_handler.time, "sleep"), mock.patch.object(
                production_handler.subprocess, "run"
            ) as force:
                with self.assertRaisesRegex(RuntimeError, "recovery is incomplete") as cleanup:
                    production_handler.ensure_firecracker_gate_collected(
                        "vane-firecracker-gate-fixture",
                        cgroup,
                        runtime,
                        original,
                    )
            self.assertIs(cleanup.exception.__cause__, original)
            self.assertEqual(force.call_count, 3)

    def firecracker_release_fixture(self) -> tuple[Path, Path]:
        release_root = self.root / f"release-{len(list(self.root.glob('release-*')))}"
        release = release_root / REVISION
        for relative in production_handler.FIRECRACKER_RELEASE_FILES:
            path = release / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if relative != "sandbox/manifest.json":
                path.write_bytes((relative + "\n").encode("ascii"))
                path.chmod(production_handler.FIRECRACKER_RELEASE_FILES[relative])
        artifacts = {}
        for name in production_handler.FIRECRACKER_ARTIFACT_NAMES:
            path = release / "sandbox" / name
            artifacts[name] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size_bytes": path.stat().st_size,
            }
        sandboxd = release / "bin/sandboxd"
        sandbox_manifest = {
            "schema": "vane.firecracker-release-artifacts/v1",
            "source_revision": REVISION,
            "architecture": "x86_64",
            "firecracker_version": "v1.16.1",
            "sandboxd_sha256": hashlib.sha256(sandboxd.read_bytes()).hexdigest(),
            "artifacts": artifacts,
        }
        manifest_path = release / "sandbox/manifest.json"
        manifest_path.write_text(json.dumps(sandbox_manifest, separators=(",", ":")) + "\n", encoding="utf-8")
        manifest_path.chmod(0o644)
        base_modes = {
            "bin/vane": 0o755, "bin/vane-research-gateway": 0o755,
            "bin/vane-migrate": 0o755, "bin/gate": 0o755,
            "bin/agentfirstretention": 0o755,
        }
        entries = []
        for relative in sorted(production_handler.FIRECRACKER_BACKEND_PATHS):
            if relative in production_handler.FIRECRACKER_RELEASE_FILES:
                path = release / relative
                sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
                size = path.stat().st_size
                mode = production_handler.FIRECRACKER_RELEASE_FILES[relative]
            else:
                sha256, size, mode = "e" * 64, 1, base_modes.get(relative, 0o644)
            entries.append({"path": relative, "sha256": sha256, "size": size, "mode": mode})
        backend_manifest = {
            "schema": 2, "component": "backend", "source_sha": REVISION,
            "archive": f"backend-{REVISION}.tar.gz", "archive_sha256": "f" * 64,
            "archive_size": 1, "files": entries,
            "server_release_contract": "fixture", "control_plane_revision": REVISION,
            "deploy_run_id": "1", "build_run_attempt": 1,
        }
        backend_path = release / "backend-manifest.json"
        backend_path.write_text(json.dumps(backend_manifest, separators=(",", ":")) + "\n", encoding="utf-8")
        backend_path.chmod(0o644)
        release_receipt = {
            "schema_version": "vane.release-receipt/v1", "source_revision": REVISION,
            "control_plane_revision": REVISION, "deploy_run_id": "1", "build_run_attempt": 1,
            "backend_archive_sha256": "f" * 64,
            "backend_manifest_sha256": hashlib.sha256(backend_path.read_bytes()).hexdigest(),
            "server_release_contract_sha256": "a" * 64, "vane_sha256": "b" * 64,
            "agentfirstretention_sha256": "c" * 64,
        }
        receipt_path = release / "release-receipt.json"
        receipt_path.write_text(json.dumps(release_receipt, separators=(",", ":")) + "\n", encoding="utf-8")
        receipt_path.chmod(0o644)
        return release_root, release

    def test_firecracker_release_binding_recomputes_every_activated_artifact_byte(self) -> None:
        release_root, release = self.firecracker_release_fixture()
        binding = production_handler.firecracker_release_binding(
            REVISION, release_root, os.geteuid()
        )
        self.assertEqual(
            binding["sandboxd_sha256"], hashlib.sha256((release / "bin/sandboxd").read_bytes()).hexdigest()
        )
        self.assertEqual(set(binding["artifacts"]), production_handler.FIRECRACKER_ARTIFACT_NAMES)

        for relative in ("sandbox/jailer", "bin/sandboxd", "sandbox/manifest.json", "backend-manifest.json", "release-receipt.json"):
            mutated_root, mutated = self.firecracker_release_fixture()
            path = mutated / relative
            path.write_bytes(path.read_bytes() + b"mutation")
            path.chmod(production_handler.FIRECRACKER_RELEASE_FILES.get(relative, 0o644))
            with self.assertRaises((RuntimeError, ValueError, json.JSONDecodeError)):
                production_handler.firecracker_release_binding(REVISION, mutated_root, os.geteuid())

        mutated_root, mutated = self.firecracker_release_fixture()
        receipt_path = mutated / "release-receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["control_plane_revision"] = "0" * 40
        receipt_path.write_text(json.dumps(receipt, separators=(",", ":")) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "identities differ"):
            production_handler.firecracker_release_binding(REVISION, mutated_root, os.geteuid())

    def test_firecracker_gate_receipt_requires_the_exact_closed_evidence_shape(self) -> None:
        digest = "a" * 64
        guest = {
            "schema": "vane.firecracker-guest-receipt/v1",
            "invocation_id": "gate-" + "b" * 16,
            "request_sha256": "b" * 64,
            "input_sha256": "c" * 64,
            "response_sha256": "d" * 64,
            "guest_uid": 10001,
            "guest_gid": 10001,
            "only_loopback": True,
            "no_default_route": True,
            "mmds_unavailable": True,
        }
        guest_payload = json.dumps(guest, separators=(",", ":")).encode("utf-8")
        value = {
            "schema": "vane.firecracker-production-gate/v1",
            "revision": REVISION,
            "release_receipt_sha256": digest,
            "backend_manifest_sha256": digest,
            "sandboxd_sha256": digest,
            "artifacts": {name: digest for name in (
                "code.raw", "firecracker", "jailer", "rootfs.cpio", "vmlinux"
            )},
            "guest_receipt_sha256": hashlib.sha256(guest_payload).hexdigest(),
            "guest_receipt_base64": base64.b64encode(guest_payload).decode("ascii"),
            "invocation_id": "gate-" + "b" * 16,
            "duration_ms": 100,
            "network_interfaces": 0,
            "mmds": False,
            "jailer_uid": 62000,
            "jailer_gid": 62000,
            "cgroup_v2": True,
            "cgroup_parent": f"system.slice/vane-firecracker-gate-{REVISION}.service",
            "cpu_max": "100000 100000",
            "memory_max_bytes": 256 << 20,
            "pids_max": 64,
            "scrubbed": True,
            "ok": True,
        }
        expected = {
            key: value[key]
            for key in ("release_receipt_sha256", "backend_manifest_sha256", "sandboxd_sha256", "artifacts")
        }
        unit = f"vane-firecracker-gate-{REVISION}"
        self.assertIs(
            production_handler.validate_firecracker_gate_receipt(value, REVISION, unit, expected),
            value,
        )
        mutations = []
        for key, replacement in (
            ("cgroup_v2", False),
            ("jailer_uid", 62001),
            ("guest_receipt_sha256", "A" * 64),
            ("release_receipt_sha256", "0" * 64),
            ("backend_manifest_sha256", "e" * 64),
            ("sandboxd_sha256", "f" * 64),
            ("pids_max", 32),
        ):
            candidate = dict(value)
            candidate[key] = replacement
            mutations.append(candidate)
        missing = dict(value)
        missing.pop("backend_manifest_sha256")
        mutations.append(missing)
        unknown = dict(value)
        unknown["unexpected"] = True
        mutations.append(unknown)
        artifacts = dict(value)
        artifacts["artifacts"] = {"firecracker": digest}
        mutations.append(artifacts)
        arbitrary_artifact = dict(value)
        arbitrary_artifact["artifacts"] = dict(value["artifacts"])
        arbitrary_artifact["artifacts"]["jailer"] = "e" * 64
        mutations.append(arbitrary_artifact)
        arbitrary_guest = dict(value)
        changed_guest = dict(guest)
        changed_guest["input_sha256"] = "e" * 64
        changed_payload = json.dumps(changed_guest, separators=(",", ":")).encode("utf-8")
        arbitrary_guest["guest_receipt_base64"] = base64.b64encode(changed_payload).decode("ascii")
        mutations.append(arbitrary_guest)
        for candidate in mutations:
            with self.assertRaisesRegex(RuntimeError, "receipt is invalid"):
                production_handler.validate_firecracker_gate_receipt(candidate, REVISION, unit, expected)

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
