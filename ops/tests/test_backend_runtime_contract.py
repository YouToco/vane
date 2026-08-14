from pathlib import Path
import importlib.util
import json
import re
import unittest


OPS = Path(__file__).resolve().parents[1]
REPO = OPS.parent
DEPLOY = OPS / "release/deploy.sh"
REMOTE = OPS / "release/remote-atomic-release.sh"
SYSTEMD = REPO / "infra/production/systemd"
SPEC = importlib.util.spec_from_file_location("artifact", OPS / "release/artifact.py")
assert SPEC and SPEC.loader
artifact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact)


class BackendRuntimeContractTest(unittest.TestCase):
    def test_toolchain_uses_fixed_security_release(self) -> None:
        lock = json.loads((REPO / "tools/toolchain.lock.json").read_text(encoding="utf-8"))
        self.assertEqual(lock["tools"]["go"]["version"], "1.26.6")
        self.assertNotIn("1.26.5", json.dumps(lock))

    def test_artifact_is_the_complete_ten_binary_process_boundary(self) -> None:
        inventory = json.loads(
            (REPO / "contracts/release/server-binaries.json").read_text(encoding="utf-8")
        )["binaries"]
        binaries = {f"bin/{entry['name']}" for entry in inventory}
        self.assertEqual(len(binaries), 10)
        self.assertEqual(
            binaries,
            {name for name in artifact.BACKEND_FILES if name.startswith("bin/")},
        )
        self.assertEqual(
            {name for name in artifact.BACKEND_FILES if name.startswith("deploy/")},
            {
                "deploy/Caddyfile",
                "deploy/docker-compose.yml",
                "deploy/vane.service",
                "deploy/vane-migrate.service",
                "deploy/vane-research-gateway.service",
                "deploy/vane-research-gateway.socket",
                "deploy/vane-legacy-compat.service",
                "deploy/dynamicconfig/development-sql.yaml",
            },
        )
        for archive_path, source_path in artifact.BACKEND_SOURCE_PATHS.items():
            if archive_path.startswith("deploy/"):
                self.assertTrue(source_path.startswith("infra/production/"))

    def test_application_release_never_uses_a_container_image(self) -> None:
        active = "\n".join(
            path.read_text(encoding="utf-8")
            for directory in (OPS / "release", OPS / "rollback")
            for path in directory.rglob("*")
            if path.is_file()
        )
        for forbidden in ("docker build", "docker push", "docker pull"):
            self.assertNotIn(forbidden, active)
        self.assertNotRegex(active, r"(?m)^\s*image:\s*vane")

    def test_release_is_one_immutable_sha_directory_and_atomic_link(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        for required in (
            "release_root=/opt/vane/releases",
            "release_dir=$release_root/$release_sha",
            "current_link=/opt/vane/current",
            "current release CAS mismatch",
            'mv -T "$pending" "$release_dir"',
            'mv -Tf "$next_link" "$current_link"',
            '"$current_link/bin/gate"',
            'readlink /proc/"$pid"/exe',
        ):
            self.assertIn(required, remote)
        self.assertNotIn("/opt/vane/bin/", remote)
        self.assertNotIn("rm -rf -- /opt/vane/releases", remote)

    def test_all_active_scripts_reject_scattered_binary_authority(self) -> None:
        for directory in (OPS / "release", OPS / "rollback", OPS / "bootstrap"):
            for path in directory.rglob("*.sh"):
                self.assertNotIn("/opt/vane/bin/", path.read_text(encoding="utf-8"), path)

    def test_server_only_release_does_not_touch_middleware(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        self.assertIn("candidate_infra_digest", remote)
        self.assertIn("current_infra_digest", remote)
        self.assertIn("infra-manifest.sha256", remote)
        compose = remote.index("docker compose up -d")
        guard = remote.rfind('if [[ $infra_changed == true ]]', 0, compose)
        self.assertGreaterEqual(guard, 0)
        self.assertIn("postgres temporal temporal-ui caddy", remote[compose:])
        self.assertNotIn("VANE_INFRA_CHANGED", DEPLOY.read_text(encoding="utf-8"))

    def test_compose_has_only_pinned_middleware(self) -> None:
        compose = (REPO / "infra/production/compose/docker-compose.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            set(re.findall(r"(?m)^  ([a-z0-9-]+):\n    image:", compose)),
            {"postgres", "temporal", "temporal-ui", "caddy"},
        )
        self.assertNotIn("build:", compose)
        self.assertIn("/opt/vane/dynamicconfig/development-sql.yaml", compose)
        self.assertIn("/opt/vane/Caddyfile", compose)

    def test_migration_precedes_current_switch_and_service_start(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        migrate = remote.index("systemd-run --quiet --wait --collect")
        switch = remote.index('mv -Tf "$next_link" "$current_link"')
        start = remote.index("systemctl start vane-research-gateway.socket")
        self.assertLess(migrate, switch)
        self.assertLess(switch, start)
        self.assertIn('"$release_dir/bin/vane-migrate"', remote[migrate:switch])

    def test_failure_after_switch_restores_whole_previous_release(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        cleanup = remote[remote.index("cleanup() {") : remote.index("trap cleanup EXIT")]
        self.assertIn('if (( status != 0 )) && [[ $switched == true ]]', cleanup)
        self.assertIn('ln -s "$current_target" "$rollback_link"', cleanup)
        self.assertIn('mv -Tf "$rollback_link" "$current_link"', cleanup)
        self.assertIn("systemctl daemon-reload", cleanup)
        self.assertIn("systemctl restart vane-research-gateway.socket", cleanup)

    def test_stage_and_existing_release_are_fail_closed(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        self.assertIn("unsafe remote stage", remote)
        self.assertIn("candidate release lacks binary", remote)
        self.assertIn("candidate release lacks infra member", remote)
        self.assertIn("immutable release replay differs", remote)
        self.assertIn("current release symlink has unsafe target", remote)
        self.assertIn("current release authority is not a symlink", remote)

    def test_live_gate_and_exact_executable_precede_success(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        ready = remote.rindex("/readyz")
        gate = remote.rindex('"$current_link/bin/gate"')
        executable = remote.rindex('readlink /proc/"$pid"/exe')
        success = remote.rindex("atomic release activated")
        self.assertLess(ready, gate)
        self.assertLess(gate, executable)
        self.assertLess(executable, success)

    def test_deploy_passes_only_verified_payload_to_atomic_remote(self) -> None:
        deploy = DEPLOY.read_text(encoding="utf-8")
        self.assertIn('backend_remote_stage="/opt/vane/.deploy-', deploy)
        self.assertIn('"$payload/bin/$binary"', deploy)
        self.assertIn('"$payload/release-receipt.json"', deploy)
        self.assertIn('<"$script_dir/remote-atomic-release.sh"', deploy)
        self.assertNotIn("legacy-remote-backend-deploy", deploy)

    def test_systemd_executes_only_current_release_binaries(self) -> None:
        for unit in SYSTEMD.glob("*.service"):
            payload = unit.read_text(encoding="utf-8")
            self.assertNotIn("ExecStart=/opt/vane/bin/", payload, unit)
            for line in payload.splitlines():
                if line.startswith("ExecStart=/opt/vane/"):
                    self.assertTrue(line.startswith("ExecStart=/opt/vane/current/bin/"), unit)

    def test_legacy_compat_unit_remains_non_root_and_explicit(self) -> None:
        unit = (SYSTEMD / "vane-legacy-compat.service").read_text(encoding="utf-8")
        for required in (
            "User=vane",
            "Group=vane",
            "EnvironmentFile=/opt/vane/env/server-owner-compat.env",
            "ExecStart=/opt/vane/current/bin/vane",
            "NoNewPrivileges=yes",
            "ProtectSystem=strict",
        ):
            self.assertEqual(unit.splitlines().count(required), 1)
        self.assertNotIn("User=root", unit)


if __name__ == "__main__":
    unittest.main()
