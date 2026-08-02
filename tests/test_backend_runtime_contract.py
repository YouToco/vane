from pathlib import Path
import importlib.util
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
DEPLOY = ROOT / "scripts" / "deploy.sh"
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
LEGACY_COMPAT_UNIT = ROOT / "deploy" / "vane-legacy-compat.service"
SPEC = importlib.util.spec_from_file_location("artifact", ROOT / "scripts/artifact.py")
assert SPEC and SPEC.loader
artifact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact)


class BackendRuntimeContractTest(unittest.TestCase):
    def test_artifact_contains_the_complete_process_boundary(self) -> None:
        expected = {
            "bin/vane",
            "bin/useradmin",
            "bin/gate",
            "bin/runtimeadmin",
            "bin/vane-migrate",
            "bin/vane-research-gateway",
            "bin/vane-research-prepare",
            "bin/researchshadow",
            "bin/researchcutover",
            "deploy/Caddyfile",
            "deploy/docker-compose.yml",
            "deploy/vane.service",
            "deploy/vane-migrate.service",
            "deploy/vane-research-gateway.service",
            "deploy/vane-research-gateway.socket",
            "deploy/dynamicconfig/development-sql.yaml",
        }
        self.assertEqual(set(artifact.BACKEND_FILES), expected)

    def test_build_upload_and_install_name_every_required_file(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        deploy = DEPLOY.read_text(encoding="utf-8")
        remote = REMOTE.read_text(encoding="utf-8")

        for binary, command in {
            "vane-migrate": "./cmd/migrate",
            "vane-research-gateway": "./cmd/researchgateway",
            "vane-research-prepare": "./cmd/researchprepare",
            "researchshadow": "./cmd/researchshadow",
            "researchcutover": "./cmd/researchcutover",
        }.items():
            self.assertIn(f"-o bin/{binary} {command}", workflow)
            self.assertIn(f'"$payload/bin/{binary}"', deploy)
            self.assertIn(binary, remote)
        for unit in (
            "vane-migrate.service",
            "vane-research-gateway.service",
            "vane-research-gateway.socket",
        ):
            self.assertIn(unit, deploy)
            self.assertIn(unit, remote)
        self.assertIn("vane-legacy-compat.service", deploy)
        self.assertIn("vane-legacy-compat.service", remote)

    def test_primary_release_contract_is_probed_and_bound_to_artifact(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        remote = REMOTE.read_text(encoding="utf-8")
        exact = (
            "vane.server-release-contract/v1 primary_store=owner_compat_v1 "
            "research_store=restricted_v1"
        )

        self.assertIn("-print-release-contract", workflow)
        self.assertIn(exact, workflow)
        self.assertIn("--server-release-contract", workflow)
        self.assertIn("steps.server_contract.outputs.value", workflow)
        self.assertIn("env -i PATH=/usr/bin:/bin", remote)
        self.assertIn("-print-release-contract", remote)
        self.assertIn(exact, remote)

    def test_legacy_compatibility_unit_is_explicit_and_audited(self) -> None:
        unit = LEGACY_COMPAT_UNIT.read_text(encoding="utf-8")
        expected = {
            "User=vane",
            "Group=vane",
            "WorkingDirectory=/opt/vane",
            "EnvironmentFile=/opt/vane/env/server-owner-compat.env",
            "ExecStart=/opt/vane/bin/vane",
            "NoNewPrivileges=yes",
            "ProtectSystem=strict",
            "ProtectHome=yes",
            "PrivateTmp=yes",
        }

        for line in expected:
            self.assertEqual(unit.splitlines().count(line), 1)
        self.assertNotIn("ExecStartPre=", unit)
        self.assertNotIn("ExecStartPost=", unit)
        self.assertNotIn("User=root", unit)

    def test_bootstrap_keeps_owner_out_of_long_lived_server(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")

        self.assertIn("systemctl restart vane-migrate.service", remote)
        self.assertIn("runtime_bootstrap_v1.complete", remote)
        self.assertIn("vane_server_runtime", remote)
        self.assertIn("vane_research_runtime", remote)
        self.assertIn("vane_research_llm_gateway_runtime", remote)
        self.assertIn("server environment contains an owner credential", remote)
        self.assertIn("vane-research-prepare", remote)
        self.assertNotIn("systemctl enable vane-research-prepare", remote)
        self.assertNotIn("systemctl restart vane-research-prepare", remote)
        self.assertIn("/opt/vane/bin/vane.next", remote)
        self.assertLess(
            remote.index("systemctl restart vane-migrate.service"),
            remote.index("systemctl stop vane\n"),
        )
        self.assertLess(
            remote.index("systemctl restart vane-research-gateway.service"),
            remote.index("systemctl stop vane\n"),
        )
        self.assertIn("gateway_exe=$(readlink", remote)
        self.assertIn(
            "for attempt in {1..12}; do\n  gateway_exe=\n"
            "  gateway_pid=$(systemctl show",
            remote,
        )
        self.assertIn("gateway_http_code == 405", remote)
        self.assertIn("runuser -u vane -- curl", remote)
        self.assertIn(
            "install -d -o root -g root -m 0711 /run/vane-research-gateway",
            remote,
        )
        self.assertIn("-L /run/vane-research-gateway", remote)
        self.assertIn("! -d /run/vane-research-gateway", remote)
        self.assertIn(
            "stat -c '%U:%G:%a' /run/vane-research-gateway) == root:root:711",
            remote,
        )
        self.assertLess(
            remote.index(
                "install -d -o root -g root -m 0711 /run/vane-research-gateway"
            ),
            remote.index("systemctl enable --now vane-research-gateway.socket"),
        )
        self.assertLess(
            remote.index(
                "install -d -o root -g root -m 0711 /run/vane-research-gateway"
            ),
            remote.index("runuser -u vane -- curl"),
        )
        self.assertIn("capability_source=legacy", remote)
        self.assertIn("capability_retired", remote)
        self.assertIn(
            "VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID=\\n",
            remote,
        )
        self.assertIn("old_vane_recovery_required=true", remote)
        self.assertIn("snapshot_previous_vane_release", remote)
        self.assertIn("restore_previous_vane_release", remote)
        self.assertIn("assert_legacy_primary_runtime_contract", remote)
        self.assertIn("assert_known_split_primary_runtime_contract", remote)
        self.assertIn("assert_audited_legacy_primary_runtime_contract", remote)
        self.assertIn("commit_legacy_primary_release", remote)
        self.assertIn("build_owner_compatible_environment", remote)
        self.assertIn("assert_research_settings_exact", remote)
        for setting in (
            "VANE_DB_RESEARCH_RUNTIME_URL",
            "VANE_DB_RESEARCH_CAPABILITY_KEY_ID",
            "VANE_DB_RESEARCH_CAPABILITY_KEY_HEX",
            "VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS",
            "VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS",
            "VANE_RESEARCH_GATEWAY_SOCKET_PATH",
            "VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID",
            "VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID",
            "VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID",
        ):
            self.assertIn(setting, remote)
        self.assertIn("owner database DSN changed during compatibility cutover", remote)
        self.assertIn("contains a forbidden process credential", remote)
        self.assertIn("assert_gateway_peer_and_credential_boundary", remote)
        self.assertIn("assert_restricted_server_environment_readonly", remote)
        self.assertIn("chown root:vane /opt/vane/env/server.env", remote)
        self.assertIn("chmod 0640 /opt/vane/env/server.env", remote)
        self.assertIn("runuser -u vane -- test -w /opt/vane/env/server.env", remote)
        self.assertIn("VANE_GATEWAY_ALLOWED_UID", remote)
        self.assertIn("runuser -u vane -- test -r", remote)
        self.assertIn(
            "vane-research-gateway:vane-research-gateway:400", remote
        )
        self.assertIn("primary runtime release fence requires legacy root", remote)
        self.assertIn("legacy owner database DSN", remote)
        self.assertIn("legacy_db_url == postgres://vane:", remote)
        self.assertIn("User=root", remote)
        self.assertIn("EnvironmentFile=/opt/vane/.env", remote)
        self.assertIn("/opt/vane/vane.service.deferred", remote)
        self.assertNotIn(
            "install -m 0644 /opt/vane/vane.service "
            "/etc/systemd/system/vane.service",
            remote,
        )
        self.assertIn("runtime-env-path", remote)
        self.assertIn("server-env-state", remote)
        self.assertIn("systemctl disable vane.service", remote)
        self.assertIn("stopped and snapshot is preserved", remote)
        self.assertNotIn("/opt/vane/bin/vane.previous", remote)
        self.assertIn("split runtime system users must have distinct UIDs", remote)
        self.assertIn("first split-runtime bootstrap refuses legacy config file", remote)
        self.assertLess(
            remote.index("initial_vane_state=$(systemctl is-active"),
            remote.index("/opt/vane/env/server.env.next"),
        )
        self.assertLess(
            remote.index("snapshot_previous_vane_release", remote.index("case")),
            remote.index("/opt/vane/env/server.env.next"),
        )
        self.assertGreater(
            remote.rindex("old_vane_recovery_required=false"),
            remote.rindex("/opt/vane/bin/gate -env /opt/vane/env/server.env"),
        )
        rollforward = remote[remote.index(
            'echo "old vane worker drain verified; starting roll-forward binary"'
        ):]
        self.assertLess(
            remote.rindex("systemctl stop vane\n", 0, remote.index(rollforward)),
            remote.index(rollforward),
        )
        self.assertIn("commit_legacy_primary_release", rollforward)
        self.assertNotIn("install -m 0755 /opt/vane/bin/vane.next", rollforward)
        commit = remote[
            remote.index("commit_legacy_primary_release()") :
            remote.index("restore_previous_vane_release()")
        ]
        self.assertIn("/etc/systemd/system/vane.service.release-next", commit)
        self.assertIn(
            "/opt/vane/env/server-owner-compat.env.release-next", commit
        )
        self.assertLess(commit.index("systemctl disable"), commit.index("mv -f"))
        self.assertLess(
            commit.index("systemctl daemon-reload"),
            commit.index("systemctl start"),
        )
        self.assertIn(
            "/opt/vane/bin/gate -env /opt/vane/env/server.env", rollforward
        )
        self.assertNotIn("/opt/vane/bin/gate -env /opt/vane/.env", remote)
        self.assertIn("-env /opt/vane/env/server.env", remote)
        self.assertNotIn("-env /opt/vane/.env", remote)


if __name__ == "__main__":
    unittest.main()
