from pathlib import Path
import importlib.util
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
DEPLOY = ROOT / "scripts" / "deploy.sh"
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
LEGACY_COMPAT_UNIT = ROOT / "deploy" / "vane-legacy-compat.service"
PRIMARY_UNIT = ROOT / "deploy" / "vane.service"
GATEWAY_UNIT = ROOT / "deploy" / "vane-research-gateway.service"
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
            "bin/agentfirstretention",
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
            "agentfirstretention": "./cmd/agentfirstretention",
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
        self.assertIn('"$payload/release-receipt.json"', deploy)
        self.assertIn(
            "incoming Agent-first release receipt differs from staged binaries",
            remote,
        )
        self.assertIn('stage_vane_digest=$(sha256sum "$stage/bin/vane"', remote)
        self.assertIn(
            'stage_collector_digest=$(sha256sum "$stage/bin/agentfirstretention"',
            remote,
        )
        self.assertNotIn("rm -rf -- /opt/vane/releases", remote)
        publisher = (ROOT / "scripts" / "publish-retention-release.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('pending=$(mktemp -d "$release_root/.', publisher)
        self.assertIn('mv -T -- "$pending" "$release_dir"', publisher)
        self.assertIn('trap cleanup EXIT', publisher)
        self.assertIn('trusted_directory "$release_root" 755', publisher)
        self.assertIn('trusted_file "$release_dir/agentfirstretention" 755', publisher)
        self.assertIn('trusted_file "$release_dir/release-receipt.json" 644', publisher)
        receipt_publish = remote.rindex('bash "$stage/publish-retention-release.sh"')
        final_gate = remote.rindex(
            "/opt/vane/bin/gate -env /opt/vane/env/server.env"
        )
        self.assertGreater(receipt_publish, final_gate)
        self.assertIn('/proc/"$vane_pid"/exe', remote[final_gate:receipt_publish])
        self.assertIn(
            "live vane process differs from the deployed artifact",
            remote[final_gate:receipt_publish],
        )

    def test_primary_release_contract_is_probed_and_bound_to_artifact(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        remote = REMOTE.read_text(encoding="utf-8")
        exact = (
            "vane.server-release-contract/v2 primary_store=owner_compat_v1 "
            "research_control_store=restricted_v1 research_store=restricted_v1"
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
            "LoadCredential=native_v3_edit_recovery_db_url:"
            "/etc/vane/credentials/native_v3_edit_recovery_db_url",
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

    def test_deploy_owned_runtime_units_do_not_depend_on_migration(self) -> None:
        primary = PRIMARY_UNIT.read_text(encoding="utf-8")
        gateway = GATEWAY_UNIT.read_text(encoding="utf-8")
        deploy = DEPLOY.read_text(encoding="utf-8")

        for unit in (primary, gateway):
            self.assertEqual(
                unit.splitlines().count("Requires=vane-research-gateway.socket"),
                1,
            )
            self.assertNotIn("vane-migrate.service", unit)
        self.assertIn('primary_unit=$(dirname "$0")/../deploy/vane.service', deploy)
        self.assertIn(
            'gateway_unit=$(dirname "$0")/../deploy/vane-research-gateway.service',
            deploy,
        )
        self.assertIn('"$primary_unit"', deploy)
        self.assertIn('"$gateway_unit"', deploy)

    def test_gateway_is_promoted_only_after_the_transient_migration_gate(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        gate = remote.index("systemd-run --quiet --wait --collect")
        promotion = remote.index(
            "mv -f -- /opt/vane/bin/vane-research-gateway.release-next"
        )

        self.assertLess(gate, promotion)
        self.assertIn(
            "install -m 0755 \"$stage/bin/vane-research-gateway\" \\\n"
            "  /opt/vane/bin/vane-research-gateway.release-next",
            remote,
        )
        before_gate = remote[:gate]
        self.assertNotIn(
            "install -m 0755 \"$stage/bin/vane-research-gateway\" "
            "/opt/vane/bin/vane-research-gateway\n",
            before_gate,
        )
        self.assertNotIn("systemctl enable vane-migrate.service", remote)
        self.assertNotIn("systemctl restart vane-migrate.service", remote)
        self.assertIn("systemctl disable vane-migrate.service", remote)
        self.assertIn(
            "migration_run_unit=vane-deploy-migrate-${stage##*/.deploy-}", remote
        )
        self.assertIn("gateway_recovery_required=true", remote[promotion - 1200 :])
        cleanup = remote[
            remote.index("cleanup_remote_deploy()") : remote.index(
                "trap cleanup_remote_deploy EXIT"
            )
        ]
        self.assertLess(
            cleanup.index("restore_previous_gateway_release"),
            cleanup.index("restore_previous_vane_release"),
        )
        self.assertGreater(
            remote.rindex("gateway_recovery_required=false"),
            remote.rindex("/opt/vane/bin/gate -env /opt/vane/env/server.env"),
        )

    def test_bootstrap_keeps_owner_out_of_long_lived_server(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")

        self.assertIn("systemd-run --quiet --wait --collect", remote)
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
            remote.index("systemd-run --quiet --wait --collect"),
            remote.index("systemctl stop vane\n"),
        )
        gateway_promotion = remote.index(
            "gateway_recovery_required=true",
            remote.index("install -d -o root -g root -m 0711"),
        )
        self.assertLess(
            remote.index(
                "systemctl start vane-research-gateway.service", gateway_promotion
            ),
            remote.index("systemctl stop vane\n", gateway_promotion),
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
            remote.index(
                "systemctl start vane-research-gateway.socket", gateway_promotion
            ),
        )
        self.assertLess(
            remote.index(
                "install -d -o root -g root -m 0711 /run/vane-research-gateway"
            ),
            remote.index("runuser -u vane -- curl", gateway_promotion),
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
        self.assertIn("stage_research_control_environment", remote)
        self.assertIn(
            "research control Store DSN does not match restricted server runtime",
            remote,
        )
        self.assertIn(
            "/opt/vane/bin/gate -env /opt/vane/env/server.env.release-next",
            remote,
        )
        for setting in (
            "VANE_DB_RESEARCH_CONTROL_URL",
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
        self.assertIn(
            "owner-compatible environment has no restricted research control Store",
            remote,
        )
        self.assertIn("contains a forbidden process credential", remote)
        self.assertIn("assert_gateway_peer_and_credential_boundary", remote)
        self.assertIn("assert_restricted_server_environment_readonly", remote)
        self.assertIn("assert_deepseek_flash_agent_route", remote)
        self.assertGreaterEqual(
            remote.count("VANE_LLM_AGENT_MODEL=deepseek-v4-flash"), 3
        )
        for forbidden in (
            "VANE_LLM_AGENT_PROVIDER",
            "VANE_LLM_AGENT_BASE_URL",
            "VANE_LLM_AGENT_API_KEY",
        ):
            self.assertIn(forbidden, remote)
        self.assertIn("primary Agent route is not exact DeepSeek v4 Flash", remote)
        self.assertIn("chown root:vane /opt/vane/env/server.env", remote)
        self.assertIn("chmod 0640 /opt/vane/env/server.env", remote)
        self.assertIn('runuser -u vane -- test -w "$path"', remote)
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

    def test_non_systemd_gates_receive_only_the_credential_directory(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        prefix = (
            "env -i PATH=/usr/bin:/bin \\\n"
            "  CREDENTIALS_DIRECTORY=/etc/vane/credentials \\\n"
            "  /opt/vane/bin/gate -env "
        )

        self.assertEqual(remote.count(prefix), 2)
        self.assertIn(prefix + "/opt/vane/env/server.env.release-next", remote)
        self.assertIn(prefix + "/opt/vane/env/server.env\n", remote)
        first_gate = remote.index(prefix)
        self.assertLess(
            remote.index(
                "\nprovision_native_v3_edit_recovery_runtime\n",
                remote.index("systemd-run --quiet --wait --collect"),
            ),
            first_gate,
        )
        self.assertLess(
            remote.index(
                "\nassert_native_v3_edit_recovery_credential\n",
                remote.index("bootstrap_marker="),
            ),
            first_gate,
        )
        second_gate = remote.rindex(prefix)
        rollforward = remote.index(
            'echo "old vane worker drain verified; starting roll-forward binary"'
        )
        self.assertLess(
            remote.index("\ncommit_legacy_primary_release\n", rollforward),
            second_gate,
        )
        self.assertNotRegex(prefix, r"postgres://|[0-9a-f]{64}")


if __name__ == "__main__":
    unittest.main()
