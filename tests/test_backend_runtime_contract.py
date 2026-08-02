from pathlib import Path
import importlib.util
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
DEPLOY = ROOT / "scripts" / "deploy.sh"
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
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

    def test_bootstrap_keeps_owner_out_of_long_lived_server(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")

        self.assertIn("systemctl restart vane-migrate.service", remote)
        self.assertIn("runtime_bootstrap_v1.complete", remote)
        self.assertIn("vane_server_runtime", remote)
        self.assertIn("vane_research_runtime", remote)
        self.assertIn("vane_research_llm_gateway_runtime", remote)
        self.assertIn("server environment contains an owner credential", remote)
        self.assertIn("/opt/vane/bin/vane.next", remote)
        self.assertLess(
            remote.index("systemctl restart vane-migrate.service"),
            remote.index("systemctl stop vane"),
        )
        self.assertLess(
            remote.index("systemctl restart vane-research-gateway.service"),
            remote.index("systemctl stop vane"),
        )
        self.assertIn("gateway_exe=$(readlink", remote)
        self.assertIn("gateway_http_code == 405", remote)
        self.assertIn("capability_source=legacy", remote)
        self.assertIn("capability_retired", remote)
        self.assertIn(
            "VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID=\\n",
            remote,
        )
        self.assertIn("old_vane_recovery_required=true", remote)
        self.assertIn("/opt/vane/bin/vane.previous", remote)
        self.assertIn("split runtime system users must have distinct UIDs", remote)
        self.assertIn("first split-runtime bootstrap refuses legacy config file", remote)
        self.assertLess(
            remote.index("systemctl stop vane"),
            remote.index("install -m 0755 /opt/vane/bin/vane.next"),
        )
        self.assertIn("-env /opt/vane/env/server.env", remote)
        self.assertNotIn("-env /opt/vane/.env", remote)


if __name__ == "__main__":
    unittest.main()
