import pathlib
import unittest


OPS = pathlib.Path(__file__).resolve().parents[1]
CLI = OPS / "bin" / "vane"
CONTROLLER = OPS / "cli" / "controller.py"
DEPLOY = OPS / "release" / "deploy.sh"
CERT = OPS / "certificates" / "renew-cert.sh"


class DeployStateOwnerTests(unittest.TestCase):
    def test_deploy_and_certificate_share_the_durable_lock(self) -> None:
        for path in (DEPLOY, CERT):
            source = path.read_text(encoding="utf-8")
            self.assertIn("$state_dir/control-plane.lock", source, path)
            self.assertIn("flock 9", source, path)

    def test_repository_cli_has_no_production_mutation_implementation(self) -> None:
        source = CONTROLLER.read_text(encoding="utf-8")
        self.assertIn("root-owned broker", source)
        self.assertIn("broker_required", source)
        for forbidden in (
            "CLOUDFLARE_API_TOKEN",
            "ALIYUN_ACCESS_KEY_SECRET",
            "VPS_SSH_KEY",
            "POSTGRES_PASSWORD",
        ):
            self.assertNotIn(forbidden, source)

    def test_only_one_operator_facing_executable_exists(self) -> None:
        entries = [path for path in (OPS / "bin").iterdir() if path.is_file()]
        self.assertEqual(entries, [CLI])
        self.assertLess(len(CLI.read_text(encoding="utf-8").splitlines()), 20)

    def test_active_scripts_do_not_depend_on_actions_environment(self) -> None:
        active = "\n".join(
            path.read_text(encoding="utf-8")
            for path in OPS.glob("**/*")
            if path.is_file() and path.suffix in {".sh", ".py"}
            and "tests" not in path.parts
        )
        for retired in ("RUNNER_TEMP", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"):
            self.assertNotIn(retired, active)
        self.assertIn("VANE_WORK_ROOT", active)
        self.assertIn("VANE_RELEASE_ATTEMPT_ID", active)


if __name__ == "__main__":
    unittest.main()
