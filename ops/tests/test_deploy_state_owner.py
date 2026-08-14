import pathlib
import unittest


OPS = pathlib.Path(__file__).resolve().parents[1]
CLI = OPS / "bin" / "vane"
CONTROLLER = OPS / "cli" / "controller.py"
BROKER = OPS / "broker" / "controller.py"
WEB_PUBLISHER = OPS / "release" / "publish_web.py"
CERT = OPS / "certificates" / "renew-cert.sh"


class DeployStateOwnerTests(unittest.TestCase):
    def test_server_web_and_certificate_mutations_are_serialized(self) -> None:
        broker = BROKER.read_text(encoding="utf-8")
        web = WEB_PUBLISHER.read_text(encoding="utf-8")
        certificate = CERT.read_text(encoding="utf-8")
        self.assertIn('work_root / "release.lock"', broker)
        self.assertIn("fcntl.flock(lock, fcntl.LOCK_EX)", broker)
        self.assertIn('state_root / "web-release.lock"', web)
        self.assertIn("fcntl.flock(state_lock, fcntl.LOCK_EX)", web)
        self.assertIn('$state_dir/control-plane.lock', certificate)
        self.assertIn("flock 9", certificate)

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
        for retired in (
            "RUNNER_TEMP",
            "GITHUB_RUN_ID",
            "GITHUB_RUN_ATTEMPT",
            "ACTIONS_RUNTIME_TOKEN",
        ):
            self.assertNotIn(retired, active)
        self.assertIn("VANE_WORK_ROOT", active)


if __name__ == "__main__":
    unittest.main()
