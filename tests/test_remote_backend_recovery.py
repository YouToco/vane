from pathlib import Path
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
BASH = r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt" else "bash"


def extract_recovery() -> str:
    script = REMOTE.read_text(encoding="utf-8")
    start = script.index("old_vane_recovery_required=false")
    finish = script.index("trap cleanup_remote_deploy EXIT", start)
    return script[start:finish]


class RemoteBackendRecoveryTest(unittest.TestCase):
    def run_recovery(
        self, *, binary_replaced: bool, restart_safe: bool
    ) -> tuple[subprocess.CompletedProcess[bytes], str]:
        with tempfile.TemporaryDirectory() as tempdir:
            log = Path(tempdir) / "recovery.log"
            script = (
                "set -euo pipefail\n"
                f"{extract_recovery()}\n"
                f"old_vane_recovery_required=true\n"
                f"vane_binary_replaced={'true' if binary_replaced else 'false'}\n"
                f"old_vane_restart_safe={'true' if restart_safe else 'false'}\n"
                "stage=/opt/vane/.deploy-" + "0" * 40 + "-1-1\n"
                "install() { printf 'install %s\\n' \"$*\" >>\"$LOG\"; }\n"
                "systemctl() { printf 'systemctl %s\\n' \"$*\" >>\"$LOG\"; return 0; }\n"
                "vane_ready() { return 0; }\n"
                "sleep() { :; }\n"
                "rm() { printf 'rm %s\\n' \"$*\" >>\"$LOG\"; }\n"
                "set +e\n"
                "(exit 23)\n"
                "cleanup_remote_deploy\n"
            )
            env = os.environ.copy()
            env["LOG"] = log.as_posix()
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, env=env, check=False
            )
            return result, log.read_text(encoding="utf-8")

    def test_clean_drain_failure_restarts_untouched_previous_service(self) -> None:
        result, log = self.run_recovery(
            binary_replaced=False, restart_safe=True
        )

        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertIn("systemctl start vane.service", log)

    def test_new_binary_failure_refuses_unsafe_rollback(self) -> None:
        result, log = self.run_recovery(
            binary_replaced=True, restart_safe=True
        )

        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertNotIn("vane.previous", log)
        self.assertNotIn("systemctl start vane.service", log)
        self.assertIn(b"automatic binary rollback is unsafe", result.stderr)

    def test_unproven_drain_refuses_to_create_a_second_writer(self) -> None:
        result, log = self.run_recovery(
            binary_replaced=False, restart_safe=False
        )

        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertNotIn("vane.previous", log)
        self.assertNotIn("systemctl start vane.service", log)
        self.assertIn(b"drain was not proven clean", result.stderr)


if __name__ == "__main__":
    unittest.main()
