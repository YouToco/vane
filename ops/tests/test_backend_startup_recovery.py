from pathlib import Path
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
REMOTE_DEPLOY = ROOT / "scripts" / "remote-backend-deploy.sh"
BEGIN = "# test-anchor: vane-startup-wait-begin"
END = "# test-anchor: vane-startup-wait-end"
BASH = (
    r"C:\Program Files\Git\bin\bash.exe"
    if os.name == "nt"
    else "bash"
)


def extract_startup_wait() -> str:
    script = REMOTE_DEPLOY.read_text(encoding="utf-8")
    start = script.index(BEGIN) + len(BEGIN)
    finish = script.index(END, start)
    return script[start:finish]


class BackendStartupRecoveryTest(unittest.TestCase):
    def run_wait(
        self, states: list[str], readiness: list[str]
    ) -> subprocess.CompletedProcess[bytes]:
        with tempfile.TemporaryDirectory() as tempdir:
            state_file = Path(tempdir) / "states"
            ready_file = Path(tempdir) / "readiness"
            wait_log = Path(tempdir) / "wait.log"
            state_file.write_text("\n".join(states) + "\n", encoding="utf-8")
            ready_file.write_text("\n".join(readiness) + "\n", encoding="utf-8")
            script = (
                "set -euo pipefail\n"
                f"{extract_startup_wait()}\n"
                "vane_service_state() {\n"
                "  local state\n"
                "  state=$(head -n 1 \"$STATE_FILE\")\n"
                "  tail -n +2 \"$STATE_FILE\" >\"$STATE_FILE.next\"\n"
                "  mv \"$STATE_FILE.next\" \"$STATE_FILE\"\n"
                "  printf '%s' \"$state\"\n"
                "}\n"
                "vane_ready() {\n"
                "  local ready\n"
                "  ready=$(head -n 1 \"$READY_FILE\")\n"
                "  tail -n +2 \"$READY_FILE\" >\"$READY_FILE.next\"\n"
                "  mv \"$READY_FILE.next\" \"$READY_FILE\"\n"
                "  [[ $ready == ready ]]\n"
                "}\n"
                "sleep() { printf 'sleep\\n' >>\"$WAIT_LOG\"; }\n"
                "print_vane_startup_diagnostics() {\n"
                "  printf 'diagnostics\\n' >>\"$WAIT_LOG\"\n"
                "}\n"
                "wait_for_vane_ready\n"
            )
            env = os.environ.copy()
            env.update(
                {
                    "STATE_FILE": state_file.as_posix(),
                    "READY_FILE": ready_file.as_posix(),
                    "WAIT_LOG": wait_log.as_posix(),
                }
            )
            return subprocess.run(
                [BASH],
                input=script.encode(),
                capture_output=True,
                env=env,
                check=False,
            )

    def test_waits_through_activating_and_accepts_ready(self) -> None:
        result = self.run_wait(["activating", "active"], ["ready"])

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertIn(b"state=activating attempt=1/12", result.stdout)

    def test_waits_when_systemd_is_active_but_http_is_not_ready(self) -> None:
        result = self.run_wait(
            ["active", "active", "active"],
            ["not-ready", "not-ready", "ready"],
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertIn(b"state=active attempt=1/12", result.stdout)
        self.assertIn(b"state=active attempt=2/12", result.stdout)

    def test_fails_after_bounded_attempts(self) -> None:
        result = self.run_wait(["activating"] * 12, ["not-ready"] * 12)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"did not become ready within 60 seconds", result.stderr)


if __name__ == "__main__":
    unittest.main()
