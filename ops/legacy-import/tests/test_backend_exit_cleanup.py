from pathlib import Path
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "scripts" / "deploy.sh"
BASH = (
    r"C:\Program Files\Git\bin\bash.exe"
    if os.name == "nt"
    else "bash"
)


def extract_backend_cleanup() -> str:
    deploy = DEPLOY.read_text(encoding="utf-8")
    start = deploy.index("backend_ssh_dir=")
    finish = deploy.index("\n\nrequire_env()", start)
    return deploy[start:finish]


class BackendExitCleanupTest(unittest.TestCase):
    def test_exit_trap_keeps_remote_and_local_cleanup_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ssh_log = root / "ssh.log"
            rm_log = root / "rm.log"
            script = (
                "set -u\n"
                f"{extract_backend_cleanup()}\n"
                "ssh() { printf '%s\\n' \"$*\" >>\"$SSH_LOG\"; }\n"
                "rm() { printf '%s\\n' \"$*\" >>\"$RM_LOG\"; }\n"
                "simulate_backend_failure() {\n"
                "  backend_ssh_dir=/tmp/vane-ssh.test\n"
                "  backend_remote_stage=/opt/vane/.deploy-test\n"
                "  backend_ssh_target=vane@example.test\n"
                "  backend_remote_stage_created=true\n"
                "  backend_ssh_opts=(-p 2222)\n"
                "  trap cleanup_backend EXIT\n"
                "  return 23\n"
                "}\n"
                "simulate_backend_failure\n"
                "exit $?\n"
            )
            env = os.environ.copy()
            env.update(
                {
                    "SSH_LOG": ssh_log.as_posix(),
                    "RM_LOG": rm_log.as_posix(),
                }
            )

            result = subprocess.run(
                [BASH],
                input=script.encode(),
                capture_output=True,
                env=env,
                check=False,
            )

            self.assertEqual(result.returncode, 23, result.stderr.decode())
            self.assertIn(
                "vane@example.test rm -rf -- /opt/vane/.deploy-test",
                ssh_log.read_text(encoding="utf-8"),
            )
            self.assertIn(
                "-rf -- /tmp/vane-ssh.test",
                rm_log.read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
