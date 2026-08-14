import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
BEGIN = "# test-anchor: remote-main-sha-begin"
END = "# test-anchor: remote-main-sha-end"
VALID_SHA = "0123456789abcdef0123456789abcdef01234567"
BASH = (
    r"C:\Program Files\Git\bin\bash.exe"
    if os.name == "nt"
    else "bash"
)


def extract_remote_main_sha() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    start = workflow.index(BEGIN)
    finish = workflow.index(END, start)
    function = workflow[start + len(BEGIN) : finish]
    return textwrap.dedent(function)


class RemoteMainSHATest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        root = Path(self.tempdir.name)
        self.secret_dir = root / "secret"
        self.secret_dir.mkdir()
        (self.secret_dir / "known_hosts").write_text(
            "github.com ssh-ed25519 test-key\n", encoding="utf-8"
        )
        self.git_called = root / "git-called"

    def run_resolver(
        self,
        *,
        output: str = "",
        git_exit: int = 0,
        key_valid: bool = True,
    ) -> subprocess.CompletedProcess[bytes]:
        script = (
            "set -euo pipefail\n"
            "secret_dir=$TEST_SECRET_DIR\n"
            "ssh-keygen() { [[ \"$FAKE_KEY_VALID\" == true ]]; }\n"
            "git() {\n"
            "  : >\"$FAKE_GIT_CALLED\"\n"
            "  printf '%b' \"$FAKE_GIT_OUTPUT\"\n"
            "  return \"$FAKE_GIT_EXIT\"\n"
            "}\n"
            f"{extract_remote_main_sha()}\n"
            "if ! backend_sha=$(remote_main_sha YouToco/vane 'test-only-key'); then\n"
            "  exit 1\n"
            "fi\n"
            "printf '%s' \"$backend_sha\"\n"
        )
        env = os.environ.copy()
        env.update(
            {
                "FAKE_GIT_CALLED": self.git_called.as_posix(),
                "FAKE_GIT_EXIT": str(git_exit),
                "FAKE_GIT_OUTPUT": output,
                "FAKE_KEY_VALID": "true" if key_valid else "false",
                "TEST_SECRET_DIR": self.secret_dir.as_posix(),
            }
        )
        return subprocess.run(
            [BASH],
            input=script.encode(),
            capture_output=True,
            env=env,
            check=False,
        )

    def assert_resolution_fails(self, result: subprocess.CompletedProcess[bytes]) -> None:
        self.assertNotEqual(result.returncode, 0, result)
        self.assertEqual(result.stdout, b"")

    def test_valid_exact_main_ref_returns_40_character_sha(self) -> None:
        result = self.run_resolver(
            output=f"{VALID_SHA}\trefs/heads/main\n",
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, VALID_SHA.encode())
        self.assertEqual(len(result.stdout), 40)

    def test_git_failure_cannot_become_empty_success(self) -> None:
        result = self.run_resolver(git_exit=23)

        self.assert_resolution_fails(result)
        self.assertIn(b"failed to resolve source main SHA", result.stderr)

    def test_empty_output_is_rejected(self) -> None:
        self.assert_resolution_fails(self.run_resolver())

    def test_wrong_ref_is_rejected(self) -> None:
        result = self.run_resolver(
            output=f"{VALID_SHA}\trefs/heads/not-main\n",
        )

        self.assert_resolution_fails(result)

    def test_malformed_sha_is_rejected(self) -> None:
        result = self.run_resolver(output="abc\trefs/heads/main\n")

        self.assert_resolution_fails(result)

    def test_extra_output_is_rejected(self) -> None:
        result = self.run_resolver(
            output=(
                f"{VALID_SHA}\trefs/heads/main\n"
                f"{VALID_SHA}\trefs/heads/extra\n"
            ),
        )

        self.assert_resolution_fails(result)

    def test_invalid_key_format_fails_before_git(self) -> None:
        result = self.run_resolver(key_valid=False)

        self.assert_resolution_fails(result)
        self.assertIn(b"source deploy key has invalid format", result.stderr)
        self.assertFalse(self.git_called.exists())


if __name__ == "__main__":
    unittest.main()
