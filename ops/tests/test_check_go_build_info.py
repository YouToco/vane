import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check-go-build-info.sh"
SHA = "ac36c9d967c0815ef1a0df3c7ac722823683b646"


class CheckGoBuildInfoTests(unittest.TestCase):
    def run_checker(
        self, output: str, strings_exit: int = 0
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            binary = root / "vane"
            binary.write_text("fixture")
            binary.chmod(0o755)

            fake_bin = root / "bin"
            fake_bin.mkdir()
            strings = fake_bin / "strings"
            strings.write_text(
                "#!/bin/sh\n"
                "printf '%s' \"$STRINGS_OUTPUT\"\n"
                "exit \"$STRINGS_EXIT\"\n"
            )
            strings.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:{env['PATH']}"
            env["STRINGS_OUTPUT"] = output
            env["STRINGS_EXIT"] = str(strings_exit)
            return subprocess.run(
                [str(CHECKER), str(binary), SHA],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

    def test_accepts_gnu_strings_build_prefix(self) -> None:
        result = self.run_checker(
            f"build\tvcs.revision={SHA}\n"
            "build\tvcs.modified=false\n"
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_bsd_strings_without_build_prefix(self) -> None:
        result = self.run_checker(
            f"vcs.revision={SHA}\n"
            "vcs.modified=false\n"
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_modified_binary(self) -> None:
        result = self.run_checker(
            f"build\tvcs.revision={SHA}\n"
            "build\tvcs.modified=true\n"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("modified worktree", result.stderr)

    def test_rejects_missing_clean_marker(self) -> None:
        result = self.run_checker(f"build\tvcs.revision={SHA}\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing the clean-worktree marker", result.stderr)

    def test_rejects_strings_failure(self) -> None:
        result = self.run_checker(
            f"build\tvcs.revision={SHA}\n"
            "build\tvcs.modified=false\n",
            strings_exit=7,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unable to inspect", result.stderr)

    def test_rejects_wrong_revision(self) -> None:
        result = self.run_checker(
            "build\tvcs.revision=0000000000000000000000000000000000000000\n"
            "build\tvcs.modified=false\n"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("wrong VCS revision", result.stderr)


if __name__ == "__main__":
    unittest.main()
