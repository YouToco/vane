import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
CHECKER = ROOT / "audit" / "check-go-build-info.sh"
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

    def test_accepts_exact_clean_release_build_id(self) -> None:
        result = self.run_checker(f"vane/{SHA}/clean\n")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_missing_clean_release_build_id(self) -> None:
        result = self.run_checker(f"vane/{SHA}/dirty\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("wrong or missing release build ID", result.stderr)

    def test_rejects_strings_failure(self) -> None:
        result = self.run_checker(
            f"vane/{SHA}/clean\n",
            strings_exit=7,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unable to inspect", result.stderr)

    def test_rejects_wrong_revision(self) -> None:
        result = self.run_checker(
            "vane/0000000000000000000000000000000000000000/clean\n"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("wrong or missing release build ID", result.stderr)


if __name__ == "__main__":
    unittest.main()
