import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SELECTOR = ROOT / "bootstrap" / "select-aliyun-tool-archive.sh"
INSTALLERS = (
    ROOT / "bootstrap" / "install-aliyun.sh",
    ROOT / "bootstrap" / "install-ossutil.sh",
)


class AliyunToolArchitectureTests(unittest.TestCase):
    def select(self, tool: str, machine: str, system: str = "Linux") -> tuple[str, str]:
        result = subprocess.run(
            [str(SELECTOR), tool, machine, system],
            check=True,
            capture_output=True,
            text=True,
        )
        fields = result.stdout.strip().split("\t")
        self.assertEqual(len(fields), 2)
        self.assertRegex(fields[1], r"^[0-9a-f]{64}$")
        return fields[0], fields[1]

    def test_aliyun_archives_are_exactly_pinned(self) -> None:
        self.assertEqual(
            self.select("aliyun", "x86_64"),
            (
                "aliyun-cli-linux-3.4.10-amd64.tgz",
                "b9edbcc21236f14bfeebbd5e272dde6f36fd946af5802fa677475ff69839ed84",
            ),
        )
        self.assertEqual(
            self.select("aliyun", "aarch64"),
            (
                "aliyun-cli-linux-3.4.10-arm64.tgz",
                "349f3d31af9cc85aa2b444899e7d805f6409f5a53d667ce74d00dafbc17f9ae5",
            ),
        )
        self.assertEqual(
            self.select("aliyun", "arm64", "Darwin"),
            (
                "aliyun-cli-macosx-3.4.10-arm64.tgz",
                "159787b71bf9dd8efbd110ca2c209f49154a0323b535a215bb471625d58a50aa",
            ),
        )

    def test_ossutil_archives_are_exactly_pinned(self) -> None:
        self.assertEqual(
            self.select("ossutil", "amd64"),
            (
                "ossutil-2.3.0-linux-amd64.zip",
                "3ae4d9fc85a7a6e9f5654d1599766f1a3a42a3692870887b5ae9338d582ef65a",
            ),
        )
        self.assertEqual(
            self.select("ossutil", "arm64"),
            (
                "ossutil-2.3.0-linux-arm64.zip",
                "f6c95ba0c2d2ef30290af686ce4d706c701f4734ce8090bee4288a77e3f1d764",
            ),
        )
        self.assertEqual(
            self.select("ossutil", "arm64", "Darwin"),
            (
                "ossutil-2.3.0-mac-arm64.zip",
                "058fd048f321f8c80def8b748030531646eefe3a82837bf16b581ba7d9c84ac7",
            ),
        )

    def test_unknown_architecture_and_tool_fail_closed(self) -> None:
        for tool, machine, message in (
            ("aliyun", "riscv64", "unsupported machine architecture"),
            ("attacker", "x86_64", "unsupported pinned Aliyun tool"),
        ):
            with self.subTest(tool=tool, machine=machine):
                result = subprocess.run(
                    [str(SELECTOR), tool, machine, "Linux"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)

    def test_installers_select_from_the_actual_runner_architecture(self) -> None:
        for installer in INSTALLERS:
            with self.subTest(installer=installer.name):
                source = installer.read_text()
                self.assertIn("select-aliyun-tool-archive.sh", source)
                self.assertIn('"$(uname -m)"', source)
                self.assertIn('"$(uname -s)"', source)
                self.assertNotIn("linux-amd64", source)
                self.assertNotIn("linux-arm64", source)


if __name__ == "__main__":
    unittest.main()
