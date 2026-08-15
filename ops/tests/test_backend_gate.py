import json
import os
import pathlib
import subprocess
import tempfile
import unittest


OPS = pathlib.Path(__file__).resolve().parents[1]
ROOT = OPS.parent
CLI = OPS / "bin" / "vane"


class LocalGatePolicyTests(unittest.TestCase):
    def test_required_release_tool_versions_are_exact(self) -> None:
        lock = json.loads(
            (ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8")
        )
        versions = {name: value["version"] for name, value in lock["tools"].items()}
        self.assertEqual(versions["go"], "1.26.6")
        self.assertEqual(versions["node"], "22.23.2")
        self.assertEqual(versions["temporal_cli"], "1.8.2")
        self.assertEqual(versions["govulncheck"], "1.7.0")
        self.assertEqual(versions["postgres"], "18")
        self.assertEqual(versions["temporal_server"], "1.29.7")
        self.assertEqual(versions["temporal_ui"], "2.52.1")
        self.assertEqual(versions["caddy"], "2.10.2")
        self.assertEqual(versions["shellcheck"], "0.11.0")
        self.assertNotIn("actionlint", versions)
        self.assertNotIn("UNRESOLVED", json.dumps(lock))

    def test_doctor_checks_real_executables_downloads_and_signer(self) -> None:
        with tempfile.TemporaryDirectory() as empty_cache:
            empty_signers = pathlib.Path(empty_cache) / "allowed_signers"
            empty_signers.write_text("# deliberately empty fixture\n", encoding="utf-8")
            result = subprocess.run(
                [
                    str(CLI),
                    "doctor",
                    "--json",
                    "--allowed-signers",
                    str(empty_signers),
                ],
                cwd=ROOT,
                env={**os.environ, "VANE_TOOL_CACHE": empty_cache},
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertEqual(result.returncode, 78, result)
        report = json.loads(result.stdout)
        self.assertFalse(report["ok"])
        self.assertTrue(
            any("locked executable" in error for error in report["errors"])
        )
        self.assertTrue(any("locked download" in error for error in report["errors"]))
        self.assertTrue(any("allowed signer" in error for error in report["errors"]))

    def test_policy_has_no_skipped_test_allowlist(self) -> None:
        policy = json.loads(
            (OPS / "policy/release-policy.json").read_text(encoding="utf-8")
        )
        self.assertEqual(policy["skip_allowlist"], [])
        self.assertEqual(
            policy["production_mutation_authority"], "external-root-owned-broker"
        )
        self.assertEqual(
            policy["broker_endpoint"], {
                "host": "188.253.125.238",
                "port": 10058,
                "known_hosts_sha256": "755558dd29ed29400289842f639f31856f12eb0c25654a6d9007fd572163e5d3",
            }
        )
        from ops.cli import controller
        self.assertEqual(
            policy["broker_endpoint"],
            {
                "host": controller.BROKER_HOST,
                "port": controller.BROKER_PORT,
                "known_hosts_sha256": controller.BROKER_KNOWN_HOSTS_SHA256,
            },
        )


if __name__ == "__main__":
    unittest.main()
