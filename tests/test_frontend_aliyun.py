import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "scripts" / "deploy.sh"
SHA = "190bf2d8264d393116f8a87da461e58f65784843"


class FrontendAliyunTests(unittest.TestCase):
    def test_uses_ossutil_v2_with_environment_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            home = root / "home"
            runner_temp = root / "runner-temp"
            payload = root / "payload"
            receipts = root / "receipts"
            fake_bin = root / "fake-bin"
            for directory in (
                home,
                runner_temp,
                payload / "dist",
                fake_bin,
            ):
                directory.mkdir(parents=True)
            (payload / "dist" / "index.html").write_text("<!doctype html>")

            flock = fake_bin / "flock"
            flock.write_text("#!/bin/sh\nexit 0\n")
            flock.chmod(0o755)

            aliyun_call_log = root / "aliyun-calls"
            aliyun = fake_bin / "aliyun"
            aliyun.write_text(
                "#!/bin/sh\n"
                "{\n"
                "  printf 'argv'\n"
                "  printf '\\t%s' \"$@\"\n"
                "  printf '\\tALI_ID=%s\\tALI_SECRET=%s\\tALI_REGION=%s"
                "\\tIGNORE_PROFILE=%s\\n' "
                "\"${ALIBABA_CLOUD_ACCESS_KEY_ID-}\" "
                "\"${ALIBABA_CLOUD_ACCESS_KEY_SECRET-}\" "
                "\"${ALIBABA_CLOUD_REGION_ID-}\" "
                "\"${ALIBABA_CLOUD_IGNORE_PROFILE-}\"\n"
                "} >>\"$ALIYUN_CALL_LOG\"\n"
            )
            aliyun.chmod(0o755)

            ossutil_call_log = root / "ossutil-calls"
            ossutil = fake_bin / "ossutil"
            ossutil.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = version ]; then\n"
                "  printf '2.3.0\\n'\n"
                "  exit 0\n"
                "fi\n"
                "{\n"
                "  printf 'argv'\n"
                "  printf '\\t%s' \"$@\"\n"
                "  printf '\\tOSS_ID=%s\\tOSS_SECRET=%s\\tOSS_REGION=%s\\n' "
                "\"${OSS_ACCESS_KEY_ID-}\" "
                "\"${OSS_ACCESS_KEY_SECRET-}\" "
                "\"${OSS_REGION-}\"\n"
                "} >>\"$OSSUTIL_CALL_LOG\"\n"
            )
            ossutil.chmod(0o755)

            access_id = "TESTACCESSID"
            access_secret = "TESTACCESSSECRET"
            env = os.environ.copy()
            env.update(
                {
                    "ALIYUN_ACCESS_KEY_ID": access_id,
                    "ALIYUN_ACCESS_KEY_SECRET": access_secret,
                    "ALIYUN_BIN": str(aliyun),
                    "ALIYUN_CALL_LOG": str(aliyun_call_log),
                    "EXPECTED_DEPLOYED_SHA": "",
                    "FRONTEND_RECEIPT_DIR": str(receipts),
                    "HOME": str(home),
                    "OSSUTIL_BIN": str(ossutil),
                    "OSSUTIL_CALL_LOG": str(ossutil_call_log),
                    "PATH": f"{fake_bin}:/usr/bin:/bin",
                    "RUNNER_TEMP": str(runner_temp),
                }
            )
            result = subprocess.run(
                [str(DEPLOY), "frontend-aliyun", str(payload), SHA],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            aliyun_calls = aliyun_call_log.read_text().splitlines()
            self.assertEqual(len(aliyun_calls), 1, aliyun_calls)
            cdn = aliyun_calls[0]
            ossutil_calls = ossutil_call_log.read_text().splitlines()
            self.assertEqual(len(ossutil_calls), 1, ossutil_calls)
            ossutil_sync = ossutil_calls[0]
            self.assertIn("\tsync\t", ossutil_sync)
            self.assertNotIn("--config-path", ossutil_sync)
            self.assertNotIn(
                access_id, ossutil_sync.split("\tOSS_ID=", 1)[0]
            )
            self.assertNotIn(
                access_secret, ossutil_sync.split("\tOSS_ID=", 1)[0]
            )
            self.assertIn(f"\tOSS_ID={access_id}", ossutil_sync)
            self.assertIn(f"\tOSS_SECRET={access_secret}", ossutil_sync)
            self.assertIn("\tOSS_REGION=cn-shenzhen", ossutil_sync)
            self.assertIn("\tcdn\tRefreshObjectCaches\t", cdn)
            self.assertNotIn(access_id, cdn.split("\tALI_ID=", 1)[0])
            self.assertNotIn(access_secret, cdn.split("\tALI_ID=", 1)[0])
            self.assertIn(f"\tALI_ID={access_id}", cdn)
            self.assertIn(f"\tALI_SECRET={access_secret}", cdn)
            self.assertIn("\tALI_REGION=cn-shenzhen", cdn)
            self.assertIn("\tIGNORE_PROFILE=TRUE", cdn)
            self.assertEqual((receipts / "aliyun.sha").read_text(), f"{SHA}\n")
            self.assertFalse(
                (
                    home
                    / ".local"
                    / "state"
                    / "vane-deploy"
                    / "deployed-vane-web.sha"
                ).exists()
            )


if __name__ == "__main__":
    unittest.main()
