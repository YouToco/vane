import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "scripts" / "deploy.sh"
SHA = "290bf2d8264d393116f8a87da461e58f65784844"


class FrontendCloudflareTests(unittest.TestCase):
    def test_verifies_canonical_owner_preview_headers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            home = root / "home"
            payload = root / "payload"
            receipts = root / "receipts"
            fake_bin = root / "fake-bin"
            for directory in (home, payload / "dist", fake_bin):
                directory.mkdir(parents=True)
            (payload / "dist" / "index.html").write_text("<!doctype html>")
            preview = (
                payload
                / "dist"
                / "_preview"
                / "p0a-7d7f47e8506f4e49aa8cb4bfdab78e42"
                / "index.html"
            )
            preview.parent.mkdir(parents=True)
            preview.write_text("<!doctype html>")

            flock = fake_bin / "flock"
            flock.write_text("#!/bin/sh\nexit 0\n")
            flock.chmod(0o755)

            wrangler_log = root / "wrangler-calls"
            wrangler = fake_bin / "wrangler"
            wrangler.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"--version\" ]; then\n"
                "  printf '4.115.0\\n'\n"
                "  exit 0\n"
                "fi\n"
                "printf '%s\\n' \"$*\" >>\"$WRANGLER_CALL_LOG\"\n"
            )
            wrangler.chmod(0o755)

            curl_log = root / "curl-calls"
            curl = fake_bin / "curl"
            curl.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' \"$*\" >>\"$CURL_CALL_LOG\"\n"
                "printf 'HTTP/2 200\\r\\n'\n"
                "printf 'Cache-Control: no-store\\r\\n'\n"
                "printf 'X-Robots-Tag: noindex, nofollow, noarchive\\r\\n'\n"
                "printf \"Content-Security-Policy: default-src 'self'; "
                "connect-src 'none'\\r\\n\"\n"
            )
            curl.chmod(0o755)

            env = os.environ.copy()
            env.update(
                {
                    "CLOUDFLARE_ACCOUNT_ID": "TESTACCOUNT",
                    "CLOUDFLARE_API_TOKEN": "TESTTOKEN",
                    "CURL_CALL_LOG": str(curl_log),
                    "EXPECTED_DEPLOYED_SHA": "",
                    "FRONTEND_RECEIPT_DIR": str(receipts),
                    "HOME": str(home),
                    "PATH": f"{fake_bin}:/usr/bin:/bin",
                    "WRANGLER_BIN": str(wrangler),
                    "WRANGLER_CALL_LOG": str(wrangler_log),
                }
            )
            result = subprocess.run(
                [str(DEPLOY), "frontend-cloudflare", str(payload), SHA],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                wrangler_log.read_text().strip(),
                "pages deploy dist --project-name vane-web --branch main",
            )
            curl_call = curl_log.read_text().strip()
            self.assertIn("--head", curl_call)
            self.assertIn(
                "https://vane-web.pages.dev/_preview/"
                "p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/",
                curl_call,
            )
            self.assertEqual(
                (receipts / "cloudflare.sha").read_text(),
                f"{SHA}\n",
            )


if __name__ == "__main__":
    unittest.main()
