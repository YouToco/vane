from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "vane_production_uat", ROOT / "ops/audit/production-uat.py"
)
assert SPEC is not None and SPEC.loader is not None
production_uat = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(production_uat)
REVISION = "0123456789abcdef0123456789abcdef01234567"


class Response:
    def __init__(self, value: object, status: int = 200) -> None:
        self.status = status
        self.payload = json.dumps(value).encode()

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        if len(self.payload) > limit:
            raise AssertionError("fixture exceeded response limit")
        return self.payload


class ProductionUATTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.credentials = Path(self.temp.name)
        (self.credentials / "uat_session_cookie").write_text(
            "fixture-cookie\n", encoding="ascii"
        )

    def run_main(
        self,
        api_origin: str = "https://api.vane.example",
        web_origin: str = "https://vane.example",
    ) -> int:
        argv = [
            "production-uat.py",
            "--api-origin",
            api_origin,
            "--web-origin",
            web_origin,
            "--sha",
            REVISION,
        ]
        with mock.patch.object(production_uat.sys, "argv", argv), mock.patch.dict(
            os.environ,
            {"CREDENTIALS_DIRECTORY": str(self.credentials)},
            clear=False,
        ):
            return production_uat.main()

    def test_authenticated_uat_checks_user_tasks_and_feishu(self) -> None:
        requests = []
        responses = iter(
            [
                Response({"email": "owner@example.test", "tenant_id": 1}),
                Response({"items": [{"id": 1}]}),
                Response({"ok": True}),
            ]
        )

        def open_request(request, timeout: int):
            requests.append((request, timeout))
            return next(responses)

        with mock.patch.object(production_uat.OPENER, "open", side_effect=open_request):
            self.assertEqual(self.run_main(), 0)

        self.assertEqual([request.get_method() for request, _ in requests], ["GET", "GET", "POST"])
        self.assertTrue(all(timeout == 20 for _, timeout in requests))
        self.assertTrue(
            all(request.get_header("Cookie") == "vane_session=fixture-cookie" for request, _ in requests)
        )
        self.assertTrue(
            all(request.get_header("Origin") == "https://vane.example" for request, _ in requests)
        )
        self.assertTrue(
            all(request.full_url.startswith("https://api.vane.example/") for request, _ in requests)
        )

    def test_uat_rejects_non_origin_url_and_bad_revision(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "HTTPS origin"):
            self.run_main("https://api.vane.example/path")
        argv = [
            "production-uat.py",
            "--api-origin",
            "https://api.vane.example",
            "--web-origin",
            "https://vane.example",
            "--sha",
            "main",
        ]
        with mock.patch.object(production_uat.sys, "argv", argv):
            with self.assertRaisesRegex(RuntimeError, "exact SHA"):
                production_uat.main()

    def test_uat_rejects_incomplete_authenticated_response(self) -> None:
        with mock.patch.object(
            production_uat.OPENER,
            "open",
            return_value=Response({"email": "", "tenant_id": 0}),
        ):
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                self.run_main()

    def test_redirects_and_duplicate_json_keys_are_rejected(self) -> None:
        self.assertIsNone(
            production_uat.RejectRedirects().redirect_request(
                object(), object(), 302, "redirect", {}, "https://other.example"
            )
        )
        with self.assertRaisesRegex(RuntimeError, "duplicate key"):
            production_uat.strict_json(b'{"ok":false,"ok":true}')


if __name__ == "__main__":
    unittest.main()
