#!/usr/bin/env python3
"""Authenticated production UAT and explicit Feishu canary."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


def request_json(origin: str, path: str, *, cookie: str, method: str = "GET") -> object:
    headers = {
        "Accept": "application/json",
        "Cookie": f"vane_session={cookie}",
        "Origin": origin,
        "User-Agent": "vane-production-uat/1",
    }
    data = b"{}" if method == "POST" else None
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request(origin.rstrip("/") + path, data=data, headers=headers, method=method)
    with urlopen(request, timeout=20) as response:
        if response.status != 200:
            raise RuntimeError(f"{path} returned HTTP {response.status}")
        return json.loads(response.read(1024 * 1024))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin", required=True)
    parser.add_argument("--sha", required=True)
    args = parser.parse_args()
    parsed = urlsplit(args.origin)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise RuntimeError("UAT origin must be an HTTPS origin")
    if not re.fullmatch(r"[0-9a-f]{40}", args.sha):
        raise RuntimeError("UAT revision is not an exact SHA")
    credentials = Path(os.environ.get("CREDENTIALS_DIRECTORY", ""))
    cookie_path = credentials / "uat_session_cookie"
    if not credentials.is_absolute() or cookie_path.is_symlink() or not cookie_path.is_file():
        raise RuntimeError("UAT session credential is unavailable")
    cookie = cookie_path.read_text(encoding="utf-8").strip()
    if not cookie or any(character.isspace() for character in cookie):
        raise RuntimeError("UAT session credential is malformed")

    me = request_json(args.origin, "/api/auth/me", cookie=cookie)
    if not isinstance(me, dict) or not me.get("email") or not me.get("tenant_id"):
        raise RuntimeError("authenticated /api/auth/me response is incomplete")
    schedules = request_json(args.origin, "/api/schedules/summary", cookie=cookie)
    if not isinstance(schedules, dict) or not isinstance(schedules.get("items"), list):
        raise RuntimeError("authenticated task summary response is incomplete")
    canary = request_json(args.origin, "/api/feishu/test", cookie=cookie, method="POST")
    if not isinstance(canary, dict) or canary.get("ok") is not True:
        raise RuntimeError("Feishu canary did not report success")
    print(
        json.dumps(
            {
                "schema": "vane.production-uat/v1",
                "revision": args.sha,
                "ok": True,
                "authenticated": True,
                "task_summary_items": len(schedules["items"]),
                "feishu_canary": True,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError, HTTPError, URLError) as error:
        print(f"production UAT refusal: {error}", file=sys.stderr)
        raise SystemExit(78)
