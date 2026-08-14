#!/usr/bin/env python3
"""SSH forced-command wrapper with no shell or arbitrary argv dispatch."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys

from controller import ALLOWED, handle, load_request, receive_bundle


def main() -> int:
    original = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    upload = re.fullmatch(r"vane-broker upload ([0-9a-f]{64}) ([1-9][0-9]{0,11})", original)
    if upload is not None:
        try:
            result = receive_bundle(
                sys.stdin.buffer,
                expected_digest=upload.group(1),
                expected_size=int(upload.group(2)),
                root=Path(os.environ.get("VANE_BROKER_REQUEST_ROOT", "/var/lib/vane-broker/requests")),
            )
            print(json.dumps(result, sort_keys=True, separators=(",", ":")))
            return 0
        except (OSError, RuntimeError, ValueError) as error:
            print(f"broker refusal: {error}", file=sys.stderr)
            return 78
    commands = {f"vane-broker {verb}": verb for verb in ALLOWED}
    verb = commands.get(original)
    if verb is None:
        print("broker refusal: command is not allowlisted", file=sys.stderr)
        return 78
    try:
        request = load_request()
        handler = None
        if os.environ.get("VANE_BROKER_TESTING") == "1" and os.geteuid() != 0:
            raw_handler = os.environ.get("VANE_BROKER_HANDLER", "")
            handler = Path(raw_handler) if raw_handler else None
        result = handle(
            verb,
            request,
            root=Path(os.environ.get("VANE_BROKER_REQUEST_ROOT", "/var/lib/vane-broker/requests")),
            repo=Path(os.environ.get("VANE_BROKER_REPO_ROOT", "/opt/vane-control/current")),
            state_root=Path(os.environ.get("VANE_BROKER_STATE_ROOT", "/var/lib/vane-broker/state")),
            handler=handler,
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result.get("ok") else 78
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"broker refusal: {error}", file=sys.stderr)
        return 78


if __name__ == "__main__":
    raise SystemExit(main())
