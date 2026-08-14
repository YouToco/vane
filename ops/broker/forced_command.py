#!/usr/bin/env python3
"""SSH forced-command wrapper with no shell or arbitrary argv dispatch."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from controller import ALLOWED, handle, load_request


def main() -> int:
    original = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    commands = {f"vane-broker {verb}": verb for verb in ALLOWED}
    verb = commands.get(original)
    if verb is None:
        print("broker refusal: command is not allowlisted", file=sys.stderr)
        return 78
    try:
        request = load_request()
        result = handle(
            verb,
            request,
            root=Path(os.environ.get("VANE_BROKER_REQUEST_ROOT", "/var/lib/vane-broker/requests")),
            repo=Path(os.environ.get("VANE_BROKER_REPO_ROOT", "/opt/vane-control")),
            state_root=Path(os.environ.get("VANE_BROKER_STATE_ROOT", "/var/lib/vane-broker/state")),
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result.get("ok") else 78
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"broker refusal: {error}", file=sys.stderr)
        return 78


if __name__ == "__main__":
    raise SystemExit(main())
