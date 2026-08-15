#!/usr/bin/env python3
"""Generate stable contracts from their canonical implementation sources."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def routes_contract() -> dict[str, object]:
    sources = [
        (ROOT / "server/api/api.go").read_text(encoding="utf-8"),
        (ROOT / "server/cmd/server/main.go").read_text(encoding="utf-8"),
        (ROOT / "server/cmd/server/telegram_wiring.go").read_text(encoding="utf-8"),
    ]
    routes = sorted(
        set(
            re.findall(
                r'Handle(?:Func)?\("([A-Z]+ /(?:api|telegram)/[^" ]*)"',
                "\n".join(sources),
            )
        )
    )
    if not routes:
        raise RuntimeError("no server API routes discovered")
    return {"schema": "vane.http-routes/v1", "routes": routes}


def migrations_contract() -> dict[str, object]:
    migration_root = ROOT / "server/store/migrations"
    files = []
    for path in sorted(migration_root.glob("*.sql")):
        files.append(
            {
                "path": path.name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    if not files:
        raise RuntimeError("no server migrations discovered")
    return {"schema": "vane.migrations/v1", "files": files}


def temporal_registration_contract() -> dict[str, object]:
    wiring = (ROOT / "server/cmd/server/main.go").read_text(encoding="utf-8")
    config = (ROOT / "server/config/config.go").read_text(encoding="utf-8")
    schedule = (ROOT / "server/scheduler/task_schedule.go").read_text(encoding="utf-8")

    workflow_calls = re.findall(r"w\.(RegisterWorkflow(?:WithOptions)?)\(([^\n]+)\)", wiring)
    activity_calls = re.findall(r"w\.(RegisterActivity(?:WithOptions)?)\(([^\n]+)\)", wiring)
    if not workflow_calls or not activity_calls:
        raise RuntimeError("no production Temporal registrations discovered")
    if any(method != "RegisterWorkflow" for method, _ in workflow_calls):
        raise RuntimeError("explicit workflow registration options require generator support")
    if any(method != "RegisterActivity" for method, _ in activity_calls):
        raise RuntimeError("explicit activity registration options require generator support")

    def symbol(arguments: str) -> str:
        match = re.fullmatch(r"(?:workflow|periodicbrief|activities|periodicActivities)\.(\w+)", arguments.strip())
        if match is None:
            raise RuntimeError(f"unsupported Temporal registration expression: {arguments}")
        return match.group(1)

    queue = re.search(
        r'v\.SetDefault\("temporal\.task_queue",\s*"([^"]+)"\)', config
    )
    converter = re.search(
        r'taskScheduleDefaultConverterID\s*=\s*"([^"]+)"', schedule
    )
    if queue is None or converter is None:
        raise RuntimeError("Temporal task queue or converter identity was not discovered")
    return {
        "schema": "vane.temporal-registration/v1",
        "default_task_queue": queue.group(1),
        "default_converter": converter.group(1),
        "workflows": sorted(symbol(arguments) for _, arguments in workflow_calls),
        "activities": sorted(symbol(arguments) for _, arguments in activity_calls),
    }


OUTPUTS = {
    ROOT / "contracts/http/routes.json": routes_contract,
    ROOT / "contracts/release/migrations.json": migrations_contract,
    ROOT / "contracts/temporal/production-registration.json": temporal_registration_contract,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    drift = []
    for path, build in OUTPUTS.items():
        expected = canonical_json(build())
        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(expected, encoding="utf-8")
        elif not path.is_file() or path.read_text(encoding="utf-8") != expected:
            drift.append(path.relative_to(ROOT).as_posix())
    if drift:
        raise SystemExit("generated contract drift: " + ", ".join(drift))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
