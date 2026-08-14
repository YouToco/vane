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
    source = (ROOT / "server/api/api.go").read_text(encoding="utf-8")
    routes = sorted(set(re.findall(r'HandleFunc\("([A-Z]+ /api/[^" ]*)"', source)))
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


OUTPUTS = {
    ROOT / "contracts/http/routes.json": routes_contract,
    ROOT / "contracts/release/migrations.json": migrations_contract,
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
