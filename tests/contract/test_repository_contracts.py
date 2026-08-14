from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path

from generate_contracts import ROOT, OUTPUTS, canonical_json


class GeneratedContractsTest(unittest.TestCase):
    def test_generated_contracts_are_current(self) -> None:
        for path, build in OUTPUTS.items():
            self.assertTrue(path.is_file(), path)
            self.assertEqual(path.read_text(encoding="utf-8"), canonical_json(build()))

    def test_temporal_registration_matches_production_wiring(self) -> None:
        contract = json.loads(
            (ROOT / "contracts/temporal/production-registration.json").read_text(
                encoding="utf-8"
            )
        )
        source = (ROOT / "server/cmd/server/main.go").read_text(encoding="utf-8")
        workflows = set(re.findall(r"w\.RegisterWorkflow\((?:workflow|periodicbrief)\.(\w+)\)", source))
        activities = set(
            re.findall(
                r"w\.RegisterActivity\((?:activities|periodicActivities)\.(\w+)\)",
                source,
            )
        )
        self.assertEqual(workflows, set(contract["workflows"]))
        self.assertEqual(activities, set(contract["activities"]))

    def test_release_binary_inventory_is_complete(self) -> None:
        contract = json.loads(
            (ROOT / "contracts/release/server-binaries.json").read_text(encoding="utf-8")
        )
        entries = contract["binaries"]
        self.assertEqual(len(entries), 10)
        self.assertEqual(len({entry["name"] for entry in entries}), 10)
        makefile = (ROOT / "server/Makefile").read_text(encoding="utf-8")
        for entry in entries:
            self.assertTrue((ROOT / "server" / entry["package"].removeprefix("./")).is_dir())
            self.assertIn(f'{entry["name"]}={entry["package"]}', makefile)


class RepositoryPolicyTest(unittest.TestCase):
    def test_no_github_actions(self) -> None:
        self.assertFalse(any(ROOT.glob("**/.github/workflows/*")))

    def test_module_and_lockfile_authorities(self) -> None:
        go_modules = sorted(path.relative_to(ROOT).as_posix() for path in ROOT.glob("**/go.mod"))
        self.assertEqual(
            go_modules,
            ["server/go.mod", "server/third_party/oapi-sdk-go/v3/go.mod"],
        )
        lockfiles = sorted(
            path.relative_to(ROOT).as_posix() for path in ROOT.glob("**/package-lock.json")
        )
        self.assertEqual(lockfiles, ["tools/wrangler/package-lock.json", "web/package-lock.json"])

    def test_no_ambiguous_root_directories(self) -> None:
        for name in ("scripts", "deploy", "common", "utils", "pkg", "packages"):
            self.assertFalse((ROOT / name).exists(), name)

    def test_no_production_go_outside_server(self) -> None:
        offenders = [
            path.relative_to(ROOT).as_posix()
            for path in ROOT.glob("**/*.go")
            if "server" not in path.relative_to(ROOT).parts
        ]
        self.assertEqual(offenders, [])

    def test_infra_is_declarative(self) -> None:
        offenders = [
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / "infra").glob("**/*")
            if path.is_file() and path.suffix in {".py", ".sh"}
        ]
        self.assertEqual(offenders, [])

    def test_ops_does_not_duplicate_runtime_config(self) -> None:
        forbidden = {"Caddyfile", "docker-compose.yml", "docker-compose.yaml"}
        offenders = [
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / "ops").glob("**/*")
            if path.is_file() and (path.name in forbidden or path.suffix in {".service", ".socket"})
        ]
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
