from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path

from generate_contracts import ROOT, OUTPUTS, canonical_json


def repository_files() -> list[Path]:
    """Return tracked plus non-ignored untracked files, never local caches."""
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [
        ROOT / value.decode()
        for value in result.stdout.split(b"\0")
        if value and (ROOT / value.decode()).is_file()
    ]


class GeneratedContractsTest(unittest.TestCase):
    def test_generated_contracts_are_current(self) -> None:
        for path, build in OUTPUTS.items():
            self.assertTrue(path.is_file(), path)
            self.assertEqual(path.read_text(encoding="utf-8"), canonical_json(build()))

    def test_telegram_webhook_is_in_http_route_contract(self) -> None:
        contract = json.loads(
            (ROOT / "contracts/http/routes.json").read_text(encoding="utf-8")
        )
        self.assertIn("POST /telegram/webhook", contract["routes"])

    def test_release_binary_inventory_is_complete(self) -> None:
        contract = json.loads(
            (ROOT / "contracts/release/server-binaries.json").read_text(encoding="utf-8")
        )
        entries = contract["binaries"]
        self.assertEqual(contract["schema"], "vane.server-binaries/v2")
        self.assertEqual(len(entries), 5)
        self.assertEqual(len({entry["name"] for entry in entries}), 5)
        self.assertEqual(
            {entry["role"] for entry in entries},
            {"service", "migration", "verification", "maintenance"},
        )
        makefile = (ROOT / "server/Makefile").read_text(encoding="utf-8")
        for entry in entries:
            self.assertTrue((ROOT / "server" / entry["package"].removeprefix("./")).is_dir())
            self.assertIn(f'{entry["name"]}={entry["package"]}', makefile)


class RepositoryPolicyTest(unittest.TestCase):
    def test_github_actions_are_immutable_and_release_labeled(self) -> None:
        """GitHub Actions are allowed (owner decision to override the former
        no-GitHub-Actions contract), but every `uses:` must be a pinned 40-char
        SHA with a release-tag comment, matching the toolchain-lock policy.
        """
        workflows = sorted((ROOT / ".github" / "workflows").glob("*.y*ml"))
        for path in workflows:
            text = path.read_text(encoding="utf-8")
            refs = re.findall(
                r"^\s*uses:\s+([^@\s]+)@([^\s#]+)(?:\s+#\s+([^\s]+))?\s*$",
                text,
                flags=re.MULTILINE,
            )
            for action, revision, release in refs:
                with self.subTest(workflow=path.name, action=action):
                    self.assertRegex(revision, r"^[0-9a-f]{40}$")
                    self.assertRegex(release or "", r"^v\d+(?:\.\d+){1,2}$")

    def test_module_and_lockfile_authorities(self) -> None:
        files = repository_files()
        go_modules = sorted(
            path.relative_to(ROOT).as_posix() for path in files if path.name == "go.mod"
        )
        self.assertEqual(
            go_modules,
            ["server/go.mod", "server/third_party/oapi-sdk-go/v3/go.mod"],
        )
        lockfiles = sorted(
            path.relative_to(ROOT).as_posix()
            for path in files
            if path.name == "package-lock.json"
        )
        self.assertEqual(
            lockfiles,
            ["ops/release/wrangler/package-lock.json", "web/package-lock.json"],
        )

    def test_no_ambiguous_root_directories(self) -> None:
        for name in ("scripts", "deploy", "common", "utils", "pkg", "packages"):
            self.assertFalse((ROOT / name).exists(), name)

    def test_no_production_go_outside_server(self) -> None:
        offenders = [
            path.relative_to(ROOT).as_posix()
            for path in repository_files()
            if path.suffix == ".go"
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

    def test_research_gateway_runtime_permissions(self) -> None:
        systemd = ROOT / "infra/production/systemd"
        socket = (systemd / "vane-research-gateway.socket").read_text(encoding="utf-8")
        for required in (
            "SocketUser=vane-research-gateway",
            "SocketGroup=vane",
            "SocketMode=0660",
            "DirectoryMode=0711",
        ):
            self.assertIn(required, socket)
        self.assertNotIn("SocketMode=0666", socket)

        service = (systemd / "vane.service").read_text(encoding="utf-8")
        self.assertIn(
            "LoadCredential=native_v3_edit_recovery_db_url:"
            "/etc/vane/credentials/native_v3_edit_recovery_db_url",
            service,
        )
        self.assertIn(
            "LoadCredential=telegram_bot_token:/etc/credstore/telegram_bot_token",
            service,
        )
        self.assertIn(
            "LoadCredential=telegram_webhook_secret:/etc/credstore/telegram_webhook_secret",
            service,
        )
        self.assertIn(
            "LoadCredential=credential_vault_active_key:"
            "/etc/credstore/credential_vault_active_key",
            service,
        )
        self.assertIn(
            "LoadCredential=credential_vault_retired_keys:"
            "/etc/credstore/credential_vault_retired_keys",
            service,
        )
        environment = (ROOT / "infra/production/env/server.env.example").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("VANE_DB_NATIVE_V3_EDIT_RECOVERY_RUNTIME_URL=", environment)

        for unit in systemd.glob("*.service"):
            payload = unit.read_text(encoding="utf-8")
            self.assertNotIn("ExecStart=/opt/vane/bin/", payload, unit)
            if "ExecStart=/opt/vane/" in payload:
                self.assertIn("ExecStart=/opt/vane/current/bin/", payload, unit)

    def test_server_is_native_and_compose_is_middleware_only(self) -> None:
        dockerfiles = [
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / "server").glob("**/Dockerfile*")
        ]
        self.assertEqual(dockerfiles, [])
        compose = (ROOT / "infra/production/compose/docker-compose.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotRegex(compose, r"(?m)^\s+build:")
        services = set(re.findall(r"(?m)^  ([a-z0-9-]+):\n    image:", compose))
        self.assertEqual(services, {"postgres", "temporal", "temporal-ui", "caddy"})
        operations = "\n".join(
            path.read_text(encoding="utf-8", errors="replace")
            for directory in (ROOT / "ops/release", ROOT / "ops/rollback")
            for path in directory.glob("**/*")
            if path.is_file()
        )
        self.assertNotIn("docker build", operations)


if __name__ == "__main__":
    unittest.main()
