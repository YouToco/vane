from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from ops.audit import full_gate


ROOT = Path(__file__).resolve().parents[2]


class BuildSupervisorPolicyTest(unittest.TestCase):
    def test_candidate_runner_has_no_home_credentials_or_docker_socket(self) -> None:
        source = (ROOT / "ops/release/build_supervisor.py").read_text(encoding="utf-8")
        for required in (
            '"--read-only"',
            '"--cap-drop=ALL"',
            '"--security-opt=no-new-privileges:true"',
            '"--tmpfs", "/home/vane-build:',
            '"VANE_FULL_GATE_DEPENDENCIES=/dependencies.json"',
            '"VANE_SOURCE_ROOT=/workspace"',
            '"GOMODCACHE=/gomodcache"',
            '"GOCACHE=/gocache"',
            '"GOTMPDIR=/gocache/tmp"',
            '"GOPROXY=off"',
            '"npm_config_cache=/npmcache"',
            '[str(go), "mod", "verify"]',
            '"/control/ops/audit/full_gate.py"',
            '"/control-bin/testpolicyscan"',
        ):
            self.assertIn(required, source)
        self.assertNotIn("/var/run/docker.sock", source)
        self.assertNotIn("CREDENTIALS_DIRECTORY", source)
        self.assertNotIn("VPS_", source)
        self.assertNotIn("CLOUDFLARE_API_TOKEN", source)
        self.assertNotIn("ALIYUN_ACCESS_KEY", source)

    def test_runner_and_dependencies_are_digest_locked(self) -> None:
        lock = json.loads((ROOT / "tools/toolchain.lock.json").read_text(encoding="utf-8"))["tools"]
        runner = lock["gate_runner"]
        self.assertEqual(runner["version"], "24.04-v1")
        self.assertRegex(runner["digest"], r"^sha256:[0-9a-f]{64}$")
        dockerfile = ROOT / runner["authority"]
        import hashlib

        self.assertEqual(
            hashlib.sha256(dockerfile.read_bytes()).hexdigest(),
            runner["dockerfile_sha256"],
        )
        source = (ROOT / "ops/release/build_supervisor.py").read_text(encoding="utf-8")
        self.assertIn("lock['postgres']['digest']", source)
        self.assertIn("lock['temporal_server']['digest']", source)
        self.assertIn("lock['gate_runner']['digest']", source)

    def test_external_dependency_manifest_is_exact_and_disposable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "dependencies.json"
            value = {
                "schema": "vane.full-gate-dependencies/v1",
                "postgres": [
                    {
                        "container_name": f"vane-full-fixture-pg-{index}",
                        "container_id": f"{index:x}" * 64,
                        "host": f"pg-{index}",
                        "url": f"postgres://vane:vane_test@pg-{index}:5432/vane_test?sslmode=disable",
                    }
                    for index in range(5)
                ],
                "temporal_address": "temporal:7233",
            }
            path.write_text(json.dumps(value), encoding="utf-8")
            urls, bindings, temporal = full_gate.load_external_dependencies(path)
            self.assertEqual(len(urls), 5)
            self.assertEqual(len(bindings), 5)
            self.assertEqual(temporal, "temporal:7233")
            value["postgres"][0]["url"] = "postgres://vane@production:5432/vane"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "disposable"):
                full_gate.load_external_dependencies(path)


if __name__ == "__main__":
    unittest.main()
