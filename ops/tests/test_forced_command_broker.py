from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from typing import Optional, Union


OPS = Path(__file__).resolve().parents[1]
BROKER = OPS / "broker" / "forced_command.py"
REVISION = "0123456789abcdef0123456789abcdef01234567"


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


class ForcedCommandBrokerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.requests = self.root / "requests"
        self.state = self.root / "state"
        self.repo = self.root / "repo"
        self.requests.mkdir()
        self.state.mkdir()
        (self.state / "broker-work").mkdir()
        (self.repo / "ops/bin").mkdir(parents=True)
        (self.repo / "ops/release").mkdir(parents=True)
        cli = self.repo / "ops/bin/vane"
        cli.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        cli.chmod(0o755)
        artifact = self.repo / "ops/release/artifact.py"
        artifact.write_text(
            "#!/usr/bin/env python3\n"
            "import pathlib,sys\n"
            "out=pathlib.Path(sys.argv[sys.argv.index('--output')+1])\n"
            "out.mkdir(parents=True)\n",
            encoding="utf-8",
        )
        self.current = self.state / "current-release.json"
        self.current.write_text('{"fixture":"N"}\n', encoding="utf-8")
        self.handler = self.root / "handler.py"

    def run_broker(
        self,
        command: str,
        request: Union[dict, bytes],
        *,
        handler: Optional[Path] = None,
    ) -> subprocess.CompletedProcess[bytes]:
        env = {
            **os.environ,
            "SSH_ORIGINAL_COMMAND": command,
            "VANE_BROKER_REQUEST_ROOT": str(self.requests),
            "VANE_BROKER_STATE_ROOT": str(self.state),
            "VANE_BROKER_REPO_ROOT": str(self.repo),
            "VANE_BROKER_TESTING": "1",
        }
        if handler is not None:
            env["VANE_BROKER_HANDLER"] = str(handler)
        payload = request if isinstance(request, bytes) else canonical(request)
        return subprocess.run(
            [str(BROKER)], input=payload, capture_output=True, check=False, env=env
        )

    def make_bundle(self) -> tuple[str, bytes]:
        source = self.root / "source"
        for directory in (
            source / "artifacts/backend-pack",
            source / "gate-evidence",
            source / "manifests",
        ):
            directory.mkdir(parents=True, exist_ok=True)
        bound: dict[str, dict[str, str]] = {}

        def evidence(stage: str, name: str, relative: str, data: bytes) -> dict[str, str]:
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            digest = hashlib.sha256(data).hexdigest()
            bound[f"{stage}:{name}"] = {"path": relative, "sha256": digest}
            return {"name": name, "sha256": digest}

        plan = {
            "stage": "plan",
            "parent": None,
            "evidence": [
                evidence("plan", "release-policy.json", "gate-evidence/release-policy.json", b"policy\n"),
                evidence("plan", "toolchain.lock.json", "gate-evidence/toolchain.lock.json", b"lock\n"),
            ],
        }
        (source / "manifests/plan.json").write_bytes(canonical(plan))
        gate = {
            "stage": "gate",
            "parent": {
                "path": "plan.json",
                "sha256": hashlib.sha256((source / "manifests/plan.json").read_bytes()).hexdigest(),
                "stage": "plan",
            },
            "evidence": [
                evidence("gate", "full-gate.json", "full-gate.json", b"gate\n"),
                evidence("gate", "server-coverage.out", "gate-evidence/coverage.out", b"coverage\n"),
                evidence("gate", "web-coverage-summary.json", "gate-evidence/web-coverage-summary.json", b"web\n"),
            ],
        }
        (source / "manifests/gate.json").write_bytes(canonical(gate))
        artifact = {
            "stage": "artifact",
            "parent": {
                "path": "gate.json",
                "sha256": hashlib.sha256((source / "manifests/gate.json").read_bytes()).hexdigest(),
                "stage": "gate",
            },
            "evidence": [
                evidence("artifact", "release-receipt.json", "gate-evidence/release-receipt.json", b"receipt\n"),
                evidence("artifact", "backend-manifest.json", f"artifacts/backend-pack/backend-{REVISION}.manifest.json", b"backend\n"),
                evidence("artifact", "controller-archive.tar.gz", f"artifacts/controller-{REVISION}.tar.gz", b"controller\n"),
            ],
        }
        (source / "manifests/artifact.json").write_bytes(canonical(artifact))
        submission = {
            "schema": "vane.broker-submission/v1",
            "revision": REVISION,
            "deploy_run_id": "123456789",
            "artifact_manifest": "manifests/artifact.json",
            "backend_pack": "artifacts/backend-pack",
            "controller_archive": f"artifacts/controller-{REVISION}.tar.gz",
            "evidence": bound,
        }
        (source / "submission.json").write_bytes(canonical(submission))
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for path in sorted(source.rglob("*")):
                archive.add(path, arcname=path.relative_to(source), recursive=False)
        payload = buffer.getvalue()
        return hashlib.sha256(payload).hexdigest(), payload

    def upload(self) -> str:
        digest, payload = self.make_bundle()
        result = self.run_broker(
            f"vane-broker upload {digest} {len(payload)}", payload
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(json.loads(result.stdout)["request_id"], digest)
        return digest

    def test_shell_and_unknown_commands_are_rejected(self) -> None:
        for command in ("", "sh", "vane-broker release; id", "vane-broker unknown"):
            with self.subTest(command=command):
                result = self.run_broker(command, {})
                self.assertEqual(result.returncode, 78, result)
                self.assertIn(b"not allowlisted", result.stderr)

    def test_upload_rejects_traversal_and_digest_mismatch(self) -> None:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            info = tarfile.TarInfo("../escape")
            info.size = 1
            archive.addfile(info, io.BytesIO(b"x"))
        payload = buffer.getvalue()
        digest = hashlib.sha256(payload).hexdigest()
        traversal = self.run_broker(
            f"vane-broker upload {digest} {len(payload)}", payload
        )
        self.assertEqual(traversal.returncode, 78, traversal)
        self.assertIn(b"unsafe member", traversal.stderr)
        mismatch = self.run_broker(
            f"vane-broker upload {'f' * 64} {len(payload)}", payload
        )
        self.assertEqual(mismatch.returncode, 78, mismatch)
        self.assertIn(b"SHA-256 mismatch", mismatch.stderr)

    def test_release_is_locked_and_fails_closed_without_handler(self) -> None:
        request_id = self.upload()
        digest = hashlib.sha256(self.current.read_bytes()).hexdigest()
        result = self.run_broker(
            "vane-broker release",
            {"request_id": request_id, "expected_current_digest": digest},
        )
        self.assertEqual(result.returncode, 78, result)
        self.assertIn(b"production handler", result.stderr)
        self.assertTrue((self.state / "broker-work/release.lock").is_file())
        self.assertEqual(self.current.read_text(encoding="utf-8"), '{"fixture":"N"}\n')

    def test_successful_handler_must_advance_current_release(self) -> None:
        request_id = self.upload()
        digest = hashlib.sha256(self.current.read_bytes()).hexdigest()
        self.handler.write_text(
            "#!/usr/bin/env python3\n"
            "import json,pathlib,sys\n"
            "state=pathlib.Path(sys.argv[4])\n"
            "(state/'current-release.json').write_text('{\"fixture\":\"N+1\"}\\n')\n"
            "print(json.dumps({'ok':True,'stage':'finalize'}))\n",
            encoding="utf-8",
        )
        self.handler.chmod(0o755)
        result = self.run_broker(
            "vane-broker release",
            {"request_id": request_id, "expected_current_digest": digest},
            handler=self.handler,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(json.loads(result.stdout)["result"]["stage"], "finalize")

    def test_stale_cas_fails_before_artifact_validation(self) -> None:
        request_id = self.upload()
        result = self.run_broker(
            "vane-broker release",
            {"request_id": request_id, "expected_current_digest": "f" * 64},
        )
        self.assertEqual(result.returncode, 78, result)
        self.assertIn(b"CAS mismatch", result.stderr)

    def test_symlinked_release_lock_fails_before_artifact_validation(self) -> None:
        request_id = self.upload()
        digest = hashlib.sha256(self.current.read_bytes()).hexdigest()
        outside = self.root / "outside.lock"
        outside.write_text("unchanged\n", encoding="utf-8")
        (self.state / "broker-work/release.lock").symlink_to(outside)
        result = self.run_broker(
            "vane-broker release",
            {"request_id": request_id, "expected_current_digest": digest},
        )
        self.assertEqual(result.returncode, 78, result)
        self.assertIn(b"lock must not be a symlink", result.stderr)
        self.assertEqual(outside.read_text(encoding="utf-8"), "unchanged\n")

    def test_mutation_refuses_missing_preprovisioned_broker_work_root(self) -> None:
        request_id = self.upload()
        digest = hashlib.sha256(self.current.read_bytes()).hexdigest()
        (self.state / "broker-work").rmdir()
        result = self.run_broker(
            "vane-broker release",
            {"request_id": request_id, "expected_current_digest": digest},
        )
        self.assertEqual(result.returncode, 78, result)
        self.assertIn(b"work root is unavailable", result.stderr)

    def test_legacy_import_cannot_return(self) -> None:
        self.assertFalse((OPS / "legacy-import").exists())


if __name__ == "__main__":
    unittest.main()
