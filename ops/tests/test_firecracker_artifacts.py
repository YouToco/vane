from __future__ import annotations

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest


OPS = Path(__file__).resolve().parents[1]
REPO = OPS.parent
SPEC = importlib.util.spec_from_file_location(
    "prepare_firecracker", OPS / "sandbox/prepare_artifacts.py"
)
assert SPEC and SPEC.loader
prepare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)


class FirecrackerArtifactContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_rootfs_is_deterministic_and_contains_only_fixed_init_layout(self) -> None:
        sandboxd = self.root / "sandboxd"
        sandboxd.write_bytes(b"static-linux-sandboxd")
        sandboxd.chmod(0o755)
        first, second = self.root / "first.cpio", self.root / "second.cpio"
        prepare.build_rootfs(sandboxd, first)
        prepare.build_rootfs(sandboxd, second)
        self.assertEqual(first.read_bytes(), second.read_bytes())
        payload = first.read_bytes()
        self.assertTrue(payload.startswith(b"070701"))
        for name in (b"dev\0", b"proc\0", b"sbin\0", b"sys\0", b"sbin/vane-sandbox-init\0", b"TRAILER!!!\0"):
            self.assertEqual(payload.count(name), 1)
        self.assertEqual(len(payload) % 512, 0)

    def test_release_member_extraction_rejects_metadata_or_digest_drift(self) -> None:
        payload = b"official-static-pie"
        archive = self.root / "release.tgz"
        with tarfile.open(archive, "w:gz") as output:
            info = tarfile.TarInfo("release/firecracker")
            info.size = len(payload)
            output.addfile(info, io.BytesIO(payload))
        destination = self.root / "firecracker"
        prepare.extract_regular(
            archive, "release/firecracker", destination, len(payload),
            hashlib.sha256(payload).hexdigest(),
        )
        self.assertEqual(destination.read_bytes(), payload)
        with self.assertRaises(ValueError):
            prepare.extract_regular(
                archive, "release/firecracker", self.root / "wrong", len(payload),
                "0" * 64,
            )

    def test_static_elf_gate_accepts_static_pie_and_rejects_interpreter(self) -> None:
        def elf(program_type: int) -> bytes:
            payload = bytearray(64 + 56)
            payload[:6] = b"\x7fELF\x02\x01"
            struct.pack_into("<H", payload, 16, 3)
            struct.pack_into("<Q", payload, 32, 64)
            struct.pack_into("<H", payload, 54, 56)
            struct.pack_into("<H", payload, 56, 1)
            struct.pack_into("<I", payload, 64, program_type)
            return bytes(payload)

        static = self.root / "static-pie"
        static.write_bytes(elf(1))
        prepare.verify_static_elf(static)
        dynamic = self.root / "dynamic"
        dynamic.write_bytes(elf(3))
        with self.assertRaisesRegex(ValueError, "interpreter"):
            prepare.verify_static_elf(dynamic)

    def test_committed_lock_is_https_content_addressed_and_non_debug(self) -> None:
        lock = json.loads(
            (OPS / "sandbox/firecracker-v1.16.1-x86_64.lock.json").read_text()
        )
        self.assertEqual(lock["firecracker_version"], "v1.16.1")
        for item in (lock["archive"], lock["kernel"]):
            self.assertTrue(item["url"].startswith("https://"))
            self.assertRegex(item["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(item["size_bytes"], 0)
        for name in ("firecracker", "jailer"):
            self.assertNotIn("debug", lock[name]["member"])
            self.assertRegex(lock[name]["sha256"], r"^[0-9a-f]{64}$")

    def test_bridge_accepts_future_bundle_without_self_activation(self) -> None:
        source = (OPS / "cli/controller.py").read_text(encoding="utf-8")
        self.assertNotIn("ops/sandbox/prepare_artifacts.py", source)
        artifact_source = (OPS / "release/artifact.py").read_text(encoding="utf-8")
        self.assertIn("set(BASE_BACKEND_FILES)", artifact_source)
        self.assertIn("set(BACKEND_FILES)", artifact_source)
        remote = (OPS / "release/remote-atomic-release.sh").read_text(encoding="utf-8")
        self.assertIn("backend-manifest.json", remote)
        self.assertIn("bound_roots=(bin deploy)", remote)
        self.assertIn('bound_roots+=(sandbox)', remote)
        self.assertIn('find "${bound_roots[@]}"', remote)


if __name__ == "__main__":
    unittest.main()
