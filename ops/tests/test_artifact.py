from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("artifact", ROOT / "release/artifact.py")
assert SPEC and SPEC.loader
artifact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact)

SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "1123456789abcdef0123456789abcdef01234567"
CONTROL_SHA = "2123456789abcdef0123456789abcdef01234567"


class ArtifactValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        source = self.root / "source"
        (source / "dist/assets").mkdir(parents=True)
        (source / "dist/index.html").write_text("<html></html>\n", encoding="utf-8")
        (source / "dist/assets/app.js").write_text(
            "console.log(1)\n", encoding="utf-8"
        )
        self.good = self.root / "good"
        artifact.pack("frontend", source, SHA, self.good)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def copy_case(self, name: str) -> Path:
        destination = self.root / name
        shutil.copytree(self.good, destination)
        return destination

    def manifest_path(self, case: Path) -> Path:
        return case / f"frontend-{SHA}.manifest.json"

    def archive_path(self, case: Path) -> Path:
        return case / f"frontend-{SHA}.tar.gz"

    def _write_backend_source(self, source: Path) -> None:
        for name, mode in artifact.BACKEND_FILES.items():
            path = source / artifact.BACKEND_SOURCE_PATHS[name]
            path.parent.mkdir(parents=True, exist_ok=True)
            if name.startswith("bin/"):
                path.write_bytes(
                    f"fixture vane/{SHA}/clean\n".encode()
                )
            else:
                path.write_text(f"fixture {name}\n", encoding="utf-8")
            path.chmod(mode)

    def refresh_archive_metadata(self, case: Path) -> None:
        archive_path = self.archive_path(case)
        digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        manifest_path = self.manifest_path(case)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["archive_sha256"] = digest
        manifest["archive_size"] = archive_path.stat().st_size
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        (case / f"frontend-{SHA}.sha256").write_text(
            f"{digest}  {archive_path.name}\n", encoding="ascii"
        )

    def assert_rejected(self, case: Path, sha: str = SHA) -> None:
        with self.assertRaises((ValueError, OSError, json.JSONDecodeError)):
            artifact.validate("frontend", sha, case, self.root / f"out-{case.name}")

    def test_valid_round_trip(self) -> None:
        output = self.root / "verified"
        artifact.validate("frontend", SHA, self.good, output)
        self.assertEqual(
            (output / "dist/index.html").read_text(encoding="utf-8"),
            "<html></html>\n",
        )

    def test_wrong_source_sha_is_rejected(self) -> None:
        self.assert_rejected(self.good, OTHER_SHA)

    def test_backend_round_trip_includes_collector_and_release_receipt(self) -> None:
        source = self.root / "backend-source"
        self._write_backend_source(source)
        packed = self.root / "backend-packed"
        artifact.pack(
            "backend",
            source,
            SHA,
            packed,
            artifact.SERVER_RELEASE_CONTRACT,
            CONTROL_SHA,
            "123456",
            2,
        )
        output = self.root / "backend-verified"
        artifact.validate(
            "backend", SHA, packed, output,
            control_plane_revision=CONTROL_SHA,
            deploy_run_id="123456",
        )
        self.assertTrue((output / "bin/vane-research-gateway").is_file())
        self.assertTrue((output / "bin/agentfirstretention").is_file())
        receipt_raw = (output / "release-receipt.json").read_text(encoding="utf-8")
        receipt = json.loads(receipt_raw, object_pairs_hook=artifact.strict_object)
        self.assertEqual(
            receipt_raw,
            json.dumps(receipt, separators=(",", ":")),
        )
        self.assertEqual(
            set(receipt),
            {
                "schema_version",
                "source_revision",
                "control_plane_revision",
                "deploy_run_id",
                "build_run_attempt",
                "backend_archive_sha256",
                "backend_manifest_sha256",
                "server_release_contract_sha256",
                "vane_sha256",
                "agentfirstretention_sha256",
            },
        )
        self.assertEqual(receipt["schema_version"], "vane.release-receipt/v1")
        self.assertEqual(receipt["source_revision"], SHA)
        self.assertEqual(receipt["control_plane_revision"], CONTROL_SHA)
        self.assertEqual(receipt["deploy_run_id"], "123456")
        self.assertEqual(receipt["build_run_attempt"], 2)
        self.assertEqual(
            receipt["backend_manifest_sha256"],
            artifact.sha256_file(packed / f"backend-{SHA}.manifest.json"),
        )
        manifest = json.loads(
            (packed / f"backend-{SHA}.manifest.json").read_text(encoding="utf-8")
        )
        gateway_entry = next(
            entry
            for entry in manifest["files"]
            if entry["path"] == "bin/vane-research-gateway"
        )
        self.assertEqual(gateway_entry["mode"], 0o755)
        collector_entry = next(
            entry
            for entry in manifest["files"]
            if entry["path"] == "bin/agentfirstretention"
        )
        self.assertEqual(
            receipt["agentfirstretention_sha256"], collector_entry["sha256"]
        )
        consumed = subprocess.run(
            [
                str(ROOT / "bin/vane"),
                "status",
                "--release-receipt",
                str(output / "release-receipt.json"),
                "--sha",
                SHA,
            ],
            cwd=ROOT.parent,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(consumed.returncode, 0, consumed.stderr)
        self.assertEqual(json.loads(consumed.stdout)["revision"], SHA)

    def test_backend_pack_is_byte_deterministic(self) -> None:
        source = self.root / "backend-deterministic-source"
        self._write_backend_source(source)
        first = self.root / "backend-deterministic-first"
        second = self.root / "backend-deterministic-second"
        for output in (first, second):
            artifact.pack(
                "backend", source, SHA, output,
                artifact.SERVER_RELEASE_CONTRACT, CONTROL_SHA, "123456", 2,
            )
        for suffix in ("tar.gz", "manifest.json", "sha256"):
            name = f"backend-{SHA}.{suffix}"
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())

    def test_backend_validate_binds_control_plane_and_run(self) -> None:
        source = self.root / "backend-identity-source"
        self._write_backend_source(source)
        packed = self.root / "backend-identity-packed"
        artifact.pack(
            "backend", source, SHA, packed,
            artifact.SERVER_RELEASE_CONTRACT, CONTROL_SHA, "123456", 2,
        )
        for control, run_id in (("f" * 40, "123456"), (CONTROL_SHA, "654321")):
            with self.assertRaises(ValueError):
                artifact.validate(
                    "backend", SHA, packed,
                    self.root / f"identity-out-{control[:4]}-{run_id}",
                    control_plane_revision=control,
                    deploy_run_id=run_id,
                )
    def test_backend_pack_fails_without_research_gateway(self) -> None:
        source = self.root / "backend-incomplete"
        for name, mode in artifact.BACKEND_FILES.items():
            if name == "bin/vane-research-gateway":
                continue
            path = source / artifact.BACKEND_SOURCE_PATHS[name]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n", encoding="utf-8")
            path.chmod(mode)
        with self.assertRaises((ValueError, OSError)):
            artifact.pack(
                "backend",
                source,
                SHA,
                self.root / "backend-rejected",
                artifact.SERVER_RELEASE_CONTRACT,
                CONTROL_SHA,
                "123456",
                2,
            )

    def test_backend_pack_requires_exact_server_release_contract(self) -> None:
        source = self.root / "backend-contract-source"
        for name, mode in artifact.BACKEND_FILES.items():
            path = source / artifact.BACKEND_SOURCE_PATHS[name]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n", encoding="utf-8")
            path.chmod(mode)
        for contract in (None, "owner_compat_v1", artifact.SERVER_RELEASE_CONTRACT + " "):
            with self.subTest(contract=contract), self.assertRaises(ValueError):
                artifact.pack(
                    "backend",
                    source,
                    SHA,
                    self.root / f"backend-contract-{len(str(contract))}",
                    contract,
                )

    def test_extra_artifact_input_is_rejected(self) -> None:
        case = self.copy_case("extra-input")
        (case / "unexpected").write_text("x", encoding="ascii")
        self.assert_rejected(case)

    def test_symlink_artifact_input_is_rejected(self) -> None:
        case = self.copy_case("symlink-input")
        checksum = case / f"frontend-{SHA}.sha256"
        checksum.unlink()
        checksum.symlink_to(self.good / checksum.name)
        self.assert_rejected(case)

    def test_fifo_artifact_input_is_rejected(self) -> None:
        case = self.copy_case("fifo-input")
        checksum = case / f"frontend-{SHA}.sha256"
        checksum.unlink()
        os.mkfifo(checksum)
        self.assertTrue(stat.S_ISFIFO(checksum.lstat().st_mode))
        self.assert_rejected(case)

    def test_path_traversal_manifest_is_rejected(self) -> None:
        case = self.copy_case("traversal")
        path = self.manifest_path(case)
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["files"][0]["path"] = "../escape"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assert_rejected(case)

    def test_duplicate_json_key_is_rejected(self) -> None:
        case = self.copy_case("duplicate-json")
        path = self.manifest_path(case)
        content = path.read_text(encoding="utf-8")
        path.write_text('{"schema": 1,' + content[1:], encoding="utf-8")
        self.assert_rejected(case)

    def test_oversized_member_is_rejected(self) -> None:
        case = self.copy_case("oversized")
        path = self.manifest_path(case)
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["files"][0]["size"] = artifact.MAX_FILE_SIZE + 1
        path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assert_rejected(case)

    def test_extra_tar_member_is_rejected(self) -> None:
        case = self.copy_case("extra-tar")
        archive_path = self.archive_path(case)
        with tarfile.open(archive_path, "w:gz") as archive:
            for name in ("dist/index.html", "dist/assets/app.js", "dist/extra"):
                data = b"x"
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(data))
        self.refresh_archive_metadata(case)
        self.assert_rejected(case)

    def test_tar_symlink_is_rejected(self) -> None:
        case = self.copy_case("tar-symlink")
        archive_path = self.archive_path(case)
        with tarfile.open(archive_path, "w:gz") as archive:
            info = tarfile.TarInfo("dist/index.html")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            archive.addfile(info)
        self.refresh_archive_metadata(case)
        self.assert_rejected(case)


if __name__ == "__main__":
    unittest.main()
