from pathlib import Path
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor


ROOT = Path(__file__).resolve().parents[1]
PUBLISHER = ROOT / "scripts" / "publish-retention-release.sh"


@unittest.skipUnless(sys.platform.startswith("linux"), "publisher targets production Linux")
class RetentionReleasePublisherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.releases = self.root / "releases"
        self.collector = self.root / "agentfirstretention"
        self.receipt = self.root / "release-receipt.json"
        self.control = self.root / "agent-first-retention-prepared-control"
        self.collector.write_bytes(b"collector-v1")
        self.collector.chmod(0o755)
        self.receipt.write_bytes(b'{"schema_version":"fixture/v1"}')
        self.receipt.chmod(0o644)
        self.control.write_bytes(b"#!/bin/sh\nexit 0\n")
        self.control.chmod(0o755)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def publish(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(PUBLISHER), str(self.releases), str(self.collector), str(self.receipt),
             str(self.control)],
            text=True, capture_output=True, check=False,
        )

    def release_dir(self) -> Path:
        digest = hashlib.sha256(self.receipt.read_bytes()).hexdigest()
        return self.releases / digest

    def test_fresh_and_replay_are_exact_and_idempotent(self) -> None:
        first = self.publish()
        self.assertEqual(first.returncode, 0, first.stderr)
        release = self.release_dir()
        self.assertEqual(Path(first.stdout.strip()), release)
        self.assertEqual((release / "agentfirstretention").read_bytes(), b"collector-v1")
        self.assertEqual((release / "agentfirstretention").stat().st_mode & 0o777, 0o755)
        self.assertEqual((release / "release-receipt.json").stat().st_mode & 0o777, 0o644)
        self.assertEqual(
            (release / "agent-first-retention-prepared-control").read_bytes(),
            b"#!/bin/sh\nexit 0\n",
        )
        self.assertEqual(
            (release / "agent-first-retention-prepared-control").stat().st_mode & 0o777,
            0o755,
        )
        second = self.publish()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(list(self.releases.glob(".*")), [])

    def test_partial_temp_does_not_poison_retry(self) -> None:
        self.releases.mkdir(mode=0o755)
        orphan = self.releases / f".{hashlib.sha256(self.receipt.read_bytes()).hexdigest()}.orphan"
        orphan.mkdir(mode=0o755)
        (orphan / "agentfirstretention").write_bytes(b"partial")
        result = self.publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.release_dir().is_dir())

    def test_concurrent_publishers_leave_one_release_and_no_pending_dirs(self) -> None:
        self.collector.write_bytes(b"collector-v1" * (1024 * 1024))
        self.collector.chmod(0o755)
        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(lambda _: self.publish(), range(16)))
        for result in results:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(Path(result.stdout.strip()), self.release_dir())
        self.assertEqual(
            sorted(path.name for path in self.releases.iterdir()),
            [self.release_dir().name],
        )

    def test_existing_permission_or_content_drift_is_rejected(self) -> None:
        self.assertEqual(self.publish().returncode, 0)
        release = self.release_dir()
        for target, mutation in (
            (release / "release-receipt.json", lambda path: path.chmod(0o666)),
            (release / "agentfirstretention", lambda path: path.write_bytes(b"changed")),
            (release / "agent-first-retention-prepared-control",
             lambda path: path.write_bytes(b"changed-control")),
        ):
            with self.subTest(target=target.name):
                original = target.read_bytes()
                mode = target.stat().st_mode & 0o777
                mutation(target)
                result = self.publish()
                self.assertNotEqual(result.returncode, 0)
                target.write_bytes(original)
                target.chmod(mode)

    def test_symlinked_or_writable_release_root_is_rejected(self) -> None:
        real = self.root / "real"
        real.mkdir(mode=0o755)
        os.symlink(real, self.releases)
        self.assertNotEqual(self.publish().returncode, 0)
        self.releases.unlink()
        self.releases.mkdir(mode=0o777)
        self.releases.chmod(0o777)
        self.assertNotEqual(self.publish().returncode, 0)


if __name__ == "__main__":
    unittest.main()
