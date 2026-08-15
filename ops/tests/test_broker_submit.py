from __future__ import annotations

import hashlib
import os
from pathlib import Path
import pwd
import tempfile
import unittest

from ops.broker import submit


class BrokerSubmitConfigTest(unittest.TestCase):
    def create_config(self, root: Path) -> Path:
        root.chmod(0o700)
        path = root / "broker-client.json"
        path.write_text("{}\n", encoding="utf-8")
        path.chmod(0o600)
        return path

    def test_private_user_owned_config_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = self.create_config(Path(temporary))
            submit.validate_config_file(path, account_home=Path(temporary))

    def test_group_readable_config_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = self.create_config(Path(temporary))
            path.chmod(0o640)
            with self.assertRaisesRegex(RuntimeError, "user-owned mode 0600"):
                submit.validate_config_file(path, account_home=Path(temporary))

    def test_symlink_config_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = self.create_config(root)
            link = root / "linked.json"
            link.symlink_to(target)
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                submit.validate_config_file(link, account_home=Path(temporary))

    def test_group_writable_config_directory_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = self.create_config(root)
            root.chmod(0o770)
            with self.assertRaisesRegex(RuntimeError, "directory must be user-owned mode 0700"):
                submit.validate_config_file(path, account_home=Path(temporary))

    def test_symlink_ancestor_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            home = base / "home"
            outside = base / "outside"
            home.mkdir(mode=0o700)
            outside.mkdir(mode=0o700)
            (home / ".config").symlink_to(outside, target_is_directory=True)
            vane = outside / "vane"
            vane.mkdir(mode=0o700)
            path = self.create_config(vane)
            through_link = home / ".config" / "vane" / path.name
            with self.assertRaisesRegex(RuntimeError, "path is unsafe"):
                submit.validate_config_file(through_link, account_home=home)

    def test_arbitrary_ssh_command_configuration_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "configuration is invalid"):
            submit.fixed_ssh_command({
                "schema": "vane.broker-client/v1",
                "ssh_command": ["/tmp/fake-broker"],
            })

    def test_runtime_constructs_the_fixed_ssh_command(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identity = root / "identity"
            known_hosts = root / "known_hosts"
            for path in (identity, known_hosts):
                path.write_text("fixture\n", encoding="utf-8")
                path.chmod(0o600)
            command = submit.fixed_ssh_command({
                "schema": "vane.broker-client/v1",
                "host": "broker.example",
                "port": 10058,
                "identity_file": str(identity),
                "known_hosts_file": str(known_hosts),
            }, known_hosts_sha256=hashlib.sha256(known_hosts.read_bytes()).hexdigest())
            self.assertEqual(command[0:3], ["/usr/bin/ssh", "-F", "/dev/null"])
            self.assertEqual(command[-1], "vane-broker@broker.example")
            self.assertIn("StrictHostKeyChecking=yes", command)
            self.assertIn("ProxyCommand=none", command)
            self.assertIn("ProxyJump=none", command)
            self.assertIn("PermitLocalCommand=no", command)
            self.assertIn("ClearAllForwardings=yes", command)

    def test_known_hosts_content_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            identity = root / "identity"
            known_hosts = root / "known_hosts"
            for path in (identity, known_hosts):
                path.write_text("fixture\n", encoding="utf-8")
                path.chmod(0o600)
            with self.assertRaisesRegex(RuntimeError, "differs from release policy"):
                submit.fixed_ssh_command({
                    "schema": "vane.broker-client/v1",
                    "host": "broker.example",
                    "port": 10058,
                    "identity_file": str(identity),
                    "known_hosts_file": str(known_hosts),
                })

    def test_default_path_is_in_account_home_not_environment_home(self) -> None:
        expected = (
            Path(pwd.getpwuid(os.getuid()).pw_dir)
            / ".config"
            / "vane"
            / "broker-client.json"
        )
        previous = os.environ.get("HOME")
        try:
            os.environ["HOME"] = "/tmp/candidate-controlled-home"
            self.assertEqual(submit.default_config_path(), expected)
        finally:
            if previous is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = previous


if __name__ == "__main__":
    unittest.main()
