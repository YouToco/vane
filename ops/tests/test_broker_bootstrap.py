from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "ops/bootstrap/install-broker.sh"
CLIENT_INSTALLER = ROOT / "ops/bootstrap/install-client.sh"


class BrokerBootstrapTest(unittest.TestCase):
    def test_installer_creates_unprivileged_forced_command_identity(self) -> None:
        source = INSTALLER.read_text(encoding="utf-8")
        for required in (
            "useradd --system --gid vane-broker",
            "AuthenticationMethods publickey",
            "PasswordAuthentication no",
            "Match all",
            'restrict,command="/usr/local/libexec/vane-broker"',
            "/usr/sbin/sshd -t",
            "/usr/sbin/visudo -cf",
            "systemctl reload ssh.service",
            "passwd -l vane-broker",
        ):
            self.assertIn(required, source)
        self.assertIn(
            "NOPASSWD: /opt/vane-control/current/ops/broker/run-production-handler.sh *",
            source,
        )
        self.assertIn(
            "NOPASSWD: /usr/local/libexec/vane-broker-promote",
            source,
        )
        self.assertIn("/usr/local/libexec/vane-broker-promote", source)
        self.assertNotIn("NOPASSWD: ALL", source)
        self.assertNotIn("passwd -d", source)
        self.assertNotIn("authorized_keys.template", source)

    def test_root_state_and_broker_work_have_separate_owners(self) -> None:
        source = INSTALLER.read_text(encoding="utf-8")
        self.assertIn(
            "install -d -o root -g vane-broker -m 0750 /var/lib/vane-broker/state",
            source,
        )
        self.assertIn("/var/lib/vane-broker/state/broker-work", source)
        self.assertIn("-o vane-broker -g vane-broker -m 0700", source)
        self.assertIn(
            'install -o vane-broker -g vane-broker -m 0600 /dev/null "$release_lock"',
            source,
        )
        self.assertIn('chown vane-broker:vane-broker "$release_lock"', source)
        self.assertIn("-o root -g root -m 0700 /var/lib/vane-broker/evidence", source)

    def test_privileged_launcher_confines_every_caller_controlled_path(self) -> None:
        source = (ROOT / "ops/broker/run-production-handler.sh").read_text(
            encoding="utf-8"
        )
        for required in (
            '[[ $verb == release || $verb == retry ]]',
            '/var/lib/vane-broker/requests/$request_id',
            '/var/lib/vane-broker/state/broker-work/inflight/',
            '[[ $state_root == /var/lib/vane-broker/state ]]',
            '[[ $repo_root == /opt/vane-control/current ]]',
            '[[ $expected_digest =~ ^[0-9a-f]{64}$ ]]',
        ):
            self.assertIn(required, source)

    def test_stable_shim_promotes_before_loading_admission_code(self) -> None:
        source = (ROOT / "ops/broker/broker-shim.sh").read_text(encoding="utf-8")
        promotion = source.index("/usr/local/libexec/vane-broker-promote")
        forced = source.index("forced_command.py")
        execute = source.index('exec "$broker"')
        self.assertLess(promotion, forced)
        self.assertLess(forced, execute)
        self.assertIn("/opt/vane-control/releases/", source)

    def test_local_client_is_root_owned_and_uses_only_forced_ssh(self) -> None:
        source = CLIENT_INSTALLER.read_text(encoding="utf-8")
        for required in (
            "broker client install requires root",
            "/usr/local/libexec/vane-broker-submit",
            "/etc/vane-broker/client.json",
            '"BatchMode=yes"',
            '"IdentitiesOnly=yes"',
            '"StrictHostKeyChecking=yes"',
            'f"vane-broker@{host}"',
            "private key must not be group/world accessible",
        ):
            self.assertIn(required, source)
        self.assertNotIn("StrictHostKeyChecking=no", source)
        self.assertNotIn("vane@{host}", source)


if __name__ == "__main__":
    unittest.main()
