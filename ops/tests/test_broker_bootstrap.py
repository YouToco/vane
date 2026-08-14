from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "ops/bootstrap/install-broker.sh"


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


if __name__ == "__main__":
    unittest.main()
