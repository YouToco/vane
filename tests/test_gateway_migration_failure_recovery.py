from pathlib import Path
import os
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
BASH = r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt" else "bash"
SHA = "1" * 40


def extract_migration_gate() -> str:
    script = REMOTE.read_text(encoding="utf-8")
    begin = "# test-anchor: gateway-migration-gate-begin\n"
    end = "# test-anchor: gateway-migration-gate-end"
    start = script.index(begin) + len(begin)
    finish = script.index(end, start)
    return script[start:finish]


def extract_recovery() -> str:
    script = REMOTE.read_text(encoding="utf-8")
    start = script.index("old_vane_recovery_required=false")
    finish = script.index("trap cleanup_remote_deploy EXIT", start)
    return script[start:finish]


def relocated_recovery(root: Path) -> tuple[str, str, str, str, str]:
    opt = (root / "opt/vane").as_posix()
    systemd = (root / "etc/systemd/system").as_posix()
    etc_vane = (root / "etc/vane").as_posix()
    runtime = (root / "run/vane-research-gateway").as_posix()
    recovery = (
        extract_recovery()
        .replace("/opt/vane", opt)
        .replace("/etc/systemd/system", systemd)
        .replace("/etc/vane", etc_vane)
        .replace("/run/vane-research-gateway", runtime)
    )
    return recovery, opt, systemd, etc_vane, runtime


def gateway_paths(opt: str, systemd: str, etc_vane: str) -> list[str]:
    return [
        f"{opt}/bin/vane-research-gateway",
        f"{opt}/vane-research-gateway.service",
        f"{opt}/vane-research-gateway.socket",
        f"{systemd}/vane-research-gateway.service",
        f"{systemd}/vane-research-gateway.socket",
        f"{opt}/env/research-gateway.env",
        f"{etc_vane}/credentials/gateway_db_url",
        f"{etc_vane}/credentials/research_llm_api_key_gen1",
    ]


def filesystem_mode(path: str) -> int:
    result = subprocess.run(
        [BASH, "-lc", f"stat -c %a {shlex.quote(path)}"],
        capture_output=True,
        check=True,
        text=True,
    )
    return int(result.stdout.strip(), 8)


class GatewayMigrationFailureRecoveryTest(unittest.TestCase):
    def test_failed_migration_does_not_touch_live_gateway_or_vane(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            opt = root / "opt/vane"
            log = root / "commands.log"
            gate = extract_migration_gate().replace("/opt/vane", opt.as_posix())
            stage = f"{opt.as_posix()}/.deploy-{SHA}-1-1"
            (opt / "bin").mkdir(parents=True)
            live_gateway = opt / "bin/vane-research-gateway"
            staged_gateway = opt / "bin/vane-research-gateway.release-next"
            live_vane = opt / "bin/vane"
            live_service = opt / "vane-research-gateway.service"
            live_socket = opt / "vane-research-gateway.socket"
            live_gateway.write_text("old-gateway\n", encoding="utf-8")
            staged_gateway.write_text("new-gateway\n", encoding="utf-8")
            live_vane.write_text("old-vane\n", encoding="utf-8")
            live_service.write_text("old-service\n", encoding="utf-8")
            live_socket.write_text("old-socket\n", encoding="utf-8")
            script = (
                "set -u -o pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"log={shlex.quote(log.as_posix())}\n"
                "systemctl() { printf 'systemctl %s\\n' \"$*\" >>\"$log\"; return 0; }\n"
                "systemd-run() { printf 'systemd-run %s\\n' \"$*\" >>\"$log\"; return 37; }\n"
                "set +e\n"
                f"(\n{gate}\n)\n"
                "status=$?\n"
                "set -e\n"
                "[[ $status -eq 37 ]]\n"
                f"[[ $(cat {shlex.quote(live_gateway.as_posix())}) == old-gateway ]]\n"
                f"[[ $(cat {shlex.quote(staged_gateway.as_posix())}) == new-gateway ]]\n"
                f"[[ $(cat {shlex.quote(live_vane.as_posix())}) == old-vane ]]\n"
                f"[[ $(cat {shlex.quote(live_service.as_posix())}) == old-service ]]\n"
                f"[[ $(cat {shlex.quote(live_socket.as_posix())}) == old-socket ]]\n"
                "! grep -Eq 'systemctl (stop|start|restart) (vane|vane-research-gateway)' \"$log\"\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )
            commands = log.read_text(encoding="utf-8")

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertIn("systemd-run --quiet --wait --collect", commands)
        self.assertIn("systemctl disable vane-migrate.service", commands)

    def test_gateway_restore_reinstates_complete_previous_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            recovery, opt, systemd, etc_vane, runtime = relocated_recovery(root)
            log = (root / "commands.log").as_posix()
            stage = f"{opt}/.deploy-{SHA}-1-1"
            rollback = f"{opt}/.rollback-vane-{SHA}-1-1"
            paths = gateway_paths(opt, systemd, etc_vane)
            old_files = "".join(
                f"printf 'old-{index}\\n' >{shlex.quote(path)}\n"
                for index, path in enumerate(paths)
            )
            new_files = "".join(
                f"printf 'new-{index}\\n' >{shlex.quote(path)}\n"
                for index, path in enumerate(paths)
            )
            assertions = "".join(
                f"[[ $(cat {shlex.quote(path)}) == old-{index} ]]\n"
                for index, path in enumerate(paths)
            )
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                f"log={shlex.quote(log)}\n"
                "systemctl() {\n"
                "  if [[ $1 == show ]]; then printf '4242\\n'; return 0; fi\n"
                "  if [[ $1 == is-active || $1 == is-enabled ]]; then return 0; fi\n"
                "  printf '%s\\n' \"$*\" >>\"$log\"\n"
                "}\n"
                "install() { local destination=${!#}; mkdir -p \"$destination\"; chmod 0700 \"$destination\"; }\n"
                f"readlink() {{ printf '%s\\n' {shlex.quote(opt + '/bin/vane-research-gateway')}; }}\n"
                "gateway_functional() { return 0; }\n"
                "sleep() { :; }\n"
                f"mkdir -p {shlex.quote(rollback)} {shlex.quote(opt + '/bin')} "
                f"{shlex.quote(opt + '/env')} {shlex.quote(systemd)} "
                f"{shlex.quote(etc_vane + '/credentials')} {shlex.quote(runtime)}\n"
                f"{old_files}"
                "previous_vane_snapshot_ready=true\n"
                "snapshot_previous_gateway_release\n"
                f"{new_files}"
                "restore_previous_gateway_release\n"
                f"{assertions}"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )
            commands = Path(log).read_text(encoding="utf-8")

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertLess(
            commands.index("start vane-research-gateway.socket"),
            commands.index("start vane-research-gateway.service"),
        )

    def test_fresh_absent_failure_trap_removes_every_gateway_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            recovery, opt, systemd, etc_vane, runtime = relocated_recovery(root)
            stage = f"{opt}/.deploy-{SHA}-1-1"
            rollback = f"{opt}/.rollback-vane-{SHA}-1-1"
            paths = gateway_paths(opt, systemd, etc_vane)
            scratch_paths = [
                f"{opt}/bin/vane-research-gateway.release-next",
                f"{opt}/vane-research-gateway.service.release-next",
                f"{opt}/vane-research-gateway.socket.release-next",
                f"{systemd}/vane-research-gateway.service.release-next",
                f"{systemd}/vane-research-gateway.socket.release-next",
                f"{opt}/env/research-gateway.env.next",
                f"{etc_vane}/credentials/gateway_db_url.next",
                f"{etc_vane}/credentials/research_llm_api_key_gen1.next",
            ]
            new_files = "".join(
                f"printf 'new\\n' >{shlex.quote(path)}\n"
                for path in paths + scratch_paths
            )
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                "systemctl() {\n"
                "  if [[ $1 == is-active || $1 == is-enabled ]]; then return 1; fi\n"
                "  return 0\n"
                "}\n"
                "install() { local destination=${!#}; mkdir -p \"$destination\"; chmod 0700 \"$destination\"; }\n"
                f"mkdir -p {shlex.quote(stage)} {shlex.quote(rollback)} "
                f"{shlex.quote(opt + '/bin')} {shlex.quote(opt + '/env')} "
                f"{shlex.quote(systemd)} {shlex.quote(etc_vane + '/credentials')}\n"
                "previous_vane_snapshot_ready=true\n"
                "snapshot_previous_gateway_release\n"
                f"{new_files}"
                f"mkdir -p {shlex.quote(runtime)}\n"
                "gateway_recovery_required=true\n"
                "trap cleanup_remote_deploy EXIT\n"
                "exit 23\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )

            self.assertEqual(result.returncode, 23, result.stderr.decode())
            for path in paths + scratch_paths:
                self.assertFalse(Path(path).exists(), path)
            self.assertFalse(Path(runtime).exists())

    def test_partial_preexisting_failure_trap_restores_exact_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            recovery, opt, systemd, etc_vane, runtime = relocated_recovery(root)
            stage = f"{opt}/.deploy-{SHA}-1-1"
            rollback = f"{opt}/.rollback-vane-{SHA}-1-1"
            paths = gateway_paths(opt, systemd, etc_vane)
            present = {
                paths[1]: "old-opt-service",
                paths[5]: "old-env",
                paths[6]: "old-db",
            }
            expected_file_mode = root / "expected-file-mode"
            expected_runtime_mode = root / "expected-runtime-mode"
            old_files = "".join(
                f"printf '{value}\\n' >{shlex.quote(path)}\n"
                for path, value in present.items()
            )
            new_files = "".join(
                f"printf 'new\\n' >{shlex.quote(path)}\n" for path in paths
            )
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                "systemctl() {\n"
                "  if [[ $1 == is-active || $1 == is-enabled ]]; then return 1; fi\n"
                "  return 0\n"
                "}\n"
                "install() { local destination=${!#}; mkdir -p \"$destination\"; chmod 0700 \"$destination\"; }\n"
                f"mkdir -p {shlex.quote(stage)} {shlex.quote(rollback)} "
                f"{shlex.quote(opt + '/bin')} {shlex.quote(opt + '/env')} "
                f"{shlex.quote(systemd)} {shlex.quote(etc_vane + '/credentials')} "
                f"{shlex.quote(runtime)}\n"
                f"{old_files}"
                "chmod 0640 "
                + " ".join(shlex.quote(path) for path in present)
                + "\n"
                + f"chmod 0751 {shlex.quote(runtime)}\n"
                + f"stat -c %a {shlex.quote(next(iter(present)))} >"
                f"{shlex.quote(expected_file_mode.as_posix())}\n"
                + f"stat -c %a {shlex.quote(runtime)} >"
                f"{shlex.quote(expected_runtime_mode.as_posix())}\n"
                "previous_vane_snapshot_ready=true\n"
                "snapshot_previous_gateway_release\n"
                f"{new_files}"
                f"chmod 0700 {shlex.quote(runtime)}\n"
                "gateway_recovery_required=true\n"
                "trap cleanup_remote_deploy EXIT\n"
                "exit 29\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )

            self.assertEqual(result.returncode, 29, result.stderr.decode())
            original_file_mode = int(expected_file_mode.read_text().strip(), 8)
            original_runtime_mode = int(expected_runtime_mode.read_text().strip(), 8)
            for path, value in present.items():
                restored = Path(path)
                self.assertEqual(restored.read_text(encoding="utf-8"), value + "\n")
                self.assertEqual(filesystem_mode(path), original_file_mode)
            for path in set(paths) - set(present):
                self.assertFalse(Path(path).exists(), path)
            self.assertTrue(Path(runtime).is_dir())
            self.assertEqual(filesystem_mode(runtime), original_runtime_mode)

    def test_recovery_is_armed_before_live_gateway_configuration_mutation(self) -> None:
        remote = REMOTE.read_text(encoding="utf-8")
        migration = remote.index("systemd-run --quiet --wait --collect")
        arm = remote.index(
            "gateway_recovery_required=true",
            remote.index("# From the first live gateway configuration mutation"),
        )
        environment_promotion = remote.index(
            "mv -f /opt/vane/env/research-gateway.env.next"
        )
        credential_promotion = remote.index(
            'mv -f "/etc/vane/credentials/$credential.next"'
        )

        self.assertLess(migration, arm)
        self.assertLess(arm, environment_promotion)
        self.assertLess(arm, credential_promotion)


if __name__ == "__main__":
    unittest.main()
