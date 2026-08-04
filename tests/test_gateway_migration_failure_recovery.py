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


def stateful_systemctl() -> str:
    return r'''stub_service_active=false
stub_socket_active=false
stub_service_enabled=false
stub_socket_enabled=false
fail_systemctl_action=
suppress_start_transition=false
unit_active() {
  case "$1" in
    vane-research-gateway.service) printf '%s\n' "$stub_service_active" ;;
    vane-research-gateway.socket) printf '%s\n' "$stub_socket_active" ;;
    *) return 1 ;;
  esac
}
unit_enabled() {
  case "$1" in
    vane-research-gateway.service) printf '%s\n' "$stub_service_enabled" ;;
    vane-research-gateway.socket) printf '%s\n' "$stub_socket_enabled" ;;
    *) return 1 ;;
  esac
}
set_unit_active() {
  case "$1" in
    vane-research-gateway.service) stub_service_active=$2 ;;
    vane-research-gateway.socket) stub_socket_active=$2 ;;
    *) return 1 ;;
  esac
}
set_unit_enabled() {
  case "$1" in
    vane-research-gateway.service) stub_service_enabled=$2 ;;
    vane-research-gateway.socket) stub_socket_enabled=$2 ;;
    *) return 1 ;;
  esac
}
systemctl() {
  local action=$1 unit value
  shift
  if [[ -n $fail_systemctl_action && $action == "$fail_systemctl_action" ]]; then
    return 70
  fi
  case "$action" in
    is-active)
      unit=${!#}
      value=$(unit_active "$unit") || return 5
      if [[ $value == true ]]; then printf 'active\n'; return 0; fi
      printf 'inactive\n'; return 3
      ;;
    is-enabled)
      unit=${!#}
      value=$(unit_enabled "$unit") || return 5
      if [[ $value == true ]]; then printf 'enabled\n'; return 0; fi
      printf 'disabled\n'; return 1
      ;;
    stop)
      for unit in "$@"; do set_unit_active "$unit" false || return 1; done
      ;;
    disable)
      for unit in "$@"; do set_unit_enabled "$unit" false || return 1; done
      ;;
    enable)
      for unit in "$@"; do set_unit_enabled "$unit" true || return 1; done
      ;;
    start)
      if [[ $suppress_start_transition == false ]]; then
        for unit in "$@"; do set_unit_active "$unit" true || return 1; done
      fi
      ;;
    reset-failed|daemon-reload) ;;
    show) printf '4242\n' ;;
    *) return 1 ;;
  esac
  printf '%s %s\n' "$action" "$*" >>"${log:-/dev/null}"
}
'''


class GatewayMigrationFailureRecoveryTest(unittest.TestCase):
    def run_quiescent_restore_fault(
        self, fault: str, exit_code: int
    ) -> tuple[subprocess.CompletedProcess[bytes], list[str], bool]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            recovery, opt, systemd, etc_vane, _ = relocated_recovery(root)
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
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                f"{stateful_systemctl()}\n"
                "install() { local destination=${!#}; mkdir -p \"$destination\"; chmod 0700 \"$destination\"; }\n"
                f"mkdir -p {shlex.quote(stage)} {shlex.quote(rollback)} "
                f"{shlex.quote(opt + '/bin')} {shlex.quote(opt + '/env')} "
                f"{shlex.quote(systemd)} {shlex.quote(etc_vane + '/credentials')}\n"
                f"{old_files}"
                "previous_vane_snapshot_ready=true\n"
                "snapshot_previous_gateway_release\n"
                f"{new_files}"
                f"{fault}\n"
                "gateway_recovery_required=true\n"
                "trap cleanup_remote_deploy EXIT\n"
                f"exit {exit_code}\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )
            live = [Path(path).read_text(encoding="utf-8").strip() for path in paths]
            snapshot_retained = Path(rollback).is_dir()
        return result, live, snapshot_retained

    def run_active_systemctl_fault(
        self, action: str = "", suppress_start: bool = False
    ) -> tuple[subprocess.CompletedProcess[bytes], list[str], bool]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            recovery, opt, systemd, etc_vane, runtime = relocated_recovery(root)
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
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                f"{stateful_systemctl()}\n"
                "install() { local destination=${!#}; mkdir -p \"$destination\"; chmod 0700 \"$destination\"; }\n"
                "gateway_functional() { return 0; }\n"
                f"mkdir -p {shlex.quote(stage)} {shlex.quote(rollback)} "
                f"{shlex.quote(opt + '/bin')} {shlex.quote(opt + '/env')} "
                f"{shlex.quote(systemd)} {shlex.quote(etc_vane + '/credentials')} "
                f"{shlex.quote(runtime)}\n"
                f"{old_files}"
                "stub_service_active=true\n"
                "stub_socket_active=true\n"
                "stub_service_enabled=true\n"
                "stub_socket_enabled=true\n"
                "previous_vane_snapshot_ready=true\n"
                "snapshot_previous_gateway_release\n"
                f"{new_files}"
                f"fail_systemctl_action={shlex.quote(action)}\n"
                f"suppress_start_transition={'true' if suppress_start else 'false'}\n"
                "gateway_recovery_required=true\n"
                "trap cleanup_remote_deploy EXIT\n"
                "exit 37\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )
            live = [Path(path).read_text(encoding="utf-8").strip() for path in paths]
            snapshot_retained = Path(rollback).is_dir()
        return result, live, snapshot_retained

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
                f"{stateful_systemctl()}\n"
                "install() { local destination=${!#}; mkdir -p \"$destination\"; chmod 0700 \"$destination\"; }\n"
                f"readlink() {{ printf '%s\\n' {shlex.quote(opt + '/bin/vane-research-gateway')}; }}\n"
                "gateway_functional() { return 0; }\n"
                "sleep() { :; }\n"
                f"mkdir -p {shlex.quote(rollback)} {shlex.quote(opt + '/bin')} "
                f"{shlex.quote(opt + '/env')} {shlex.quote(systemd)} "
                f"{shlex.quote(etc_vane + '/credentials')} {shlex.quote(runtime)}\n"
                f"{old_files}"
                "stub_service_active=true\n"
                "stub_socket_active=true\n"
                "stub_service_enabled=true\n"
                "stub_socket_enabled=true\n"
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
        self.assertIn("start vane-research-gateway.socket", commands, commands)
        self.assertIn("start vane-research-gateway.service", commands, commands)
        self.assertLess(
            commands.index("start vane-research-gateway.socket"),
            commands.index("start vane-research-gateway.service"),
            commands,
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
                f"{systemd}/vane-research-gateway.service.rollback-next",
                f"{systemd}/vane-research-gateway.socket.release-next",
                f"{systemd}/vane-research-gateway.socket.rollback-next",
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
                f"{stateful_systemctl()}\n"
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
                f"{stateful_systemctl()}\n"
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

    def test_missing_snapshot_member_fails_before_live_mutation(self) -> None:
        result, live, retained = self.run_quiescent_restore_fault(
            'rm -f -- "$rollback_dir/gateway/llm-credential"', 31
        )

        self.assertEqual(result.returncode, 31, result.stderr.decode())
        self.assertTrue(retained)
        self.assertEqual(live, [f"new-{index}" for index in range(8)])
        stderr = result.stderr.decode()
        self.assertIn("snapshot is missing member: llm-credential", stderr)
        self.assertIn("recovery failed", stderr)
        self.assertNotIn("contract restored", stderr)

    def test_prepare_copy_failure_preserves_snapshot_without_live_mutation(self) -> None:
        result, live, retained = self.run_quiescent_restore_fault(
            "cp() { return 71; }", 32
        )

        self.assertEqual(result.returncode, 32, result.stderr.decode())
        self.assertTrue(retained)
        self.assertEqual(live, [f"new-{index}" for index in range(8)])
        stderr = result.stderr.decode()
        self.assertIn("failed to prepare research gateway restore: binary", stderr)
        self.assertIn("recovery failed", stderr)
        self.assertNotIn("contract restored", stderr)

    def test_copy_zero_success_is_verified_by_effect(self) -> None:
        faults = {
            "no-op": "cp() { return 0; }",
            "wrong-content": r'''cp() {
  local destination=${!#}
  printf 'wrong-content\n' >"$destination"
  return 0
}''',
        }
        for name, fault in faults.items():
            with self.subTest(name=name):
                result, live, retained = self.run_quiescent_restore_fault(
                    fault, 34
                )
                self.assertEqual(result.returncode, 34, result.stderr.decode())
                self.assertTrue(retained)
                self.assertEqual(live, [f"new-{index}" for index in range(8)])
                stderr = result.stderr.decode()
                self.assertIn(
                    "prepared research gateway restore does not match snapshot: binary",
                    stderr,
                )
                self.assertIn("recovery failed", stderr)
                self.assertNotIn("contract restored", stderr)

    def test_move_zero_success_is_verified_by_effect(self) -> None:
        faults = {
            "no-op": "mv() { return 0; }",
            "wrong-content": r'''mv() {
  local destination=${!#}
  printf 'wrong-content\n' >"$destination"
  return 0
}''',
        }
        for name, fault in faults.items():
            with self.subTest(name=name):
                result, live, retained = self.run_quiescent_restore_fault(
                    fault, 35
                )
                self.assertEqual(result.returncode, 35, result.stderr.decode())
                self.assertTrue(retained)
                if name == "no-op":
                    self.assertEqual(
                        live, [f"new-{index}" for index in range(8)]
                    )
                else:
                    self.assertEqual(live[0], "wrong-content")
                    self.assertEqual(
                        live[1:], [f"new-{index}" for index in range(1, 8)]
                    )
                stderr = result.stderr.decode()
                self.assertIn(
                    "committed research gateway restore does not match snapshot: binary",
                    stderr,
                )
                self.assertIn("recovery failed", stderr)
                self.assertNotIn("contract restored", stderr)

    def test_commit_failure_detects_mixed_state_and_preserves_snapshot(self) -> None:
        fault = r'''fail_mv_target=${gateway_snapshot_paths[5]}
mv() {
  if [[ ${!#} == "$fail_mv_target" ]]; then return 72; fi
  command mv "$@"
}'''
        result, live, retained = self.run_quiescent_restore_fault(fault, 33)

        self.assertEqual(result.returncode, 33, result.stderr.decode())
        self.assertTrue(retained)
        self.assertEqual(live[:5], [f"old-{index}" for index in range(5)])
        self.assertEqual(live[5:], [f"new-{index}" for index in range(5, 8)])
        stderr = result.stderr.decode()
        self.assertIn("failed to commit research gateway restore: environment", stderr)
        self.assertIn("recovery failed", stderr)
        self.assertNotIn("contract restored", stderr)

    def test_systemctl_failures_are_not_swallowed(self) -> None:
        for action in ("stop", "disable", "reset-failed"):
            with self.subTest(action=action):
                result, live, retained = self.run_active_systemctl_fault(action)
                self.assertEqual(result.returncode, 37, result.stderr.decode())
                self.assertTrue(retained)
                self.assertEqual(live, [f"new-{index}" for index in range(8)])
                stderr = result.stderr.decode()
                self.assertIn("recovery failed", stderr)
                self.assertNotIn("contract recovery verified", stderr)

    def test_final_systemd_state_mismatch_fails_and_preserves_snapshot(self) -> None:
        result, live, retained = self.run_active_systemctl_fault(
            suppress_start=True
        )

        self.assertEqual(result.returncode, 37, result.stderr.decode())
        self.assertTrue(retained)
        self.assertEqual(live, [f"old-{index}" for index in range(8)])
        stderr = result.stderr.decode()
        self.assertIn("active state did not converge", stderr)
        self.assertIn("recovery failed", stderr)
        self.assertNotIn("contract recovery verified", stderr)

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
