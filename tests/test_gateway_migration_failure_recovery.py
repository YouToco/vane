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
            opt = (root / "opt/vane").as_posix()
            systemd = (root / "etc/systemd/system").as_posix()
            etc_vane = (root / "etc/vane").as_posix()
            run = (root / "run").as_posix()
            log = (root / "commands.log").as_posix()
            stage = f"{opt}/.deploy-{SHA}-1-1"
            recovery = (
                extract_recovery()
                .replace("/opt/vane", opt)
                .replace("/etc/systemd/system", systemd)
                .replace("/etc/vane", etc_vane)
                .replace("/run/vane-research-gateway", run)
            )
            snapshot = f"{opt}/.rollback-vane-{SHA}-1-1/gateway"
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
                f"readlink() {{ printf '%s\\n' {shlex.quote(opt + '/bin/vane-research-gateway')}; }}\n"
                "gateway_functional() { return 0; }\n"
                "sleep() { :; }\n"
                f"mkdir -p {shlex.quote(snapshot)} {shlex.quote(opt + '/bin')} "
                f"{shlex.quote(opt + '/env')} {shlex.quote(systemd)} "
                f"{shlex.quote(etc_vane + '/credentials')}\n"
                f"printf 'present\\n' >{shlex.quote(snapshot + '/state')}\n"
                f"printf 'old-binary\\n' >{shlex.quote(snapshot + '/vane-research-gateway')}\n"
                f"printf 'old-service\\n' >{shlex.quote(snapshot + '/vane-research-gateway.service')}\n"
                f"printf 'old-socket\\n' >{shlex.quote(snapshot + '/vane-research-gateway.socket')}\n"
                f"printf 'old-env\\n' >{shlex.quote(snapshot + '/research-gateway.env')}\n"
                f"printf 'old-db\\n' >{shlex.quote(snapshot + '/gateway_db_url')}\n"
                f"printf 'old-key\\n' >{shlex.quote(snapshot + '/research_llm_api_key_gen1')}\n"
                f"printf 'new-binary\\n' >{shlex.quote(opt + '/bin/vane-research-gateway')}\n"
                f"printf 'new-service\\n' >{shlex.quote(systemd + '/vane-research-gateway.service')}\n"
                f"printf 'new-socket\\n' >{shlex.quote(systemd + '/vane-research-gateway.socket')}\n"
                f"printf 'new-env\\n' >{shlex.quote(opt + '/env/research-gateway.env')}\n"
                f"printf 'new-db\\n' >{shlex.quote(etc_vane + '/credentials/gateway_db_url')}\n"
                f"printf 'new-key\\n' >{shlex.quote(etc_vane + '/credentials/research_llm_api_key_gen1')}\n"
                "previous_gateway_snapshot_ready=true\n"
                "restore_previous_gateway_release\n"
                f"[[ $(cat {shlex.quote(opt + '/bin/vane-research-gateway')}) == old-binary ]]\n"
                f"[[ $(cat {shlex.quote(systemd + '/vane-research-gateway.service')}) == old-service ]]\n"
                f"[[ $(cat {shlex.quote(systemd + '/vane-research-gateway.socket')}) == old-socket ]]\n"
                f"[[ $(cat {shlex.quote(opt + '/env/research-gateway.env')}) == old-env ]]\n"
                f"[[ $(cat {shlex.quote(etc_vane + '/credentials/gateway_db_url')}) == old-db ]]\n"
                f"[[ $(cat {shlex.quote(etc_vane + '/credentials/research_llm_api_key_gen1')}) == old-key ]]\n"
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


if __name__ == "__main__":
    unittest.main()
