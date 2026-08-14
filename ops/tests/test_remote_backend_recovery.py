from pathlib import Path
import os
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
BASH = r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt" else "bash"
SHA = "0" * 40


def extract_recovery() -> str:
    script = REMOTE.read_text(encoding="utf-8")
    start = script.index("old_vane_recovery_required=false")
    finish = script.index("trap cleanup_remote_deploy EXIT", start)
    return script[start:finish]


def extract_gateway_boundary() -> str:
    script = REMOTE.read_text(encoding="utf-8")
    start = script.index("assert_gateway_peer_and_credential_boundary()")
    finish = script.index('install -m 0755 "$stage/bin/vane"', start)
    return script[start:finish]


def extract_legacy_unit_validator() -> str:
    script = REMOTE.read_text(encoding="utf-8")
    start = script.index("validate_legacy_compat_unit()")
    finish = script.index("owner_snapshot_path()", start)
    return script[start:finish]


class RemoteBackendRecoveryTest(unittest.TestCase):
    def test_existing_legacy_unit_can_upgrade_but_new_commit_is_strict(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            unit = Path(tempdir) / "vane.service"
            unit.write_text(
                "[Service]\n"
                "User=vane\nGroup=vane\nWorkingDirectory=/opt/vane\n"
                "EnvironmentFile=/opt/vane/env/server-owner-compat.env\n"
                "ExecStart=/opt/vane/bin/vane\n"
                "NoNewPrivileges=yes\nProtectSystem=strict\n"
                "ProtectHome=yes\nPrivateTmp=yes\n",
                encoding="utf-8",
            )
            script = (
                "set -euo pipefail\n"
                f"{extract_legacy_unit_validator()}\n"
                f"unit={shlex.quote(unit.as_posix())}\n"
                "validate_legacy_compat_unit \"$unit\" existing\n"
                "set +e\n"
                "validate_legacy_compat_unit \"$unit\"\n"
                "strict_status=$?\n"
                "set -e\n"
                "[[ $strict_status -ne 0 ]]\n"
                "printf '%s\\n' 'LoadCredential=native_v3_edit_recovery_db_url:/attacker' >>\"$unit\"\n"
                "set +e\n"
                "validate_legacy_compat_unit \"$unit\" existing\n"
                "unsafe_status=$?\n"
                "set -e\n"
                "[[ $unsafe_status -ne 0 ]]\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertIn(b"no exact native V3 edit recovery credential", result.stderr)
        self.assertIn(b"unsafe native V3 edit recovery credential", result.stderr)

    def test_active_owner_v1_contract_converges_to_v2_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            opt = (root / "opt/vane").as_posix()
            systemd = (root / "etc/systemd/system").as_posix()
            stage = f"{opt}/.deploy-{SHA}-1-1"
            recovery = extract_recovery().replace("/opt/vane", opt).replace(
                "/etc/systemd/system", systemd
            )
            shared = (
                "VANE_LLM_AGENT_PROVIDER=kimi\n"
                "VANE_LLM_AGENT_BASE_URL=https://api.moonshot.cn/v1\n"
                "VANE_LLM_AGENT_API_KEY=retired-kimi-key\n"
                "VANE_LLM_AGENT_MODEL=kimi-k2.6\n"
                "VANE_DB_RESEARCH_RUNTIME_URL="
                "postgres://vane_research_runtime:research@db/vane\n"
                "VANE_DB_RESEARCH_CAPABILITY_KEY_ID=fixture\n"
                f"VANE_DB_RESEARCH_CAPABILITY_KEY_HEX={'a' * 64}\n"
                "VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS=\n"
                "VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS=90\n"
                "VANE_RESEARCH_GATEWAY_SOCKET_PATH=/run/gateway.sock\n"
                "VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID=kimi\n"
                "VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID=\n"
                "VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID=\n"
            )
            owner = "VANE_DB_URL=postgres://vane:owner@db/vane\n" + shared
            restricted = (
                "VANE_DB_URL=postgres://vane_server_runtime:server@db/vane\n"
                + shared
            )
            unit = (
                "[Service]\nUser=vane\nGroup=vane\n"
                f"WorkingDirectory={opt}\n"
                f"EnvironmentFile={opt}/env/server-owner-compat.env\n"
                f"ExecStart={opt}/bin/vane\n"
                "NoNewPrivileges=yes\nProtectSystem=strict\n"
                "ProtectHome=yes\nPrivateTmp=yes\n"
            )
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                "stat() { if [[ ${2:-} == %U:%G:%a ]]; then "
                "printf 'root:vane:640\\n'; else command stat \"$@\"; fi; }\n"
                "chown() { :; }\n"
                "chmod() { :; }\n"
                f"mkdir -p {shlex.quote(opt + '/bin')} "
                f"{shlex.quote(opt + '/env')} {shlex.quote(systemd)} "
                '"$rollback_dir"\n'
                f"printf %s {shlex.quote(owner)} >"
                f"{shlex.quote(opt + '/.env')}\n"
                f"printf %s {shlex.quote(owner)} >"
                f"{shlex.quote(opt + '/env/server-owner-compat.env')}\n"
                f"printf %s {shlex.quote(restricted)} >"
                f"{shlex.quote(opt + '/env/server.env')}\n"
                f"printf %s {shlex.quote(unit)} >"
                f"{shlex.quote(systemd + '/vane.service')}\n"
                f"cp {shlex.quote(systemd + '/vane.service')} "
                '"$rollback_dir/vane.service"\n'
                f"cp {shlex.quote(opt + '/env/server-owner-compat.env')} "
                '"$rollback_dir/runtime.env"\n'
                f"cp {shlex.quote(opt + '/.env')} "
                '"$rollback_dir/legacy.env"\n'
                f"printf '%s\\n' {shlex.quote(opt + '/env/server-owner-compat.env')} "
                '>"$rollback_dir/runtime-env-path"\n'
                "previous_vane_snapshot_ready=true\n"
                "assert_existing_audited_primary_runtime_contract\n"
                f"stage_research_control_environment "
                f"{shlex.quote(opt + '/env/server.env')} "
                f"{shlex.quote(opt + '/env/server.env.release-next')}\n"
                "build_owner_compatible_environment "
                '"$rollback_dir/legacy.env" '
                f"{shlex.quote(opt + '/env/server-owner-compat.env.next')} "
                f"{shlex.quote(opt + '/env/server.env.release-next')}\n"
                f"mv {shlex.quote(opt + '/env/server-owner-compat.env.next')} "
                f"{shlex.quote(opt + '/env/server-owner-compat.env')}\n"
                f"mv {shlex.quote(opt + '/env/server.env.release-next')} "
                f"{shlex.quote(opt + '/env/server.env')}\n"
                "assert_audited_legacy_primary_runtime_contract existing\n"
                f"cat {shlex.quote(opt + '/env/server-owner-compat.env')}\n"
            )
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        output = result.stdout.decode()
        self.assertEqual(output.count("VANE_DB_RESEARCH_CONTROL_URL="), 1)
        self.assertIn(
            "VANE_DB_RESEARCH_CONTROL_URL="
            "postgres://vane_server_runtime:server@db/vane\n",
            output,
        )
        self.assertEqual(output.count("VANE_LLM_AGENT_MODEL=deepseek-v4-flash"), 1)
        for retired in (
            "VANE_LLM_AGENT_PROVIDER=",
            "VANE_LLM_AGENT_BASE_URL=",
            "VANE_LLM_AGENT_API_KEY=",
            "kimi-k2.6",
            "retired-kimi-key",
        ):
            self.assertNotIn(retired, output)

    def run_research_control_convergence(
        self, initial: str
    ) -> subprocess.CompletedProcess[bytes]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            opt = (root / "opt/vane").as_posix()
            stage = f"{opt}/.deploy-{SHA}-1-1"
            recovery = extract_recovery().replace("/opt/vane", opt)
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{recovery}\n"
                "chown() { :; }\n"
                "chmod() { :; }\n"
                f"mkdir -p {shlex.quote(opt + '/env')}\n"
                f"printf %s {shlex.quote(initial)} >"
                f"{shlex.quote(opt + '/env/server.env')}\n"
                f"stage_research_control_environment "
                f"{shlex.quote(opt + '/env/server.env')} "
                f"{shlex.quote(opt + '/env/server.env.release-next')}\n"
                "set +e\n"
                "(false; cleanup_remote_deploy)\n"
                "cleanup_status=$?\n"
                "set -e\n"
                "[[ $cleanup_status -eq 1 ]]\n"
                f"! grep -q '^VANE_DB_RESEARCH_CONTROL_URL=' "
                f"{shlex.quote(opt + '/env/server.env')}\n"
                f"[[ ! -e {shlex.quote(opt + '/env/server.env.release-next')} ]]\n"
                f"stage_research_control_environment "
                f"{shlex.quote(opt + '/env/server.env')} "
                f"{shlex.quote(opt + '/env/server.env.release-next')}\n"
                f"cat {shlex.quote(opt + '/env/server.env.release-next')}\n"
            )
            return subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )

    def test_research_control_url_converges_once_from_restricted_primary(self) -> None:
        result = self.run_research_control_convergence(
            "VANE_DB_URL=postgres://vane_server_runtime:secret@db/vane\nKEEP=1\n"
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        output = result.stdout.decode()
        self.assertEqual(output.count("VANE_DB_RESEARCH_CONTROL_URL="), 1)
        self.assertIn(
            "VANE_DB_RESEARCH_CONTROL_URL="
            "postgres://vane_server_runtime:secret@db/vane\n",
            output,
        )
        self.assertIn("KEEP=1\n", output)

    def test_research_control_url_rejects_identity_drift(self) -> None:
        result = self.run_research_control_convergence(
            "VANE_DB_URL=postgres://vane_server_runtime:secret@db/vane\n"
            "VANE_DB_RESEARCH_CONTROL_URL="
            "postgres://vane_research_runtime:wrong@db/vane\n"
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            b"research control Store DSN does not match restricted server runtime",
            result.stderr,
        )

    def run_gateway_boundary(
        self, *, allowed_uid: str = "4242", primary_can_read: bool = False
    ) -> subprocess.CompletedProcess[bytes]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            opt = (root / "opt/vane").as_posix()
            etc = (root / "etc/vane").as_posix()
            boundary = extract_gateway_boundary().replace("/opt/vane", opt).replace(
                "/etc/vane", etc
            )
            script = (
                "set -euo pipefail\n"
                f"{boundary}\n"
                "vane_uid=4242\n"
                "stat() { printf 'vane-research-gateway:vane-research-gateway:400\\n'; }\n"
                f"runuser() {{ {'return 0' if primary_can_read else 'return 1'}; }}\n"
                f"mkdir -p {shlex.quote(opt + '/env')} {shlex.quote(etc + '/credentials')}\n"
                f"printf 'VANE_GATEWAY_ALLOWED_UID={allowed_uid}\\n' "
                f">{shlex.quote(opt + '/env/research-gateway.env')}\n"
                f"printf 'gateway-secret\\n' >{shlex.quote(etc + '/credentials/gateway_db_url')}\n"
                f"printf 'llm-secret\\n' >{shlex.quote(etc + '/credentials/research_llm_api_key_gen1')}\n"
                "assert_gateway_peer_and_credential_boundary\n"
            )
            return subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )

    def run_cleanup(
        self,
        *,
        restart_safe: bool,
        snapshot_ready: bool,
        restore_succeeds: bool,
    ) -> tuple[subprocess.CompletedProcess[bytes], str]:
        with tempfile.TemporaryDirectory() as tempdir:
            log = Path(tempdir) / "recovery.log"
            stage = f"/opt/vane/.deploy-{SHA}-1-1"
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{extract_recovery()}\n"
                "old_vane_recovery_required=true\n"
                f"old_vane_restart_safe={'true' if restart_safe else 'false'}\n"
                "previous_vane_snapshot_ready="
                f"{'true' if snapshot_ready else 'false'}\n"
                "restore_previous_vane_release() {\n"
                "  printf 'restore runtime contract\\n' >>\"$LOG\"\n"
                f"  {'return 0' if restore_succeeds else 'return 17'}\n"
                "}\n"
                "rm() { printf 'rm %s\\n' \"$*\" >>\"$LOG\"; }\n"
                "set +e\n"
                "(exit 23)\n"
                "cleanup_remote_deploy\n"
            )
            env = os.environ.copy()
            env["LOG"] = log.as_posix()
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, env=env, check=False
            )
            return result, log.read_text(encoding="utf-8")

    def runtime_fixture_script(self, root: Path, environment: str) -> str:
        opt = (root / "opt/vane").as_posix()
        systemd = (root / "etc/systemd/system").as_posix()
        transformed = extract_recovery().replace("/opt/vane", opt).replace(
            "/etc/systemd/system", systemd
        )
        if environment == "legacy":
            env_path = f"{opt}/.env"
            service_user = "root"
            runtime_contents = (
                "VANE_DB_URL=postgres://vane:fixture@db/vane\\nold runtime env\\n"
            )
        elif environment == "split":
            env_path = f"{opt}/env/server.env"
            service_user = "vane"
            runtime_contents = (
                "VANE_DB_URL=postgres://vane_server_runtime:fixture@db/vane\\n"
                "old split runtime env\\n"
            )
        else:
            env_path = f"{opt}/env/server-owner-compat.env"
            service_user = "vane"
            runtime_contents = (
                "VANE_DB_URL=postgres://vane:fixture@db/vane\\n"
                "old owner-compatible runtime env\\n"
            )
        stage = f"{opt}/.deploy-{SHA}-1-1"
        return (
            "set -euo pipefail\n"
            f"stage={shlex.quote(stage)}\n"
            f"{transformed}\n"
            "install() {\n"
            "  local destination=${!#}\n"
            "  mkdir -p -- \"$destination\"\n"
            "  chmod 0700 \"$destination\"\n"
            "}\n"
            "SERVICE_STATE=active\n"
            "systemctl() {\n"
            "  printf 'systemctl %s\\n' \"$*\" >>\"$LOG\"\n"
            "  case \"$1\" in\n"
            "    stop) SERVICE_STATE=inactive ;;\n"
            "    start) SERVICE_STATE=active ;;\n"
            "    is-active)\n"
            "      if [[ $SERVICE_STATE == active ]]; then return 0; fi\n"
            "      if [[ ${2:-} != --quiet ]]; then printf '%s\\n' \"$SERVICE_STATE\"; fi\n"
            "      return 3 ;;\n"
            "  esac\n"
            "}\n"
            "vane_ready() { return 0; }\n"
            "sleep() { :; }\n"
            "stat() {\n"
            "  case \"$2\" in\n"
            "    %U) printf 'root\\n' ;;\n"
            "    %a) printf '600\\n' ;;\n"
            "    *) command stat \"$@\" ;;\n"
            "  esac\n"
            "}\n"
            "legacy_env_value() {\n"
            f"  sed -n 's/^VANE_DB_URL=//p' {shlex.quote(opt + '/.env')}\n"
            "}\n"
            f"mkdir -p {shlex.quote(opt + '/bin')} {shlex.quote(opt + '/env')} "
            f"{shlex.quote(systemd)}\n"
            f"printf 'old binary\\n' >{shlex.quote(opt + '/bin/vane')}\n"
            f"printf '[Service]\\nUser={service_user}\\nEnvironmentFile={env_path}\\n"
            f"ExecStart={opt}/bin/vane\\n' "
            f">{shlex.quote(systemd + '/vane.service')}\n"
            f"printf {shlex.quote(runtime_contents)} "
            f">{shlex.quote(env_path)}\n"
            + (
                f"printf 'VANE_DB_URL=postgres://vane:fixture@db/vane\\n"
                f"legacy owner env\\n' >{shlex.quote(opt + '/.env')}\n"
                if environment != "legacy"
                else ""
            )
            +
            "snapshot_previous_vane_release\n"
            "previous_vane_restart_expected=true\n"
            + (
                f"printf 'changed owner compat env\\n' "
                f">{shlex.quote(opt + '/env/server-owner-compat.env')}\n"
                if environment == "owner_compat"
                else ""
            )
            +
            f"printf 'new binary\\n' >{shlex.quote(opt + '/bin/vane')}\n"
            f"printf '[Service]\\nUser=vane\\nEnvironmentFile={opt}/env/server.env\\n"
            f"ExecStart={opt}/bin/vane\\n' "
            f">{shlex.quote(systemd + '/vane.service')}\n"
            f"printf 'new server env\\n' >{shlex.quote(opt + '/env/server.env')}\n"
        )

    def run_runtime_restore(
        self,
        environment: str,
        *,
        fail_unit_move: bool = False,
        assert_primary_fence: bool = False,
    ) -> tuple[subprocess.CompletedProcess[bytes], dict[str, str | bool]]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            log = root / "commands.log"
            script = self.runtime_fixture_script(root, environment)
            if fail_unit_move:
                script += (
                    "mv() {\n"
                    "  if [[ $* == *vane.service.rollback-next* ]]; then return 17; fi\n"
                    "  command mv \"$@\"\n"
                    "}\n"
                    "set +e\n"
                    "restore_previous_vane_release\n"
                    "exit $?\n"
                )
            else:
                script += "restore_previous_vane_release\n"
                if assert_primary_fence:
                    script += "assert_legacy_primary_runtime_contract\n"
            env = os.environ.copy()
            env["LOG"] = log.as_posix()
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, env=env, check=False
            )
            opt = root / "opt/vane"
            systemd = root / "etc/systemd/system"
            state: dict[str, str | bool] = {
                "binary": (opt / "bin/vane").read_text(encoding="utf-8"),
                "unit": (systemd / "vane.service").read_text(encoding="utf-8"),
                "server_env_present": (opt / "env/server.env").exists(),
                "server_env": (
                    (opt / "env/server.env").read_text(encoding="utf-8")
                    if (opt / "env/server.env").exists()
                    else ""
                ),
                "legacy_env": (
                    (opt / ".env").read_text(encoding="utf-8")
                    if (opt / ".env").exists()
                    else ""
                ),
                "owner_compat_env": (
                    (opt / "env/server-owner-compat.env").read_text(
                        encoding="utf-8"
                    )
                    if (opt / "env/server-owner-compat.env").exists()
                    else ""
                ),
                "log": log.read_text(encoding="utf-8") if log.exists() else "",
            }
            return result, state

    def run_inactive_split_convergence(
        self,
        *,
        readiness_succeeds: bool,
        unit_user: str = "vane",
        legacy_owner: str = "root",
        legacy_mode: str = "600",
    ) -> tuple[subprocess.CompletedProcess[bytes], dict[str, str]]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            opt = (root / "opt/vane").as_posix()
            systemd = (root / "etc/systemd/system").as_posix()
            stage = f"{opt}/.deploy-{SHA}-1-1"
            transformed = extract_recovery().replace("/opt/vane", opt).replace(
                "/etc/systemd/system", systemd
            )
            log = root / "commands.log"
            script = (
                "set -euo pipefail\n"
                f"stage={shlex.quote(stage)}\n"
                f"{transformed}\n"
                "install() {\n"
                "  local destination=${!#}\n"
                "  if [[ $1 == -d ]]; then\n"
                "    mkdir -p -- \"$destination\"\n"
                "  else\n"
                "    local source=${@: -2:1}\n"
                "    command cp -- \"$source\" \"$destination\"\n"
                "  fi\n"
                "}\n"
                "SERVER_ENV_HARDENED=false\n"
                "chown() {\n"
                "  if [[ $1 == root:vane && ${!#} == *server.env.release-next* ]]; then\n"
                "    SERVER_ENV_HARDENED=true\n"
                "    printf 'harden restricted server env\\n' >>\"$LOG\"\n"
                "  fi\n"
                "}\n"
                "runuser() { printf 'check vane cannot write server env\\n' >>\"$LOG\"; return 1; }\n"
                "stat() {\n"
                "  case \"$2\" in\n"
                f"    %U) printf '{legacy_owner}\\n' ;;\n"
                f"    %a) printf '{legacy_mode}\\n' ;;\n"
                "    %U:%G:%a)\n"
                "      if [[ $3 == */server.env.release-next ||\n"
                "            ($3 == */server.env && $SERVER_ENV_HARDENED == true) ]]; then\n"
                "        printf 'root:vane:640\\n';\n"
                "      elif [[ $3 == */server.env ]]; then printf 'vane:vane:600\\n';\n"
                "      else printf 'root:vane:640\\n'; fi ;;\n"
                "    *) command stat \"$@\" ;;\n"
                "  esac\n"
                "}\n"
                "SERVICE_STATE=inactive\n"
                "systemctl() {\n"
                "  printf 'systemctl %s\\n' \"$*\" >>\"$LOG\"\n"
                "  case \"$1\" in\n"
                "    stop) SERVICE_STATE=inactive ;;\n"
                "    start) SERVICE_STATE=active ;;\n"
                "    is-active)\n"
                "      if [[ $SERVICE_STATE == active ]]; then return 0; fi\n"
                "      if [[ ${2:-} != --quiet ]]; then printf '%s\\n' \"$SERVICE_STATE\"; fi\n"
                "      return 3 ;;\n"
                "  esac\n"
                "}\n"
                f"wait_for_vane_ready() {{ {'return 0' if readiness_succeeds else 'return 19'}; }}\n"
                "legacy_env_value() {\n"
                f"  sed -n 's/^VANE_DB_URL=//p' {shlex.quote(opt + '/.env')}\n"
                "}\n"
                f"mkdir -p {shlex.quote(opt + '/bin')} {shlex.quote(opt + '/env')} "
                f"{shlex.quote(systemd)}\n"
                f"printf 'failed split binary\\n' >{shlex.quote(opt + '/bin/vane')}\n"
                f"printf '[Service]\\nUser=vane\\nEnvironmentFile={opt}/env/server.env\\n"
                f"ExecStart={opt}/bin/vane\\n' >{shlex.quote(systemd + '/vane.service')}\n"
                f"printf 'VANE_DB_URL=postgres://vane_server_runtime:split@db/vane\\n"
                "VANE_DB_RESEARCH_CONTROL_URL=postgres://vane_server_runtime:split@db/vane\\n"
                "VANE_DB_RESEARCH_RUNTIME_URL=postgres://vane_research_runtime:research-secret@db/vane\\n"
                "VANE_DB_RESEARCH_CAPABILITY_KEY_ID=fixture-key\\n"
                f"VANE_DB_RESEARCH_CAPABILITY_KEY_HEX={'a' * 64}\\n"
                "VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS=\\n"
                "VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS=90\\n"
                "VANE_RESEARCH_GATEWAY_SOCKET_PATH=/run/vane-research-gateway/gateway.sock\\n"
                "VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID=\\n"
                "VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID=\\n"
                "VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID=recovery-task\\n' "
                f">{shlex.quote(opt + '/env/server.env')}\n"
                f"printf 'VANE_DB_URL=postgres://vane:owner@db/vane\\nOWNER_ONLY=1\\n' "
                f">{shlex.quote(opt + '/.env')}\n"
                "snapshot_previous_vane_release\n"
                "previous_vane_restart_expected=false\n"
                "assert_known_split_primary_runtime_contract\n"
                f"printf 'new owner-compatible binary\\n' >{shlex.quote(opt + '/bin/vane.next')}\n"
                f"cat >{shlex.quote(opt + '/vane-legacy-compat.service')} <<'EOF'\n"
                "[Unit]\nDescription=fixture\n[Service]\nType=simple\n"
                f"User={unit_user}\nGroup={unit_user}\n"
                f"WorkingDirectory={opt}\n"
                f"EnvironmentFile={opt}/env/server-owner-compat.env\n"
                "LoadCredential=native_v3_edit_recovery_db_url:"
                "/etc/vane/credentials/native_v3_edit_recovery_db_url\n"
                f"ExecStart={opt}/bin/vane\nRestart=on-failure\n"
                "NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n"
                "EOF\n"
                "old_vane_recovery_required=true\n"
                "old_vane_restart_safe=true\n"
                f"stage_research_control_environment "
                f"{shlex.quote(opt + '/env/server.env')} "
                f"{shlex.quote(opt + '/env/server.env.release-next')}\n"
                "set +e\n"
                "commit_legacy_primary_release\n"
                "commit_status=$?\n"
                "set -e\n"
                "if ((commit_status != 0)); then restore_previous_vane_release; fi\n"
                "exit $commit_status\n"
            )
            env = os.environ.copy()
            env["LOG"] = log.as_posix()
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, env=env, check=False
            )
            state = {
                "binary": (root / "opt/vane/bin/vane").read_text(encoding="utf-8"),
                "unit": (root / "etc/systemd/system/vane.service").read_text(
                    encoding="utf-8"
                ),
                "legacy_env": (root / "opt/vane/.env").read_text(encoding="utf-8"),
                "server_env": (root / "opt/vane/env/server.env").read_text(
                    encoding="utf-8"
                ),
                "owner_compat_env": (
                    (root / "opt/vane/env/server-owner-compat.env").read_text(
                        encoding="utf-8"
                    )
                    if (root / "opt/vane/env/server-owner-compat.env").exists()
                    else ""
                ),
                "log": log.read_text(encoding="utf-8") if log.exists() else "",
            }
            return result, state

    def test_clean_drain_failure_restores_complete_runtime_contract(self) -> None:
        result, log = self.run_cleanup(
            restart_safe=True, snapshot_ready=True, restore_succeeds=True
        )

        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertIn("restore runtime contract", log)
        self.assertNotIn("snapshot retained", result.stderr.decode())

    def test_gateway_accepts_vane_peer_uid_without_exposing_credentials(self) -> None:
        result = self.run_gateway_boundary()

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertNotIn(b"gateway-secret", result.stdout + result.stderr)
        self.assertNotIn(b"llm-secret", result.stdout + result.stderr)

    def test_gateway_rejects_wrong_peer_uid_and_readable_credentials(self) -> None:
        wrong_uid = self.run_gateway_boundary(allowed_uid="0")
        readable = self.run_gateway_boundary(primary_can_read=True)

        self.assertNotEqual(wrong_uid.returncode, 0)
        self.assertIn(b"peer UID does not match", wrong_uid.stderr)
        self.assertNotEqual(readable.returncode, 0)
        self.assertIn(b"can read a gateway credential", readable.stderr)

    def test_unproven_drain_refuses_recovery_and_retains_snapshot(self) -> None:
        result, log = self.run_cleanup(
            restart_safe=False, snapshot_ready=True, restore_succeeds=True
        )

        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertNotIn("restore runtime contract", log)
        self.assertIn(b"automatic recovery is refused", result.stderr)
        self.assertNotIn("rm -rf -- /opt/vane/.rollback-vane-", log)

    def test_recovery_failure_is_fail_closed_and_retains_snapshot(self) -> None:
        result, log = self.run_cleanup(
            restart_safe=True, snapshot_ready=True, restore_succeeds=False
        )

        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertIn("restore runtime contract", log)
        self.assertIn(b"service remains stopped", result.stderr)
        self.assertIn(b"snapshot is preserved", result.stderr)

    def test_legacy_contract_restores_binary_unit_and_removes_new_server_env(self) -> None:
        result, state = self.run_runtime_restore(
            "legacy", assert_primary_fence=True
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(state["binary"], "old binary\n")
        self.assertIn("/.env", state["unit"])
        self.assertEqual(
            state["legacy_env"],
            "VANE_DB_URL=postgres://vane:fixture@db/vane\nold runtime env\n",
        )
        self.assertFalse(state["server_env_present"])
        self.assertIn("systemctl stop vane.service", state["log"])
        self.assertIn("systemctl start vane.service", state["log"])

    def test_split_contract_restores_previous_server_environment(self) -> None:
        result, state = self.run_runtime_restore("split")

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(state["binary"], "old binary\n")
        self.assertIn("/env/server.env", state["unit"])
        self.assertTrue(state["server_env_present"])
        self.assertEqual(
            state["server_env"],
            "VANE_DB_URL=postgres://vane_server_runtime:fixture@db/vane\n"
            "old split runtime env\n",
        )
        self.assertIn("systemctl start vane.service", state["log"])

    def test_split_contract_is_preserved_but_primary_recutover_is_fenced(self) -> None:
        result, state = self.run_runtime_restore(
            "split", assert_primary_fence=True
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(state["binary"], "old binary\n")
        self.assertEqual(
            state["server_env"],
            "VANE_DB_URL=postgres://vane_server_runtime:fixture@db/vane\n"
            "old split runtime env\n",
        )
        self.assertIn(b"requires legacy root + .env contract", result.stderr)

    def test_owner_compat_contract_environment_is_snapshotted_and_restored(self) -> None:
        result, state = self.run_runtime_restore("owner_compat")

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(state["binary"], "old binary\n")
        self.assertIn("/env/server-owner-compat.env", state["unit"])
        self.assertEqual(
            state["owner_compat_env"],
            "VANE_DB_URL=postgres://vane:fixture@db/vane\n"
            "old owner-compatible runtime env\n",
        )

    def test_partial_restore_failure_never_restarts_mixed_release(self) -> None:
        result, state = self.run_runtime_restore("split", fail_unit_move=True)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("systemctl stop vane.service", state["log"])
        self.assertIn("systemctl disable vane.service", state["log"])
        self.assertNotIn("systemctl start vane.service", state["log"])

    def test_inactive_split_converges_to_active_audited_legacy_contract(self) -> None:
        result, state = self.run_inactive_split_convergence(readiness_succeeds=True)

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(state["binary"], "new owner-compatible binary\n")
        self.assertIn("User=vane", state["unit"])
        self.assertIn("Group=vane", state["unit"])
        self.assertIn("/env/server-owner-compat.env", state["unit"])
        self.assertEqual(
            state["unit"].splitlines().count(
                "LoadCredential=native_v3_edit_recovery_db_url:"
                "/etc/vane/credentials/native_v3_edit_recovery_db_url"
            ),
            1,
        )
        self.assertEqual(
            state["legacy_env"],
            "VANE_DB_URL=postgres://vane:owner@db/vane\nOWNER_ONLY=1\n",
        )
        self.assertTrue(
            state["owner_compat_env"].startswith(
                "VANE_DB_URL=postgres://vane:owner@db/vane\nOWNER_ONLY=1\n"
            )
        )
        self.assertIn(
            "VANE_DB_RESEARCH_CONTROL_URL="
            "postgres://vane_server_runtime:split@db/vane\n",
            state["owner_compat_env"],
        )
        self.assertIn(
            "VANE_DB_RESEARCH_RUNTIME_URL="
            "postgres://vane_research_runtime:research-secret@db/vane\n",
            state["owner_compat_env"],
        )
        self.assertIn(
            "VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID="
            "recovery-task\n",
            state["owner_compat_env"],
        )
        self.assertNotIn("VANE_MIGRATION_DB_URL=", state["owner_compat_env"])
        self.assertNotIn(
            "VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL=", state["owner_compat_env"]
        )
        self.assertIn("vane_server_runtime", state["server_env"])
        self.assertNotIn("postgres://vane:owner", state["server_env"])
        self.assertIn("systemctl start vane.service", state["log"])
        self.assertIn("harden restricted server env", state["log"])
        self.assertIn("check vane cannot write server env", state["log"])
        self.assertNotIn("research-secret", state["log"])
        self.assertNotIn("a" * 64, state["log"])

    def test_failed_split_recutover_failure_restores_and_stays_disabled(self) -> None:
        result, state = self.run_inactive_split_convergence(readiness_succeeds=False)

        self.assertEqual(result.returncode, 19, result.stderr.decode())
        self.assertEqual(state["binary"], "failed split binary\n")
        self.assertIn("User=vane", state["unit"])
        self.assertIn("/env/server.env", state["unit"])
        self.assertIn("vane_server_runtime", state["server_env"])
        self.assertEqual(state["owner_compat_env"], "")
        self.assertEqual(state["log"].count("systemctl start vane.service"), 1)
        self.assertTrue(state["log"].rstrip().endswith("systemctl daemon-reload"))

    def test_root_compatibility_service_is_rejected_before_cutover(self) -> None:
        result, state = self.run_inactive_split_convergence(
            readiness_succeeds=True, unit_user="root"
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"does not match the audited contract", result.stderr)
        self.assertEqual(state["binary"], "failed split binary\n")
        self.assertEqual(state["owner_compat_env"], "")
        self.assertNotIn("systemctl start vane.service", state["log"])

    def test_unsafe_legacy_owner_environment_is_rejected(self) -> None:
        result, state = self.run_inactive_split_convergence(
            readiness_succeeds=True, legacy_owner="vane", legacy_mode="666"
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"unsafe ownership or write mode", result.stderr)
        self.assertEqual(state["owner_compat_env"], "")
        self.assertNotIn("systemctl start vane.service", state["log"])


if __name__ == "__main__":
    unittest.main()
