from pathlib import Path
import os
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
REMOTE = ROOT / "scripts" / "remote-backend-deploy.sh"
BASH = r"C:\Program Files\Git\bin\bash.exe" if os.name == "nt" else "bash"


def extract_function(script: str, name: str, next_name: str) -> str:
    start = script.index(f"{name}() {{")
    finish = script.index(f"{next_name}() {{", start)
    return script[start:finish]


def extracted_runtime(root: Path) -> str:
    script = REMOTE.read_text(encoding="utf-8")
    provision_start = script.index("provision_native_v3_edit_recovery_runtime() {")
    provision_finish = script.index(
        "\n# Serialize every VPS-side backend mutation", provision_start
    )
    functions = (
        extract_function(script, "read_hex_secret", "assert_native_v3_edit_recovery_credential")
        + extract_function(
            script,
            "assert_native_v3_edit_recovery_credential",
            "provision_native_v3_edit_recovery_runtime",
        )
        + script[provision_start:provision_finish]
    )
    return functions.replace("/etc/vane", (root / "etc/vane").as_posix()).replace(
        "/opt/vane", (root / "opt/vane").as_posix()
    )


class NativeV3EditRecoveryRuntimeTest(unittest.TestCase):
    def run_upgrade(
        self, *, fault: str = "", symlink: str = "", existing_bootstrap: bool = True
    ) -> tuple[subprocess.CompletedProcess[bytes], dict[str, str]]:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            credentials = root / "etc/vane/credentials"
            opt = root / "opt/vane"
            capture = root / "capture"
            credentials.mkdir(parents=True)
            opt.mkdir(parents=True)
            capture.mkdir()
            os.chmod(credentials, 0o700)
            if existing_bootstrap:
                (credentials / "runtime_bootstrap_v1.complete").write_text(
                    "complete\n", encoding="utf-8"
                )
            if fault:
                (capture / fault).write_text("once\n", encoding="utf-8")
            if symlink == "pending":
                target = root / "attacker-pending"
                target.mkdir()
                try:
                    (
                        credentials
                        / ".native-v3-edit-recovery-runtime-v1.pending"
                    ).symlink_to(target, target_is_directory=True)
                except OSError as exc:
                    raise unittest.SkipTest("host cannot create symlinks") from exc
            elif symlink == "credential":
                target = root / "attacker-credential"
                target.write_text("attacker\n", encoding="utf-8")
                (credentials / "native_v3_edit_recovery_runtime_v1.complete").write_text(
                    "complete\n", encoding="utf-8"
                )
                os.chmod(
                    credentials / "native_v3_edit_recovery_runtime_v1.complete", 0o600
                )
                try:
                    (credentials / "native_v3_edit_recovery_db_url").symlink_to(target)
                except OSError as exc:
                    raise unittest.SkipTest("host cannot create symlinks") from exc

            script = f"""set -euo pipefail
{extracted_runtime(root)}
capture={shlex.quote(capture.as_posix())}
credentials={shlex.quote(credentials.as_posix())}
install() {{
  [[ $1 == -d ]]
  local destination=${{@: -1}}
  command mkdir -p -- "$destination"
  command chmod 0700 "$destination"
}}
chown() {{ :; }}
stat() {{
  if [[ $1 == -c && $2 == %U:%G:%a ]]; then
    local path=$3 owner mode
    case "$path" in
      *native_v3_edit_recovery_db_url*) owner=vane:vane; mode=400 ;;
      *.pending) owner=root:root; mode=700 ;;
      *.pending/password) owner=root:root; mode=600 ;;
      *.complete) owner=root:root; mode=600 ;;
      *) owner=root:root; mode=$(command stat -c %a "$path") ;;
    esac
    printf '%s:%s\n' "$owner" "$mode"
  else
    command stat "$@"
  fi
}}
docker() {{
  local count=0
  [[ ! -f $capture/count ]] || read -r count <$capture/count
  count=$((count + 1))
  printf '%s\n' "$count" >$capture/count
  printf '%s\n' "$*" >$capture/args-$count
  cat >$capture/sql-$count
  if [[ -f $capture/fail-alter-once ]]; then
    command rm -f $capture/fail-alter-once
    return 17
  fi
}}
mv() {{
  command mv "$@"
  local destination=${{@: -1}}
  if [[ $destination == $credentials/native_v3_edit_recovery_db_url &&
        -f $capture/fail-credential-mv-once ]]; then
    command rm -f $capture/fail-credential-mv-once
    return 19
  fi
}}
if [[ {shlex.quote('true' if symlink else 'false')} == true ]]; then
  set +e
  (set -e; provision_native_v3_edit_recovery_runtime)
  status=$?
  set -e
  printf 'status=%s\n' "$status"
  [[ $status -ne 0 ]]
else
  set +e
  (set -e; provision_native_v3_edit_recovery_runtime)
  first_status=$?
  set -e
  [[ $first_status -ne 0 ]]
  password=$(tr -d '\\r\\n' \
    <$credentials/.native-v3-edit-recovery-runtime-v1.pending/password)
  (set -e; provision_native_v3_edit_recovery_runtime)
  [[ $(cat $capture/count) == 2 ]]
  cmp -s $capture/sql-1 $capture/sql-2
  ! grep -Fq -- "$password" $capture/args-1
  ! grep -Fq -- "$password" $capture/args-2
  grep -Fxq \
    "postgres://vane_native_v3_edit_recovery_runtime:$password@127.0.0.1:5432/vane?sslmode=disable" \
    $credentials/native_v3_edit_recovery_db_url
  [[ $(tr -d '\\r\\n' \
    <$credentials/native_v3_edit_recovery_runtime_v1.complete) == complete ]]
  [[ ! -e $credentials/.native-v3-edit-recovery-runtime-v1.pending ]]
  if [[ {shlex.quote('true' if existing_bootstrap else 'false')} == true ]]; then
    [[ $(tr -d '\\r\\n' <$credentials/runtime_bootstrap_v1.complete) == complete ]]
  else
    [[ ! -e $credentials/runtime_bootstrap_v1.complete ]]
  fi
  (set -e; provision_native_v3_edit_recovery_runtime)
  [[ $(cat $capture/count) == 2 ]]
  printf 'first_status=%s complete\n' "$first_status"
fi
"""
            result = subprocess.run(
                [BASH], input=script.encode(), capture_output=True, check=False
            )
            state = {
                "stdout": result.stdout.decode(),
                "stderr": result.stderr.decode(),
                "target": "",
            }
            if symlink:
                target = root / f"attacker-{symlink}"
                if target.is_file():
                    state["target"] = target.read_text(encoding="utf-8")
            return result, state

    def test_existing_bootstrap_upgrade_replays_after_alter_response_loss(self) -> None:
        result, state = self.run_upgrade(fault="fail-alter-once")

        self.assertEqual(result.returncode, 0, state["stderr"])
        self.assertIn("first_status=17 complete", state["stdout"])
        self.assertNotRegex(state["stdout"] + state["stderr"], r"[0-9a-f]{64}")

    def test_credential_rename_response_loss_replays_same_password(self) -> None:
        result, state = self.run_upgrade(fault="fail-credential-mv-once")

        self.assertEqual(result.returncode, 0, state["stderr"])
        self.assertIn("first_status=19 complete", state["stdout"])
        self.assertNotRegex(state["stdout"] + state["stderr"], r"[0-9a-f]{64}")

    def test_fresh_bootstrap_uses_the_same_resumable_upgrade(self) -> None:
        result, state = self.run_upgrade(
            fault="fail-alter-once", existing_bootstrap=False
        )

        self.assertEqual(result.returncode, 0, state["stderr"])
        self.assertIn("first_status=17 complete", state["stdout"])

    def test_pending_symlink_fails_closed(self) -> None:
        result, state = self.run_upgrade(symlink="pending")

        self.assertEqual(result.returncode, 0, state["stderr"])
        self.assertIn("pending path is unsafe", state["stderr"])

    def test_completed_marker_rejects_credential_symlink(self) -> None:
        result, state = self.run_upgrade(symlink="credential")

        self.assertEqual(result.returncode, 0, state["stderr"])
        self.assertIn("credential has unsafe file metadata", state["stderr"])
        self.assertEqual(state["target"], "attacker\n")

    def test_unit_and_secret_transport_are_exact(self) -> None:
        script = REMOTE.read_text(encoding="utf-8")
        exact_load = (
            "LoadCredential=native_v3_edit_recovery_db_url:"
            "/etc/vane/credentials/native_v3_edit_recovery_db_url"
        )

        self.assertIn("validate_native_v3_edit_recovery_unit \"$stage/vane.service\"", script)
        self.assertIn(exact_load, script)
        self.assertLess(
            script.index("systemd-run --quiet --wait --collect"),
            script.index(
                "provision_native_v3_edit_recovery_runtime\n",
                script.index("systemd-run --quiet --wait --collect"),
            ),
        )
        provision_start = script.index("provision_native_v3_edit_recovery_runtime() {")
        provision = script[
            provision_start : script.index(
                "\n# Serialize every VPS-side backend mutation", provision_start
            )
        ]
        self.assertIn("printf \"ALTER ROLE", provision)
        self.assertIn("|\n      docker compose exec", provision)
        self.assertNotIn("export ", provision)
        self.assertNotRegex(provision, r"docker .*\$password")
        self.assertNotRegex(provision, r"echo .*\$password")


if __name__ == "__main__":
    unittest.main()
