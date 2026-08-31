#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 5 ]]; then
  echo "usage: $0 OPERATION_ID PARENT_DIGEST TEMPORAL_HOST NAMESPACE TASK_QUEUE" >&2
  exit 2
fi
operation_id=$1
parent_digest=$2
temporal_host=$3
temporal_namespace=$4
temporal_task_queue=$5
[[ $EUID -eq 0 &&
   $operation_id =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ &&
   $parent_digest =~ ^[0-9a-f]{64}$ &&
   $temporal_host =~ ^[A-Za-z0-9.:[\]-]{1,512}$ &&
   $temporal_namespace =~ ^[A-Za-z0-9._/-]{1,255}$ &&
   $temporal_task_queue =~ ^[A-Za-z0-9._/-]{1,255}$ ]] || {
  echo "prepared retention control arguments are invalid" >&2
  exit 2
}

command -v flock >/dev/null
[[ -d /opt/vane && ! -L /opt/vane ]] || {
  echo "release authority is unavailable" >&2
  exit 1
}
# Serialize with atomic releases, which take the same lock in
# tools/release/remote-atomic-release.sh: prepared collection stops the
# service, so it must never overlap a deployment cutover.
exec 8>/opt/vane/.release.lock
flock 8

current_release=/opt/vane/current
release_root=/opt/vane/releases
[[ -L $current_release && -d $release_root && ! -L $release_root ]] || {
  echo "active Server release authority is unavailable" >&2
  exit 1
}
release_dir=$(readlink -f "$current_release")
release_revision=$(basename -- "$release_dir")
[[ $release_revision =~ ^[0-9a-f]{40}$ &&
   $release_dir == "$release_root/$release_revision" ]] || {
  echo "active Server release target is invalid" >&2
  exit 1
}
collector=$release_dir/bin/agentfirstretention
receipt=$release_dir/release-receipt.json
[[ -d $release_dir && ! -L $release_dir &&
   -f $collector && ! -L $collector && -x $collector &&
   -f $receipt && ! -L $receipt &&
   $(stat -c '%U:%G:%a' "$release_dir") == root:root:755 &&
   $(stat -c '%U:%G:%a' "$collector") == root:root:755 &&
   $(stat -c '%U:%G:%a' "$receipt") == root:root:644 &&
   -x "$current_release/bin/vane" && ! -L "$current_release/bin/vane" ]] || {
  echo "active retention release files are unsafe" >&2
  exit 1
}
install -d -o vane-migrate -g vane-migrate -m 0700 \
  /var/lib/vane/agent-first-retention
[[ -f /etc/vane/credentials/migration_db_url &&
   ! -L /etc/vane/credentials/migration_db_url &&
   $(stat -c '%U:%G:%a' /etc/vane/credentials/migration_db_url) == \
     vane-migrate:vane-migrate:400 ]] || {
  echo "migration owner credential is unavailable" >&2
  exit 1
}

run_collector() {
  local phase=$1 output=$2 unit
  unit=agent-first-retention-${phase}-${operation_id//-/}
  local -a parent=()
  if [[ $phase == prepared ]]; then
    parent=(--parent-digest "$parent_digest")
  fi
  systemd-run --quiet --wait --collect --pipe --unit="$unit" \
    --property=Type=oneshot \
    --property=User=vane-migrate \
    --property=Group=vane-migrate \
    --property=WorkingDirectory=/opt/vane \
    --property=LoadCredential=migration_db_url:/etc/vane/credentials/migration_db_url \
    --property=NoNewPrivileges=yes \
    --property=ProtectSystem=strict \
    --property=ProtectHome=yes \
    --property=PrivateTmp=yes \
    --property=PrivateDevices=yes \
    --property=ProtectProc=invisible \
    --property=RestrictSUIDSGID=yes \
    --property=LockPersonality=yes \
    --property=MemoryDenyWriteExecute=yes \
    --property='RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6' \
    --property=ReadWritePaths=/var/lib/vane/agent-first-retention \
    --property=TimeoutStartSec=35min \
    "$collector" "$phase" \
    --temporal-host "$temporal_host" \
    --temporal-namespace "$temporal_namespace" \
    --temporal-task-queue "$temporal_task_queue" \
    --operation-id "$operation_id" \
    --release-receipt "$receipt" \
    --evidence-directory /var/lib/vane/agent-first-retention \
    --live-vane-binary "$current_release/bin/vane" \
    "${parent[@]}" >"$output"
}

prime_output=$(mktemp /run/agent-first-retention-prime.XXXXXX)
prepared_output=$(mktemp /run/agent-first-retention-prepared.XXXXXX)
service_stopped=false
stop_attempted=false
restart_authorized=false
cleanup() {
  local status=$?
  trap - EXIT
  rm -f -- "$prime_output"
  if [[ $stop_attempted == true ]]; then
    if [[ $restart_authorized == true ]]; then
      systemctl unmask --runtime vane.service >/dev/null 2>&1 || true
      if systemctl start vane.service; then
        for _ in {1..12}; do
          if systemctl is-active --quiet vane.service &&
             curl -fsS --max-time 5 http://127.0.0.1:8080/readyz >/dev/null; then
            env -i PATH=/usr/bin:/bin \
              CREDENTIALS_DIRECTORY=/etc/vane/credentials \
              "$current_release/bin/gate" -env /opt/vane/env/server.env >/dev/null || status=1
            service_stopped=false
            stop_attempted=false
            break
          fi
          sleep 5
        done
      fi
      [[ $service_stopped == false ]] || status=1
    elif [[ $(systemctl is-active vane.service 2>/dev/null || true) != active ]]; then
      echo "unsafe drain left vane stopped; operator recovery is required" >&2
      status=1
    fi
  fi
  if [[ $status -eq 0 ]]; then
    cat "$prepared_output"
  fi
  rm -f -- "$prepared_output"
  exit "$status"
}
trap cleanup EXIT

[[ $(systemctl is-active vane.service 2>/dev/null || true) == active ]] || {
  echo "vane must be active before priming retention clock" >&2
  exit 1
}
curl -fsS --max-time 5 http://127.0.0.1:8080/readyz >/dev/null
run_collector prime-clock "$prime_output"
grep -Fq '"schema_version":"vane.agent-first-retention-clock-prime-result/v1"' \
  "$prime_output" || {
  echo "retention clock prime receipt is invalid" >&2
  exit 1
}

invocation=$(systemctl show vane.service --property=InvocationID --value)
drain_started_at=$(date +%s)
test -n "$invocation"
stop_attempted=true
systemctl stop vane.service
[[ $(systemctl is-active vane.service 2>/dev/null || true) == inactive ]] || {
  echo "vane did not become inactive for prepared collection" >&2
  exit 1
}
service_stopped=true
drain_log=$(journalctl "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager -o cat)
grep -Fq "关停完成" <<<"$drain_log" || {
  echo "prepared collection drain has no graceful-shutdown proof" >&2
  exit 1
}
stop_log=$(journalctl -u vane.service --since "@$drain_started_at" --no-pager -o cat)
if grep -Eiq \
  "(stop-sigterm.*timed out|timed out.*killing|signal SIGKILL|status=9/KILL|code=killed.*KILL)" \
  <<<"$drain_log"$'\n'"$stop_log"; then
  echo "prepared collection drain hit a timeout or SIGKILL" >&2
  exit 1
fi
systemctl mask --runtime vane.service >/dev/null
for process_exe in /proc/[0-9]*/exe; do
  executable=$(readlink "$process_exe" 2>/dev/null || true)
  case "$executable" in
    "$release_dir/bin/vane"|"$release_dir/bin/vane (deleted)")
      echo "vane process remains during prepared collection" >&2
      exit 1
      ;;
  esac
done
restart_authorized=true

run_collector prepared "$prepared_output"
grep -Fq '"schema_version":"vane.agent-first-retention-prepared-result/v1"' \
  "$prepared_output" || {
  echo "prepared retention receipt is invalid" >&2
  exit 1
}
