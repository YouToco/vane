#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 1 ]]; then
  echo "usage: $0 REMOTE_STAGE" >&2
  exit 2
fi

stage=$1
[[ $stage =~ ^/opt/vane/\.deploy-[0-9a-f]{40}-[0-9]+-[0-9]+$ ]] || {
  echo "unsafe remote stage path" >&2
  exit 1
}
trap 'rm -rf -- "$stage"' EXIT

mkdir -p /opt/vane/bin /opt/vane/dynamicconfig
install -m 0755 "$stage/bin/vane" /opt/vane/bin/vane
install -m 0755 "$stage/bin/useradmin" /opt/vane/bin/useradmin
install -m 0755 "$stage/bin/gate" /opt/vane/bin/gate
install -m 0755 "$stage/bin/runtimeadmin" /opt/vane/bin/runtimeadmin
install -m 0644 "$stage/Caddyfile" /opt/vane/Caddyfile
install -m 0644 "$stage/docker-compose.yml" /opt/vane/docker-compose.yml
install -m 0644 "$stage/vane.service" /opt/vane/vane.service
install -m 0644 \
  "$stage/dynamicconfig/development-sql.yaml" \
  /opt/vane/dynamicconfig/development-sql.yaml

install -m 0644 /opt/vane/vane.service /etc/systemd/system/vane.service
systemctl daemon-reload
(
  cd /opt/vane
  docker compose up -d
)

# scp/install replaces a single-file bind mount's inode. Compare the container
# view with the host content; only a container recreation repairs a detached
# bind mount.
if docker exec vane-caddy-1 cat /etc/caddy/Caddyfile \
  | cmp -s - /opt/vane/Caddyfile; then
  docker exec vane-caddy-1 caddy reload --config /etc/caddy/Caddyfile
else
  (
    cd /opt/vane
    docker compose up -d --force-recreate caddy
  )
fi

test -x /opt/vane/bin/runtimeadmin

# Migration 047's cross-version writer fence requires proof that the previous
# worker completed its own graceful shutdown before the new binary starts.
if systemctl is-active --quiet vane; then
  old_invocation=$(systemctl show vane --property=InvocationID --value)
  drain_started_at=$(date +%s)
  test -n "$old_invocation"
  echo "draining old vane invocation=$old_invocation started_at=$drain_started_at"

  systemctl stop vane

  stopped_state=$(systemctl is-active vane || true)
  if [[ $stopped_state != inactive ]]; then
    echo "old vane invocation did not become inactive (state=$stopped_state)" >&2
    exit 1
  fi

  old_invocation_log=$(
    journalctl "_SYSTEMD_INVOCATION_ID=$old_invocation" --no-pager -o cat
  )
  if ! grep -Fq "关停完成" <<<"$old_invocation_log"; then
    echo "old vane invocation has no application graceful-shutdown proof" >&2
    exit 1
  fi

  stop_log=$(journalctl -u vane --since "@$drain_started_at" --no-pager -o cat)
  if grep -Eiq \
    "(stop-sigterm.*timed out|timed out.*killing|signal SIGKILL|status=9/KILL|code=killed.*KILL)" \
    <<<"$old_invocation_log"$'\n'"$stop_log"; then
    echo "old vane invocation hit a stop timeout or SIGKILL" >&2
    exit 1
  fi
else
  stopped_state=$(systemctl is-active vane || true)
  if [[ $stopped_state != inactive ]]; then
    echo "vane has no active invocation but is not inactive (state=$stopped_state)" >&2
    exit 1
  fi
  echo "no active vane invocation; skipping old-journal drain proof"
fi

leftover_pids=
for process_exe in /proc/[0-9]*/exe; do
  executable=$(readlink "$process_exe" 2>/dev/null || true)
  case "$executable" in
    "/opt/vane/bin/vane"|"/opt/vane/bin/vane (deleted)")
      pid=${process_exe#/proc/}
      pid=${pid%/exe}
      leftover_pids="$leftover_pids $pid"
      ;;
  esac
done
if [[ -n $leftover_pids ]]; then
  echo "old /opt/vane/bin/vane processes remain:$leftover_pids" >&2
  exit 1
fi

echo "old vane worker drain verified; starting roll-forward binary"
systemctl start vane
sleep 5
systemctl is-active vane
curl -fsS --max-time 5 http://127.0.0.1:8080/readyz

ctype=$(
  curl -sSL -o /dev/null -w '%{content_type}' --max-time 10 \
    https://vane.zhuoqidev.com/.well-known/agent-card.json
)
case "$ctype" in
  *json*) ;;
  *)
    echo "主域 well-known 终点不是 json（$ctype），well-known 路由断了" >&2
    exit 1
    ;;
esac
echo "deploy verified: readyz OK"

# Red (1) and probe failure (2) fail the deployment; yellow remains exit 0.
/opt/vane/bin/gate -env /opt/vane/.env
