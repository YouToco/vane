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
old_vane_recovery_required=false
old_vane_restart_safe=false
vane_binary_replaced=false

cleanup_remote_deploy() {
  local status=$? attempt
  trap - EXIT
  set +e
  if [[ $old_vane_recovery_required == true ]]; then
    if [[ $vane_binary_replaced == false && $old_vane_restart_safe == true ]]; then
      echo "deployment failed after a proven clean drain; restarting untouched previous vane" >&2
      systemctl start vane.service || true
      for attempt in {1..12}; do
        if systemctl is-active --quiet vane.service && vane_ready; then
          echo "previous vane service recovery verified" >&2
          break
        fi
        sleep 5
      done
      if ! systemctl is-active --quiet vane.service || ! vane_ready; then
        echo "previous vane service recovery failed" >&2
      fi
    elif [[ $vane_binary_replaced == true ]]; then
      echo "new vane failed before readiness; automatic binary rollback is unsafe" >&2
    else
      echo "old vane drain was not proven clean; automatic restart is unsafe" >&2
    fi
  fi
  rm -rf -- "$stage"
  exit "$status"
}
trap cleanup_remote_deploy EXIT

ensure_system_user() {
  local user=$1 entry uid shell
  if ! getent passwd "$user" >/dev/null; then
    useradd --system --home /nonexistent --shell /usr/sbin/nologin "$user"
  fi
  entry=$(getent passwd "$user")
  IFS=: read -r _ _ uid _ _ _ shell <<<"$entry"
  [[ $uid =~ ^[0-9]+$ && $uid -ne 0 && $shell == /usr/sbin/nologin ]] || {
    echo "system user has unsafe identity attributes: $user" >&2
    return 1
  }
  getent group "$user" >/dev/null || {
    echo "system user has no same-name group: $user" >&2
    return 1
  }
}

legacy_env_value_allow_empty() {
  local name=$1 value
  [[ -f /opt/vane/.env && ! -L /opt/vane/.env ]] || {
    echo "legacy owner environment is unavailable" >&2
    return 1
  }
  [[ $(grep -c "^${name}=" /opt/vane/.env) -eq 1 ]] || {
    echo "legacy environment must contain exactly one $name" >&2
    return 1
  }
  value=$(sed -n "s/^${name}=//p" /opt/vane/.env)
  [[ $value != *$'\n'* && $value != *$'\r'* ]] || {
    echo "legacy environment value is invalid: $name" >&2
    return 1
  }
  if [[ ${#value} -ge 2 ]]; then
    if [[ ${value:0:1} == '"' && ${value: -1} == '"' ]] ||
       [[ ${value:0:1} == "'" && ${value: -1} == "'" ]]; then
      value=${value:1:${#value}-2}
    fi
  fi
  printf '%s' "$value"
}

legacy_env_value() {
  local name=$1 value
  value=$(legacy_env_value_allow_empty "$name")
  [[ -n $value ]] || {
    echo "legacy environment value is empty: $name" >&2
    return 1
  }
  printf '%s' "$value"
}

read_hex_secret() {
  local path=$1 value
  [[ -f $path && ! -L $path ]] || return 1
  value=$(tr -d '\r\n' <"$path")
  [[ $value =~ ^[0-9a-f]{64}$ ]] || return 1
  printf '%s' "$value"
}

ensure_system_user vane
ensure_system_user vane-migrate
ensure_system_user vane-research-gateway
vane_uid=$(id -u vane)
migrate_uid=$(id -u vane-migrate)
gateway_uid=$(id -u vane-research-gateway)
[[ $vane_uid != "$migrate_uid" && $vane_uid != "$gateway_uid" &&
   $migrate_uid != "$gateway_uid" ]] || {
  echo "split runtime system users must have distinct UIDs" >&2
  exit 1
}
install -d -m 0755 /opt/vane/bin /opt/vane/dynamicconfig /opt/vane/env
install -d -o root -g root -m 0700 /etc/vane /etc/vane/credentials

# The new server binary stays beside the live binary until the old worker has
# drained. Migration/bootstrap failures therefore leave the proven worker and
# its old unit untouched.
install -m 0755 "$stage/bin/vane" /opt/vane/bin/vane.next
for binary in useradmin gate runtimeadmin vane-migrate vane-research-gateway \
  researchshadow researchcutover; do
  install -m 0755 "$stage/bin/$binary" "/opt/vane/bin/$binary"
done
install -m 0644 "$stage/Caddyfile" /opt/vane/Caddyfile
install -m 0644 "$stage/docker-compose.yml" /opt/vane/docker-compose.yml
install -m 0644 "$stage/vane.service" /opt/vane/vane.service
install -m 0644 "$stage/vane-migrate.service" /opt/vane/vane-migrate.service
install -m 0644 "$stage/vane-research-gateway.service" \
  /opt/vane/vane-research-gateway.service
install -m 0644 "$stage/vane-research-gateway.socket" \
  /opt/vane/vane-research-gateway.socket
install -m 0644 \
  "$stage/dynamicconfig/development-sql.yaml" \
  /opt/vane/dynamicconfig/development-sql.yaml

(
  cd /opt/vane
  docker compose up -d
)

# Bootstrap the split process boundary exactly once. A durable pending set is
# created before ALTER ROLE so an interrupted first run can resume with the
# same passwords instead of losing access to roles it already changed.
migration_db_url=$(legacy_env_value VANE_DB_URL)
llm_api_key=$(legacy_env_value VANE_LLM_API_KEY)
printf '%s\n' "$migration_db_url" >/etc/vane/credentials/migration_db_url.next
chown vane-migrate:vane-migrate /etc/vane/credentials/migration_db_url.next
chmod 0400 /etc/vane/credentials/migration_db_url.next
mv -f /etc/vane/credentials/migration_db_url.next \
  /etc/vane/credentials/migration_db_url

install -m 0644 /opt/vane/vane-migrate.service \
  /etc/systemd/system/vane-migrate.service
install -m 0644 /opt/vane/vane-research-gateway.service \
  /etc/systemd/system/vane-research-gateway.service
install -m 0644 /opt/vane/vane-research-gateway.socket \
  /etc/systemd/system/vane-research-gateway.socket
systemctl daemon-reload
systemctl enable vane-migrate.service vane-research-gateway.service \
  vane-research-gateway.socket >/dev/null
systemctl restart vane-migrate.service

bootstrap_marker=/etc/vane/credentials/runtime_bootstrap_v1.complete
pending_dir=/etc/vane/credentials/.runtime-bootstrap-v1.pending
if [[ ! -f $bootstrap_marker ]]; then
  for config_file in /opt/vane/config.yaml /opt/vane/config/config.yaml; do
    if [[ -e $config_file || -L $config_file ]]; then
      echo "first split-runtime bootstrap refuses legacy config file: $config_file" >&2
      exit 1
    fi
  done
  capability_id_count=$(grep -c '^VANE_DB_RESEARCH_CAPABILITY_KEY_ID=' \
    /opt/vane/.env || true)
  capability_key_count=$(grep -c '^VANE_DB_RESEARCH_CAPABILITY_KEY_HEX=' \
    /opt/vane/.env || true)
  capability_retired_count=$(grep -c '^VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS=' \
    /opt/vane/.env || true)
  capability_ttl_count=$(grep -c '^VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS=' \
    /opt/vane/.env || true)
  capability_source=generated
  capability_id=research-capability-v1
  capability_retired=
  capability_ttl=90
  if ((capability_id_count == 1 && capability_key_count == 1)); then
    ((capability_retired_count <= 1 && capability_ttl_count <= 1)) || {
      echo "legacy research capability keyring has duplicate fields" >&2
      exit 1
    }
    capability_source=legacy
    capability_id=$(legacy_env_value VANE_DB_RESEARCH_CAPABILITY_KEY_ID)
    capability_key=$(legacy_env_value VANE_DB_RESEARCH_CAPABILITY_KEY_HEX)
    if ((capability_retired_count == 1)); then
      capability_retired=$(legacy_env_value_allow_empty \
        VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS)
    fi
    if ((capability_ttl_count == 1)); then
      capability_ttl=$(legacy_env_value VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS)
    fi
    [[ $capability_id =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ &&
       $capability_key =~ ^[0-9a-f]{64}$ &&
       $capability_key != 0000000000000000000000000000000000000000000000000000000000000000 &&
       $capability_ttl =~ ^[0-9]+$ ]] || {
      echo "legacy research capability keyring is malformed" >&2
      exit 1
    }
    ((capability_ttl >= 7 && capability_ttl <= 400)) || {
      echo "legacy research capability TTL is outside 7..400 days" >&2
      exit 1
    }
    declare -A capability_ids=(["$capability_id"]=1)
    if [[ -n $capability_retired ]]; then
      IFS=, read -r -a retired_entries <<<"$capability_retired"
      for retired_entry in "${retired_entries[@]}"; do
        retired_id=${retired_entry%%=*}
        retired_key=${retired_entry#*=}
        [[ $retired_entry == *=* &&
           $retired_id =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ &&
           $retired_key =~ ^[0-9a-f]{64}$ &&
           $retired_key != 0000000000000000000000000000000000000000000000000000000000000000 &&
           -z ${capability_ids[$retired_id]+x} ]] || {
          echo "legacy retired research capability keyring is malformed" >&2
          exit 1
        }
        capability_ids[$retired_id]=1
      done
    fi
  elif ((capability_id_count != 0 || capability_key_count != 0 ||
          capability_retired_count != 0 || capability_ttl_count != 0)); then
    echo "legacy research capability keyring is partial" >&2
    exit 1
  fi

  install -d -o root -g root -m 0700 "$pending_dir"
  for secret in server_password research_password gateway_password; do
    if [[ ! -e $pending_dir/$secret ]]; then
      openssl rand -hex 32 >"$pending_dir/$secret.next"
      chmod 0600 "$pending_dir/$secret.next"
      mv -f "$pending_dir/$secret.next" "$pending_dir/$secret"
    fi
  done
  server_password=$(read_hex_secret "$pending_dir/server_password") || {
    echo "pending server password is invalid" >&2; exit 1;
  }
  research_password=$(read_hex_secret "$pending_dir/research_password") || {
    echo "pending research password is invalid" >&2; exit 1;
  }
  gateway_password=$(read_hex_secret "$pending_dir/gateway_password") || {
    echo "pending gateway password is invalid" >&2; exit 1;
  }
  if [[ $capability_source == generated ]]; then
    if [[ ! -e $pending_dir/capability_key ]]; then
      openssl rand -hex 32 >"$pending_dir/capability_key.next"
      chmod 0600 "$pending_dir/capability_key.next"
      mv -f "$pending_dir/capability_key.next" "$pending_dir/capability_key"
    fi
    capability_key=$(read_hex_secret "$pending_dir/capability_key") || {
      echo "pending capability key is invalid" >&2; exit 1;
    }
  fi

  (
    cd /opt/vane
    {
      printf "ALTER ROLE vane_server_runtime LOGIN PASSWORD '%s';\n" \
        "$server_password"
      printf "ALTER ROLE vane_research_runtime LOGIN PASSWORD '%s';\n" \
        "$research_password"
      printf "ALTER ROLE vane_research_llm_gateway_runtime LOGIN PASSWORD '%s';\n" \
        "$gateway_password"
    } | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U vane -d vane
  )

  awk '!/^(POSTGRES_PASSWORD|VANE_MIGRATION_DB_URL|VANE_DB_URL|VANE_DB_RESEARCH_RUNTIME_URL|VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL|VANE_DB_RESEARCH_CAPABILITY_KEY_ID|VANE_DB_RESEARCH_CAPABILITY_KEY_HEX|VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS|VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS|VANE_RESEARCH_GATEWAY_SOCKET_PATH|VANE_GATEWAY_[A-Z0-9_]+|VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID|VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID)=/' \
    /opt/vane/.env >/opt/vane/env/server.env.next
  {
    printf 'VANE_DB_URL=postgres://vane_server_runtime:%s@127.0.0.1:5432/vane?sslmode=disable\n' \
      "$server_password"
    printf 'VANE_DB_RESEARCH_RUNTIME_URL=postgres://vane_research_runtime:%s@127.0.0.1:5432/vane?sslmode=disable\n' \
      "$research_password"
    printf 'VANE_DB_RESEARCH_CAPABILITY_KEY_ID=%s\n' "$capability_id"
    printf 'VANE_DB_RESEARCH_CAPABILITY_KEY_HEX=%s\n' "$capability_key"
    printf 'VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS=%s\n' "$capability_retired"
    printf 'VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS=%s\n' "$capability_ttl"
    printf 'VANE_RESEARCH_GATEWAY_SOCKET_PATH=/run/vane-research-gateway/gateway.sock\n'
    # First split-runtime bootstrap is always hard-dark. Boss Gate 1 later
    # changes the durable server environment explicitly; legacy env/config
    # cannot smuggle a canary into this infrastructure migration.
    printf 'VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID=\n'
    printf 'VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID=\n'
  } >>/opt/vane/env/server.env.next
  chown vane:vane /opt/vane/env/server.env.next
  chmod 0600 /opt/vane/env/server.env.next
  mv -f /opt/vane/env/server.env.next /opt/vane/env/server.env

  {
    printf 'VANE_GATEWAY_ALLOWED_UID=%s\n' "$vane_uid"
    printf '%s\n' 'VANE_GATEWAY_LLM_ROUTES_JSON='"'"'[{"provider":"deepseek","endpoint_id":"deepseek-compatible-primary","endpoint_generation":1,"credential_id":"llm-primary","credential_generation":1,"base_url":"https://api.deepseek.com/v1"}]'"'"''
  } >/opt/vane/env/research-gateway.env.next
  chown vane-research-gateway:vane-research-gateway \
    /opt/vane/env/research-gateway.env.next
  chmod 0600 /opt/vane/env/research-gateway.env.next
  mv -f /opt/vane/env/research-gateway.env.next \
    /opt/vane/env/research-gateway.env

  printf 'postgres://vane_research_llm_gateway_runtime:%s@127.0.0.1:5432/vane?sslmode=disable\n' \
    "$gateway_password" >/etc/vane/credentials/gateway_db_url.next
  printf '%s\n' "$llm_api_key" \
    >/etc/vane/credentials/research_llm_api_key_gen1.next
  for credential in gateway_db_url research_llm_api_key_gen1; do
    chown vane-research-gateway:vane-research-gateway \
      "/etc/vane/credentials/$credential.next"
    chmod 0400 "/etc/vane/credentials/$credential.next"
    mv -f "/etc/vane/credentials/$credential.next" \
      "/etc/vane/credentials/$credential"
  done
  printf 'complete\n' >"$bootstrap_marker.next"
  chmod 0600 "$bootstrap_marker.next"
  mv -f "$bootstrap_marker.next" "$bootstrap_marker"
  rm -rf -- "$pending_dir"
fi

[[ $(tr -d '\r\n' <"$bootstrap_marker") == complete ]] || {
  echo "runtime bootstrap marker is invalid" >&2
  exit 1
}
[[ -s /opt/vane/env/server.env && ! -L /opt/vane/env/server.env ]] || exit 1
[[ -s /opt/vane/env/research-gateway.env && \
   ! -L /opt/vane/env/research-gateway.env ]] || exit 1
for credential in migration_db_url gateway_db_url research_llm_api_key_gen1; do
  [[ -s /etc/vane/credentials/$credential && \
     ! -L /etc/vane/credentials/$credential ]] || {
    echo "runtime credential is unavailable: $credential" >&2
    exit 1
  }
done
chown vane:vane /opt/vane/env/server.env
chmod 0600 /opt/vane/env/server.env
chown vane-research-gateway:vane-research-gateway \
  /opt/vane/env/research-gateway.env \
  /etc/vane/credentials/gateway_db_url \
  /etc/vane/credentials/research_llm_api_key_gen1
chmod 0600 /opt/vane/env/research-gateway.env
chmod 0400 /etc/vane/credentials/gateway_db_url \
  /etc/vane/credentials/research_llm_api_key_gen1
grep -Eq '^VANE_DB_URL=postgres://vane_server_runtime:' \
  /opt/vane/env/server.env
grep -Eq '^VANE_DB_RESEARCH_RUNTIME_URL=postgres://vane_research_runtime:' \
  /opt/vane/env/server.env
if grep -Eq '^POSTGRES_PASSWORD=|^VANE_DB_URL=postgres://vane:' \
  /opt/vane/env/server.env; then
  echo "server environment contains an owner credential" >&2
  exit 1
fi
systemctl enable --now vane-research-gateway.socket
systemctl restart vane-research-gateway.service
if ! systemctl is-active --quiet vane-research-gateway.service; then
  echo "research gateway preflight did not become active" >&2
  systemctl status vane-research-gateway.service --no-pager --full >&2 || true
  journalctl -u vane-research-gateway.service --no-pager -o cat -n 100 >&2 || true
  exit 1
fi
gateway_exe=$(readlink /proc/"$(systemctl show vane-research-gateway.service \
  --property=MainPID --value)"/exe 2>/dev/null || true)
[[ $gateway_exe == /opt/vane/bin/vane-research-gateway ]] || {
  echo "research gateway preflight is not running the installed binary" >&2
  exit 1
}
[[ $(stat -c '%U:%G:%a' /run/vane-research-gateway/gateway.sock) == \
   vane-research-gateway:vane:660 ]] || {
  echo "research gateway socket ownership or mode is unsafe" >&2
  exit 1
}
gateway_http_code=
for attempt in {1..12}; do
  gateway_http_code=$(runuser -u vane -- curl --silent --show-error \
    --output /dev/null --write-out '%{http_code}' --max-time 2 \
    --unix-socket /run/vane-research-gateway/gateway.sock \
    http://research-gateway/v1/research/llm/execute 2>/dev/null || true)
  if [[ $gateway_http_code == 405 ]] && \
     systemctl is-active --quiet vane-research-gateway.service; then
    break
  fi
  sleep 1
done
[[ $gateway_http_code == 405 ]] || {
  echo "research gateway functional preflight failed" >&2
  systemctl status vane-research-gateway.service --no-pager --full >&2 || true
  journalctl -u vane-research-gateway.service --no-pager -o cat -n 100 >&2 || true
  exit 1
}

# Validate the complete new process configuration and non-owner database path
# while the proven old worker is still serving traffic.
/opt/vane/bin/gate -env /opt/vane/env/server.env >/dev/null

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

# test-anchor: vane-startup-wait-begin
vane_service_state() {
  systemctl is-active vane 2>/dev/null || true
}

print_vane_startup_diagnostics() {
  local invocation
  invocation=$(systemctl show vane --property=InvocationID --value || true)
  echo "vane service status after startup failure:" >&2
  systemctl status vane --no-pager --full >&2 || true
  if [[ -n $invocation ]]; then
    echo "vane startup journal for invocation=$invocation:" >&2
    journalctl "_SYSTEMD_INVOCATION_ID=$invocation" \
      --no-pager -o cat -n 100 >&2 || true
  else
    echo "vane startup journal (no invocation ID available):" >&2
    journalctl -u vane --no-pager -o cat -n 100 >&2 || true
  fi
}

vane_ready() {
  curl -fsS --max-time 5 http://127.0.0.1:8080/readyz >/dev/null
}

wait_for_vane_ready() {
  local attempt state
  for attempt in {1..12}; do
    state=$(vane_service_state)
    if [[ $state == active ]] && vane_ready; then
      return 0
    fi
    echo "waiting for vane readiness: state=$state attempt=$attempt/12"
    sleep 5
  done
  echo "vane did not become ready within 60 seconds" >&2
  print_vane_startup_diagnostics
  return 1
}
# test-anchor: vane-startup-wait-end

# Migration 047's cross-version writer fence requires proof that the previous
# worker completed its own graceful shutdown before the new binary starts.
if systemctl is-active --quiet vane; then
  [[ -f /opt/vane/bin/vane && ! -L /opt/vane/bin/vane &&
     -f /etc/systemd/system/vane.service &&
     ! -L /etc/systemd/system/vane.service ]] || {
    echo "active vane release cannot be backed up safely" >&2
    exit 1
  }
  install -m 0755 /opt/vane/bin/vane /opt/vane/bin/vane.previous
  install -m 0644 /etc/systemd/system/vane.service \
    /opt/vane/vane.service.previous
  old_invocation=$(systemctl show vane --property=InvocationID --value)
  drain_started_at=$(date +%s)
  test -n "$old_invocation"
  echo "draining old vane invocation=$old_invocation started_at=$drain_started_at"

  old_vane_recovery_required=true
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
  stopped_state=$(vane_service_state)
  if [[ $stopped_state == activating ||
        $stopped_state == deactivating ||
        $stopped_state == failed ]]; then
    # A previous deployment can leave systemd retrying a binary that never
    # reached active. It has no healthy old worker to drain. Stop the failed
    # invocation explicitly; the process scan below still proves no writer
    # survived before the roll-forward binary starts.
    echo "stopping unhealthy vane invocation (state=$stopped_state)"
    print_vane_startup_diagnostics
    systemctl stop vane
    stopped_state=$(vane_service_state)
  fi
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
old_vane_restart_safe=true

echo "old vane worker drain verified; starting roll-forward binary"
vane_binary_replaced=true
install -m 0755 /opt/vane/bin/vane.next /opt/vane/bin/vane
rm -f /opt/vane/bin/vane.next
install -m 0644 /opt/vane/vane.service /etc/systemd/system/vane.service
systemctl daemon-reload
systemctl enable vane.service >/dev/null
systemctl start vane
wait_for_vane_ready
old_vane_recovery_required=false

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
/opt/vane/bin/gate -env /opt/vane/env/server.env
