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
release_suffix=${stage##*/.deploy-}
release_sha=${release_suffix:0:40}
[[ $release_sha =~ ^[0-9a-f]{40}$ ]] || {
  echo "unsafe release SHA" >&2
  exit 1
}
old_vane_recovery_required=false
old_vane_restart_safe=false
previous_vane_snapshot_ready=false
previous_vane_restart_expected=false
preserve_vane_snapshot=false
gateway_recovery_required=false
previous_gateway_snapshot_ready=false
preserve_gateway_snapshot=false
rollback_dir=/opt/vane/.rollback-vane-${stage##*/.deploy-}
research_legacy_env_keys=(
  VANE_DB_RESEARCH_RUNTIME_URL
  VANE_DB_RESEARCH_CAPABILITY_KEY_ID
  VANE_DB_RESEARCH_CAPABILITY_KEY_HEX
  VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS
  VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS
  VANE_RESEARCH_GATEWAY_SOCKET_PATH
  VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID
  VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID
  VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID
)
research_primary_env_keys=(
  VANE_DB_RESEARCH_CONTROL_URL
  "${research_legacy_env_keys[@]}"
)
[[ $rollback_dir =~ ^/opt/vane/\.rollback-vane-[0-9a-f]{40}-[0-9]+-[0-9]+$ ]] || {
  echo "unsafe vane rollback path" >&2
  exit 1
}

snapshot_previous_vane_release() {
  local live_binary=/opt/vane/bin/vane
  local live_unit=/etc/systemd/system/vane.service
  local runtime_env_path environment_count server_env_state legacy_env_state
  local owner_compat_env_state

  [[ ! -e $rollback_dir && ! -L $rollback_dir ]] || {
    echo "vane rollback snapshot path already exists" >&2
    return 1
  }
  [[ -f $live_binary && ! -L $live_binary &&
     -f $live_unit && ! -L $live_unit ]] || {
    echo "active vane release cannot be snapshotted safely" >&2
    return 1
  }
  environment_count=$(grep -Ec '^EnvironmentFile=' "$live_unit" || true)
  [[ $environment_count -eq 1 ]] || {
    echo "active vane unit must have exactly one runtime environment" >&2
    return 1
  }
  if grep -Fxq 'EnvironmentFile=/opt/vane/.env' "$live_unit"; then
    runtime_env_path=/opt/vane/.env
  elif grep -Fxq 'EnvironmentFile=/opt/vane/env/server.env' "$live_unit"; then
    runtime_env_path=/opt/vane/env/server.env
  elif grep -Fxq \
    'EnvironmentFile=/opt/vane/env/server-owner-compat.env' "$live_unit"; then
    runtime_env_path=/opt/vane/env/server-owner-compat.env
  else
    echo "active vane unit has an unsupported runtime environment" >&2
    return 1
  fi
  [[ -f $runtime_env_path && ! -L $runtime_env_path ]] || {
    echo "active vane runtime environment cannot be snapshotted safely" >&2
    return 1
  }
  if [[ -e /opt/vane/env/server.env || -L /opt/vane/env/server.env ]]; then
    [[ -f /opt/vane/env/server.env && ! -L /opt/vane/env/server.env ]] || {
      echo "server runtime environment is not a regular file" >&2
      return 1
    }
    server_env_state=present
  else
    server_env_state=absent
  fi
  if [[ -e /opt/vane/.env || -L /opt/vane/.env ]]; then
    [[ -f /opt/vane/.env && ! -L /opt/vane/.env ]] || {
      echo "legacy owner environment is not a regular file" >&2
      return 1
    }
    legacy_env_state=present
  else
    legacy_env_state=absent
  fi
  if [[ -e /opt/vane/env/server-owner-compat.env ||
        -L /opt/vane/env/server-owner-compat.env ]]; then
    [[ -f /opt/vane/env/server-owner-compat.env &&
       ! -L /opt/vane/env/server-owner-compat.env ]] || {
      echo "owner-compatible environment is not a regular file" >&2
      return 1
    }
    owner_compat_env_state=present
  else
    owner_compat_env_state=absent
  fi

  install -d -o root -g root -m 0700 "$rollback_dir"
  cp --archive --reflink=auto -- "$live_binary" "$rollback_dir/vane"
  cp --archive --reflink=auto -- "$live_unit" "$rollback_dir/vane.service"
  cp --archive --reflink=auto -- "$runtime_env_path" "$rollback_dir/runtime.env"
  printf '%s\n' "$runtime_env_path" >"$rollback_dir/runtime-env-path"
  printf '%s\n' "$server_env_state" >"$rollback_dir/server-env-state"
  printf '%s\n' "$legacy_env_state" >"$rollback_dir/legacy-env-state"
  printf '%s\n' "$owner_compat_env_state" \
    >"$rollback_dir/owner-compat-env-state"
  if [[ $server_env_state == present &&
        $runtime_env_path != /opt/vane/env/server.env ]]; then
    cp --archive --reflink=auto -- /opt/vane/env/server.env \
      "$rollback_dir/server.env"
  fi
  if [[ $legacy_env_state == present && $runtime_env_path != /opt/vane/.env ]]; then
    cp --archive --reflink=auto -- /opt/vane/.env "$rollback_dir/legacy.env"
  fi
  if [[ $owner_compat_env_state == present &&
        $runtime_env_path != /opt/vane/env/server-owner-compat.env ]]; then
    cp --archive --reflink=auto -- /opt/vane/env/server-owner-compat.env \
      "$rollback_dir/owner-compat.env"
  fi
  previous_vane_snapshot_ready=true
}

assert_legacy_owner_environment() {
  local legacy_env_state legacy_db_url legacy_mode
  read -r legacy_env_state <"$rollback_dir/legacy-env-state"
  [[ $legacy_env_state == present && -f /opt/vane/.env &&
     ! -L /opt/vane/.env ]] || {
    echo "trusted legacy owner environment is unavailable" >&2
    return 1
  }
  legacy_mode=$(stat -c '%a' /opt/vane/.env)
  [[ $(stat -c '%U' /opt/vane/.env) == root &&
     $legacy_mode =~ ^[0-7]{3,4}$ &&
     $((8#$legacy_mode & 0022)) -eq 0 ]] || {
    echo "legacy owner environment has unsafe ownership or write mode" >&2
    return 1
  }
  if [[ -f $rollback_dir/legacy.env ]]; then
    cmp -s -- "$rollback_dir/legacy.env" /opt/vane/.env || {
      echo "legacy owner environment drifted after its rollback snapshot" >&2
      return 1
    }
  else
    cmp -s -- "$rollback_dir/runtime.env" /opt/vane/.env || {
      echo "legacy owner environment drifted after its rollback snapshot" >&2
      return 1
    }
  fi
  legacy_db_url=$(legacy_env_value VANE_DB_URL)
  [[ $legacy_db_url == postgres://vane:* &&
     $legacy_db_url != postgres://vane_server_runtime:* ]] || {
    echo "primary runtime release fence requires the legacy owner database DSN" >&2
    return 1
  }
}

# Temporary release fence: the primary Store still contains recovery and
# reconciliation paths which are not proven safe under vane_server_runtime.
# Keep the proven owner-compatible process contract until the complete Store
# RLS graph has a real PostgreSQL gate. Research migration and gateway
# processes remain independently split below.
assert_legacy_primary_runtime_contract() {
  local live_unit=/etc/systemd/system/vane.service runtime_env_path
  [[ $previous_vane_snapshot_ready == true ]] || {
    echo "primary runtime release fence has no active release snapshot" >&2
    return 1
  }
  read -r runtime_env_path <"$rollback_dir/runtime-env-path"
  [[ $runtime_env_path == /opt/vane/.env &&
     $(grep -Ec '^User=' "$live_unit" || true) -eq 1 &&
     $(grep -c -Fx 'User=root' "$live_unit" || true) -eq 1 &&
     $(grep -Ec '^EnvironmentFile=' "$live_unit" || true) -eq 1 &&
     $(grep -c -Fx 'EnvironmentFile=/opt/vane/.env' \
       "$live_unit" || true) -eq 1 &&
     $(grep -Ec '^ExecStart=' "$live_unit" || true) -eq 1 &&
     $(grep -c -Fx 'ExecStart=/opt/vane/bin/vane' \
       "$live_unit" || true) -eq 1 ]] || {
    echo "primary runtime release fence requires legacy root + .env contract" >&2
    return 1
  }
  assert_legacy_owner_environment
  cmp -s -- "$rollback_dir/vane.service" "$live_unit" &&
    cmp -s -- "$rollback_dir/runtime.env" /opt/vane/.env || {
      echo "primary runtime contract drifted after its rollback snapshot" >&2
      return 1
    }
}

assert_known_split_primary_runtime_contract() {
  local live_unit=/etc/systemd/system/vane.service runtime_env_path
  read -r runtime_env_path <"$rollback_dir/runtime-env-path"
  [[ $previous_vane_snapshot_ready == true &&
     $runtime_env_path == /opt/vane/env/server.env &&
     $(grep -Ec '^User=' "$live_unit" || true) -eq 1 &&
     $(grep -c -Fx 'User=vane' "$live_unit" || true) -eq 1 &&
     $(grep -Ec '^EnvironmentFile=' "$live_unit" || true) -eq 1 &&
     $(grep -c -Fx 'EnvironmentFile=/opt/vane/env/server.env' \
       "$live_unit" || true) -eq 1 &&
     $(grep -Ec '^ExecStart=' "$live_unit" || true) -eq 1 &&
     $(grep -c -Fx 'ExecStart=/opt/vane/bin/vane' \
       "$live_unit" || true) -eq 1 ]] || {
    echo "inactive primary runtime is not the known split contract" >&2
    return 1
  }
  cmp -s -- "$rollback_dir/vane.service" "$live_unit" &&
    cmp -s -- "$rollback_dir/runtime.env" /opt/vane/env/server.env || {
      echo "split primary runtime drifted after its rollback snapshot" >&2
      return 1
    }
  assert_legacy_owner_environment
}

validate_legacy_compat_unit() {
  local unit=$1 mode=${2:-strict}
  local load_credential_count recovery_credential_count
  local recovery_credential_exact_count
  [[ -f $unit && ! -L $unit &&
     $(grep -Ec '^User=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'User=vane' "$unit" || true) -eq 1 &&
     $(grep -Ec '^Group=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'Group=vane' "$unit" || true) -eq 1 &&
     $(grep -Ec '^WorkingDirectory=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'WorkingDirectory=/opt/vane' "$unit" || true) -eq 1 &&
     $(grep -Ec '^EnvironmentFile=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx \
       'EnvironmentFile=/opt/vane/env/server-owner-compat.env' \
       "$unit" || true) -eq 1 &&
     $(grep -Ec '^ExecStart=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'ExecStart=/opt/vane/bin/vane' "$unit" || true) -eq 1 &&
     $(grep -Ec '^Exec(StartPre|StartPost|Reload)=' "$unit" || true) -eq 0 &&
     $(grep -c -Fx 'NoNewPrivileges=yes' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'ProtectSystem=strict' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'ProtectHome=yes' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'PrivateTmp=yes' "$unit" || true) -eq 1 ]] || {
    echo "legacy compatibility unit does not match the audited contract" >&2
    return 1
  }
  load_credential_count=$(grep -Ec '^LoadCredential=' "$unit" || true)
  recovery_credential_count=$(grep -Ec \
    '^LoadCredential=native_v3_edit_recovery_db_url:' "$unit" || true)
  recovery_credential_exact_count=$(grep -c -Fx \
    'LoadCredential=native_v3_edit_recovery_db_url:/etc/vane/credentials/native_v3_edit_recovery_db_url' \
    "$unit" || true)
  case "$mode" in
    strict)
      [[ $load_credential_count -eq 1 &&
         $recovery_credential_count -eq 1 &&
         $recovery_credential_exact_count -eq 1 ]] || {
        echo "legacy compatibility unit has no exact native V3 edit recovery credential" >&2
        return 1
      }
      ;;
    existing)
      [[ $load_credential_count -eq 0 ||
         ($load_credential_count -eq 1 &&
          $recovery_credential_count -eq 1 &&
          $recovery_credential_exact_count -eq 1) ]] || {
        echo "existing legacy compatibility unit has an unsafe native V3 edit recovery credential" >&2
        return 1
      }
      ;;
    *)
      echo "unknown legacy compatibility unit validation mode" >&2
      return 1
      ;;
  esac
}

validate_native_v3_edit_recovery_unit() {
  local unit=$1
  [[ -f $unit && ! -L $unit &&
     $(grep -Ec '^LoadCredential=native_v3_edit_recovery_db_url:' \
       "$unit" || true) -eq 1 &&
     $(grep -c -Fx \
       'LoadCredential=native_v3_edit_recovery_db_url:/etc/vane/credentials/native_v3_edit_recovery_db_url' \
       "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'Requires=vane-research-gateway.socket' \
       "$unit" || true) -eq 1 &&
     $(grep -c 'vane-migrate.service' "$unit" || true) -eq 0 ]] || {
    echo "primary unit has no exact native V3 edit recovery credential" >&2
    return 1
  }
}

validate_gateway_unit() {
  local unit=$1
  [[ -f $unit && ! -L $unit &&
     $(grep -Ec '^User=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'User=vane-research-gateway' "$unit" || true) -eq 1 &&
     $(grep -Ec '^Group=' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'Group=vane-research-gateway' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'Requires=vane-research-gateway.socket' \
       "$unit" || true) -eq 1 &&
     $(grep -c 'vane-migrate.service' "$unit" || true) -eq 0 &&
     $(grep -c -Fx \
       'EnvironmentFile=/opt/vane/env/research-gateway.env' \
       "$unit" || true) -eq 1 &&
     $(grep -c -Fx \
       'LoadCredential=gateway_db_url:/etc/vane/credentials/gateway_db_url' \
       "$unit" || true) -eq 1 &&
     $(grep -c -Fx \
       'LoadCredential=llm_api_key_gen1:/etc/vane/credentials/research_llm_api_key_gen1' \
       "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'ExecStart=/opt/vane/bin/vane-research-gateway' \
       "$unit" || true) -eq 1 &&
     $(grep -Ec '^Exec(StartPre|StartPost|Reload)=' "$unit" || true) -eq 0 &&
     $(grep -c -Fx 'NoNewPrivileges=yes' "$unit" || true) -eq 1 &&
     $(grep -c -Fx 'ProtectSystem=strict' "$unit" || true) -eq 1 ]] || {
    echo "research gateway unit does not match the deploy-owned contract" >&2
    return 1
  }
}

owner_snapshot_path() {
  if [[ -f $rollback_dir/legacy.env ]]; then
    printf '%s' "$rollback_dir/legacy.env"
  else
    printf '%s' "$rollback_dir/runtime.env"
  fi
}

exact_env_line() {
  local path=$1 name=$2
  [[ -f $path && ! -L $path &&
     $(grep -c "^${name}=" "$path" || true) -eq 1 ]] || return 1
  grep "^${name}=" "$path"
}

assert_research_settings_exact() {
  local destination=$1 source=${2:-/opt/vane/env/server.env}
  local name source_line dest_line
  [[ -f $source && ! -L $source && -f $destination &&
     ! -L $destination ]] || return 1
  [[ $(stat -c '%U:%G:%a' "$source") == root:vane:640 ]] || {
    echo "restricted server environment has unsafe ownership or mode" >&2
    return 1
  }
  if LC_ALL=C grep -q $'\r' "$source" "$destination"; then
    echo "runtime environment contains a carriage return" >&2
    return 1
  fi
  for name in "${research_primary_env_keys[@]}"; do
    source_line=$(exact_env_line "$source" "$name") || {
      echo "restricted server environment is missing a required research setting: $name" >&2
      return 1
    }
    dest_line=$(exact_env_line "$destination" "$name") || {
      echo "owner-compatible environment is missing a required research setting: $name" >&2
      return 1
    }
    [[ $dest_line == "$source_line" ]] || {
      echo "owner-compatible research setting does not match restricted source: $name" >&2
      return 1
    }
  done
  if grep -Eq \
    '^(POSTGRES_PASSWORD|VANE_MIGRATION_DB_URL|VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL|VANE_GATEWAY_[A-Z0-9_]+)=' \
    "$destination"; then
    echo "owner-compatible environment contains a forbidden process credential" >&2
    return 1
  fi
}

assert_deepseek_flash_agent_route() {
  local path=$1
  [[ -f $path && ! -L $path &&
     $(grep -c '^VANE_LLM_AGENT_MODEL=deepseek-v4-flash$' "$path" || true) -eq 1 &&
     $(grep -Ec '^VANE_LLM_AGENT_(PROVIDER|BASE_URL|API_KEY)=' "$path" || true) -eq 0 ]] || {
    echo "primary Agent route is not exact DeepSeek v4 Flash" >&2
    return 1
  }
}

assert_legacy_research_settings_exact() {
  local destination=$1 source=${2:-/opt/vane/env/server.env}
  local name source_line dest_line
  [[ -f $source && ! -L $source && -f $destination &&
     ! -L $destination ]] || return 1
  [[ $(stat -c '%U:%G:%a' "$source") == root:vane:640 ]] || {
    echo "restricted server environment has unsafe ownership or mode" >&2
    return 1
  }
  [[ $(grep -c '^VANE_DB_RESEARCH_CONTROL_URL=' "$source" || true) -eq 0 &&
     $(grep -c '^VANE_DB_RESEARCH_CONTROL_URL=' "$destination" || true) -eq 0 ]] || {
    echo "legacy owner-compatible contract has an unexpected research control Store" >&2
    return 1
  }
  if LC_ALL=C grep -q $'\r' "$source" "$destination"; then
    echo "runtime environment contains a carriage return" >&2
    return 1
  fi
  for name in "${research_legacy_env_keys[@]}"; do
    source_line=$(exact_env_line "$source" "$name") || {
      echo "legacy restricted environment is missing a required research setting: $name" >&2
      return 1
    }
    dest_line=$(exact_env_line "$destination" "$name") || {
      echo "legacy owner-compatible environment is missing a required research setting: $name" >&2
      return 1
    }
    [[ $dest_line == "$source_line" ]] || {
      echo "legacy owner-compatible research setting drifted: $name" >&2
      return 1
    }
  done
  if grep -Eq \
    '^(POSTGRES_PASSWORD|VANE_MIGRATION_DB_URL|VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL|VANE_GATEWAY_[A-Z0-9_]+)=' \
    "$destination"; then
    echo "legacy owner-compatible environment contains a forbidden process credential" >&2
    return 1
  fi
}

build_owner_compatible_environment() {
  local owner_env_source=$1 destination=$2
  local server_env_source=${3:-/opt/vane/env/server.env} name
  [[ -f $owner_env_source && ! -L $owner_env_source &&
     -f $server_env_source && ! -L $server_env_source ]] || {
    echo "owner-compatible environment inputs are unavailable" >&2
    return 1
  }
  awk '!/^(POSTGRES_PASSWORD|VANE_MIGRATION_DB_URL|VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL|VANE_GATEWAY_[A-Z0-9_]+|VANE_DB_RESEARCH_CONTROL_URL|VANE_DB_RESEARCH_RUNTIME_URL|VANE_DB_RESEARCH_CAPABILITY_KEY_ID|VANE_DB_RESEARCH_CAPABILITY_KEY_HEX|VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS|VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS|VANE_RESEARCH_GATEWAY_SOCKET_PATH|VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID|VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID|VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID|VANE_LLM_AGENT_PROVIDER|VANE_LLM_AGENT_BASE_URL|VANE_LLM_AGENT_API_KEY|VANE_LLM_AGENT_MODEL)=/' \
    "$owner_env_source" >"$destination"
  printf 'VANE_LLM_AGENT_MODEL=deepseek-v4-flash\n' >>"$destination"
  for name in "${research_primary_env_keys[@]}"; do
    exact_env_line "$server_env_source" "$name" >>"$destination" || {
      echo "restricted server environment is missing a required research setting: $name" >&2
      return 1
    }
  done
  assert_research_settings_exact "$destination" "$server_env_source"
  assert_deepseek_flash_agent_route "$destination"
}

stage_research_control_environment() {
  local source=$1 destination=$2
  local next=${destination}.stage-next
  local count primary_line primary_url control_line
  [[ $source == /opt/vane/env/server.env &&
     $destination == /opt/vane/env/server.env.release-next ]] || {
    echo "unsafe research control environment staging path" >&2
    return 1
  }
  [[ -f $source && ! -L $source ]] || {
    echo "restricted server environment is unavailable" >&2
    return 1
  }
  count=$(grep -c '^VANE_DB_RESEARCH_CONTROL_URL=' "$source" || true)
  primary_line=$(exact_env_line "$source" VANE_DB_URL) || {
    echo "restricted server environment has no exact primary runtime DSN" >&2
    return 1
  }
  primary_url=${primary_line#VANE_DB_URL=}
  [[ $primary_url == postgres://vane_server_runtime:* ]] || {
    echo "research control Store requires the restricted server runtime DSN" >&2
    return 1
  }
  rm -f -- "$next" "$destination"
  awk '!/^(VANE_DB_RESEARCH_CONTROL_URL|VANE_LLM_AGENT_PROVIDER|VANE_LLM_AGENT_BASE_URL|VANE_LLM_AGENT_API_KEY|VANE_LLM_AGENT_MODEL)=/' \
    "$source" >"$next"
  printf 'VANE_LLM_AGENT_MODEL=deepseek-v4-flash\n' >>"$next"
  case "$count" in
    0)
      printf 'VANE_DB_RESEARCH_CONTROL_URL=%s\n' "$primary_url" >>"$next"
      ;;
    1)
      control_line=$(exact_env_line "$source" VANE_DB_RESEARCH_CONTROL_URL) || return 1
      [[ ${control_line#VANE_DB_RESEARCH_CONTROL_URL=} == "$primary_url" ]] || {
        echo "research control Store DSN does not match restricted server runtime" >&2
        return 1
      }
      printf '%s\n' "$control_line" >>"$next"
      ;;
    *)
      echo "restricted server environment has duplicate research control DSNs" >&2
      return 1
      ;;
  esac
  chown root:vane "$next"
  chmod 0640 "$next"
  assert_deepseek_flash_agent_route "$next"
  mv -f -- "$next" "$destination"
}

assert_restricted_server_environment_readonly() {
  local path=${1:-/opt/vane/env/server.env}
  [[ -f $path && ! -L $path &&
     $(stat -c '%U:%G:%a' "$path") == root:vane:640 ]] || {
    echo "restricted server environment is not root-owned read-only runtime data" >&2
    return 1
  }
  if runuser -u vane -- test -w "$path"; then
    echo "primary service user can write the restricted server environment" >&2
    return 1
  fi
}

assert_owner_compatible_primary_identity() {
  local mode=${1:-strict}
  local owner_env_source snapshot_db_url live_db_url
  validate_legacy_compat_unit /etc/systemd/system/vane.service "$mode"
  [[ -f /opt/vane/env/server-owner-compat.env &&
     ! -L /opt/vane/env/server-owner-compat.env &&
     $(stat -c '%U:%G:%a' /opt/vane/env/server-owner-compat.env) == \
       root:vane:640 ]] || {
    echo "owner-compatible environment has unsafe ownership or mode" >&2
    return 1
  }
  owner_env_source=$(owner_snapshot_path)
  snapshot_db_url=$(sed -n 's/^VANE_DB_URL=//p' "$owner_env_source")
  live_db_url=$(sed -n 's/^VANE_DB_URL=//p' \
    /opt/vane/env/server-owner-compat.env)
  [[ -n $snapshot_db_url && $live_db_url == "$snapshot_db_url" &&
     $live_db_url == postgres://vane:* &&
     $live_db_url != postgres://vane_server_runtime:* ]] || {
    echo "owner database DSN changed during compatibility cutover" >&2
     return 1
  }
}

assert_audited_legacy_primary_runtime_contract() {
  local mode=${1:-strict}
  local research_control_url
  assert_owner_compatible_primary_identity "$mode"
  research_control_url=$(sed -n 's/^VANE_DB_RESEARCH_CONTROL_URL=//p' \
    /opt/vane/env/server-owner-compat.env)
  [[ $research_control_url == postgres://vane_server_runtime:* ]] || {
    echo "owner-compatible environment has no restricted research control Store" >&2
    return 1
  }
  assert_research_settings_exact /opt/vane/env/server-owner-compat.env
}

assert_audited_legacy_primary_runtime_contract_v1() {
  local mode=${1:-strict}
  assert_owner_compatible_primary_identity "$mode"
  assert_legacy_research_settings_exact \
    /opt/vane/env/server-owner-compat.env
}

assert_existing_audited_primary_runtime_contract() {
  local live_count restricted_count
  live_count=$(grep -c '^VANE_DB_RESEARCH_CONTROL_URL=' \
    /opt/vane/env/server-owner-compat.env || true)
  restricted_count=$(grep -c '^VANE_DB_RESEARCH_CONTROL_URL=' \
    /opt/vane/env/server.env || true)
  if [[ $live_count -eq 0 && $restricted_count -eq 0 ]]; then
    assert_audited_legacy_primary_runtime_contract_v1 existing
  elif [[ $live_count -eq 1 && $restricted_count -eq 1 ]]; then
    assert_audited_legacy_primary_runtime_contract existing
  else
    echo "owner-compatible runtime has a mixed research control contract" >&2
    return 1
  fi
  cmp -s -- "$rollback_dir/vane.service" \
    /etc/systemd/system/vane.service &&
    cmp -s -- "$rollback_dir/runtime.env" \
      /opt/vane/env/server-owner-compat.env || {
      echo "audited primary runtime drifted after its rollback snapshot" >&2
      return 1
    }
}

gateway_functional() {
  local http_code
  http_code=$(runuser -u vane -- curl --silent --show-error \
    --output /dev/null --write-out '%{http_code}' --max-time 2 \
    --unix-socket /run/vane-research-gateway/gateway.sock \
    http://research-gateway/v1/research/llm/execute 2>/dev/null || true)
  [[ $http_code == 405 ]] &&
    systemctl is-active --quiet vane-research-gateway.service
}

gateway_snapshot_keys=(
  binary
  opt-service
  opt-socket
  systemd-service
  systemd-socket
  environment
  database-credential
  llm-credential
)
gateway_snapshot_paths=(
  /opt/vane/bin/vane-research-gateway
  /opt/vane/vane-research-gateway.service
  /opt/vane/vane-research-gateway.socket
  /etc/systemd/system/vane-research-gateway.service
  /etc/systemd/system/vane-research-gateway.socket
  /opt/vane/env/research-gateway.env
  /etc/vane/credentials/gateway_db_url
  /etc/vane/credentials/research_llm_api_key_gen1
)

snapshot_gateway_regular_path() {
  local snapshot=$1 key=$2 path=$3
  if [[ -e $path || -L $path ]]; then
    [[ -f $path && ! -L $path ]] || {
      echo "research gateway path is not a regular file: $path" >&2
      return 1
    }
    cp --archive --reflink=auto -- "$path" "$snapshot/$key"
    printf 'present\n' >"$snapshot/$key.state"
  else
    printf 'absent\n' >"$snapshot/$key.state"
  fi
}

snapshot_previous_gateway_release() {
  local snapshot=$rollback_dir/gateway index key path state
  local service_active=false socket_active=false
  local service_enabled=false socket_enabled=false
  [[ $previous_vane_snapshot_ready == true && -d $rollback_dir &&
     ! -L $rollback_dir && ! -e $snapshot && ! -L $snapshot ]] || {
    echo "research gateway rollback snapshot path is unsafe" >&2
    return 1
  }
  install -d -o root -g root -m 0700 "$snapshot"

  for index in "${!gateway_snapshot_keys[@]}"; do
    key=${gateway_snapshot_keys[$index]}
    path=${gateway_snapshot_paths[$index]}
    snapshot_gateway_regular_path "$snapshot" "$key" "$path"
  done

  if [[ -e /run/vane-research-gateway ||
        -L /run/vane-research-gateway ]]; then
    [[ -d /run/vane-research-gateway &&
       ! -L /run/vane-research-gateway ]] || {
      echo "research gateway runtime path is not a real directory" >&2
      return 1
    }
    printf 'present\n' >"$snapshot/runtime-directory.state"
    stat -c '%u' /run/vane-research-gateway \
      >"$snapshot/runtime-directory.uid"
    stat -c '%g' /run/vane-research-gateway \
      >"$snapshot/runtime-directory.gid"
    stat -c '%a' /run/vane-research-gateway \
      >"$snapshot/runtime-directory.mode"
  else
    printf 'absent\n' >"$snapshot/runtime-directory.state"
  fi

  if systemctl is-active --quiet vane-research-gateway.service; then
    service_active=true
  fi
  if systemctl is-active --quiet vane-research-gateway.socket; then
    socket_active=true
  fi
  if systemctl is-enabled --quiet vane-research-gateway.service; then
    service_enabled=true
  fi
  if systemctl is-enabled --quiet vane-research-gateway.socket; then
    socket_enabled=true
  fi
  printf '%s\n' "$service_active" >"$snapshot/service-active"
  printf '%s\n' "$socket_active" >"$snapshot/socket-active"
  printf '%s\n' "$service_enabled" >"$snapshot/service-enabled"
  printf '%s\n' "$socket_enabled" >"$snapshot/socket-enabled"

  if [[ $service_active == true ]]; then
    [[ $socket_active == true ]] || {
      echo "active research gateway has no active socket" >&2
      return 1
    }
    for key in binary systemd-service systemd-socket environment \
      database-credential llm-credential; do
      read -r state <"$snapshot/$key.state"
      [[ $state == present ]] || {
        echo "active research gateway snapshot is incomplete: $key" >&2
        return 1
      }
    done
    gateway_functional || {
      echo "active research gateway is not functional" >&2
      return 1
    }
  fi
  previous_gateway_snapshot_ready=true
}

gateway_regular_matches_snapshot() {
  local snapshot=$1 key=$2 path=$3 state
  local snapshot_metadata path_metadata
  read -r state <"$snapshot/$key.state" || return 1
  if [[ $state == absent ]]; then
    if [[ ! -e $path && ! -L $path ]]; then return 0; fi
    return 1
  fi
  [[ $state == present && -f $snapshot/$key && ! -L $snapshot/$key &&
     -f $path && ! -L $path ]] || return 1
  cmp -s -- "$snapshot/$key" "$path" || return 1
  snapshot_metadata=$(stat -c '%u:%g:%a' "$snapshot/$key") || return 1
  path_metadata=$(stat -c '%u:%g:%a' "$path") || return 1
  [[ $path_metadata == "$snapshot_metadata" ]] || return 1
  return 0
}

verify_gateway_runtime_restore() {
  local snapshot=$1 state expected actual uid gid mode
  read -r state <"$snapshot/runtime-directory.state" || return 1
  if [[ $state == absent ]]; then
    [[ ! -e /run/vane-research-gateway &&
       ! -L /run/vane-research-gateway ]] || {
      echo "research gateway runtime directory should be absent" >&2
      return 1
    }
    return 0
  fi
  [[ $state == present && -d /run/vane-research-gateway &&
     ! -L /run/vane-research-gateway ]] || {
    echo "research gateway runtime directory type did not restore" >&2
    return 1
  }
  read -r uid <"$snapshot/runtime-directory.uid" || return 1
  read -r gid <"$snapshot/runtime-directory.gid" || return 1
  read -r mode <"$snapshot/runtime-directory.mode" || return 1
  expected=$uid:$gid:$mode
  actual=$(stat -c '%u:%g:%a' /run/vane-research-gateway) || return 1
  [[ $actual == "$expected" ]] || {
    echo "research gateway runtime directory metadata did not restore" >&2
    return 1
  }
}

prepare_gateway_regular_restore() {
  local snapshot=$1 key=$2 path=$3 state
  read -r state <"$snapshot/$key.state" || return 1
  [[ $state == present || $state == absent ]] || {
    echo "research gateway snapshot has an invalid path state: $key" >&2
    return 1
  }
  rm -f -- "$path.rollback-next" || return 1
  if [[ $state == present ]]; then
    [[ -f $snapshot/$key && ! -L $snapshot/$key ]] || {
      echo "research gateway snapshot is incomplete: $key" >&2
      return 1
    }
    cp --archive --reflink=auto -- "$snapshot/$key" \
      "$path.rollback-next" || {
      echo "failed to prepare research gateway restore: $key" >&2
      return 1
    }
  fi
  gateway_regular_matches_snapshot "$snapshot" "$key" \
    "$path.rollback-next" || {
    echo "prepared research gateway restore does not match snapshot: $key" >&2
    return 1
  }
}

commit_gateway_regular_restore() {
  local snapshot=$1 key=$2 path=$3 state
  read -r state <"$snapshot/$key.state" || return 1
  if [[ $state == present ]]; then
    mv -f -- "$path.rollback-next" "$path" || {
      echo "failed to commit research gateway restore: $key" >&2
      return 1
    }
  else
    rm -f -- "$path" || {
      echo "failed to remove absent research gateway path: $key" >&2
      return 1
    }
  fi
  gateway_regular_matches_snapshot "$snapshot" "$key" "$path" || {
    echo "committed research gateway restore does not match snapshot: $key" >&2
    return 1
  }
}

validate_gateway_restore_snapshot() {
  local snapshot=$1 index key state name
  local service_active socket_active
  [[ -d $snapshot && ! -L $snapshot ]] || return 1
  for name in service-active socket-active service-enabled socket-enabled; do
    [[ -f $snapshot/$name && ! -L $snapshot/$name ]] || {
      echo "research gateway snapshot is missing systemd state: $name" >&2
      return 1
    }
    read -r state <"$snapshot/$name" || return 1
    [[ $state == true || $state == false ]] || {
      echo "research gateway snapshot has invalid systemd state: $name" >&2
      return 1
    }
  done
  read -r service_active <"$snapshot/service-active" || return 1
  read -r socket_active <"$snapshot/socket-active" || return 1
  [[ $service_active == false || $socket_active == true ]] || {
    echo "research gateway snapshot has active service without socket" >&2
    return 1
  }

  for index in "${!gateway_snapshot_keys[@]}"; do
    key=${gateway_snapshot_keys[$index]}
    [[ -f $snapshot/$key.state && ! -L $snapshot/$key.state ]] || {
      echo "research gateway snapshot is missing path state: $key" >&2
      return 1
    }
    read -r state <"$snapshot/$key.state" || return 1
    [[ $state == present || $state == absent ]] || {
      echo "research gateway snapshot has invalid path state: $key" >&2
      return 1
    }
    if [[ $state == present ]]; then
      [[ -f $snapshot/$key && ! -L $snapshot/$key ]] || {
        echo "research gateway snapshot is missing member: $key" >&2
        return 1
      }
    fi
  done

  [[ -f $snapshot/runtime-directory.state &&
     ! -L $snapshot/runtime-directory.state ]] || {
    echo "research gateway snapshot is missing runtime directory state" >&2
    return 1
  }
  read -r state <"$snapshot/runtime-directory.state" || return 1
  [[ $state == present || $state == absent ]] || {
    echo "research gateway snapshot has invalid runtime directory state" >&2
    return 1
  }
  if [[ $state == present ]]; then
    for name in uid gid mode; do
      [[ -f $snapshot/runtime-directory.$name &&
         ! -L $snapshot/runtime-directory.$name ]] || {
        echo "research gateway snapshot is missing runtime metadata: $name" >&2
        return 1
      }
    done
    read -r state <"$snapshot/runtime-directory.uid" || return 1
    [[ $state =~ ^[0-9]+$ ]] || return 1
    read -r state <"$snapshot/runtime-directory.gid" || return 1
    [[ $state =~ ^[0-9]+$ ]] || return 1
    read -r state <"$snapshot/runtime-directory.mode" || return 1
    [[ $state =~ ^[0-7]{3,4}$ ]] || return 1
  fi
}

gateway_unit_active_boolean() {
  local unit=$1 output status=0
  if output=$(systemctl is-active "$unit" 2>/dev/null); then
    status=0
  else
    status=$?
  fi
  case "$output:$status" in
    active:0) printf 'true\n' ;;
    inactive:*|failed:*|unknown:*) printf 'false\n' ;;
    *)
      echo "unable to determine research gateway active state: $unit" >&2
      return 1
      ;;
  esac
}

gateway_unit_enabled_boolean() {
  local unit=$1 output status=0
  if output=$(systemctl is-enabled "$unit" 2>/dev/null); then
    status=0
  else
    status=$?
  fi
  case "$output:$status" in
    enabled:0|enabled-runtime:0|linked:0|linked-runtime:0|alias:0)
      printf 'true\n'
      ;;
    disabled:*|masked:*|masked-runtime:*|static:*|indirect:*|generated:*|transient:*|not-found:*|bad:*)
      printf 'false\n'
      ;;
    *)
      echo "unable to determine research gateway enablement: $unit" >&2
      return 1
      ;;
  esac
}

quiesce_gateway_unit() {
  local unit=$1 active enabled
  active=$(gateway_unit_active_boolean "$unit") || return 1
  if [[ $active == true ]]; then
    systemctl stop "$unit" || {
      echo "failed to stop research gateway unit: $unit" >&2
      return 1
    }
    active=$(gateway_unit_active_boolean "$unit") || return 1
    [[ $active == false ]] || {
      echo "research gateway unit remained active after stop: $unit" >&2
      return 1
    }
  fi
  enabled=$(gateway_unit_enabled_boolean "$unit") || return 1
  if [[ $enabled == true ]]; then
    systemctl disable "$unit" >/dev/null || {
      echo "failed to disable research gateway unit: $unit" >&2
      return 1
    }
    enabled=$(gateway_unit_enabled_boolean "$unit") || return 1
    [[ $enabled == false ]] || {
      echo "research gateway unit remained enabled after disable: $unit" >&2
      return 1
    }
  fi
}

verify_gateway_systemd_snapshot() {
  local snapshot=$1 expected actual name unit
  for name in service socket; do
    unit=vane-research-gateway.$name
    read -r expected <"$snapshot/$name-active" || return 1
    actual=$(gateway_unit_active_boolean "$unit") || return 1
    [[ $actual == "$expected" ]] || {
      echo "research gateway active state did not converge: $unit" >&2
      return 1
    }
    read -r expected <"$snapshot/$name-enabled" || return 1
    actual=$(gateway_unit_enabled_boolean "$unit") || return 1
    [[ $actual == "$expected" ]] || {
      echo "research gateway enablement did not converge: $unit" >&2
      return 1
    }
  done
}

restore_previous_gateway_release() (
  set -uo pipefail
  local snapshot=$rollback_dir/gateway index key path state name
  local runtime_uid runtime_gid runtime_mode
  local service_active socket_active service_enabled socket_enabled
  local service_unit_known=false socket_unit_known=false
  local gateway_pid gateway_exe attempt
  [[ $previous_gateway_snapshot_ready == true && -d $snapshot &&
     ! -L $snapshot ]] || return 1
  validate_gateway_restore_snapshot "$snapshot" || return 1
  read -r service_active <"$snapshot/service-active" || return 1
  read -r socket_active <"$snapshot/socket-active" || return 1
  read -r service_enabled <"$snapshot/service-enabled" || return 1
  read -r socket_enabled <"$snapshot/socket-enabled" || return 1

  # Every snapshot member and state is validated before even scratch files are
  # prepared. Every prepare must succeed before live services or paths change.
  for index in "${!gateway_snapshot_keys[@]}"; do
    key=${gateway_snapshot_keys[$index]}
    path=${gateway_snapshot_paths[$index]}
    prepare_gateway_regular_restore "$snapshot" "$key" "$path" || return 1
  done
  read -r state <"$snapshot/runtime-directory.state" || return 1
  if [[ $state == present ]]; then
    read -r runtime_uid <"$snapshot/runtime-directory.uid" || return 1
    read -r runtime_gid <"$snapshot/runtime-directory.gid" || return 1
    read -r runtime_mode <"$snapshot/runtime-directory.mode" || return 1
  fi

  if [[ -e /etc/systemd/system/vane-research-gateway.service ||
        -L /etc/systemd/system/vane-research-gateway.service ]]; then
    service_unit_known=true
  fi
  if [[ -e /etc/systemd/system/vane-research-gateway.socket ||
        -L /etc/systemd/system/vane-research-gateway.socket ]]; then
    socket_unit_known=true
  fi
  read -r state <"$snapshot/systemd-service.state" || return 1
  if [[ $state == present ]]; then service_unit_known=true; fi
  read -r state <"$snapshot/systemd-socket.state" || return 1
  if [[ $state == present ]]; then socket_unit_known=true; fi

  quiesce_gateway_unit vane-research-gateway.service || return 1
  quiesce_gateway_unit vane-research-gateway.socket || return 1
  if [[ $service_unit_known == true ]]; then
    systemctl reset-failed vane-research-gateway.service || return 1
  fi
  if [[ $socket_unit_known == true ]]; then
    systemctl reset-failed vane-research-gateway.socket || return 1
  fi

  for index in "${!gateway_snapshot_keys[@]}"; do
    key=${gateway_snapshot_keys[$index]}
    path=${gateway_snapshot_paths[$index]}
    commit_gateway_regular_restore "$snapshot" "$key" "$path" || return 1
  done

  read -r state <"$snapshot/runtime-directory.state" || return 1
  if [[ $state == present ]]; then
    if [[ -e /run/vane-research-gateway ||
          -L /run/vane-research-gateway ]]; then
      [[ -d /run/vane-research-gateway &&
         ! -L /run/vane-research-gateway ]] || return 1
    else
      mkdir -- /run/vane-research-gateway || return 1
    fi
    chown "$runtime_uid:$runtime_gid" \
      /run/vane-research-gateway || return 1
    chmod "$runtime_mode" /run/vane-research-gateway || return 1
  elif [[ -e /run/vane-research-gateway ||
          -L /run/vane-research-gateway ]]; then
    [[ -d /run/vane-research-gateway &&
       ! -L /run/vane-research-gateway ]] || return 1
    rmdir -- /run/vane-research-gateway || return 1
  fi
  verify_gateway_runtime_restore "$snapshot" || return 1
  systemctl daemon-reload || return 1
  if [[ $socket_enabled == true ]]; then
    systemctl enable vane-research-gateway.socket >/dev/null || return 1
  fi
  if [[ $service_enabled == true ]]; then
    systemctl enable vane-research-gateway.service >/dev/null || return 1
  fi
  if [[ $socket_active == true ]]; then
    systemctl start vane-research-gateway.socket || return 1
  fi
  if [[ $service_active == true ]]; then
    systemctl start vane-research-gateway.service || return 1
  fi
  verify_gateway_systemd_snapshot "$snapshot" || return 1
  verify_gateway_runtime_restore "$snapshot" || return 1
  if [[ $service_active == false ]]; then
    echo "previous quiescent research gateway contract restored" >&2
    return 0
  fi
  for attempt in {1..12}; do
    gateway_pid=$(systemctl show vane-research-gateway.service \
      --property=MainPID --value) || return 1
    gateway_exe=
    if [[ $gateway_pid =~ ^[1-9][0-9]*$ ]]; then
      gateway_exe=$(readlink /proc/"$gateway_pid"/exe 2>/dev/null || true)
    fi
    if [[ $gateway_exe == /opt/vane/bin/vane-research-gateway ]] &&
       gateway_functional; then
      verify_gateway_systemd_snapshot "$snapshot" || return 1
      verify_gateway_runtime_restore "$snapshot" || return 1
      echo "previous research gateway contract recovery verified" >&2
      return 0
    fi
    sleep 1
  done
  echo "previous research gateway contract recovery failed readiness" >&2
  return 1
)

commit_legacy_primary_release() {
  local owner_env_source
  [[ $previous_vane_snapshot_ready == true &&
     -f /opt/vane/bin/vane.next && ! -L /opt/vane/bin/vane.next &&
     -f /opt/vane/vane-legacy-compat.service &&
     ! -L /opt/vane/vane-legacy-compat.service ]] || {
    echo "new legacy-compatible primary release is incomplete" >&2
    return 1
  }
  assert_legacy_owner_environment || return 1
  validate_legacy_compat_unit /opt/vane/vane-legacy-compat.service || return 1
  owner_env_source=$(owner_snapshot_path)
  [[ -f $owner_env_source && ! -L $owner_env_source ]] || {
    echo "trusted owner environment snapshot is unavailable" >&2
    return 1
  }

  rm -f -- /etc/systemd/system/vane.service.release-next \
    /opt/vane/env/server-owner-compat.env.release-next
  [[ -f /opt/vane/env/server.env.release-next &&
     ! -L /opt/vane/env/server.env.release-next ]] || {
    echo "staged restricted server environment is unavailable" >&2
    return 1
  }
  install -m 0644 /opt/vane/vane-legacy-compat.service \
    /etc/systemd/system/vane.service.release-next
  assert_restricted_server_environment_readonly \
    /opt/vane/env/server.env.release-next || return 1
  build_owner_compatible_environment "$owner_env_source" \
    /opt/vane/env/server-owner-compat.env.release-next \
    /opt/vane/env/server.env.release-next || return 1
  chown root:vane /opt/vane/env/server-owner-compat.env.release-next
  chmod 0640 /opt/vane/env/server-owner-compat.env.release-next

  # No process may observe a mixed binary/unit/environment release. Disable
  # boot activation, commit every staged member, reload, then explicitly
  # enable and start the complete audited contract.
  systemctl disable vane.service >/dev/null
  mv -f -- /opt/vane/bin/vane.next /opt/vane/bin/vane
  mv -f -- /etc/systemd/system/vane.service.release-next \
    /etc/systemd/system/vane.service
  mv -f -- /opt/vane/env/server-owner-compat.env.release-next \
    /opt/vane/env/server-owner-compat.env
  mv -f -- /opt/vane/env/server.env.release-next \
    /opt/vane/env/server.env
  systemctl daemon-reload
  assert_restricted_server_environment_readonly || return 1
  assert_audited_legacy_primary_runtime_contract || return 1
  assert_deepseek_flash_agent_route /opt/vane/env/server.env || return 1
  assert_deepseek_flash_agent_route \
    /opt/vane/env/server-owner-compat.env || return 1
  systemctl enable vane.service >/dev/null
  systemctl start vane.service
  wait_for_vane_ready
}

restore_previous_vane_release() (
  set -euo pipefail
  local runtime_env_path server_env_state legacy_env_state state attempt
  local owner_compat_env_state
  [[ $previous_vane_snapshot_ready == true && -d $rollback_dir &&
     ! -L $rollback_dir ]] || return 1
  read -r runtime_env_path <"$rollback_dir/runtime-env-path"
  read -r server_env_state <"$rollback_dir/server-env-state"
  read -r legacy_env_state <"$rollback_dir/legacy-env-state"
  read -r owner_compat_env_state <"$rollback_dir/owner-compat-env-state"
  case "$runtime_env_path" in
    /opt/vane/.env|/opt/vane/env/server.env|/opt/vane/env/server-owner-compat.env) ;;
    *) echo "rollback snapshot has an unsafe runtime environment path" >&2; return 1 ;;
  esac
  [[ $server_env_state == present || $server_env_state == absent ]] || {
    echo "rollback snapshot has an invalid server environment state" >&2
    return 1
  }
  [[ $legacy_env_state == present || $legacy_env_state == absent ]] || {
    echo "rollback snapshot has an invalid legacy environment state" >&2
    return 1
  }
  [[ $owner_compat_env_state == present ||
     $owner_compat_env_state == absent ]] || {
    echo "rollback snapshot has an invalid owner-compatible environment state" >&2
    return 1
  }
  [[ -f $rollback_dir/vane && ! -L $rollback_dir/vane &&
     -f $rollback_dir/vane.service && ! -L $rollback_dir/vane.service &&
     -f $rollback_dir/runtime.env && ! -L $rollback_dir/runtime.env &&
     -d /opt/vane/bin && ! -L /opt/vane/bin &&
     -d /opt/vane/env && ! -L /opt/vane/env &&
     -d /etc/systemd/system && ! -L /etc/systemd/system ]] || {
    echo "rollback snapshot or destination is unsafe" >&2
    return 1
  }
  [[ $(grep -Ec '^EnvironmentFile=' "$rollback_dir/vane.service" || true) -eq 1 &&
     $(grep -c -F "EnvironmentFile=$runtime_env_path" \
       "$rollback_dir/vane.service" || true) -eq 1 ]] || {
    echo "rollback unit and runtime environment snapshot do not match" >&2
    return 1
  }
  if [[ $server_env_state == present &&
        $runtime_env_path != /opt/vane/env/server.env ]]; then
    [[ -f $rollback_dir/server.env && ! -L $rollback_dir/server.env ]] || {
      echo "rollback server environment snapshot is unavailable" >&2
      return 1
    }
  fi
  if [[ $legacy_env_state == present && $runtime_env_path != /opt/vane/.env ]]; then
    [[ -f $rollback_dir/legacy.env && ! -L $rollback_dir/legacy.env ]] || {
      echo "rollback legacy environment snapshot is unavailable" >&2
      return 1
    }
  fi
  if [[ $owner_compat_env_state == present &&
        $runtime_env_path != /opt/vane/env/server-owner-compat.env ]]; then
    [[ -f $rollback_dir/owner-compat.env &&
       ! -L $rollback_dir/owner-compat.env ]] || {
      echo "rollback owner-compatible environment snapshot is unavailable" >&2
      return 1
    }
  fi

  # Stop all restart attempts before replacing any member of the runtime
  # contract. If a later filesystem operation fails, the service stays down;
  # it can never start a mixed binary/unit/environment release.
  systemctl stop vane.service
  state=$(systemctl is-active vane.service 2>/dev/null || true)
  [[ $state == inactive ]] || {
    echo "failed vane release did not become inactive for rollback (state=$state)" >&2
    return 1
  }
  # Remove boot activation before the multi-file commit. A host interruption
  # during restoration therefore leaves the service disabled as well as
  # stopped; only the complete old contract is enabled again below.
  systemctl disable vane.service >/dev/null

  rm -f -- /opt/vane/bin/vane.rollback-next \
    /etc/systemd/system/vane.service.rollback-next \
    "$runtime_env_path.rollback-next" \
    /opt/vane/env/server.env.rollback-next \
    /opt/vane/.env.rollback-next \
    /opt/vane/env/server-owner-compat.env.rollback-next
  cp --archive --reflink=auto -- "$rollback_dir/vane" \
    /opt/vane/bin/vane.rollback-next
  cp --archive --reflink=auto -- "$rollback_dir/vane.service" \
    /etc/systemd/system/vane.service.rollback-next
  cp --archive --reflink=auto -- "$rollback_dir/runtime.env" \
    "$runtime_env_path.rollback-next"
  if [[ $server_env_state == present &&
        $runtime_env_path != /opt/vane/env/server.env ]]; then
    cp --archive --reflink=auto -- "$rollback_dir/server.env" \
      /opt/vane/env/server.env.rollback-next
  fi
  if [[ $legacy_env_state == present && $runtime_env_path != /opt/vane/.env ]]; then
    cp --archive --reflink=auto -- "$rollback_dir/legacy.env" \
      /opt/vane/.env.rollback-next
  fi
  if [[ $owner_compat_env_state == present &&
        $runtime_env_path != /opt/vane/env/server-owner-compat.env ]]; then
    cp --archive --reflink=auto -- "$rollback_dir/owner-compat.env" \
      /opt/vane/env/server-owner-compat.env.rollback-next
  fi

  mv -f -- /opt/vane/bin/vane.rollback-next /opt/vane/bin/vane
  mv -f -- /etc/systemd/system/vane.service.rollback-next \
    /etc/systemd/system/vane.service
  mv -f -- "$runtime_env_path.rollback-next" "$runtime_env_path"
  if [[ $runtime_env_path != /opt/vane/env/server.env ]]; then
    if [[ $server_env_state == present ]]; then
      mv -f -- /opt/vane/env/server.env.rollback-next \
        /opt/vane/env/server.env
    else
      rm -f -- /opt/vane/env/server.env
    fi
  fi
  if [[ $runtime_env_path != /opt/vane/.env ]]; then
    if [[ $legacy_env_state == present ]]; then
      mv -f -- /opt/vane/.env.rollback-next /opt/vane/.env
    else
      rm -f -- /opt/vane/.env
    fi
  fi
  if [[ $runtime_env_path != /opt/vane/env/server-owner-compat.env ]]; then
    if [[ $owner_compat_env_state == present ]]; then
      mv -f -- /opt/vane/env/server-owner-compat.env.rollback-next \
        /opt/vane/env/server-owner-compat.env
    else
      rm -f -- /opt/vane/env/server-owner-compat.env
    fi
  fi

  systemctl daemon-reload
  if [[ $previous_vane_restart_expected != true ]]; then
    echo "previous inactive vane runtime contract restored and left disabled" >&2
    return 0
  fi
  systemctl enable vane.service >/dev/null
  systemctl start vane.service
  for attempt in {1..12}; do
    if systemctl is-active --quiet vane.service && vane_ready; then
      echo "previous vane runtime contract recovery verified" >&2
      return 0
    fi
    sleep 5
  done
  echo "previous vane runtime contract recovery failed readiness" >&2
  return 1
)

cleanup_remote_deploy() {
  local status=$?
  trap - EXIT
  set +e
  if [[ $status -ne 0 && $gateway_recovery_required == true ]]; then
    if [[ $previous_gateway_snapshot_ready == true ]]; then
      echo "deployment failed after research gateway promotion;" \
        "restoring its previous runtime contract" >&2
      if ! restore_previous_gateway_release; then
        preserve_gateway_snapshot=true
        echo "previous research gateway recovery failed; snapshot is preserved at $rollback_dir" >&2
      fi
    else
      preserve_gateway_snapshot=true
      echo "research gateway promotion has no trusted rollback snapshot; automatic recovery is refused" >&2
    fi
  fi
  if [[ $old_vane_recovery_required == true ]]; then
    if [[ $old_vane_restart_safe == true &&
          $previous_vane_snapshot_ready == true ]]; then
      echo "deployment failed after a proven clean drain;" \
        "restoring the previous vane runtime contract" >&2
      if ! restore_previous_vane_release; then
        preserve_vane_snapshot=true
        echo "previous vane runtime contract recovery failed; service remains" \
          "stopped and snapshot is preserved at $rollback_dir" >&2
      fi
    else
      preserve_vane_snapshot=true
      echo "old vane drain or rollback snapshot was not proven safe; automatic recovery is refused" >&2
    fi
  fi
  if [[ -e $rollback_dir || -L $rollback_dir ]]; then
    if [[ $preserve_vane_snapshot == true ||
          $preserve_gateway_snapshot == true ]]; then
      echo "vane rollback snapshot retained: $rollback_dir" >&2
    else
      rm -rf -- "$rollback_dir"
    fi
  fi
  rm -f -- /etc/systemd/system/vane.service.release-next \
    /etc/systemd/system/vane-research-gateway.service.release-next \
    /etc/systemd/system/vane-research-gateway.service.rollback-next \
    /etc/systemd/system/vane-research-gateway.socket.release-next \
    /etc/systemd/system/vane-research-gateway.socket.rollback-next \
    /opt/vane/bin/vane-research-gateway.release-next \
    /opt/vane/bin/vane-research-gateway.rollback-next \
    /opt/vane/vane-research-gateway.service.release-next \
    /opt/vane/vane-research-gateway.service.rollback-next \
    /opt/vane/vane-research-gateway.socket.release-next \
    /opt/vane/vane-research-gateway.socket.rollback-next \
    /opt/vane/env/research-gateway.env.next \
    /opt/vane/env/research-gateway.env.rollback-next \
    /etc/vane/credentials/gateway_db_url.next \
    /etc/vane/credentials/gateway_db_url.rollback-next \
    /etc/vane/credentials/research_llm_api_key_gen1.next \
    /etc/vane/credentials/research_llm_api_key_gen1.rollback-next \
    /opt/vane/env/server-owner-compat.env.release-next \
    /opt/vane/env/server.env.release-next \
    /opt/vane/env/server.env.release-next.stage-next
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

assert_native_v3_edit_recovery_credential() {
  local credential=/etc/vane/credentials/native_v3_edit_recovery_db_url
  [[ -f $credential && ! -L $credential &&
     $(stat -c '%U:%G:%a' "$credential") == vane:vane:400 &&
     $(wc -l <"$credential") -eq 1 ]] || {
    echo "native V3 edit recovery credential has unsafe file metadata" >&2
    return 1
  }
  LC_ALL=C grep -Eq \
    '^postgres://vane_native_v3_edit_recovery_runtime:[0-9a-f]{64}@127\.0\.0\.1:5432/vane\?sslmode=disable$' \
    "$credential" || {
    echo "native V3 edit recovery credential has an invalid database URL" >&2
    return 1
  }
}

# Provision the independent edit-recovery login after migrations have created
# its NOLOGIN role. The pending password is root-only and survives every
# client-side response-loss point. A retry therefore repeats ALTER ROLE and
# the atomic credential write with exactly the same secret.
provision_native_v3_edit_recovery_runtime() {
  local marker=/etc/vane/credentials/native_v3_edit_recovery_runtime_v1.complete
  local pending=/etc/vane/credentials/.native-v3-edit-recovery-runtime-v1.pending
  local password_file=$pending/password
  local credential=/etc/vane/credentials/native_v3_edit_recovery_db_url
  local password

  [[ $- != *x* ]] || {
    echo "native V3 edit recovery provisioning refuses shell tracing" >&2
    return 1
  }

  if [[ -e $marker || -L $marker ]]; then
    [[ -f $marker && ! -L $marker &&
       $(stat -c '%U:%G:%a' "$marker") == root:root:600 &&
       $(tr -d '\r\n' <"$marker") == complete ]] || {
      echo "native V3 edit recovery upgrade marker is invalid" >&2
      return 1
    }
    assert_native_v3_edit_recovery_credential
    if [[ -e $pending || -L $pending ]]; then
      [[ -d $pending && ! -L $pending &&
         $(stat -c '%U:%G:%a' "$pending") == root:root:700 ]] || {
        echo "native V3 edit recovery pending path is unsafe" >&2
        return 1
      }
      rm -rf -- "$pending"
    fi
    return 0
  fi

  if [[ -e $pending || -L $pending ]]; then
    [[ -d $pending && ! -L $pending &&
       $(stat -c '%U:%G:%a' "$pending") == root:root:700 ]] || {
      echo "native V3 edit recovery pending path is unsafe" >&2
      return 1
    }
  else
    install -d -o root -g root -m 0700 "$pending"
  fi
  if [[ -e $password_file || -L $password_file ]]; then
    [[ -f $password_file && ! -L $password_file &&
       $(stat -c '%U:%G:%a' "$password_file") == root:root:600 ]] || {
      echo "native V3 edit recovery pending password is unsafe" >&2
      return 1
    }
  else
    if [[ -e $password_file.next || -L $password_file.next ]]; then
      [[ -f $password_file.next && ! -L $password_file.next ]] || {
        echo "native V3 edit recovery next pending password is unsafe" >&2
        return 1
      }
    fi
    openssl rand -hex 32 >"$password_file.next"
    chown root:root "$password_file.next"
    chmod 0600 "$password_file.next"
    mv -f -- "$password_file.next" "$password_file"
  fi
  password=$(read_hex_secret "$password_file") || {
    echo "native V3 edit recovery pending password is invalid" >&2
    return 1
  }

  (
    cd /opt/vane
    printf "ALTER ROLE vane_native_v3_edit_recovery_runtime LOGIN PASSWORD '%s';\n" \
      "$password" |
      docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U vane -d vane
  )

  if [[ -e $credential || -L $credential ]]; then
    [[ -f $credential && ! -L $credential &&
       $(stat -c '%U:%G:%a' "$credential") == vane:vane:400 ]] || {
      echo "native V3 edit recovery credential target is unsafe" >&2
      return 1
    }
  fi
  if [[ -e $credential.next || -L $credential.next ]]; then
    [[ -f $credential.next && ! -L $credential.next ]] || {
      echo "native V3 edit recovery next credential is unsafe" >&2
      return 1
    }
  fi
  printf 'postgres://vane_native_v3_edit_recovery_runtime:%s@127.0.0.1:5432/vane?sslmode=disable\n' \
    "$password" >"$credential.next"
  chown vane:vane "$credential.next"
  chmod 0400 "$credential.next"
  mv -f -- "$credential.next" "$credential"
  assert_native_v3_edit_recovery_credential

  if [[ -e $marker.next || -L $marker.next ]]; then
    [[ -f $marker.next && ! -L $marker.next ]] || {
      echo "native V3 edit recovery next marker is unsafe" >&2
      return 1
    }
  fi
  printf 'complete\n' >"$marker.next"
  chown root:root "$marker.next"
  chmod 0600 "$marker.next"
  mv -f -- "$marker.next" "$marker"
  rm -rf -- "$pending"
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

# Snapshot the complete live process contract before split-runtime bootstrap
# can create or update server.env. The previous b9 deployment may be inactive
# or failed under the reviewed split unit; that state is accepted only when a
# trustworthy legacy owner environment is also available for convergence.
initial_vane_state=$(systemctl is-active vane.service 2>/dev/null || true)
case "$initial_vane_state" in
  active)
    previous_vane_restart_expected=true
    snapshot_previous_vane_release
    if grep -Fxq 'EnvironmentFile=/opt/vane/.env' \
      /etc/systemd/system/vane.service; then
      assert_legacy_primary_runtime_contract
    else
      assert_existing_audited_primary_runtime_contract
    fi
    ;;
  inactive|failed)
    previous_vane_restart_expected=false
    snapshot_previous_vane_release
    if grep -Fxq 'EnvironmentFile=/opt/vane/.env' \
      /etc/systemd/system/vane.service; then
      assert_legacy_primary_runtime_contract
    elif grep -Fxq \
      'EnvironmentFile=/opt/vane/env/server-owner-compat.env' \
      /etc/systemd/system/vane.service; then
      assert_existing_audited_primary_runtime_contract
    else
      assert_known_split_primary_runtime_contract
    fi
    systemctl stop vane.service
    [[ $(systemctl is-active vane.service 2>/dev/null || true) == inactive ]] || {
      echo "inactive vane transition could not be made quiescent" >&2
      exit 1
    }
    inactive_leftover_pids=
    for process_exe in /proc/[0-9]*/exe; do
      executable=$(readlink "$process_exe" 2>/dev/null || true)
      case "$executable" in
        "/opt/vane/bin/vane"|"/opt/vane/bin/vane (deleted)")
          pid=${process_exe#/proc/}
          pid=${pid%/exe}
          inactive_leftover_pids="$inactive_leftover_pids $pid"
          ;;
      esac
    done
    [[ -z $inactive_leftover_pids ]] || {
      echo "inactive vane contract still has processes:$inactive_leftover_pids" >&2
      exit 1
    }
    # From this point, any bootstrap failure restores the complete split
    # contract but deliberately leaves its previous inactive state disabled.
    old_vane_recovery_required=true
    old_vane_restart_safe=true
    ;;
  *)
    echo "unsupported initial vane service state: $initial_vane_state" >&2
    exit 1
    ;;
esac

# The new server binary stays beside the live binary until the old worker has
# drained. Migration/bootstrap failures therefore leave the proven worker and
# its old unit untouched.
validate_legacy_compat_unit "$stage/vane-legacy-compat.service"
validate_native_v3_edit_recovery_unit "$stage/vane.service"
validate_gateway_unit "$stage/vane-research-gateway.service"
expected_server_release_contract='vane.server-release-contract/v2 primary_store=owner_compat_v1 research_control_store=restricted_v1 research_store=restricted_v1'
actual_server_release_contract=$(
  env -i PATH=/usr/bin:/bin "$stage/bin/vane" -print-release-contract
)
[[ $actual_server_release_contract == "$expected_server_release_contract" ]] || {
  echo "incoming vane binary has an unexpected release contract" >&2
  exit 1
}
[[ -f $stage/bin/agentfirstretention && ! -L $stage/bin/agentfirstretention &&
   -x $stage/bin/agentfirstretention &&
   -f $stage/release-receipt.json && ! -L $stage/release-receipt.json ]] || {
  echo "incoming Agent-first collector release authority is incomplete" >&2
  exit 1
}
stage_vane_digest=$(sha256sum "$stage/bin/vane" | awk '{print $1}')
stage_collector_digest=$(sha256sum "$stage/bin/agentfirstretention" | awk '{print $1}')
release_receipt_digest=$(sha256sum "$stage/release-receipt.json" | awk '{print $1}')
[[ $release_receipt_digest =~ ^[0-9a-f]{64}$ ]] || {
  echo "incoming Agent-first release receipt digest is invalid" >&2
  exit 1
}
release_dir=/opt/vane/releases/$release_receipt_digest
grep -Fq '"source_revision":"'"$release_sha"'"' \
  "$stage/release-receipt.json" &&
  grep -Fq '"vane_sha256":"'"$stage_vane_digest"'"' \
    "$stage/release-receipt.json" &&
  grep -Fq '"agentfirstretention_sha256":"'"$stage_collector_digest"'"' \
    "$stage/release-receipt.json" || {
      echo "incoming Agent-first release receipt differs from staged binaries" >&2
      exit 1
    }

# The current gateway remains live throughout the migration gate. Capture its
# complete process contract before bootstrap can rotate its environment or
# credentials. First deployments record a deliberate absent state.
snapshot_previous_gateway_release

assert_gateway_peer_and_credential_boundary() {
  local allowed_uid credential
  [[ -f /opt/vane/env/research-gateway.env &&
     ! -L /opt/vane/env/research-gateway.env &&
     $(grep -c '^VANE_GATEWAY_ALLOWED_UID=' \
       /opt/vane/env/research-gateway.env || true) -eq 1 ]] || {
    echo "research gateway peer UID contract is unavailable" >&2
    return 1
  }
  allowed_uid=$(sed -n 's/^VANE_GATEWAY_ALLOWED_UID=//p' \
    /opt/vane/env/research-gateway.env)
  [[ $allowed_uid == "$vane_uid" ]] || {
    echo "research gateway peer UID does not match the primary service user" >&2
    return 1
  }
  for credential in gateway_db_url research_llm_api_key_gen1; do
    [[ $(stat -c '%U:%G:%a' "/etc/vane/credentials/$credential") == \
       vane-research-gateway:vane-research-gateway:400 ]] || {
      echo "gateway credential ownership or mode is unsafe: $credential" >&2
      return 1
    }
    if runuser -u vane -- test -r "/etc/vane/credentials/$credential"; then
      echo "primary service user can read a gateway credential: $credential" >&2
      return 1
    fi
  done
}
install -m 0755 "$stage/bin/vane" /opt/vane/bin/vane.next
install -m 0755 "$stage/bin/vane-research-gateway" \
  /opt/vane/bin/vane-research-gateway.release-next
for binary in useradmin gate runtimeadmin vane-migrate \
  vane-research-prepare researchshadow researchcutover; do
  install -m 0755 "$stage/bin/$binary" "/opt/vane/bin/$binary"
done
install -m 0644 "$stage/Caddyfile" /opt/vane/Caddyfile
install -m 0644 "$stage/docker-compose.yml" /opt/vane/docker-compose.yml
# Keep the reviewed split unit available for the later RLS-graph cutover, but
# never install it as the live primary unit while the release fence is active.
install -m 0644 "$stage/vane.service" /opt/vane/vane.service.deferred
install -m 0644 "$stage/vane-legacy-compat.service" \
  /opt/vane/vane-legacy-compat.service
install -m 0644 "$stage/vane-migrate.service" /opt/vane/vane-migrate.service
install -m 0644 "$stage/vane-research-gateway.service" \
  /opt/vane/vane-research-gateway.service.release-next
install -m 0644 "$stage/vane-research-gateway.socket" \
  /opt/vane/vane-research-gateway.socket.release-next
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

# test-anchor: gateway-migration-gate-begin
# Migration is a deploy-time gate, never a boot dependency or enabled unit.
# A uniquely named transient unit prevents a failure from propagating through
# an older live gateway's Requires=vane-migrate.service relationship.
systemctl disable vane-migrate.service >/dev/null || true
migration_run_unit=vane-deploy-migrate-${stage##*/.deploy-}
systemd-run --quiet --wait --collect --unit="$migration_run_unit" \
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
  --property=TimeoutStartSec=6min \
  /opt/vane/bin/vane-migrate
# test-anchor: gateway-migration-gate-end
# Only a successful gate may update the disabled diagnostic unit on disk.
install -m 0644 /opt/vane/vane-migrate.service \
  /etc/systemd/system/vane-migrate.service
systemctl daemon-reload
systemctl disable vane-migrate.service >/dev/null || true
provision_native_v3_edit_recovery_runtime

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

  awk '!/^(POSTGRES_PASSWORD|VANE_MIGRATION_DB_URL|VANE_DB_URL|VANE_DB_RESEARCH_CONTROL_URL|VANE_DB_RESEARCH_RUNTIME_URL|VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL|VANE_DB_RESEARCH_CAPABILITY_KEY_ID|VANE_DB_RESEARCH_CAPABILITY_KEY_HEX|VANE_DB_RESEARCH_CAPABILITY_RETIRED_KEYS|VANE_DB_RESEARCH_CAPABILITY_TTL_DAYS|VANE_RESEARCH_GATEWAY_SOCKET_PATH|VANE_GATEWAY_[A-Z0-9_]+|VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID|VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID|VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID|VANE_LLM_AGENT_PROVIDER|VANE_LLM_AGENT_BASE_URL|VANE_LLM_AGENT_API_KEY|VANE_LLM_AGENT_MODEL)=/' \
    /opt/vane/.env >/opt/vane/env/server.env.next
  {
    printf 'VANE_LLM_AGENT_MODEL=deepseek-v4-flash\n'
    printf 'VANE_DB_URL=postgres://vane_server_runtime:%s@127.0.0.1:5432/vane?sslmode=disable\n' \
      "$server_password"
    printf 'VANE_DB_RESEARCH_CONTROL_URL=postgres://vane_server_runtime:%s@127.0.0.1:5432/vane?sslmode=disable\n' \
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
    printf 'VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID=\n'
  } >>/opt/vane/env/server.env.next
  chown vane:vane /opt/vane/env/server.env.next
  chmod 0600 /opt/vane/env/server.env.next
  mv -f /opt/vane/env/server.env.next /opt/vane/env/server.env

  # From the first live gateway configuration mutation onward, every failure
  # must restore the per-path snapshot captured before the migration gate.
  gateway_recovery_required=true
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
stage_research_control_environment /opt/vane/env/server.env \
  /opt/vane/env/server.env.release-next
for credential in migration_db_url gateway_db_url research_llm_api_key_gen1 \
  native_v3_edit_recovery_db_url; do
  [[ -s /etc/vane/credentials/$credential && \
     ! -L /etc/vane/credentials/$credential ]] || {
    echo "runtime credential is unavailable: $credential" >&2
    exit 1
  }
done
assert_native_v3_edit_recovery_credential
chown root:vane /opt/vane/env/server.env
chmod 0640 /opt/vane/env/server.env
assert_restricted_server_environment_readonly
assert_restricted_server_environment_readonly \
  /opt/vane/env/server.env.release-next
# Existing bootstraps skip the creation block above, so arm recovery before
# the first unconditional gateway metadata mutation as well.
gateway_recovery_required=true
chown vane-research-gateway:vane-research-gateway \
  /opt/vane/env/research-gateway.env \
  /etc/vane/credentials/gateway_db_url \
  /etc/vane/credentials/research_llm_api_key_gen1
chmod 0600 /opt/vane/env/research-gateway.env
chmod 0400 /etc/vane/credentials/gateway_db_url \
  /etc/vane/credentials/research_llm_api_key_gen1
assert_gateway_peer_and_credential_boundary
grep -Eq '^VANE_DB_URL=postgres://vane_server_runtime:' \
  /opt/vane/env/server.env.release-next
grep -Eq '^VANE_DB_RESEARCH_CONTROL_URL=postgres://vane_server_runtime:' \
  /opt/vane/env/server.env.release-next
grep -Eq '^VANE_DB_RESEARCH_RUNTIME_URL=postgres://vane_research_runtime:' \
  /opt/vane/env/server.env.release-next
if grep -Eq '^POSTGRES_PASSWORD=|^VANE_DB_URL=postgres://vane:' \
  /opt/vane/env/server.env.release-next; then
  echo "server environment contains an owner credential" >&2
  exit 1
fi
# DirectoryMode in the socket unit only applies when systemd creates the
# directory. Repair upgrades from the original root:root 0750 directory too,
# while refusing a replaced path. The directory is traversable but not
# listable; the socket inode remains gateway:vane 0660.
if [[ -L /run/vane-research-gateway || \
      (-e /run/vane-research-gateway && ! -d /run/vane-research-gateway) ]]; then
  echo "research gateway runtime path is not a real directory" >&2
  exit 1
fi
install -d -o root -g root -m 0711 /run/vane-research-gateway
[[ -f /opt/vane/bin/vane-research-gateway.release-next &&
   ! -L /opt/vane/bin/vane-research-gateway.release-next &&
   -f /opt/vane/vane-research-gateway.service.release-next &&
   ! -L /opt/vane/vane-research-gateway.service.release-next &&
   -f /opt/vane/vane-research-gateway.socket.release-next &&
   ! -L /opt/vane/vane-research-gateway.socket.release-next ]] || {
  echo "research gateway promotion set is incomplete" >&2
  exit 1
}

# Migration and bootstrap are complete. From the first availability-changing
# operation until final deploy verification, every failure restores the
# captured gateway contract; database migrations are intentionally forward-only.
gateway_recovery_required=true
systemctl stop vane-research-gateway.service || true
systemctl stop vane-research-gateway.socket || true
systemctl disable vane-research-gateway.service \
  vane-research-gateway.socket >/dev/null || true
install -m 0644 /opt/vane/vane-research-gateway.service.release-next \
  /etc/systemd/system/vane-research-gateway.service.release-next
install -m 0644 /opt/vane/vane-research-gateway.socket.release-next \
  /etc/systemd/system/vane-research-gateway.socket.release-next
mv -f -- /opt/vane/bin/vane-research-gateway.release-next \
  /opt/vane/bin/vane-research-gateway
mv -f -- /opt/vane/vane-research-gateway.service.release-next \
  /opt/vane/vane-research-gateway.service
mv -f -- /opt/vane/vane-research-gateway.socket.release-next \
  /opt/vane/vane-research-gateway.socket
mv -f -- /etc/systemd/system/vane-research-gateway.service.release-next \
  /etc/systemd/system/vane-research-gateway.service
mv -f -- /etc/systemd/system/vane-research-gateway.socket.release-next \
  /etc/systemd/system/vane-research-gateway.socket
systemctl daemon-reload
systemctl reset-failed vane-research-gateway.service \
  vane-research-gateway.socket || true
systemctl enable vane-research-gateway.socket \
  vane-research-gateway.service >/dev/null
systemctl start vane-research-gateway.socket
systemctl start vane-research-gateway.service
gateway_exe=
for attempt in {1..12}; do
  gateway_exe=
  gateway_pid=$(systemctl show vane-research-gateway.service \
    --property=MainPID --value)
  if [[ $gateway_pid =~ ^[1-9][0-9]*$ ]]; then
    gateway_exe=$(readlink /proc/"$gateway_pid"/exe 2>/dev/null || true)
  fi
  if [[ $gateway_exe == /opt/vane/bin/vane-research-gateway ]] && \
     systemctl is-active --quiet vane-research-gateway.service; then
    break
  fi
  sleep 1
done
[[ $gateway_exe == /opt/vane/bin/vane-research-gateway ]] || {
  echo "research gateway preflight is not running the installed binary" >&2
  systemctl status vane-research-gateway.service --no-pager --full >&2 || true
  journalctl -u vane-research-gateway.service --no-pager -o cat -n 100 >&2 || true
  exit 1
}
[[ $(stat -c '%U:%G:%a' /run/vane-research-gateway) == root:root:711 ]] || {
  echo "research gateway runtime directory ownership or mode is unsafe" >&2
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
# while the proven old worker is still serving traffic. Gate is not started by
# systemd, so pass only the credential directory (never its secret payload) in
# an otherwise empty process environment.
env -i PATH=/usr/bin:/bin \
  CREDENTIALS_DIRECTORY=/etc/vane/credentials \
  /opt/vane/bin/gate -env /opt/vane/env/server.env.release-next >/dev/null

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
  [[ $previous_vane_snapshot_ready == true ]] || {
    echo "active vane release has no complete rollback snapshot" >&2
    exit 1
  }
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
old_vane_recovery_required=true

echo "old vane worker drain verified; starting roll-forward binary"
commit_legacy_primary_release

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
env -i PATH=/usr/bin:/bin \
  CREDENTIALS_DIRECTORY=/etc/vane/credentials \
  /opt/vane/bin/gate -env /opt/vane/env/server.env

# Publish the offline retention authority only after the new service passed
# the final Gate and its actual process image is byte-identical to both the
# installed binary and this deployment's staged artifact. A failed deploy must
# not leave a receipt that a later operator can mistake for a live release.
vane_pid=$(systemctl show vane.service --property=MainPID --value)
[[ $vane_pid =~ ^[1-9][0-9]*$ &&
   $(readlink /proc/"$vane_pid"/exe 2>/dev/null || true) == /opt/vane/bin/vane &&
   -r /proc/"$vane_pid"/exe ]] || {
  echo "live vane process authority is unavailable" >&2
  exit 1
}
cmp -s -- /proc/"$vane_pid"/exe /opt/vane/bin/vane &&
  cmp -s -- /proc/"$vane_pid"/exe "$stage/bin/vane" || {
    echo "live vane process differs from the deployed artifact" >&2
    exit 1
  }
[[ -f $stage/publish-retention-release.sh &&
   ! -L $stage/publish-retention-release.sh ]] || {
  echo "retention release publisher is unavailable" >&2
  exit 1
}
bash "$stage/publish-retention-release.sh" /opt/vane/releases \
  "$stage/bin/agentfirstretention" "$stage/release-receipt.json" >/dev/null
old_vane_recovery_required=false
gateway_recovery_required=false
