#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 3 ]]; then
  echo "usage: $0 COMPONENT VERIFIED_PAYLOAD EXACT_SHA" >&2
  exit 2
fi

component=$1
payload=$2
source_sha=$3
case "$component" in
  backend|frontend-aliyun|frontend-cloudflare|frontend-finalize) ;;
  *)
    echo "invalid component: $component" >&2
    exit 2
    ;;
esac
[[ $source_sha =~ ^[0-9a-f]{40}$ ]] || {
  echo "invalid exact source SHA: $source_sha" >&2
  exit 1
}
[[ -d $payload ]] || {
  echo "verified payload directory is missing: $payload" >&2
  exit 1
}

state_root=${XDG_STATE_HOME:-"$HOME/.local/state"}
state_dir=$state_root/vane-deploy
mkdir -p "$state_dir"
chmod 700 "$state_dir"
command -v flock >/dev/null

# cert-renew.sh uses this same lock. Workflow concurrency covers queued runs;
# flock also protects manual/local invocations on the deployment VM.
exec 9>"$state_dir/control-plane.lock"
flock 9

require_env() {
  local name
  for name in "$@"; do
    if [[ -z ${!name:-} ]]; then
      echo "required environment variable is empty: $name" >&2
      exit 1
    fi
  done
}

read_state() {
  local state_file=$1
  local value=
  if [[ -f $state_file ]]; then
    value=$(tr -d '\r\n' <"$state_file")
    [[ $value =~ ^[0-9a-f]{40}$ ]] || {
      echo "durable state is malformed: $state_file" >&2
      exit 1
    }
  fi
  printf '%s' "$value"
}

write_state() {
  local state_file=$1
  local sha=$2
  local temp_state
  temp_state=$(mktemp "$state_dir/.deployed-sha.XXXXXX")
  printf '%s\n' "$sha" >"$temp_state"
  chmod 600 "$temp_state"
  mv -f "$temp_state" "$state_file"
}

deploy_backend() {
  local state_file=$state_dir/deployed-vane.sha
  local current_state
  current_state=$(read_state "$state_file")
  if [[ $current_state == "$source_sha" ]]; then
    echo "backend already deployed: $source_sha"
    return
  fi
  [[ ${EXPECTED_DEPLOYED_SHA+x} == x ]] || {
    echo "EXPECTED_DEPLOYED_SHA is not set" >&2
    exit 1
  }
  [[ $current_state == "$EXPECTED_DEPLOYED_SHA" ]] || {
    echo "backend state changed after planning; refusing stale deployment" >&2
    exit 1
  }

  require_env \
    VPS_HOST VPS_PORT VPS_USER VPS_SSH_KEY \
    VPS_SSH_HOST_ED25519_FINGERPRINT RUNNER_TEMP \
    GITHUB_RUN_ID GITHUB_RUN_ATTEMPT
  [[ $VPS_HOST =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]] || {
    echo "VPS_HOST must be a DNS name or IPv4 address" >&2
    exit 1
  }
  [[ $VPS_PORT =~ ^[0-9]{1,5}$ ]] || {
    echo "VPS_PORT must be numeric" >&2
    exit 1
  }
  ((VPS_PORT >= 1 && VPS_PORT <= 65535)) || {
    echo "VPS_PORT is outside 1..65535" >&2
    exit 1
  }
  [[ $VPS_USER =~ ^[a-z_][a-z0-9_-]*$ ]] || {
    echo "VPS_USER has an unsafe value" >&2
    exit 1
  }
  [[ $VPS_SSH_HOST_ED25519_FINGERPRINT =~ ^SHA256:[A-Za-z0-9+/]+$ ]] || {
    echo "VPS Ed25519 fingerprint is malformed" >&2
    exit 1
  }
  [[ $GITHUB_RUN_ID =~ ^[0-9]+$ && $GITHUB_RUN_ATTEMPT =~ ^[0-9]+$ ]] || {
    echo "GitHub run ID and attempt must be numeric" >&2
    exit 1
  }

  local binary infra
  for binary in vane useradmin gate runtimeadmin; do
    [[ -x $payload/bin/$binary ]] || {
      echo "missing verified backend binary: $binary" >&2
      exit 1
    }
    # The Python artifact validator performs the same build-info checks without
    # requiring Go on this VM. Keep this assertion close to deployment too.
    "$(dirname "$0")/check-go-build-info.sh" \
      "$payload/bin/$binary" "$source_sha"
  done
  for infra in \
    Caddyfile \
    docker-compose.yml \
    vane.service \
    dynamicconfig/development-sql.yaml; do
    [[ -f $payload/deploy/$infra && ! -L $payload/deploy/$infra ]] || {
      echo "missing verified backend infra file: $infra" >&2
      exit 1
    }
  done

  local ssh_dir remote_stage ssh_target actual_fingerprint
  local remote_stage_created=false
  local -a ssh_opts scp_opts
  ssh_dir=$(mktemp -d "$RUNNER_TEMP/vane-ssh.XXXXXX")
  remote_stage="/opt/vane/.deploy-${source_sha}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
  ssh_target="$VPS_USER@$VPS_HOST"
  cleanup_backend() {
    local status=$?
    trap - EXIT
    if [[ $remote_stage_created == true && -n ${remote_stage:-} ]]; then
      ssh "${ssh_opts[@]}" "$ssh_target" \
        rm -rf -- "$remote_stage" >/dev/null 2>&1 || true
    fi
    rm -rf -- "$ssh_dir"
    exit "$status"
  }
  trap cleanup_backend EXIT

  chmod 700 "$ssh_dir"
  printf '%s\n' "$VPS_SSH_KEY" >"$ssh_dir/id"
  chmod 600 "$ssh_dir/id"
  ssh-keyscan -p "$VPS_PORT" -t ed25519 "$VPS_HOST" \
    2>/dev/null >"$ssh_dir/known_hosts"
  [[ $(wc -l <"$ssh_dir/known_hosts") -eq 1 ]] || {
    echo "expected exactly one VPS Ed25519 host key" >&2
    exit 1
  }
  actual_fingerprint=$(
    ssh-keygen -lf "$ssh_dir/known_hosts" -E sha256 |
      awk 'NR == 1 { print $2 }'
  )
  [[ $actual_fingerprint == "$VPS_SSH_HOST_ED25519_FINGERPRINT" ]] || {
    echo "VPS Ed25519 fingerprint mismatch" >&2
    exit 1
  }

  ssh_opts=(
    -i "$ssh_dir/id"
    -p "$VPS_PORT"
    -o BatchMode=yes
    -o IdentitiesOnly=yes
    -o StrictHostKeyChecking=yes
    -o "UserKnownHostsFile=$ssh_dir/known_hosts"
    -o ConnectTimeout=15
    -o ServerAliveInterval=15
    -o ServerAliveCountMax=4
  )
  scp_opts=(
    -i "$ssh_dir/id"
    -P "$VPS_PORT"
    -o BatchMode=yes
    -o IdentitiesOnly=yes
    -o StrictHostKeyChecking=yes
    -o "UserKnownHostsFile=$ssh_dir/known_hosts"
    -o ConnectTimeout=15
  )

  ssh "${ssh_opts[@]}" "$ssh_target" \
    mkdir -p "$remote_stage/bin" "$remote_stage/dynamicconfig"
  remote_stage_created=true
  scp "${scp_opts[@]}" \
    "$payload/bin/vane" \
    "$payload/bin/useradmin" \
    "$payload/bin/gate" \
    "$payload/bin/runtimeadmin" \
    "$ssh_target:$remote_stage/bin/"
  scp "${scp_opts[@]}" \
    "$payload/deploy/Caddyfile" \
    "$payload/deploy/docker-compose.yml" \
    "$payload/deploy/vane.service" \
    "$ssh_target:$remote_stage/"
  scp "${scp_opts[@]}" \
    "$payload/deploy/dynamicconfig/development-sql.yaml" \
    "$ssh_target:$remote_stage/dynamicconfig/"

  ssh "${ssh_opts[@]}" "$ssh_target" \
    bash -s -- "$remote_stage" \
    <"$(dirname "$0")/remote-backend-deploy.sh"
  remote_stage=
  remote_stage_created=false
  trap - EXIT
  rm -rf -- "$ssh_dir"

  # remote-backend-deploy.sh ends with the production Gate. Only that complete
  # success advances durable deployment state.
  write_state "$state_file" "$source_sha"
  echo "backend deployment recorded after Gate: $source_sha"
}

frontend_state_check() {
  local state_file=$state_dir/deployed-vane-web.sha
  current_state=
  current_state=$(read_state "$state_file")
  if [[ $current_state == "$source_sha" ]]; then
    return 10
  fi
  [[ ${EXPECTED_DEPLOYED_SHA+x} == x ]] || {
    echo "EXPECTED_DEPLOYED_SHA is not set" >&2
    exit 1
  }
  [[ $current_state == "$EXPECTED_DEPLOYED_SHA" ]] || {
    echo "frontend state changed after planning; refusing stale deployment" >&2
    exit 1
  }
  return 0
}

write_frontend_receipt() {
  local channel=$1
  require_env FRONTEND_RECEIPT_DIR
  mkdir -p "$FRONTEND_RECEIPT_DIR"
  chmod 700 "$FRONTEND_RECEIPT_DIR"
  local receipt_temp
  receipt_temp=$(mktemp "$FRONTEND_RECEIPT_DIR/.receipt.XXXXXX")
  printf '%s\n' "$source_sha" >"$receipt_temp"
  chmod 600 "$receipt_temp"
  mv -f "$receipt_temp" "$FRONTEND_RECEIPT_DIR/$channel.sha"
}

deploy_frontend_aliyun() {
  if frontend_state_check; then
    :
  elif [[ $? -eq 10 ]]; then
    echo "frontend already deployed: $source_sha"
    write_frontend_receipt aliyun
    return
  else
    return 1
  fi

  require_env \
    ALIYUN_BIN ALIYUN_ACCESS_KEY_ID ALIYUN_ACCESS_KEY_SECRET \
    RUNNER_TEMP
  [[ -x $ALIYUN_BIN ]] || {
    echo "pinned Aliyun CLI is missing" >&2
    exit 1
  }
  [[ -f $payload/dist/index.html && ! -L $payload/dist/index.html ]] || {
    echo "verified frontend dist/index.html is missing" >&2
    exit 1
  }

  (
    aliyun_temp=$(mktemp -d "$RUNNER_TEMP/aliyun-config.XXXXXX")
    aliyun_config=$aliyun_temp/config.json
    chmod 700 "$aliyun_temp"
    trap 'rm -rf -- "$aliyun_temp"' EXIT

    "$ALIYUN_BIN" configure set \
      --config-path "$aliyun_config" \
      --profile default \
      --mode AK \
      --access-key-id "$ALIYUN_ACCESS_KEY_ID" \
      --access-key-secret "$ALIYUN_ACCESS_KEY_SECRET" \
      --region cn-shenzhen

    "$ALIYUN_BIN" oss sync \
      "$payload/dist/" oss://zhuoqidev-vane-web/ \
      --delete --force \
      --config-path "$aliyun_config" \
      --profile default

    for attempt in 1 2 3; do
      if "$ALIYUN_BIN" cdn RefreshObjectCaches \
        --ObjectPath "https://vane.zhuoqidev.com/" \
        --ObjectType Directory \
        --config-path "$aliyun_config" \
        --profile default; then
        break
      fi
      [[ $attempt -lt 3 ]] || exit 1
      sleep $((attempt * 5))
    done
  )
  write_frontend_receipt aliyun
  echo "frontend Aliyun line deployed: $source_sha"
}

deploy_frontend_cloudflare() {
  if frontend_state_check; then
    :
  elif [[ $? -eq 10 ]]; then
    echo "frontend already deployed: $source_sha"
    write_frontend_receipt cloudflare
    return
  else
    return 1
  fi

  require_env \
    WRANGLER_BIN CLOUDFLARE_API_TOKEN CLOUDFLARE_ACCOUNT_ID \
    FRONTEND_RECEIPT_DIR
  [[ -x $WRANGLER_BIN ]] || {
    echo "preinstalled Wrangler is missing" >&2
    exit 1
  }
  wrangler_version=$("$WRANGLER_BIN" --version | tail -n 1)
  [[ $wrangler_version == "4.111.0" ]] || {
    echo "unexpected preinstalled Wrangler version: $wrangler_version" >&2
    exit 1
  }
  [[ -f $payload/dist/index.html && ! -L $payload/dist/index.html ]] || {
    echo "verified frontend dist/index.html is missing" >&2
    exit 1
  }

  (
    cd "$payload"
    "$WRANGLER_BIN" pages deploy dist \
      --project-name vane-web \
      --branch main
  )
  write_frontend_receipt cloudflare
  echo "frontend Cloudflare line deployed: $source_sha"
}

finalize_frontend() {
  local state_file=$state_dir/deployed-vane-web.sha
  if frontend_state_check; then
    :
  elif [[ $? -eq 10 ]]; then
    echo "frontend already finalized: $source_sha"
    return
  else
    return 1
  fi

  require_env FRONTEND_RECEIPT_DIR
  for channel in aliyun cloudflare; do
    receipt=$FRONTEND_RECEIPT_DIR/$channel.sha
    [[ -f $receipt && ! -L $receipt ]] || {
      echo "frontend $channel success receipt is missing" >&2
      exit 1
    }
    [[ $(tr -d '\r\n' <"$receipt") == "$source_sha" ]] || {
      echo "frontend $channel receipt SHA is stale" >&2
      exit 1
    }
  done

  # Both independent distribution lines succeeded in this run.
  write_state "$state_file" "$source_sha"
  echo "frontend dual-line deployment recorded: $source_sha"
}

case "$component" in
  backend) deploy_backend ;;
  frontend-aliyun) deploy_frontend_aliyun ;;
  frontend-cloudflare) deploy_frontend_cloudflare ;;
  frontend-finalize) finalize_frontend ;;
esac
