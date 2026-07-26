#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BACKEND_REPOSITORY FRONTEND_REPOSITORY" >&2
  exit 2
fi

backend_repo=$1
frontend_repo=$2
state_root=${XDG_STATE_HOME:-"$HOME/.local/state"}
state_dir=$state_root/vane-deploy
backend_state=$state_dir/deployed-vane.sha
frontend_state=$state_dir/deployed-vane-web.sha

mkdir -p "$state_dir"
chmod 700 "$state_dir"
command -v flock >/dev/null

# This descriptor remains held through every production mutation and state
# commit. cert-renew.sh uses the same lock.
exec 9>"$state_dir/control-plane.lock"
flock 9

read_sha() {
  local repo=$1
  local sha
  sha=$(git -C "$repo" rev-parse HEAD)
  [[ $sha =~ ^[0-9a-f]{40}$ ]] || {
    echo "invalid checked-out commit for $repo: $sha" >&2
    exit 1
  }
  printf '%s' "$sha"
}

deployed_sha() {
  local state_file=$1
  if [[ -f $state_file ]]; then
    tr -d '\r\n' <"$state_file"
  fi
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

require_env() {
  local name
  for name in "$@"; do
    if [[ -z ${!name:-} ]]; then
      echo "required environment variable is empty: $name" >&2
      exit 1
    fi
  done
}

backend_sha=$(read_sha "$backend_repo")
frontend_sha=$(read_sha "$frontend_repo")
[[ $backend_sha == "${BACKEND_PLANNED_SHA:-}" ]] || {
  echo "backend checkout changed after planning" >&2
  exit 1
}
[[ $frontend_sha == "${FRONTEND_PLANNED_SHA:-}" ]] || {
  echo "frontend checkout changed after planning" >&2
  exit 1
}

backend_changed=false
frontend_changed=false
if [[ $(deployed_sha "$backend_state") != "$backend_sha" ]]; then
  backend_changed=true
fi
if [[ $(deployed_sha "$frontend_state") != "$frontend_sha" ]]; then
  frontend_changed=true
fi

ssh_dir=
remote_stage=
ssh_target=
ssh_opts=()
scp_opts=()

cleanup() {
  local status=$?
  if [[ -n $remote_stage && -n $ssh_target && ${#ssh_opts[@]} -gt 0 ]]; then
    ssh "${ssh_opts[@]}" "$ssh_target" rm -rf -- "$remote_stage" >/dev/null 2>&1 || true
  fi
  if [[ -n $ssh_dir ]]; then
    rm -rf -- "$ssh_dir"
  fi
  exit "$status"
}
trap cleanup EXIT

prepare_ssh() {
  require_env VPS_HOST VPS_PORT VPS_USER VPS_SSH_KEY VPS_SSH_HOST_ED25519_FINGERPRINT
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

  ssh_dir=$(mktemp -d "$RUNNER_TEMP/vane-ssh.XXXXXX")
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
    ssh-keygen -lf "$ssh_dir/known_hosts" -E sha256 | awk 'NR == 1 { print $2 }'
  )
  if [[ $actual_fingerprint != "$VPS_SSH_HOST_ED25519_FINGERPRINT" ]]; then
    echo "VPS Ed25519 fingerprint mismatch" >&2
    exit 1
  fi

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
  ssh_target="$VPS_USER@$VPS_HOST"
}

deploy_backend() {
  local binary
  for binary in vane useradmin gate runtimeadmin; do
    [[ -x $backend_repo/bin/$binary ]] || {
      echo "missing built backend binary: $binary" >&2
      exit 1
    }
  done
  for infra in \
    Caddyfile \
    docker-compose.yml \
    vane.service \
    dynamicconfig/development-sql.yaml; do
    [[ -f $backend_repo/deploy/$infra ]] || {
      echo "missing backend infra file: $infra" >&2
      exit 1
    }
  done

  prepare_ssh
  remote_stage="/opt/vane/.deploy-${backend_sha}-${GITHUB_RUN_ID:-manual}-${GITHUB_RUN_ATTEMPT:-1}"
  ssh "${ssh_opts[@]}" "$ssh_target" \
    mkdir -p "$remote_stage/bin" "$remote_stage/dynamicconfig"

  scp "${scp_opts[@]}" \
    "$backend_repo/bin/vane" \
    "$backend_repo/bin/useradmin" \
    "$backend_repo/bin/gate" \
    "$backend_repo/bin/runtimeadmin" \
    "$ssh_target:$remote_stage/bin/"
  scp "${scp_opts[@]}" \
    "$backend_repo/deploy/Caddyfile" \
    "$backend_repo/deploy/docker-compose.yml" \
    "$backend_repo/deploy/vane.service" \
    "$ssh_target:$remote_stage/"
  scp "${scp_opts[@]}" \
    "$backend_repo/deploy/dynamicconfig/development-sql.yaml" \
    "$ssh_target:$remote_stage/dynamicconfig/"

  ssh "${ssh_opts[@]}" "$ssh_target" \
    bash -s -- "$remote_stage" \
    <"$(dirname "$0")/remote-backend-deploy.sh"
  remote_stage=

  # The remote script ends with the production Gate. Only now is the checked-out
  # commit considered deployed.
  write_state "$backend_state" "$backend_sha"
  echo "backend deployment recorded: $backend_sha"
}

deploy_frontend() {
  require_env \
    ALIYUN_BIN ALIYUN_ACCESS_KEY_ID ALIYUN_ACCESS_KEY_SECRET \
    WRANGLER_BIN CLOUDFLARE_API_TOKEN CLOUDFLARE_ACCOUNT_ID
  [[ -x $ALIYUN_BIN ]] || {
    echo "pinned Aliyun CLI is missing" >&2
    exit 1
  }
  [[ -x $WRANGLER_BIN ]] || {
    echo "pinned Wrangler is missing" >&2
    exit 1
  }
  [[ -f $frontend_repo/dist/index.html ]] || {
    echo "frontend dist/index.html is missing" >&2
    exit 1
  }

  (
    local aliyun_temp aliyun_config attempt
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
      "$frontend_repo/dist/" oss://zhuoqidev-vane-web/ \
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

  (
    cd "$frontend_repo"
    "$WRANGLER_BIN" pages deploy dist \
      --project-name vane-web \
      --branch main
  )
  write_state "$frontend_state" "$frontend_sha"
  echo "frontend deployment recorded: $frontend_sha"
}

if [[ $backend_changed == true ]]; then
  deploy_backend
else
  echo "backend already deployed: $backend_sha"
fi

if [[ $frontend_changed == true ]]; then
  deploy_frontend
else
  echo "frontend already deployed: $frontend_sha"
fi
