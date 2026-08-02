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

# Backend deployment cleanup runs from an EXIT trap after deploy_backend's
# local scope has unwound. Keep its state at script scope; Bash removes local
# variables before invoking the EXIT trap, which previously turned a real Gate
# failure into an additional "unbound variable" error and leaked remote_stage.
backend_ssh_dir=
backend_remote_stage=
backend_ssh_target=
backend_remote_stage_created=false
declare -a backend_ssh_opts=()

cleanup_backend() {
  local status=$?
  trap - EXIT
  if [[ $backend_remote_stage_created == true &&
        -n $backend_remote_stage ]]; then
    ssh "${backend_ssh_opts[@]}" "$backend_ssh_target" \
      rm -rf -- "$backend_remote_stage" >/dev/null 2>&1 || true
  fi
  if [[ -n $backend_ssh_dir ]]; then
    rm -rf -- "$backend_ssh_dir"
  fi
  exit "$status"
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
  for binary in vane useradmin gate runtimeadmin vane-migrate \
    vane-research-gateway researchshadow researchcutover; do
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
    vane-migrate.service \
    vane-research-gateway.service \
    vane-research-gateway.socket \
    dynamicconfig/development-sql.yaml; do
    [[ -f $payload/deploy/$infra && ! -L $payload/deploy/$infra ]] || {
      echo "missing verified backend infra file: $infra" >&2
      exit 1
    }
  done

  local actual_fingerprint
  local -a scp_opts
  backend_ssh_dir=$(mktemp -d "$RUNNER_TEMP/vane-ssh.XXXXXX")
  backend_remote_stage="/opt/vane/.deploy-${source_sha}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
  backend_ssh_target="$VPS_USER@$VPS_HOST"
  backend_remote_stage_created=false
  backend_ssh_opts=()
  trap cleanup_backend EXIT

  chmod 700 "$backend_ssh_dir"
  printf '%s\n' "$VPS_SSH_KEY" >"$backend_ssh_dir/id"
  chmod 600 "$backend_ssh_dir/id"
  ssh-keyscan -p "$VPS_PORT" -t ed25519 "$VPS_HOST" \
    2>/dev/null >"$backend_ssh_dir/known_hosts"
  [[ $(wc -l <"$backend_ssh_dir/known_hosts") -eq 1 ]] || {
    echo "expected exactly one VPS Ed25519 host key" >&2
    exit 1
  }
  actual_fingerprint=$(
    ssh-keygen -lf "$backend_ssh_dir/known_hosts" -E sha256 |
      awk 'NR == 1 { print $2 }'
  )
  [[ $actual_fingerprint == "$VPS_SSH_HOST_ED25519_FINGERPRINT" ]] || {
    echo "VPS Ed25519 fingerprint mismatch" >&2
    exit 1
  }

  backend_ssh_opts=(
    -i "$backend_ssh_dir/id"
    -p "$VPS_PORT"
    -o BatchMode=yes
    -o IdentitiesOnly=yes
    -o StrictHostKeyChecking=yes
    -o "UserKnownHostsFile=$backend_ssh_dir/known_hosts"
    -o ConnectTimeout=15
    -o ServerAliveInterval=15
    -o ServerAliveCountMax=4
  )
  scp_opts=(
    -i "$backend_ssh_dir/id"
    -P "$VPS_PORT"
    -o BatchMode=yes
    -o IdentitiesOnly=yes
    -o StrictHostKeyChecking=yes
    -o "UserKnownHostsFile=$backend_ssh_dir/known_hosts"
    -o ConnectTimeout=15
  )

  ssh "${backend_ssh_opts[@]}" "$backend_ssh_target" \
    mkdir -p "$backend_remote_stage/bin" "$backend_remote_stage/dynamicconfig"
  backend_remote_stage_created=true
  scp "${scp_opts[@]}" \
    "$payload/bin/vane" \
    "$payload/bin/useradmin" \
    "$payload/bin/gate" \
    "$payload/bin/runtimeadmin" \
    "$payload/bin/vane-migrate" \
    "$payload/bin/vane-research-gateway" \
    "$payload/bin/researchshadow" \
    "$payload/bin/researchcutover" \
    "$backend_ssh_target:$backend_remote_stage/bin/"
  scp "${scp_opts[@]}" \
    "$payload/deploy/Caddyfile" \
    "$payload/deploy/docker-compose.yml" \
    "$payload/deploy/vane.service" \
    "$payload/deploy/vane-migrate.service" \
    "$payload/deploy/vane-research-gateway.service" \
    "$payload/deploy/vane-research-gateway.socket" \
    "$backend_ssh_target:$backend_remote_stage/"
  scp "${scp_opts[@]}" \
    "$payload/deploy/dynamicconfig/development-sql.yaml" \
    "$backend_ssh_target:$backend_remote_stage/dynamicconfig/"

  ssh "${backend_ssh_opts[@]}" "$backend_ssh_target" \
    bash -s -- "$backend_remote_stage" \
    <"$(dirname "$0")/remote-backend-deploy.sh"
  backend_remote_stage=
  backend_remote_stage_created=false
  trap - EXIT
  rm -rf -- "$backend_ssh_dir"
  backend_ssh_dir=
  backend_ssh_opts=()

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

check_frontend_release_receipt() {
  local planned_receipt=$1
  local release_dir=$state_dir/releases/$source_sha
  local release_receipt=$release_dir/frontend-aliyun.json
  [[ ! -L $state_dir/releases && ! -L $release_dir ]] || {
    echo "durable frontend release directory must not be a symlink" >&2
    exit 1
  }
  if [[ -e $release_receipt || -L $release_receipt ]]; then
    [[ -f $release_receipt && ! -L $release_receipt ]] || {
      echo "durable frontend release receipt is not a regular file" >&2
      exit 1
    }
    cmp -s "$planned_receipt" "$release_receipt" || {
      echo "durable frontend release receipt conflicts with this payload" >&2
      exit 1
    }
  fi
}

persist_frontend_release_receipt() {
  local planned_receipt=$1
  local release_dir=$state_dir/releases/$source_sha
  local release_receipt=$release_dir/frontend-aliyun.json
  local receipt_temp
  [[ ! -L $state_dir/releases && ! -L $release_dir ]] || {
    echo "durable frontend release directory must not be a symlink" >&2
    exit 1
  }
  mkdir -p "$release_dir"
  chmod 700 "$state_dir/releases" "$release_dir"
  if [[ -e $release_receipt || -L $release_receipt ]]; then
    [[ -f $release_receipt && ! -L $release_receipt ]] || {
      echo "durable frontend release receipt is not a regular file" >&2
      exit 1
    }
    cmp -s "$planned_receipt" "$release_receipt" || {
      echo "durable frontend release receipt changed during publication" >&2
      exit 1
    }
    return
  fi
  receipt_temp=$(mktemp "$release_dir/.frontend-aliyun.XXXXXX")
  cp "$planned_receipt" "$receipt_temp"
  chmod 600 "$receipt_temp"
  mv "$receipt_temp" "$release_receipt"
}

stat_oss_object_with_size() {
  local object=$1
  local expected_size=$2
  local object_meta
  [[ $expected_size =~ ^[0-9]+$ ]] || {
    echo "invalid expected OSS object size for $object" >&2
    return 1
  }
  object_meta=$(
    "$OSSUTIL_BIN" stat "oss://zhuoqidev-vane-web/$object"
  )
  if ! printf '%s\n' "$object_meta" |
    grep -Eiq \
      "^[[:space:]]*Content-Length[[:space:]]*:[[:space:]]*${expected_size}[[:space:]]*$"; then
    echo "OSS object Content-Length does not match local file: $object" >&2
    return 1
  fi
  printf '%s\n' "$object_meta"
}

deploy_frontend_aliyun() {
  local owner_preview_object="_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html"
  local publication_plan
  local publication_plan_cleanup
  local -a refresh_urls
  if frontend_state_check; then
    :
  elif [[ $? -eq 10 ]]; then
    echo "frontend already deployed: $source_sha"
    write_frontend_receipt aliyun
    return
  else
    return 1
  fi

  require_env FRONTEND_RECEIPT_DIR
  command -v cmp >/dev/null
  command -v python3 >/dev/null
  mkdir -p "$FRONTEND_RECEIPT_DIR"
  chmod 700 "$FRONTEND_RECEIPT_DIR"
  publication_plan=$(mktemp -d "$FRONTEND_RECEIPT_DIR/.aliyun-plan.XXXXXX")
  printf -v publication_plan_cleanup 'rm -rf -- %q' "$publication_plan"
  trap "$publication_plan_cleanup" EXIT
  "$(dirname "$0")/frontend-release.py" \
    --dist "$payload/dist" \
    --sha "$source_sha" \
    --output "$publication_plan"
  check_frontend_release_receipt "$publication_plan/release.json"

  require_env \
    ALIYUN_BIN OSSUTIL_BIN \
    ALIYUN_ACCESS_KEY_ID ALIYUN_ACCESS_KEY_SECRET
  [[ -x $ALIYUN_BIN ]] || {
    echo "pinned Aliyun CLI is missing" >&2
    exit 1
  }
  [[ -x $OSSUTIL_BIN ]] || {
    echo "pinned ossutil is missing" >&2
    exit 1
  }
  ossutil_version=$("$OSSUTIL_BIN" version)
  [[ $ossutil_version == "2.3.0" ]] || {
    echo "unexpected pinned ossutil version: $ossutil_version" >&2
    exit 1
  }
  if [[ -e $payload/dist/$owner_preview_object ]]; then
    [[ -f $payload/dist/$owner_preview_object &&
      ! -L $payload/dist/$owner_preview_object ]] || {
      echo "owner preview HTML is not a regular file" >&2
      exit 1
    }
  fi

  (
    # `aliyun oss` is the deprecated ossutil v1 bridge and does not understand
    # the outer CLI's --config-path flag. Use the separately SHA-pinned
    # ossutil v2 binary and its supported environment credential provider.
    # Both distribution commands keep secrets out of argv and credential files.
    export OSS_ACCESS_KEY_ID="$ALIYUN_ACCESS_KEY_ID"
    export OSS_ACCESS_KEY_SECRET="$ALIYUN_ACCESS_KEY_SECRET"
    export OSS_REGION=cn-shenzhen

    # Immutable and non-HTML objects become available first. No object is
    # deleted: older HTML can continue loading its content-hashed assets.
    while IFS= read -r object; do
      [[ -n $object ]] || continue
      "$OSSUTIL_BIN" cp \
        "$payload/dist/$object" \
        "oss://zhuoqidev-vane-web/$object" \
        --force
    done <"$publication_plan/assets.list"

    # The new entry and manifests must not be cut over until OSS reports the
    # exact local Content-Length for every referenced asset.
    while IFS=$'\t' read -r expected_size object; do
      [[ -n $object ]] || continue
      stat_oss_object_with_size "$object" "$expected_size" >/dev/null
    done <"$publication_plan/critical-assets.list"

    # Publish secondary HTML first. The canonical root entry is a separate,
    # final object write after all earlier publication checks have passed.
    while IFS= read -r object; do
      [[ -n $object ]] || continue
      "$OSSUTIL_BIN" cp \
        "$payload/dist/$object" \
        "oss://zhuoqidev-vane-web/$object" \
        --force
    done <"$publication_plan/html-before-entry.list"

    if [[ -f $payload/dist/$owner_preview_object ]]; then
      "$OSSUTIL_BIN" set-props \
        "oss://zhuoqidev-vane-web/$owner_preview_object" \
        --cache-control no-store \
        --metadata-directive update \
        --force
      owner_preview_meta=$(
        stat_oss_object_with_size \
          "$owner_preview_object" \
          "$(wc -c <"$payload/dist/$owner_preview_object")"
      )
      if ! printf '%s\n' "$owner_preview_meta" |
        grep -Eiq 'Cache-Control[^[:alnum:]]+no-store'; then
        echo "owner preview OSS object is missing Cache-Control:no-store" >&2
        exit 1
      fi
    fi

    "$OSSUTIL_BIN" cp \
      "$payload/dist/index.html" \
      oss://zhuoqidev-vane-web/index.html \
      --force
    stat_oss_object_with_size \
      index.html \
      "$(wc -c <"$payload/dist/index.html")" >/dev/null

    refresh_urls=()
    while IFS= read -r refresh_path; do
      [[ -n $refresh_path ]] || continue
      refresh_urls+=("https://vane.zhuoqidev.com$refresh_path")
    done <"$publication_plan/cdn-refresh-paths.list"
    for refresh_url in "${refresh_urls[@]}"; do
      for attempt in 1 2 3; do
        if ALIBABA_CLOUD_IGNORE_PROFILE=TRUE \
          ALIBABA_CLOUD_ACCESS_KEY_ID="$ALIYUN_ACCESS_KEY_ID" \
          ALIBABA_CLOUD_ACCESS_KEY_SECRET="$ALIYUN_ACCESS_KEY_SECRET" \
          ALIBABA_CLOUD_REGION_ID=cn-shenzhen \
          "$ALIYUN_BIN" cdn RefreshObjectCaches \
          --ObjectPath "$refresh_url" \
          --ObjectType File; then
          break
        fi
        [[ $attempt -lt 3 ]] || exit 1
        sleep $((attempt * 5))
      done
    done
  )
  persist_frontend_release_receipt "$publication_plan/release.json"
  write_frontend_receipt aliyun
  rm -rf -- "$publication_plan"
  trap - EXIT
  echo "frontend Aliyun line deployed: $source_sha"
}

deploy_frontend_cloudflare() {
  local owner_preview_path="_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/"
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
  [[ $wrangler_version == "4.115.0" ]] || {
    echo "unexpected preinstalled Wrangler version: $wrangler_version" >&2
    exit 1
  }
  [[ -f $payload/dist/index.html && ! -L $payload/dist/index.html ]] || {
    echo "verified frontend dist/index.html is missing" >&2
    exit 1
  }
  command -v curl >/dev/null

  (
    cd "$payload"
    "$WRANGLER_BIN" pages deploy dist \
      --project-name vane-web \
      --branch main

    if [[ -f $payload/dist/${owner_preview_path}index.html ]]; then
      for attempt in 1 2 3 4 5; do
        owner_preview_headers=$(
          curl --fail --silent --show-error --head \
            "https://vane-web.pages.dev/$owner_preview_path" || true
        )
        normalized_headers=$(printf '%s\n' "$owner_preview_headers" | tr -d '\r')
        if printf '%s\n' "$normalized_headers" |
          grep -Eiq '^Cache-Control:.*no-store' &&
          printf '%s\n' "$normalized_headers" |
            grep -Eiq '^X-Robots-Tag:.*noindex.*nofollow.*noarchive' &&
          printf '%s\n' "$normalized_headers" |
            grep -Eiq "^Content-Security-Policy:.*connect-src 'none'"; then
          break
        fi
        [[ $attempt -lt 5 ]] || {
          echo "Cloudflare owner preview headers did not converge" >&2
          exit 1
        }
        sleep $((attempt * 3))
      done
    fi
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
