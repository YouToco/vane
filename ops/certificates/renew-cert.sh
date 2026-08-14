#!/usr/bin/env bash
set -euo pipefail
umask 077

require_env() {
  local name
  for name in "$@"; do
    if [[ -z ${!name:-} ]]; then
      echo "required environment variable is empty: $name" >&2
      exit 1
    fi
  done
}

require_env \
  ALIYUN_BIN ALIYUN_ACCESS_KEY_ID ALIYUN_ACCESS_KEY_SECRET \
  ACME_ACCOUNT_EMAIL RUNNER_TEMP
[[ -x $ALIYUN_BIN ]] || {
  echo "pinned Aliyun CLI is missing" >&2
  exit 1
}

state_root=${XDG_STATE_HOME:-"$HOME/.local/state"}
state_dir=$state_root/vane-deploy
acme_root=$state_dir/acme
acme_home=$acme_root/home
acme_config=$acme_root/config
acme_certs=$acme_root/certs
acme_commit=3661fd86b6304115e42f43910e6dd452ab9866d6
domain=vane.zhuoqidev.com
fullchain=$acme_certs/${domain}_ecc/fullchain.cer
private_key=$acme_certs/${domain}_ecc/${domain}.key
attempt_state=$acme_root/last-issuance-attempt
verified_state=$acme_root/last-verified-fingerprint

mkdir -p "$state_dir" "$acme_root" "$acme_home" "$acme_config" "$acme_certs"
chmod 700 "$state_dir" "$acme_root" "$acme_home" "$acme_config" "$acme_certs"
command -v flock >/dev/null

# deploy.sh uses this same lock. Issuance, upload, edge verification, and state
# updates are one VM-wide critical section.
exec 9>"$state_dir/control-plane.lock"
flock 9

work_dir=$(mktemp -d "$RUNNER_TEMP/vane-cert.XXXXXX")
account_temp=$work_dir/account.conf
aliyun_config=$work_dir/aliyun-config.json
# shellcheck disable=SC2317 # Invoked indirectly by the EXIT trap.
cleanup() {
  local status=$?
  trap - EXIT
  rm -rf -- "$work_dir"
  exit "$status"
}
trap cleanup EXIT

git init -q "$work_dir/acme-source"
git -C "$work_dir/acme-source" remote add origin \
  https://github.com/acmesh-official/acme.sh.git
git -C "$work_dir/acme-source" fetch --quiet --depth 1 origin "$acme_commit"
git -C "$work_dir/acme-source" checkout --quiet --detach FETCH_HEAD
[[ $(git -C "$work_dir/acme-source" rev-parse HEAD) == "$acme_commit" ]]

(
  cd "$work_dir/acme-source"
  ./acme.sh \
    --install \
    --home "$acme_home" \
    --config-home "$acme_config" \
    --cert-home "$acme_certs" \
    --accountemail "$ACME_ACCOUNT_EMAIL" \
    --no-cron \
    --no-profile
)

persistent_account=$acme_config/account.conf
if [[ -f $persistent_account ]]; then
  sed -E '/^(SAVED_)?Ali_(Key|Secret)=/d' \
    "$persistent_account" >"$account_temp"
else
  : >"$account_temp"
fi
chmod 600 "$account_temp"
# Repair any residue from an older implementation before dns_ali runs. The
# plugin below writes credentials only to the per-run account copy.
install -m 0600 "$account_temp" "$persistent_account"

certificate_days_remaining() {
  local certificate=$1
  local end expiry now
  end=$(openssl x509 -in "$certificate" -noout -enddate | cut -d= -f2)
  expiry=$(date -d "$end" +%s)
  now=$(date +%s)
  printf '%s' "$(((expiry - now) / 86400))"
}

atomic_write() {
  local destination=$1
  local value=$2
  local temporary
  temporary=$(mktemp "$acme_root/.state.XXXXXX")
  printf '%s\n' "$value" >"$temporary"
  chmod 600 "$temporary"
  mv -f "$temporary" "$destination"
}

need_issuance=true
if [[ -s $fullchain && ! -s $private_key ]] || \
  [[ ! -s $fullchain && -s $private_key ]]; then
  echo "local certificate state is incomplete; refusing automatic issuance" >&2
  exit 1
fi
if [[ -s $fullchain && -s $private_key ]]; then
  local_days=$(certificate_days_remaining "$fullchain")
  echo "local certificate has $local_days days remaining"
  if ((local_days >= 60)); then
    need_issuance=false
  fi
fi

if [[ $need_issuance == true ]]; then
  now=$(date +%s)
  previous_attempt=0
  if [[ -f $attempt_state ]]; then
    previous_attempt=$(tr -d '\r\n' <"$attempt_state")
    [[ $previous_attempt =~ ^[0-9]+$ ]]
  fi
  if ((now - previous_attempt < 86400)); then
    echo "certificate issuance is throttled for 24h after the prior attempt" >&2
    exit 1
  fi
  # Persist before contacting the CA. A killed/failed run cannot create a tight
  # manual-dispatch loop against Let's Encrypt rate limits.
  atomic_write "$attempt_state" "$now"

  acme_common=(
    --home "$acme_home"
    --config-home "$acme_config"
    --cert-home "$acme_certs"
    --accountconf "$account_temp"
    --domain "$domain"
    --server letsencrypt
  )
  if [[ -s $fullchain ]]; then
    Ali_Key=$ALIYUN_ACCESS_KEY_ID \
    Ali_Secret=$ALIYUN_ACCESS_KEY_SECRET \
      "$acme_home/acme.sh" \
        --renew "${acme_common[@]}" --ecc --force
  else
    Ali_Key=$ALIYUN_ACCESS_KEY_ID \
    Ali_Secret=$ALIYUN_ACCESS_KEY_SECRET \
      "$acme_home/acme.sh" \
        --issue "${acme_common[@]}" \
        --dns dns_ali \
        --keylength ec-256
  fi

  [[ -s $fullchain && -s $private_key ]] || {
    echo "acme.sh did not produce the expected ECC certificate files" >&2
    exit 1
  }
  local_days=$(certificate_days_remaining "$fullchain")
  ((local_days >= 60)) || {
    echo "new local certificate has fewer than 60 days remaining" >&2
    exit 1
  }
fi

# Persist non-secret account metadata atomically. dns_ali credentials remain
# confined to work_dir and are removed by the EXIT trap.
sanitized_account=$work_dir/account.sanitized
sed -E '/^(SAVED_)?Ali_(Key|Secret)=/d' \
  "$account_temp" >"$sanitized_account"
if grep -Eq 'Ali_(Key|Secret)' "$sanitized_account"; then
  echo "refusing to persist Aliyun credentials in acme.sh state" >&2
  exit 1
fi
chmod 600 "$sanitized_account"
mv -f "$sanitized_account" "$persistent_account"

certificate_public_key=$(
  openssl x509 -in "$fullchain" -pubkey -noout |
    openssl pkey -pubin -outform DER 2>/dev/null |
    sha256sum |
    awk '{ print $1 }'
)
private_public_key=$(
  openssl pkey -in "$private_key" -pubout -outform DER 2>/dev/null |
    sha256sum |
    awk '{ print $1 }'
)
[[ $certificate_public_key == "$private_public_key" ]] || {
  echo "local certificate and private key do not match" >&2
  exit 1
}

local_fingerprint=$(
  openssl x509 -in "$fullchain" -noout -fingerprint -sha256 |
    cut -d= -f2
)
[[ $local_fingerprint =~ ^([0-9A-F]{2}:){31}[0-9A-F]{2}$ ]]

"$ALIYUN_BIN" configure set \
  --config-path "$aliyun_config" \
  --profile default \
  --mode AK \
  --access-key-id "$ALIYUN_ACCESS_KEY_ID" \
  --access-key-secret "$ALIYUN_ACCESS_KEY_SECRET" \
  --region cn-shenzhen

"$ALIYUN_BIN" cdn SetCdnDomainSSLCertificate \
  --DomainName "$domain" \
  --SSLProtocol on \
  --CertType upload \
  --CertName "vane-le-$(date +%Y%m%d)" \
  --SSLPub "$(<"$fullchain")" \
  --SSLPri "$(<"$private_key")" \
  --config-path "$aliyun_config" \
  --profile default

# The edge must present the exact leaf uploaded above. Remaining lifetime alone
# could incorrectly accept the old certificate while propagation is pending.
for attempt in {1..12}; do
  edge_chain=$work_dir/edge-chain.pem
  edge_leaf=$work_dir/edge-leaf.pem
  if openssl s_client \
    -servername "$domain" \
    -connect "$domain.w.kunlunaq.com:443" \
    -showcerts </dev/null >"$edge_chain" 2>/dev/null; then
    awk '
      /-----BEGIN CERTIFICATE-----/ { capture = 1 }
      capture { print }
      /-----END CERTIFICATE-----/ { exit }
    ' "$edge_chain" >"$edge_leaf"
    if [[ -s $edge_leaf ]]; then
      edge_fingerprint=$(
        openssl x509 -in "$edge_leaf" -noout -fingerprint -sha256 |
          cut -d= -f2
      )
      edge_days=$(certificate_days_remaining "$edge_leaf")
      echo "edge fingerprint=$edge_fingerprint days=$edge_days"
      if [[ $edge_fingerprint == "$local_fingerprint" ]] && ((edge_days >= 60)); then
        atomic_write "$verified_state" \
          "$local_fingerprint $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "edge presents the exact uploaded certificate"
        exit 0
      fi
    fi
  fi
  [[ $attempt -lt 12 ]] && sleep 10
done

echo "edge did not present the exact uploaded >=60-day certificate" >&2
exit 1
