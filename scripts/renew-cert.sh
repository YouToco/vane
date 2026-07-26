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

mkdir -p "$state_dir" "$acme_root" "$acme_home" "$acme_config" "$acme_certs"
chmod 700 "$state_dir" "$acme_root" "$acme_home" "$acme_config" "$acme_certs"
command -v flock >/dev/null

# deploy.sh uses the same lock, in addition to repository-wide Actions
# concurrency. No production mutation can overlap another one on this VM.
exec 9>"$state_dir/control-plane.lock"
flock 9

work_dir=$(mktemp -d "$RUNNER_TEMP/vane-cert.XXXXXX")
account_temp=$work_dir/account.conf
aliyun_config=$work_dir/aliyun-config.json
# shellcheck disable=SC2317 # Invoked indirectly by the EXIT trap.
cleanup() {
  local status=$?
  rm -rf -- "$work_dir"
  exit "$status"
}
trap cleanup EXIT

git init -q "$work_dir/acme-source"
git -C "$work_dir/acme-source" remote add origin \
  https://github.com/acmesh-official/acme.sh.git
git -C "$work_dir/acme-source" fetch --quiet --depth 1 origin "$acme_commit"
git -C "$work_dir/acme-source" checkout --quiet --detach FETCH_HEAD
if [[ $(git -C "$work_dir/acme-source" rev-parse HEAD) != "$acme_commit" ]]; then
  echo "acme.sh did not resolve to the pinned commit" >&2
  exit 1
fi

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
# Repair any residue from an interrupted older implementation before the DNS
# plugin runs. The plugin writes only to account_temp below.
install -m 0600 "$account_temp" "$persistent_account"

# dns_ali saves Ali_Key/Ali_Secret into the selected account file. Use a
# per-run copy so credentials never enter persistent acme.sh state.
Ali_Key=$ALIYUN_ACCESS_KEY_ID \
Ali_Secret=$ALIYUN_ACCESS_KEY_SECRET \
  "$acme_home/acme.sh" \
    --issue \
    --home "$acme_home" \
    --config-home "$acme_config" \
    --cert-home "$acme_certs" \
    --accountconf "$account_temp" \
    --dns dns_ali \
    --domain vane.zhuoqidev.com \
    --keylength ec-256 \
    --server letsencrypt \
    --force

# Persist non-secret account metadata atomically, and assert the filter.
sanitized_account=$work_dir/account.sanitized
sed -E '/^(SAVED_)?Ali_(Key|Secret)=/d' \
  "$account_temp" >"$sanitized_account"
if grep -Eq 'Ali_(Key|Secret)' "$sanitized_account"; then
  echo "refusing to persist Aliyun credentials in acme.sh state" >&2
  exit 1
fi
chmod 600 "$sanitized_account"
mv -f "$sanitized_account" "$persistent_account"

fullchain=$acme_certs/vane.zhuoqidev.com_ecc/fullchain.cer
private_key=$acme_certs/vane.zhuoqidev.com_ecc/vane.zhuoqidev.com.key
[[ -s $fullchain && -s $private_key ]] || {
  echo "acme.sh did not produce the expected ECC certificate files" >&2
  exit 1
}

"$ALIYUN_BIN" configure set \
  --config-path "$aliyun_config" \
  --profile default \
  --mode AK \
  --access-key-id "$ALIYUN_ACCESS_KEY_ID" \
  --access-key-secret "$ALIYUN_ACCESS_KEY_SECRET" \
  --region cn-shenzhen

"$ALIYUN_BIN" cdn SetCdnDomainSSLCertificate \
  --DomainName vane.zhuoqidev.com \
  --SSLProtocol on \
  --CertType upload \
  --CertName "vane-le-$(date +%Y%m%d)" \
  --SSLPub "$(<"$fullchain")" \
  --SSLPri "$(<"$private_key")" \
  --config-path "$aliyun_config" \
  --profile default

# Poll for at most two minutes and succeed as soon as the edge presents a
# certificate with at least 60 days remaining.
for attempt in {1..12}; do
  end=$(
    echo |
      openssl s_client \
        -servername vane.zhuoqidev.com \
        -connect vane.zhuoqidev.com.w.kunlunaq.com:443 2>/dev/null |
      openssl x509 -noout -enddate |
      cut -d= -f2
  )
  if [[ -n $end ]]; then
    expiry=$(date -d "$end" +%s)
    now=$(date +%s)
    days=$(((expiry - now) / 86400))
    echo "edge cert notAfter: $end ($days days)"
    if ((days >= 60)); then
      exit 0
    fi
  fi
  [[ $attempt -lt 12 ]] && sleep 10
done

echo "edge certificate still has fewer than 60 days remaining" >&2
exit 1
