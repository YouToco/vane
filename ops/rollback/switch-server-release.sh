#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $# -eq 2 ]] || { echo "usage: $0 TARGET_SHA EXPECTED_CURRENT_SHA" >&2; exit 2; }
target_sha=$1
expected_current=$2
[[ $target_sha =~ ^[0-9a-f]{40}$ && $expected_current =~ ^[0-9a-f]{40}$ ]] || {
  echo "rollback revisions must be exact SHAs" >&2; exit 1;
}

release_root=/opt/vane/releases
target=$release_root/$target_sha
current=/opt/vane/current
[[ -d $target && ! -L $target && -L $current ]] || {
  echo "rollback target or current authority is unavailable" >&2; exit 1;
}
current_target=$(readlink "$current")
[[ $current_target == "$release_root/$expected_current" ]] || {
  echo "server rollback CAS mismatch" >&2; exit 1;
}
for required in \
  bin/vane bin/gate \
  deploy/Caddyfile deploy/docker-compose.yml \
  deploy/dynamicconfig/development-sql.yaml \
  deploy/vane.service \
  deploy/vane-research-gateway.service deploy/vane-research-gateway.socket \
  infra-manifest.sha256; do
  [[ -f $target/$required && ! -L $target/$required ]] || {
    echo "rollback target lacks bound member: $required" >&2; exit 1;
  }
done

target_infra_digest=$(sha256sum "$target/infra-manifest.sha256" | awk '{print $1}')
current_infra_digest=$(sha256sum "$current_target/infra-manifest.sha256" | awk '{print $1}')
infra_changed=false
if [[ $target_infra_digest != "$current_infra_digest" ]]; then
  infra_changed=true
fi

backup=$(mktemp -d /opt/vane/.rollback-backup.XXXXXX)
switched=false
cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )) && [[ $switched == true ]]; then
    rescue=$release_root/.rollback-rescue.$$
    ln -s "$current_target" "$rescue"
    mv -Tf "$rescue" "$current"
    if [[ $infra_changed == true ]]; then
      cp --archive "$backup/Caddyfile" /opt/vane/Caddyfile
      cp --archive "$backup/docker-compose.yml" /opt/vane/docker-compose.yml
      cp --archive "$backup/development-sql.yaml" /opt/vane/dynamicconfig/development-sql.yaml
      (cd /opt/vane && docker compose up -d postgres temporal temporal-ui caddy) || true
    fi
    for unit in vane.service vane-research-gateway.service vane-research-gateway.socket; do
      cp --archive "$backup/$unit" "/etc/systemd/system/$unit"
    done
    systemctl daemon-reload || true
    systemctl restart vane-research-gateway.socket vane-research-gateway.service vane.service || true
  fi
  rm -rf -- "$backup"
  exit "$status"
}
trap cleanup EXIT

if [[ $infra_changed == true ]]; then
  cp --archive /opt/vane/Caddyfile "$backup/Caddyfile"
  cp --archive /opt/vane/docker-compose.yml "$backup/docker-compose.yml"
  cp --archive /opt/vane/dynamicconfig/development-sql.yaml "$backup/development-sql.yaml"
fi
for unit in vane.service vane-research-gateway.service vane-research-gateway.socket; do
  cp --archive "/etc/systemd/system/$unit" "$backup/$unit"
done

systemctl stop vane.service vane-research-gateway.service vane-research-gateway.socket
if [[ $infra_changed == true ]]; then
  install -m 0644 "$target/deploy/Caddyfile" /opt/vane/Caddyfile
  install -m 0644 "$target/deploy/docker-compose.yml" /opt/vane/docker-compose.yml
  install -m 0644 "$target/deploy/dynamicconfig/development-sql.yaml" /opt/vane/dynamicconfig/development-sql.yaml
fi
for unit in vane.service vane-research-gateway.service vane-research-gateway.socket; do
  install -m 0644 "$target/deploy/$unit" "/etc/systemd/system/$unit"
done
next=$release_root/.rollback-$target_sha.$$
ln -s "$target" "$next"
mv -Tf "$next" "$current"
switched=true
if [[ $infra_changed == true ]]; then
  (cd /opt/vane && docker compose up -d postgres temporal temporal-ui caddy)
fi
systemctl daemon-reload
systemctl start vane-research-gateway.socket vane-research-gateway.service vane.service
for _ in {1..90}; do
  curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8080/readyz >/dev/null && break
  sleep 2
done
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8080/readyz >/dev/null
env -i PATH=/usr/bin:/bin CREDENTIALS_DIRECTORY=/etc/vane/credentials \
  "$current/bin/gate" -env /opt/vane/env/server.env
pid=$(systemctl show vane.service --property=MainPID --value)
[[ $pid =~ ^[1-9][0-9]*$ && $(readlink /proc/"$pid"/exe) == "$target/bin/vane" ]] || {
  echo "rollback process is not bound to the target release" >&2; exit 1;
}
switched=false
rm -rf -- "$backup"
trap - EXIT
echo "server rollback activated: $target_sha"
