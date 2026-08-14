#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $# -eq 2 ]] || { echo "usage: $0 STAGE EXPECTED_CURRENT_SHA_OR_NONE" >&2; exit 2; }
stage=$1
expected_current=$2
[[ $stage =~ ^/opt/vane/\.deploy-([0-9a-f]{40})-[0-9]+-[0-9]+$ ]] || {
  echo "unsafe remote stage" >&2; exit 1;
}
release_sha=${BASH_REMATCH[1]}
[[ $expected_current == none || $expected_current =~ ^[0-9a-f]{40}$ ]] || {
  echo "invalid expected current release" >&2; exit 1;
}

release_root=/opt/vane/releases
release_dir=$release_root/$release_sha
current_link=/opt/vane/current
lock_file=/opt/vane/.release.lock
mkdir -p "$release_root"
exec 9>"$lock_file"
flock 9

current_target=
if [[ -L $current_link ]]; then
  current_target=$(readlink "$current_link")
  [[ $current_target =~ ^/opt/vane/releases/([0-9a-f]{40})$ ]] || {
    echo "current release symlink has unsafe target" >&2; exit 1;
  }
  current_sha=${BASH_REMATCH[1]}
  [[ -d $current_target && ! -L $current_target ]] || {
    echo "current release directory is unavailable or unsafe" >&2; exit 1;
  }
else
  [[ ! -e $current_link ]] || { echo "current release authority is not a symlink" >&2; exit 1; }
  current_sha=none
fi
[[ $current_sha == "$expected_current" ]] || {
  echo "current release CAS mismatch: expected=$expected_current actual=$current_sha" >&2
  exit 1
}

binaries=(vane useradmin gate runtimeadmin vane-migrate agentfirstretention vane-research-gateway vane-research-prepare researchshadow researchcutover)
infra_files=(Caddyfile docker-compose.yml vane.service vane-migrate.service vane-research-gateway.service vane-research-gateway.socket vane-legacy-compat.service dynamicconfig/development-sql.yaml)
for binary in "${binaries[@]}"; do
  [[ -f $stage/bin/$binary && ! -L $stage/bin/$binary && -x $stage/bin/$binary ]] || {
    echo "candidate release lacks binary: $binary" >&2; exit 1;
  }
done
for file in "${infra_files[@]}"; do
  [[ -f $stage/$file && ! -L $stage/$file ]] || {
    echo "candidate release lacks infra member: $file" >&2; exit 1;
  }
done
[[ -f $stage/release-receipt.json && ! -L $stage/release-receipt.json ]] || {
  echo "candidate release lacks receipt" >&2; exit 1;
}

pending=$(mktemp -d "$release_root/.pending-$release_sha.XXXXXX")
unit_backup=$(mktemp -d "/etc/systemd/system/.vane-release-backup.XXXXXX")
runtime_backup=$(mktemp -d "/opt/vane/.runtime-backup.XXXXXX")
switched=false
infra_applied=false
cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )); then
    if [[ $switched == true ]]; then
      if [[ -n $current_target ]]; then
        rollback_link=$release_root/.current-rollback.$$
        ln -s "$current_target" "$rollback_link"
        mv -Tf "$rollback_link" "$current_link"
      else
        rm -f -- "$current_link"
      fi
      for unit in vane.service vane-migrate.service vane-research-gateway.service vane-research-gateway.socket; do
        if [[ -f $unit_backup/$unit ]]; then
          install -m 0644 "$unit_backup/$unit" "/etc/systemd/system/$unit"
        else
          rm -f -- "/etc/systemd/system/$unit"
        fi
      done
      systemctl daemon-reload || true
      systemctl restart vane-research-gateway.socket vane-research-gateway.service vane.service || true
    fi
    if [[ $infra_applied == true ]]; then
      install -m 0644 "$runtime_backup/Caddyfile" /opt/vane/Caddyfile
      install -m 0644 "$runtime_backup/docker-compose.yml" /opt/vane/docker-compose.yml
      install -m 0644 "$runtime_backup/development-sql.yaml" /opt/vane/dynamicconfig/development-sql.yaml
      (cd /opt/vane && docker compose up -d postgres temporal temporal-ui caddy) || true
    fi
  fi
  rm -rf -- "$pending" "$unit_backup" "$runtime_backup"
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$pending/bin" "$pending/deploy/dynamicconfig"
for binary in "${binaries[@]}"; do install -m 0755 "$stage/bin/$binary" "$pending/bin/$binary"; done
for file in "${infra_files[@]}"; do
  destination=$pending/deploy/$file
  mkdir -p "$(dirname "$destination")"
  install -m 0644 "$stage/$file" "$destination"
done
install -m 0644 "$stage/release-receipt.json" "$pending/release-receipt.json"
(
  cd "$pending"
  find bin deploy -type f -print0 | sort -z | xargs -0 sha256sum >infra-bound-files.sha256
  find deploy -type f -print0 | sort -z | xargs -0 sha256sum >infra-manifest.sha256
)
printf '%s\n' "$release_sha" >"$pending/monorepo-revision"
if [[ -e $release_dir ]]; then
  diff -qr --no-dereference "$pending" "$release_dir" >/dev/null || {
    echo "immutable release replay differs from existing SHA directory" >&2; exit 1;
  }
  rm -rf -- "$pending"
  pending=$(mktemp -d "$release_root/.pending-$release_sha.XXXXXX")
else
  mv -T "$pending" "$release_dir"
  pending=$(mktemp -d "$release_root/.pending-$release_sha.XXXXXX")
fi

candidate_infra_digest=$(sha256sum "$release_dir/infra-manifest.sha256" | awk '{print $1}')
current_infra_digest=none
if [[ -n $current_target ]]; then
  [[ -f $current_target/infra-manifest.sha256 && ! -L $current_target/infra-manifest.sha256 ]] || {
    echo "current release lacks bound infra manifest" >&2; exit 1;
  }
  current_infra_digest=$(sha256sum "$current_target/infra-manifest.sha256" | awk '{print $1}')
fi
infra_changed=false
if [[ $candidate_infra_digest != "$current_infra_digest" ]]; then
  infra_changed=true
fi

if [[ $infra_changed == true ]]; then
  for runtime_file in \
    /opt/vane/Caddyfile \
    /opt/vane/docker-compose.yml \
    /opt/vane/dynamicconfig/development-sql.yaml; do
    [[ -f $runtime_file && ! -L $runtime_file ]] || {
      echo "cannot atomically change infra without a canonical prior runtime file: $runtime_file" >&2
      exit 1
    }
  done
  cp --archive /opt/vane/Caddyfile "$runtime_backup/Caddyfile"
  cp --archive /opt/vane/docker-compose.yml "$runtime_backup/docker-compose.yml"
  cp --archive /opt/vane/dynamicconfig/development-sql.yaml "$runtime_backup/development-sql.yaml"
  infra_applied=true
  install -m 0644 "$release_dir/deploy/Caddyfile" /opt/vane/Caddyfile
  install -m 0644 "$release_dir/deploy/docker-compose.yml" /opt/vane/docker-compose.yml
  install -m 0644 "$release_dir/deploy/dynamicconfig/development-sql.yaml" /opt/vane/dynamicconfig/development-sql.yaml
  (cd /opt/vane && docker compose up -d postgres temporal temporal-ui caddy)
fi

systemd-run --quiet --wait --collect --unit="vane-migrate-$release_sha" \
  --property=Type=oneshot --property=User=vane-migrate --property=Group=vane-migrate \
  --property=WorkingDirectory=/opt/vane \
  --property=LoadCredential=migration_db_url:/etc/vane/credentials/migration_db_url \
  --property=NoNewPrivileges=yes --property=ProtectSystem=strict --property=ProtectHome=yes \
  --property=TimeoutStartSec=6min "$release_dir/bin/vane-migrate"

for unit in vane.service vane-migrate.service vane-research-gateway.service vane-research-gateway.socket; do
  [[ ! -f /etc/systemd/system/$unit ]] || cp --archive "/etc/systemd/system/$unit" "$unit_backup/$unit"
done
systemctl stop vane.service vane-research-gateway.service vane-research-gateway.socket
for unit in vane.service vane-migrate.service vane-research-gateway.service vane-research-gateway.socket; do
  install -m 0644 "$release_dir/deploy/$unit" "/etc/systemd/system/$unit"
done
next_link=$release_root/.current-$release_sha.$$
ln -s "$release_dir" "$next_link"
mv -Tf "$next_link" "$current_link"
switched=true
systemctl daemon-reload
systemctl enable vane-research-gateway.socket vane-research-gateway.service vane.service >/dev/null
systemctl start vane-research-gateway.socket vane-research-gateway.service vane.service

for _ in {1..90}; do
  curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8080/readyz >/dev/null && break
  sleep 2
done
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8080/readyz >/dev/null
env -i PATH=/usr/bin:/bin CREDENTIALS_DIRECTORY=/etc/vane/credentials \
  "$current_link/bin/gate" -env /opt/vane/env/server.env
pid=$(systemctl show vane.service --property=MainPID --value)
[[ $pid =~ ^[1-9][0-9]*$ && $(readlink /proc/"$pid"/exe) == "$release_dir/bin/vane" ]] || {
  echo "live process is not bound to candidate SHA release" >&2; exit 1;
}
switched=false
rm -rf -- "$pending" "$unit_backup" "$runtime_backup" "$stage"
trap - EXIT
echo "atomic release activated: $release_sha"
