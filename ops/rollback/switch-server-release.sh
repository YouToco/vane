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
  bin/vane bin/vane-research-gateway bin/gate; do
  [[ -f $target/$required && ! -L $target/$required ]] || {
    echo "rollback target lacks bound member: $required" >&2; exit 1;
  }
done

backup=$(mktemp -d /opt/vane/.rollback-backup.XXXXXX)
switched=false
cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )) && [[ $switched == true ]]; then
    rescue=$release_root/.rollback-rescue.$$
    ln -s "$current_target" "$rescue"
    mv -Tf "$rescue" "$current"
    systemctl restart vane-research-gateway.socket vane-research-gateway.service vane.service || true
  fi
  rm -rf -- "$backup"
  exit "$status"
}
trap cleanup EXIT

systemctl stop vane.service vane-research-gateway.service vane-research-gateway.socket
next=$release_root/.rollback-$target_sha.$$
ln -s "$target" "$next"
mv -Tf "$next" "$current"
switched=true
systemctl start vane-research-gateway.socket vane-research-gateway.service vane.service
for _ in {1..90}; do
  curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8080/readyz >/dev/null && break
  sleep 2
done
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8080/readyz >/dev/null
env -i PATH=/usr/bin:/bin CREDENTIALS_DIRECTORY=/etc/vane/credentials \
  "$current/bin/gate" -env /opt/vane/env/server-owner-compat.env
pid=$(systemctl show vane.service --property=MainPID --value)
[[ $pid =~ ^[1-9][0-9]*$ && $(readlink /proc/"$pid"/exe) == "$target/bin/vane" ]] || {
  echo "rollback process is not bound to the target release" >&2; exit 1;
}
switched=false
rm -rf -- "$backup"
trap - EXIT
echo "server rollback activated: $target_sha"
