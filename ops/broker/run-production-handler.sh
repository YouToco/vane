#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $# -eq 6 ]] || {
  echo "usage: $0 VERB REQUEST_ROOT VALIDATED_ROOT STATE_ROOT REPO EXPECTED_DIGEST" >&2
  exit 2
}
verb=$1
request_root=$2
validated_root=$3
state_root=$4
repo_root=$5
expected_digest=$6
[[ $verb == release || $verb == retry ]] || {
  echo "production handler verb is invalid" >&2
  exit 78
}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
request_id=$(basename -- "$request_root")
[[ $request_id =~ ^[0-9a-f]{64}$ ]] || {
  echo "production handler request ID is invalid" >&2
  exit 78
}
[[ $request_root == "/var/lib/vane-broker/requests/$request_id" ]] || {
  echo "production handler request root is not canonical" >&2
  exit 78
}
[[ $validated_root =~ ^/var/lib/vane-broker/state/broker-work/inflight/${request_id}\.[A-Za-z0-9._-]+$ ]] || {
  echo "production handler validation root is not canonical" >&2
  exit 78
}
[[ $state_root == /var/lib/vane-broker/state ]] || {
  echo "production handler state root is not canonical" >&2
  exit 78
}
[[ $repo_root == /opt/vane-control/current ]] || {
  echo "production handler controller root is not canonical" >&2
  exit 78
}
[[ $expected_digest =~ ^[0-9a-f]{64}$ ]] || {
  echo "production handler current digest is invalid" >&2
  exit 78
}
[[ $(id -u) -eq 0 ]] || {
  echo "production handler launcher requires root" >&2
  exit 78
}

exec systemd-run \
  --quiet --wait --pipe --collect --service-type=exec \
  --unit="vane-broker-handler-${request_id}" \
  --property=Type=exec \
  --property=User=root \
  --property=Group=root \
  --property=UMask=0077 \
  --property=NoNewPrivileges=yes \
  --property=ProtectSystem=strict \
  --property=ProtectHome=yes \
  --property=PrivateTmp=yes \
  --property=PrivateDevices=yes \
  --property=ProtectProc=invisible \
  --property=RestrictSUIDSGID=yes \
  --property=LockPersonality=yes \
  --property=ReadWritePaths=/opt/vane \
  --property=ReadWritePaths=/opt/vane-control \
  --property=ReadWritePaths=/var/lib/vane-broker \
  --property=ReadWritePaths=/etc/systemd/system \
  --property=LoadCredential=broker_signing_key:/etc/vane-broker/credentials/broker_signing_key \
  "$script_dir/production_handler.py" \
  "$verb" "$request_root" "$validated_root" "$state_root" "$repo_root" "$expected_digest"
