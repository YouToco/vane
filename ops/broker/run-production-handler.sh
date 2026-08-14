#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $# -eq 6 ]] || {
  echo "usage: $0 VERB REQUEST_ROOT VALIDATED_ROOT STATE_ROOT REPO EXPECTED_DIGEST" >&2
  exit 2
}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
request_id=$(basename -- "$2")
[[ $request_id =~ ^[0-9a-f]{64}$ ]] || {
  echo "production handler request ID is invalid" >&2
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
  --property=LoadCredential=uat_session_cookie:/etc/vane/credentials/uat_session_cookie \
  --property=LoadCredential=broker_signing_key:/etc/vane-broker/credentials/broker_signing_key \
  "$script_dir/production_handler.py" "$@"
