#!/usr/bin/env bash
set -euo pipefail
umask 077

broker=$(/usr/bin/sudo --non-interactive -- /usr/local/libexec/vane-broker-promote)
[[ $broker =~ ^/opt/vane-control/releases/[0-9a-f]{40}/ops/broker/forced_command.py$ && \
    -f $broker && ! -L $broker && -x $broker ]] || {
  echo "broker refusal: active root-owned controller is unavailable" >&2
  exit 78
}
exec "$broker"
