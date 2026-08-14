#!/usr/bin/env bash
set -euo pipefail
umask 077

broker=/opt/vane-control/current/ops/broker/forced_command.py
[[ -f $broker && ! -L $broker && -x $broker ]] || {
  echo "broker refusal: active root-owned controller is unavailable" >&2
  exit 78
}
exec "$broker"
