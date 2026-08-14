#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $(id -u) -eq 0 ]] || { echo "broker install requires root" >&2; exit 1; }
[[ $# -eq 1 && $1 == /* ]] || { echo "usage: $0 ABSOLUTE_INSTALL_ROOT" >&2; exit 2; }
install_root=$1
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
broker_dir=$(cd -- "$script_dir/../broker" && pwd -P)
install -d -o root -g root -m 0755 "$install_root/libexec"
install -d -o root -g root -m 0700 "$install_root/state" "$install_root/requests"
install -o root -g root -m 0755 "$broker_dir/forced_command.py" "$install_root/libexec/vane-broker"
install -o root -g root -m 0644 "$broker_dir/controller.py" "$install_root/libexec/controller.py"
echo "broker installed without production handlers; mutation remains fail-closed"
