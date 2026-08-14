#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $(id -u) -eq 0 ]] || {
  echo "broker client install requires root" >&2
  exit 1
}
[[ $# -eq 4 ]] || {
  echo "usage: $0 ABSOLUTE_TRANSPORT_PRIVATE_KEY BROKER_HOST BROKER_PORT ABSOLUTE_KNOWN_HOSTS" >&2
  exit 2
}
private_key=$1
broker_host=$2
broker_port=$3
known_hosts=$4

for path in "$private_key" "$known_hosts"; do
  [[ $path == /* && -f $path && ! -L $path ]] || {
    echo "broker client input is not a safe absolute regular file: $path" >&2
    exit 1
  }
done
[[ $broker_host =~ ^[A-Za-z0-9.-]+$ && $broker_host != .* && $broker_host != *. ]] || {
  echo "broker host is invalid" >&2
  exit 1
}
[[ $broker_port =~ ^[1-9][0-9]{0,4}$ ]] || {
  echo "broker port is invalid" >&2
  exit 1
}
(( broker_port <= 65535 )) || {
  echo "broker port is outside the TCP range" >&2
  exit 1
}
private_mode=$(stat -f '%Lp' "$private_key" 2>/dev/null || stat -c '%a' "$private_key")
[[ $private_mode =~ ^[0-7]00$ ]] || {
  echo "broker transport private key must not be group/world accessible" >&2
  exit 1
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
submitter=$(cd -- "$script_dir/../broker" && pwd -P)/submit.py
[[ -f $submitter && ! -L $submitter ]] || {
  echo "broker submitter source is unavailable" >&2
  exit 1
}
root_group=$(id -gn 0)
install -d -o root -g "$root_group" -m 0755 /usr/local/libexec
install -o root -g "$root_group" -m 0755 \
  "$submitter" /usr/local/libexec/vane-broker-submit
install -d -o root -g "$root_group" -m 0755 /etc/vane-broker
config=$(mktemp /etc/vane-broker/.client.XXXXXX)
python3 - "$private_key" "$broker_host" "$broker_port" "$known_hosts" >"$config" <<'PY'
import json
import sys

private_key, host, port, known_hosts = sys.argv[1:]
value = {
    "schema": "vane.broker-client/v1",
    "ssh_command": [
        "/usr/bin/ssh",
        "-T",
        "-i", private_key,
        "-p", port,
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes",
        "-o", f"UserKnownHostsFile={known_hosts}",
        f"vane-broker@{host}",
    ],
}
print(json.dumps(value, sort_keys=True, separators=(",", ":")))
PY
chown root:"$root_group" "$config"
chmod 0644 "$config"
mv -f "$config" /etc/vane-broker/client.json

echo "root-owned local broker client installed"
