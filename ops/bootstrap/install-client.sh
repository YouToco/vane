#!/usr/bin/env bash
set -euo pipefail
umask 077

[[ $(id -u) -ne 0 ]] || {
  echo "broker client must be installed by the release user, not root" >&2
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
policy=$(cd -- "$script_dir/../policy" && pwd -P)/release-policy.json
[[ -f $submitter && ! -L $submitter ]] || {
  echo "broker submitter source is unavailable" >&2
  exit 1
}
python3 - "$policy" "$broker_host" "$broker_port" "$known_hosts" <<'PY'
import hashlib
import json
import sys

policy_path, host, port, known_hosts = sys.argv[1:]
with open(policy_path, encoding="utf-8") as handle:
    policy = json.load(handle)
endpoint = policy.get("broker_endpoint")
if not isinstance(endpoint, dict):
    raise SystemExit("release policy lacks broker endpoint")
with open(known_hosts, "rb") as handle:
    known_hosts_sha256 = hashlib.sha256(handle.read()).hexdigest()
if endpoint != {
    "host": host,
    "port": int(port),
    "known_hosts_sha256": known_hosts_sha256,
}:
    raise SystemExit("broker endpoint differs from exact release policy")
PY

user_home=$(python3 - <<'PY'
import os
import pwd

print(pwd.getpwuid(os.getuid()).pw_dir)
PY
)
[[ $user_home == /* && -d $user_home && ! -L $user_home ]] || {
  echo "release user home is unavailable or unsafe" >&2
  exit 1
}
local_root=$user_home/.local
executable_dir=$local_root/libexec
config_root=$user_home/.config
config_dir=$config_root/vane
for path in "$local_root" "$executable_dir" "$config_root" "$config_dir"; do
  [[ ! -L $path ]] || {
    echo "broker client install directory must not be a symlink: $path" >&2
    exit 1
  }
done
install -d -m 0700 "$executable_dir" "$config_dir"
chmod 0700 "$executable_dir" "$config_dir"

installed_submitter=$executable_dir/vane-broker-submit
submitter_temp=$(mktemp "$executable_dir/.vane-broker-submit.XXXXXX")
install -m 0700 "$submitter" "$submitter_temp"
mv -f "$submitter_temp" "$installed_submitter"

config_temp=$(mktemp "$config_dir/.broker-client.XXXXXX")
python3 - "$private_key" "$broker_host" "$broker_port" "$known_hosts" >"$config_temp" <<'PY'
import json
import sys

private_key, host, port, known_hosts = sys.argv[1:]
value = {
    "schema": "vane.broker-client/v1",
    "host": host,
    "port": int(port),
    "identity_file": private_key,
    "known_hosts_file": known_hosts,
}
print(json.dumps(value, sort_keys=True, separators=(",", ":")))
PY
chmod 0600 "$config_temp"
mv -f "$config_temp" "$config_dir/broker-client.json"

echo "user-scoped broker client installed: $installed_submitter"
