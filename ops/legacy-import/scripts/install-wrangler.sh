#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <empty-install-directory>" >&2
  exit 64
fi

install_dir=$1
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
lock_source=$script_dir/../tools/wrangler
wrangler_version=4.115.0

[[ $install_dir == "$RUNNER_TEMP/"* ]] || {
  echo "Wrangler install directory must be below RUNNER_TEMP" >&2
  exit 65
}
[[ ! -e $install_dir ]] || {
  echo "Wrangler install directory already exists" >&2
  exit 66
}

node_bin=$(command -v node)
npm_bin=$(command -v npm)
[[ -x $node_bin && -x $npm_bin ]]
[[ $("$node_bin" --version) == "v22.23.1" ]] || {
  echo "unexpected deployment Node version" >&2
  exit 67
}

install -d -m 0700 "$install_dir"
install -m 0600 "$lock_source/package.json" "$install_dir/package.json"
install -m 0600 \
  "$lock_source/package-lock.json" "$install_dir/package-lock.json"

(
  cd "$install_dir"
  npm_config_cache="$install_dir/.npm-cache" \
    "$npm_bin" ci --ignore-scripts --no-audit --no-fund
)
rm -rf -- "$install_dir/.npm-cache"

wrangler_entry=$install_dir/node_modules/wrangler/bin/wrangler.js
[[ -f $wrangler_entry ]]
[[ $("$node_bin" "$wrangler_entry" --version | tail -n 1) == \
  "$wrangler_version" ]]
"$node_bin" "$wrangler_entry" pages deploy --help >/dev/null

wrapper=$install_dir/wrangler
printf '%s\n' \
  '#!/bin/sh' \
  "exec '$node_bin' '$wrangler_entry' \"\$@\"" \
  >"$wrapper"
chmod 0700 "$wrapper"
[[ $("$wrapper" --version | tail -n 1) == "$wrangler_version" ]]
