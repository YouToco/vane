#!/usr/bin/env bash
set -euo pipefail
umask 022

if [[ $EUID -ne 0 ]]; then
  echo "run this provisioning script as root, outside GitHub Actions" >&2
  exit 1
fi

runner_user=${VANE_DEPLOY_RUNNER_USER:-vane-deploy-runner}
install_root=/opt/vane-deploy-tools
wrangler_version=4.115.0
node_version=22.23.1
node_archive_name=node-v${node_version}-linux-arm64.tar.xz
node_archive_sha256=0294e8b915ab75f92c7513d2fcb830ae06e10684e6c603e99a87dbf8835389c1
node_archive_url="https://nodejs.org/dist/v${node_version}/${node_archive_name}"
node_target=$install_root/node-v${node_version}-linux-arm64
wrangler_target=$install_root/wrangler-$wrangler_version
wrapper=$install_root/wrangler
script_dir=$(cd "$(dirname "$0")" && pwd)
lock_source=$script_dir/../tools/wrangler

id "$runner_user" >/dev/null
command -v curl >/dev/null
command -v sha256sum >/dev/null
command -v tar >/dev/null
command -v runuser >/dev/null
[[ -f $lock_source/package.json && -f $lock_source/package-lock.json ]]

mkdir -p "$install_root"
chmod 755 "$install_root"
[[ ! -e $wrangler_target && ! -e $wrapper ]] || {
  echo "Wrangler target or wrapper already exists; refusing overwrite" >&2
  exit 1
}

node_staging=
wrangler_staging=
node_archive=
cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n $node_staging && -d $node_staging ]]; then
    rm -rf -- "$node_staging"
  fi
  if [[ -n $wrangler_staging && -d $wrangler_staging ]]; then
    rm -rf -- "$wrangler_staging"
  fi
  if [[ -n $node_archive && -f $node_archive ]]; then
    rm -f -- "$node_archive"
  fi
  exit "$status"
}
trap cleanup EXIT

if [[ -e $node_target ]]; then
  [[ -f $node_target/.archive-sha256 ]]
  [[ $(tr -d '\r\n' <"$node_target/.archive-sha256") == \
    "$node_archive_sha256  $node_archive_name" ]]
  [[ $("$node_target/bin/node" --version) == "v$node_version" ]]
else
  node_staging=$(mktemp -d "$install_root/.node-$node_version.XXXXXX")
  node_archive=$(mktemp "$install_root/.node-archive.XXXXXX")
  curl --fail --silent --show-error --location \
    --proto '=https' --tlsv1.2 \
    --output "$node_archive" "$node_archive_url"
  printf '%s  %s\n' "$node_archive_sha256" "$node_archive" |
    sha256sum --check --status

  archive_prefix=
  while IFS= read -r member; do
    [[ $member != /* && $member != *"/../"* && $member != */.. ]]
    [[ $member != ../* && $member != *\\* ]]
    member_prefix=${member%%/*}
    [[ -n $member_prefix ]]
    if [[ -z $archive_prefix ]]; then
      archive_prefix=$member_prefix
    fi
    [[ $member_prefix == "$archive_prefix" ]]
  done < <(tar -tJf "$node_archive")
  [[ $archive_prefix == "node-v${node_version}-linux-arm64" ]]

  tar -xJf "$node_archive" -C "$node_staging" --strip-components=1
  printf '%s  %s\n' "$node_archive_sha256" "$node_archive_name" \
    >"$node_staging/.archive-sha256"
  [[ $("$node_staging/bin/node" --version) == "v$node_version" ]]
  chown -R root:root "$node_staging"
  chmod -R go-w "$node_staging"
  chmod 755 "$node_staging"
  mv "$node_staging" "$node_target"
  node_staging=
  rm -f -- "$node_archive"
  node_archive=
fi
chmod 755 "$node_target"
if find "$node_target" \
  \( ! -user root -o \( ! -type l -a -perm /022 \) \) \
  -print -quit |
  grep -q .; then
  echo "pinned Node tree is not root-owned/read-only" >&2
  exit 1
fi

node_bin=$node_target/bin/node
npm_cli=$node_target/lib/node_modules/npm/bin/npm-cli.js
[[ -x $node_bin && -f $npm_cli ]]

wrangler_staging=$(mktemp -d "$install_root/.wrangler-$wrangler_version.XXXXXX")
install -m 0644 "$lock_source/package.json" "$wrangler_staging/package.json"
install -m 0644 \
  "$lock_source/package-lock.json" "$wrangler_staging/package-lock.json"
chown -R "$runner_user" "$wrangler_staging"

# npm itself is executed by the pinned Node binary. Dependency lifecycle scripts
# are disabled; Wrangler's version and Pages CLI are verified before promotion.
# shellcheck disable=SC2016 # Positional args expand in the child bash.
runuser -u "$runner_user" -- \
  env \
    "PATH=$node_target/bin:/usr/bin:/bin" \
    "npm_config_cache=$wrangler_staging/.npm-cache" \
    bash -c \
      'cd "$1" && exec "$2" "$3" ci --ignore-scripts --no-audit --no-fund' \
      bash "$wrangler_staging" "$node_bin" "$npm_cli"
rm -rf -- "$wrangler_staging/.npm-cache"

wrangler_entry=$wrangler_staging/node_modules/wrangler/bin/wrangler.js
[[ -f $wrangler_entry ]]
[[ $("$node_bin" "$wrangler_entry" --version | tail -n 1) == \
  "$wrangler_version" ]]
"$node_bin" "$wrangler_entry" pages deploy --help >/dev/null

chown -R root:root "$wrangler_staging"
chmod -R go-w "$wrangler_staging"
chmod 755 "$wrangler_staging"
mv "$wrangler_staging" "$wrangler_target"
wrangler_staging=
if find "$wrangler_target" \
  \( ! -user root -o \( ! -type l -a -perm /022 \) \) \
  -print -quit |
  grep -q .; then
  echo "Wrangler tree is not root-owned/read-only" >&2
  exit 1
fi

wrapper_temp=$install_root/.wrangler-wrapper-$wrangler_version
printf '%s\n' \
  '#!/bin/sh' \
  "exec '$node_bin' '$wrangler_target/node_modules/wrangler/bin/wrangler.js' \"\$@\"" \
  >"$wrapper_temp"
chown root:root "$wrapper_temp"
chmod 0755 "$wrapper_temp"
mv "$wrapper_temp" "$wrapper"

runner_wrangler_version=$(
  runuser -u "$runner_user" -- \
    bash -c 'cd / && exec "$1" --version' bash "$wrapper" |
    tail -n 1
)
[[ $runner_wrangler_version == "$wrangler_version" ]]
runuser -u "$runner_user" -- \
  bash -c 'cd / && exec "$1" pages deploy --help >/dev/null' bash "$wrapper"
[[ $(stat -c '%U:%G:%a' "$wrapper") == root:root:755 ]]

trap - EXIT
echo "installed pinned Node v$node_version and root-owned Wrangler $wrangler_version"
