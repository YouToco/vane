#!/usr/bin/env bash
set -euo pipefail
umask 077

version=3.4.10
[[ -n ${VANE_TOOL_CACHE:-} && $VANE_TOOL_CACHE == /* &&
   -d $VANE_TOOL_CACHE && ! -L $VANE_TOOL_CACHE ]] || {
  echo "VANE_TOOL_CACHE must be an existing absolute directory" >&2
  exit 1
}
[[ -n ${VANE_WORK_ROOT:-} && $VANE_WORK_ROOT == /* &&
   -d $VANE_WORK_ROOT && ! -L $VANE_WORK_ROOT ]] || {
  echo "VANE_WORK_ROOT must be an existing absolute directory" >&2
  exit 1
}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
selection=$(
  "$script_dir/select-aliyun-tool-archive.sh" aliyun "$(uname -m)"
)
IFS=$'\t' read -r archive_name archive_sha256 <<<"$selection"
[[ -n "$archive_name" && "$archive_sha256" =~ ^[0-9a-f]{64}$ ]]
archive_url="https://github.com/aliyun/aliyun-cli/releases/download/v${version}/${archive_name}"
install_dir="$VANE_TOOL_CACHE/aliyun_cli/$version"

mkdir -p "$install_dir"
archive=$(mktemp "$VANE_WORK_ROOT/aliyun-cli.XXXXXX.tgz")
trap 'rm -f "$archive"' EXIT

curl --fail --silent --show-error --location \
  --proto '=https' --tlsv1.2 \
  --output "$archive" "$archive_url"
printf '%s  %s\n' "$archive_sha256" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$install_dir"
chmod 755 "$install_dir/aliyun"

actual_version=$("$install_dir/aliyun" version)
if [[ $actual_version != "$version" ]]; then
  echo "unexpected Aliyun CLI version: $actual_version" >&2
  exit 1
fi
