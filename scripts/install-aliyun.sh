#!/usr/bin/env bash
set -euo pipefail
umask 077

version=3.4.10
archive_name=aliyun-cli-linux-3.4.10-arm64.tgz
archive_sha256=349f3d31af9cc85aa2b444899e7d805f6409f5a53d667ce74d00dafbc17f9ae5
archive_url="https://github.com/aliyun/aliyun-cli/releases/download/v${version}/${archive_name}"
install_dir="$RUNNER_TEMP/aliyun-$version"

mkdir -p "$install_dir"
archive=$(mktemp "$RUNNER_TEMP/aliyun-cli.XXXXXX.tgz")
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
