#!/usr/bin/env bash
set -euo pipefail
umask 077

version=3.4.8
archive_name=aliyun-cli-linux-3.4.8-arm64.tgz
archive_sha256=a8b22c72c1984e0ef4db441ab1e7f1720a03553fae79cff9fb113806229e7876
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
