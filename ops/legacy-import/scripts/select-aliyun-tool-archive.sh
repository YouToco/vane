#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <aliyun|ossutil> <machine-architecture>" >&2
  exit 64
fi

tool=$1
machine=$2
case "$machine" in
  x86_64 | amd64)
    architecture=amd64
    ;;
  aarch64 | arm64)
    architecture=arm64
    ;;
  *)
    echo "unsupported Linux machine architecture: $machine" >&2
    exit 65
    ;;
esac

case "$tool:$architecture" in
  aliyun:amd64)
    archive_name=aliyun-cli-linux-3.4.10-amd64.tgz
    archive_sha256=b9edbcc21236f14bfeebbd5e272dde6f36fd946af5802fa677475ff69839ed84
    ;;
  aliyun:arm64)
    archive_name=aliyun-cli-linux-3.4.10-arm64.tgz
    archive_sha256=349f3d31af9cc85aa2b444899e7d805f6409f5a53d667ce74d00dafbc17f9ae5
    ;;
  ossutil:amd64)
    archive_name=ossutil-2.3.0-linux-amd64.zip
    archive_sha256=3ae4d9fc85a7a6e9f5654d1599766f1a3a42a3692870887b5ae9338d582ef65a
    ;;
  ossutil:arm64)
    archive_name=ossutil-2.3.0-linux-arm64.zip
    archive_sha256=f6c95ba0c2d2ef30290af686ce4d706c701f4734ce8090bee4288a77e3f1d764
    ;;
  *)
    echo "unsupported pinned Aliyun tool: $tool" >&2
    exit 66
    ;;
esac

printf '%s\t%s\n' "$archive_name" "$archive_sha256"
