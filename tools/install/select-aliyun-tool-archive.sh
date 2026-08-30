#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <aliyun|ossutil> <machine-architecture> <system>" >&2
  exit 64
fi

tool=$1
machine=$2
system=$3
case "$machine" in
  x86_64 | amd64)
    architecture=amd64
    ;;
  aarch64 | arm64)
    architecture=arm64
    ;;
  *)
    echo "unsupported machine architecture: $machine" >&2
    exit 65
    ;;
esac

case "$system" in
  Linux) platform=linux ;;
  Darwin) platform=darwin ;;
  *) echo "unsupported operating system: $system" >&2; exit 65 ;;
esac

case "$tool:$platform:$architecture" in
  aliyun:linux:amd64)
    archive_name=aliyun-cli-linux-3.4.10-amd64.tgz
    archive_sha256=b9edbcc21236f14bfeebbd5e272dde6f36fd946af5802fa677475ff69839ed84
    ;;
  aliyun:linux:arm64)
    archive_name=aliyun-cli-linux-3.4.10-arm64.tgz
    archive_sha256=349f3d31af9cc85aa2b444899e7d805f6409f5a53d667ce74d00dafbc17f9ae5
    ;;
  aliyun:darwin:arm64)
    archive_name=aliyun-cli-macosx-3.4.10-arm64.tgz
    archive_sha256=159787b71bf9dd8efbd110ca2c209f49154a0323b535a215bb471625d58a50aa
    ;;
  ossutil:linux:amd64)
    archive_name=ossutil-2.3.0-linux-amd64.zip
    archive_sha256=3ae4d9fc85a7a6e9f5654d1599766f1a3a42a3692870887b5ae9338d582ef65a
    ;;
  ossutil:linux:arm64)
    archive_name=ossutil-2.3.0-linux-arm64.zip
    archive_sha256=f6c95ba0c2d2ef30290af686ce4d706c701f4734ce8090bee4288a77e3f1d764
    ;;
  ossutil:darwin:arm64)
    archive_name=ossutil-2.3.0-mac-arm64.zip
    archive_sha256=058fd048f321f8c80def8b748030531646eefe3a82837bf16b581ba7d9c84ac7
    ;;
  *)
    echo "unsupported pinned Aliyun tool: $tool" >&2
    exit 66
    ;;
esac

printf '%s\t%s\n' "$archive_name" "$archive_sha256"
