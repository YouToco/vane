#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 3 ]]; then
  echo "usage: $0 RELEASE_ROOT COLLECTOR RECEIPT" >&2
  exit 2
fi

release_root=$1
collector=$2
receipt=$3
[[ $release_root == /* && $release_root != / &&
   -f $collector && ! -L $collector && -x $collector &&
   -f $receipt && ! -L $receipt ]] || {
  echo "retention release publication input is unsafe" >&2
  exit 1
}

trusted_uid=$(id -u)
trusted_file() {
  local path=$1 mode=$2 actual_uid
  [[ -f $path && ! -L $path && $(stat -c '%a' "$path") == "$mode" ]] || return 1
  actual_uid=$(stat -c '%u' "$path")
  [[ $actual_uid == 0 || $actual_uid == "$trusted_uid" ]]
}
trusted_directory() {
  local path=$1 mode=$2 actual_uid
  [[ -d $path && ! -L $path && $(stat -c '%a' "$path") == "$mode" ]] || return 1
  actual_uid=$(stat -c '%u' "$path")
  [[ $actual_uid == 0 || $actual_uid == "$trusted_uid" ]]
}

if [[ -e $release_root || -L $release_root ]]; then
  trusted_directory "$release_root" 755 || {
    echo "versioned collector release root is unsafe" >&2
    exit 1
  }
else
  install -d -m 0755 "$release_root"
fi

receipt_digest=$(sha256sum "$receipt" | awk '{print $1}')
[[ $receipt_digest =~ ^[0-9a-f]{64}$ ]] || exit 1
release_dir=$release_root/$receipt_digest

validate_release() {
  trusted_directory "$release_dir" 755 &&
    trusted_file "$release_dir/agentfirstretention" 755 &&
    trusted_file "$release_dir/release-receipt.json" 644 &&
    cmp -s -- "$collector" "$release_dir/agentfirstretention" &&
    cmp -s -- "$receipt" "$release_dir/release-receipt.json" &&
    [[ $(sha256sum "$release_dir/release-receipt.json" | awk '{print $1}') == \
       "$receipt_digest" ]]
}

if [[ -e $release_dir || -L $release_dir ]]; then
  validate_release || {
    echo "existing versioned collector release differs" >&2
    exit 1
  }
  printf '%s\n' "$release_dir"
  exit 0
fi

pending=$(mktemp -d "$release_root/.${receipt_digest}.XXXXXX")
cleanup() {
  local status=$?
  trap - EXIT
  rm -rf -- "$pending"
  exit "$status"
}
trap cleanup EXIT
chmod 0755 "$pending"
install -m 0755 "$collector" "$pending/agentfirstretention"
install -m 0644 "$receipt" "$pending/release-receipt.json"

if ! mv -T -- "$pending" "$release_dir"; then
  validate_release || {
    echo "concurrent versioned collector release differs" >&2
    exit 1
  }
fi
pending=
trap - EXIT
validate_release || {
  echo "published versioned collector release is unsafe" >&2
  exit 1
}
printf '%s\n' "$release_dir"
