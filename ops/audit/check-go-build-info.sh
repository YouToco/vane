#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BINARY EXACT_SHA" >&2
  exit 2
fi

binary=$1
source_sha=$2
[[ -x $binary ]] || {
  echo "backend binary is missing or not executable: $binary" >&2
  exit 1
}
[[ $source_sha =~ ^[0-9a-f]{40}$ ]] || {
  echo "invalid exact source SHA: $source_sha" >&2
  exit 1
}
command -v strings >/dev/null

expected="vane/$source_sha/clean"
check_status=0
strings "$binary" | awk -v expected="$expected" '$0 == expected { found = 1 } END { if (!found) exit 10 }' || check_status=$?

case "$check_status" in
  0) ;;
  10)
    echo "backend binary has the wrong or missing release build ID: $binary" >&2
    exit 1
    ;;
  *)
    echo "unable to inspect backend binary build info: $binary" >&2
    exit 1
    ;;
esac
