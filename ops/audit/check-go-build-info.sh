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

# GNU strings keeps Go's "build<TAB>" prefix on module build-info records,
# while BSD strings may expose only the setting. Accept those two exact forms,
# consume the full stream, and inspect each potentially large binary once.
check_status=0
strings "$binary" |
  awk \
    -v revision="vcs.revision=$source_sha" \
    -v clean="vcs.modified=false" \
    -v modified="vcs.modified=true" '
      $0 == revision || $0 == "build\t" revision { has_revision = 1 }
      $0 == clean || $0 == "build\t" clean { has_clean = 1 }
      $0 == modified || $0 == "build\t" modified { has_modified = 1 }
      END {
        if (has_modified) exit 12
        if (!has_revision) exit 10
        if (!has_clean) exit 11
      }
    ' || check_status=$?

case "$check_status" in
  0) ;;
  10)
    echo "backend binary has the wrong VCS revision: $binary" >&2
    exit 1
    ;;
  11)
    echo "backend binary is missing the clean-worktree marker: $binary" >&2
    exit 1
    ;;
  12)
    echo "backend binary was built from a modified worktree: $binary" >&2
    exit 1
    ;;
  *)
    echo "unable to inspect backend binary build info: $binary" >&2
    exit 1
    ;;
esac
