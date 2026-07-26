#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BACKEND_REPOSITORY FRONTEND_REPOSITORY" >&2
  exit 2
fi

backend_repo=$1
frontend_repo=$2
state_root=${XDG_STATE_HOME:-"$HOME/.local/state"}
state_dir=$state_root/vane-deploy

mkdir -p "$state_dir"
chmod 700 "$state_dir"

read_sha() {
  local repo=$1
  local sha
  sha=$(git -C "$repo" rev-parse HEAD)
  if [[ ! $sha =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid checked-out commit for $repo: $sha" >&2
    exit 1
  fi
  printf '%s' "$sha"
}

is_changed() {
  local state_file=$1
  local wanted_sha=$2
  local deployed_sha=
  if [[ -f $state_file ]]; then
    deployed_sha=$(<"$state_file")
  fi
  [[ $deployed_sha != "$wanted_sha" ]]
}

backend_sha=$(read_sha "$backend_repo")
frontend_sha=$(read_sha "$frontend_repo")
backend_changed=false
frontend_changed=false

if is_changed "$state_dir/deployed-vane.sha" "$backend_sha"; then
  backend_changed=true
fi
if is_changed "$state_dir/deployed-vane-web.sha" "$frontend_sha"; then
  frontend_changed=true
fi

any_changed=false
if [[ $backend_changed == true || $frontend_changed == true ]]; then
  any_changed=true
fi

{
  echo "backend_sha=$backend_sha"
  echo "frontend_sha=$frontend_sha"
  echo "backend_changed=$backend_changed"
  echo "frontend_changed=$frontend_changed"
  echo "any_changed=$any_changed"
} >>"$GITHUB_OUTPUT"

echo "vane:     $backend_sha (changed=$backend_changed)"
echo "vane-web: $frontend_sha (changed=$frontend_changed)"
