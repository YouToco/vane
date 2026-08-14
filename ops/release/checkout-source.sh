#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 3 ]]; then
  echo "usage: $0 OWNER/REPOSITORY EXACT_SHA DESTINATION" >&2
  exit 2
fi

repository=$1
expected_sha=$2
destination=$3

[[ $repository =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || {
  echo "invalid GitHub repository: $repository" >&2
  exit 1
}
[[ $expected_sha =~ ^[0-9a-f]{40}$ ]] || {
  echo "invalid exact source SHA: $expected_sha" >&2
  exit 1
}
[[ -n ${SOURCE_READ_KEY:-} ]] || {
  echo "SOURCE_READ_KEY is required" >&2
  exit 1
}
[[ ! -e $destination ]] || {
  echo "checkout destination already exists: $destination" >&2
  exit 1
}

checkout_secret_dir=$(mktemp -d "$RUNNER_TEMP/source-checkout.XXXXXX")
cleanup() {
  local status=$?
  trap - EXIT
  rm -rf -- "$checkout_secret_dir"
  exit "$status"
}
trap cleanup EXIT

printf '%s\n' "$SOURCE_READ_KEY" >"$checkout_secret_dir/id"
chmod 600 "$checkout_secret_dir/id"

ssh-keyscan -t ed25519 github.com 2>/dev/null |
  awk '$1 == "github.com" && $2 == "ssh-ed25519" && NF == 3' |
  sort -u >"$checkout_secret_dir/known_hosts"
[[ $(wc -l <"$checkout_secret_dir/known_hosts") -eq 1 ]] || {
  echo "expected exactly one GitHub Ed25519 host key" >&2
  exit 1
}
github_fingerprint=$(
  ssh-keygen -lf "$checkout_secret_dir/known_hosts" -E sha256 |
    awk 'NR == 1 { print $2 }'
)
if [[ $github_fingerprint != \
  "SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU" ]]; then
  echo "GitHub Ed25519 host fingerprint mismatch" >&2
  exit 1
fi

git init -q "$destination"
git -C "$destination" remote add origin "git@github.com:$repository.git"
ssh_command=$(
  printf "ssh -i '%s' -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile='%s'" \
    "$checkout_secret_dir/id" "$checkout_secret_dir/known_hosts"
)
GIT_SSH_COMMAND=$ssh_command \
  git -C "$destination" fetch --quiet --depth 1 origin "$expected_sha"
actual_sha=$(git -C "$destination" rev-parse FETCH_HEAD)
[[ $actual_sha == "$expected_sha" ]] || {
  echo "fetched SHA does not match plan: expected=$expected_sha actual=$actual_sha" >&2
  exit 1
}
git -C "$destination" checkout --quiet --detach "$expected_sha"
git -C "$destination" remote remove origin

[[ -z $(git -C "$destination" status --porcelain --untracked-files=all) ]] || {
  echo "exact-SHA checkout is not clean" >&2
  exit 1
}

# The EXIT trap deletes the key and known_hosts before the next workflow step,
# where source-controlled commands are allowed to execute.
