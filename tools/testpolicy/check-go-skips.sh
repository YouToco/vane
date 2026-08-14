#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo_root/server"
exec go test ./internal/testgate -run 'TestRepositoryHasNoDirectTestingSkips|TestSkipAllowlistIsValid' -count=1
