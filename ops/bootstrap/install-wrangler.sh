#!/usr/bin/env bash
set -euo pipefail
umask 077

version=4.115.0
node_version=22.23.2
package_lock_sha256=50652df66656b7bba737605b76f474463d884da4f162f49a0975a9c17aea776d

[[ -n ${VANE_TOOL_CACHE:-} && $VANE_TOOL_CACHE == /* &&
   -d $VANE_TOOL_CACHE && ! -L $VANE_TOOL_CACHE ]] || {
  echo "VANE_TOOL_CACHE must be an existing absolute directory" >&2
  exit 1
}
[[ -n ${VANE_WORK_ROOT:-} && $VANE_WORK_ROOT == /* &&
   -d $VANE_WORK_ROOT && ! -L $VANE_WORK_ROOT ]] || {
  echo "VANE_WORK_ROOT must be an existing absolute directory" >&2
  exit 1
}

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
node_root="$VANE_TOOL_CACHE/node/$node_version"
node="$node_root/bin/node"
npm_cli="$node_root/lib/node_modules/npm/bin/npm-cli.js"
source_dir="$repo_root/ops/release/wrangler"
lock_file="$source_dir/package-lock.json"
install_parent="$VANE_TOOL_CACHE/wrangler"
install_dir="$install_parent/$version"

validate_install() {
  local root=$1
  local expected="$root/node_modules/wrangler/bin/wrangler.js"
  local link="$root/node_modules/.bin/wrangler"
  [[ -d $root && ! -L $root && -L $link && -f $expected && ! -L $expected ]] || return 1
  "$node" -e '
    const fs = require("fs");
    const path = require("path");
    const root = fs.realpathSync(process.argv[1]);
    const link = fs.realpathSync(process.argv[2]);
    const expected = fs.realpathSync(process.argv[3]);
    if (link !== expected || !expected.startsWith(root + path.sep) ||
        !fs.statSync(expected).isFile()) process.exit(1);
  ' "$root" "$link" "$expected" >/dev/null 2>&1 || return 1
  [[ $("$node" "$expected" --version) == "$version" ]]
}

[[ -x $node && -f $npm_cli && ! -L $node && ! -L $npm_cli ]] || {
  echo "locked Node $node_version is unavailable for Wrangler installation" >&2
  exit 1
}
[[ $($node --version) == "v$node_version" ]] || {
  echo "locked Node version differs from $node_version" >&2
  exit 1
}

actual_lock_sha256=$(
  "$node" -e 'const fs=require("fs"),crypto=require("crypto"); process.stdout.write(crypto.createHash("sha256").update(fs.readFileSync(process.argv[1])).digest("hex"))' "$lock_file"
)
[[ $actual_lock_sha256 == "$package_lock_sha256" ]] || {
  echo "Wrangler package-lock SHA-256 mismatch" >&2
  exit 1
}

if [[ -e $install_dir ]]; then
  validate_install "$install_dir" || {
    echo "existing Wrangler installation is unsafe or has the wrong version" >&2
    exit 1
  }
  exit 0
fi

if [[ -e $install_parent || -L $install_parent ]]; then
  [[ -d $install_parent && ! -L $install_parent ]] || {
    echo "Wrangler install parent is unsafe" >&2
    exit 1
  }
else
  mkdir "$install_parent"
fi
temporary=$(mktemp -d "$VANE_WORK_ROOT/wrangler.XXXXXX")
trap 'rm -rf -- "$temporary"' EXIT
mkdir "$temporary/package"
cp "$source_dir/package.json" "$lock_file" "$temporary/package/"
(
  cd "$temporary/package"
  PATH="$node_root/bin:/usr/bin:/bin" \
    "$node" "$npm_cli" ci --ignore-scripts --no-audit --no-fund
)
validate_install "$temporary/package" || {
  echo "installed Wrangler failed exact-version validation" >&2
  exit 1
}
mv "$temporary/package" "$install_dir"
