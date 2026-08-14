#!/usr/bin/env bash
set -euo pipefail
umask 077

version=2.3.0
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
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
selection=$(
  "$script_dir/select-aliyun-tool-archive.sh" ossutil "$(uname -m)"
)
IFS=$'\t' read -r archive_name archive_sha256 <<<"$selection"
[[ -n "$archive_name" && "$archive_sha256" =~ ^[0-9a-f]{64}$ ]]
archive_url="https://gosspublic.alicdn.com/ossutil/v2/${version}/${archive_name}"
install_dir="$VANE_TOOL_CACHE/ossutil/$version"
binary_path="$install_dir/ossutil"

mkdir -p "$install_dir"
archive=$(mktemp "$VANE_WORK_ROOT/ossutil.XXXXXX")
trap 'rm -f "$archive"' EXIT

curl --fail --silent --show-error --location \
  --proto '=https' --tlsv1.2 \
  --output "$archive" "$archive_url"
printf '%s  %s\n' "$archive_sha256" "$archive" |
  sha256sum --check --status

python3 - "$archive" "$binary_path" "$archive_name" <<'PY'
import pathlib
import shutil
import sys
import zipfile

archive = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
archive_name = sys.argv[3]
directory = archive_name.removesuffix(".zip")
expected_directory = f"{directory}/"
expected_binary = f"{directory}/ossutil"

with zipfile.ZipFile(archive) as bundle:
    members = bundle.infolist()
    if [member.filename for member in members] != [
        expected_directory,
        expected_binary,
    ]:
        raise SystemExit("ossutil archive has unexpected members")
    binary = members[1]
    if binary.is_dir() or binary.file_size <= 0 or binary.file_size > 30_000_000:
        raise SystemExit("ossutil archive binary has an invalid size")
    if destination.exists():
        raise SystemExit("ossutil destination already exists")
    with bundle.open(binary) as source, destination.open("xb") as target:
        shutil.copyfileobj(source, target)
PY

chmod 755 "$binary_path"
actual_version=$("$binary_path" version)
if [[ $actual_version != "$version" ]]; then
  echo "unexpected ossutil version: $actual_version" >&2
  exit 1
fi
