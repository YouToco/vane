#!/usr/bin/env bash
set -euo pipefail
umask 077

version=2.3.0
archive_name=ossutil-2.3.0-linux-arm64.zip
archive_sha256=f6c95ba0c2d2ef30290af686ce4d706c701f4734ce8090bee4288a77e3f1d764
archive_url="https://gosspublic.alicdn.com/ossutil/v2/${version}/${archive_name}"
install_dir="$RUNNER_TEMP/aliyun-3.4.10"
binary_path="$install_dir/ossutil"

mkdir -p "$install_dir"
archive=$(mktemp "$RUNNER_TEMP/ossutil.XXXXXX")
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
