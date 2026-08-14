#!/usr/bin/env python3
"""Plan a safe Web publication and build its deterministic release receipt."""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
from urllib.parse import quote, unquote, urlsplit


RECEIPT_SCHEMA = "vane.web.aliyun-release/v1"
BUCKET = "zhuoqidev-vane-web"
RELEASE_MARKER_PATH = "vane-release.json"
MANIFEST_SUFFIXES = (".webmanifest", ".json")
MANIFEST_NAMES = ("manifest",)
RUNTIME_HASH_RE = re.compile(
    r"^.+[._-][A-Za-z0-9_-]{8,}\.(?:css|js)$"
)
CONTENT_HASH_RE = re.compile(
    r"^.+[._-][A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9]+)+$"
)
REFERENCE_KEYS = {
    "assets",
    "css",
    "file",
    "href",
    "icons",
    "screenshots",
    "src",
    "url",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_sha(value: str) -> str:
    if len(value) != 40 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"invalid exact source SHA: {value!r}")
    return value


def validate_relative_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        any(ord(char) < 32 or ord(char) == 127 for char in value)
        or "\\" in value
        or path.is_absolute()
        or str(path) != value
        or not path.parts
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise ValueError(f"unsafe Web path: {value!r}")
    return path


class HTMLReferences(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.references: list[str] = []
        self.runtime_references: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        tag = tag.lower()
        normalized_attrs = {
            name.lower(): value for name, value in attrs if value is not None
        }
        link_rel = set(normalized_attrs.get("rel", "").lower().split())
        for name, value in attrs:
            if value is None:
                continue
            name = name.lower()
            if name == "src" or (name == "href" and tag == "link"):
                self.references.append(value)
                if tag == "script" or (
                    tag == "link"
                    and link_rel.intersection(("modulepreload", "stylesheet"))
                ):
                    self.runtime_references.append(value)
            elif name == "srcset":
                for candidate in value.split(","):
                    reference = candidate.strip().split(maxsplit=1)[0]
                    if reference:
                        self.references.append(reference)


def manifest_references(value: object, active: bool = False) -> list[str]:
    references: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_active = key in REFERENCE_KEYS
            references.extend(manifest_references(child, child_active))
    elif isinstance(value, list):
        for child in value:
            references.extend(manifest_references(child, active))
    elif active and isinstance(value, str):
        references.append(value)
    return references


def is_vite_manifest(path: str, value: object) -> bool:
    if path == ".vite/manifest.json":
        return True
    return isinstance(value, dict) and any(
        isinstance(entry, dict)
        and any(
            field in entry
            for field in ("dynamicImports", "file", "imports")
        )
        for entry in value.values()
    )


def vite_manifest_references(
    path: str, value: object
) -> tuple[list[str], list[str]]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"Vite manifest root is empty or invalid: {path}")
    references: list[str] = []
    runtime_references: list[str] = []
    manifest_keys = set(value)
    if not all(isinstance(key, str) for key in manifest_keys):
        raise ValueError(f"Vite manifest has a non-string key: {path}")

    for key, entry in value.items():
        if not isinstance(entry, dict):
            raise ValueError(f"Vite manifest entry is not an object: {path}:{key}")
        file_value = entry.get("file")
        if not isinstance(file_value, str) or not file_value:
            raise ValueError(f"Vite manifest entry has no file: {path}:{key}")
        references.append(file_value)
        runtime_references.append(file_value)

        css_values = entry.get("css", [])
        if not isinstance(css_values, list) or not all(
            isinstance(item, str) and item for item in css_values
        ):
            raise ValueError(f"Vite manifest css list is invalid: {path}:{key}")
        references.extend(css_values)
        runtime_references.extend(css_values)

        asset_values = entry.get("assets", [])
        if not isinstance(asset_values, list) or not all(
            isinstance(item, str) and item for item in asset_values
        ):
            raise ValueError(f"Vite manifest assets list is invalid: {path}:{key}")
        references.extend(asset_values)

        for field in ("imports", "dynamicImports"):
            dependencies = entry.get(field, [])
            if not isinstance(dependencies, list) or not all(
                isinstance(item, str) and item for item in dependencies
            ):
                raise ValueError(
                    f"Vite manifest {field} list is invalid: {path}:{key}"
                )
            missing = sorted(set(dependencies) - manifest_keys)
            if missing:
                raise ValueError(
                    f"Vite manifest {field} references missing keys: "
                    f"{path}:{key}: {missing}"
                )
    return references, runtime_references


def resolve_reference(reference: str, source: str) -> str | None:
    split = urlsplit(reference.strip())
    if split.scheme or split.netloc or not split.path:
        return None
    raw_path = unquote(split.path)
    if raw_path.startswith("/"):
        candidate = PurePosixPath(raw_path.removeprefix("/"))
    else:
        candidate = PurePosixPath(source).parent / raw_path
    normalized_parts: list[str] = []
    for part in candidate.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not normalized_parts:
                raise ValueError(
                    f"Web reference escapes dist/: {reference!r} from {source!r}"
                )
            normalized_parts.pop()
        else:
            normalized_parts.append(part)
    if not normalized_parts:
        return None
    return validate_relative_path("/".join(normalized_parts)).as_posix()


def is_manifest(path: str) -> bool:
    name = PurePosixPath(path).name.lower()
    return any(token in name for token in MANIFEST_NAMES) and name.endswith(
        MANIFEST_SUFFIXES
    )


def write_text_lf(path: Path, value: str) -> None:
    # Path.write_text gained its newline parameter only in newer Python
    # releases. The production runner intentionally uses the distribution
    # Python, so use the long-supported open() contract while preserving
    # deterministic UTF-8/LF release receipts.
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(value)


def write_list(path: Path, values: list[str]) -> None:
    write_text_lf(path, "".join(f"{value}\n" for value in values))


def write_sized_list(
    path: Path, values: list[str], files: dict[str, Path]
) -> None:
    write_text_lf(
        path,
        "".join(f"{files[value].stat().st_size}\t{value}\n" for value in values),
    )


def is_content_addressed(path: str) -> bool:
    return bool(CONTENT_HASH_RE.fullmatch(PurePosixPath(path).name))


def plan(dist: Path, source_sha: str, output: Path) -> None:
    source_sha = validate_sha(source_sha)
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"plan output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    entry_path = dist / "index.html"
    if not entry_path.is_file() or entry_path.is_symlink():
        raise ValueError("Web dist/index.html is missing or unsafe")

    files: dict[str, Path] = {}
    for file_path in sorted(dist.rglob("*")):
        file_stat = file_path.lstat()
        if stat.S_ISLNK(file_stat.st_mode):
            raise ValueError(f"Web dist contains symlink: {file_path}")
        if stat.S_ISDIR(file_stat.st_mode):
            continue
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError(f"Web dist contains non-file: {file_path}")
        relative = file_path.relative_to(dist).as_posix()
        validate_relative_path(relative)
        files[relative] = file_path

    html_files = sorted(
        path for path in files if PurePosixPath(path).suffix.lower() in (".htm", ".html")
    )
    if RELEASE_MARKER_PATH not in files:
        raise ValueError(f"Web dist/{RELEASE_MARKER_PATH} is missing")
    # The release marker is the public commit object.  It is intentionally not
    # a normal asset: the publisher uploads it only after the entrypoint and
    # all exact-byte provider checks have passed.
    assets = sorted(
        path
        for path in files
        if path not in html_files and path != RELEASE_MARKER_PATH
    )
    html_before_entry = [path for path in html_files if path != "index.html"]

    referenced: set[str] = set()
    runtime_referenced: set[str] = set()
    manifests_to_parse: set[str] = {path for path in files if is_manifest(path)}
    for html_path in html_files:
        parser = HTMLReferences()
        parser.feed(files[html_path].read_text(encoding="utf-8"))
        for raw_reference in parser.references:
            resolved = resolve_reference(raw_reference, html_path)
            if resolved is not None:
                referenced.add(resolved)
                if is_manifest(resolved):
                    manifests_to_parse.add(resolved)
        for raw_reference in parser.runtime_references:
            resolved = resolve_reference(raw_reference, html_path)
            if resolved is not None:
                runtime_referenced.add(resolved)

    for manifest_path in sorted(manifests_to_parse):
        if manifest_path not in files:
            raise ValueError(f"referenced manifest is missing: {manifest_path}")
        try:
            manifest = json.loads(files[manifest_path].read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid Web manifest: {manifest_path}") from error
        if is_vite_manifest(manifest_path, manifest):
            raw_references, raw_runtime_references = vite_manifest_references(
                manifest_path, manifest
            )
        else:
            raw_references = manifest_references(manifest)
            raw_runtime_references = []
        for raw_reference in raw_references:
            # Vite's JSON manifest values are dist-root relative even though
            # the manifest commonly lives at .vite/manifest.json. Web app
            # manifest URLs retain normal URL-relative semantics.
            reference_base = (
                manifest_path if manifest_path.lower().endswith(".webmanifest") else ""
            )
            resolved = resolve_reference(raw_reference, reference_base)
            if resolved is not None:
                referenced.add(resolved)
        for raw_reference in raw_runtime_references:
            resolved = resolve_reference(raw_reference, "")
            if resolved is not None:
                runtime_referenced.add(resolved)

    critical_assets: list[str] = []
    for reference in sorted(referenced):
        if reference not in files:
            raise ValueError(f"referenced Web file is missing: {reference}")
        if reference in assets:
            critical_assets.append(reference)

    for reference in sorted(runtime_referenced):
        if reference not in files:
            raise ValueError(
                f"referenced Web runtime file is missing: {reference}"
            )
        if PurePosixPath(reference).suffix.lower() not in (".css", ".js"):
            continue
        if not RUNTIME_HASH_RE.fullmatch(PurePosixPath(reference).name):
            raise ValueError(
                "runtime JavaScript/CSS is not content-addressed with an "
                f"8+ character Vite hash: {reference}"
            )

    stable_public_objects = sorted(
        path
        for path in assets
        if not path.startswith(".vite/") and not is_content_addressed(path)
    )
    refresh_objects = sorted(
        set(html_files + stable_public_objects + [RELEASE_MARKER_PATH])
    )
    cdn_refresh_paths = ["/"] + [
        f"/{quote(path, safe='/-._~')}" for path in refresh_objects
    ]

    receipt_files = []
    for relative, file_path in sorted(files.items()):
        receipt_files.append(
            {
                "path": relative,
                "sha256": sha256_file(file_path),
                "size": file_path.stat().st_size,
            }
        )
    entry = next(item for item in receipt_files if item["path"] == "index.html")
    receipt = {
        "bucket": BUCKET,
        "entry_path": "index.html",
        "entry_sha256": entry["sha256"],
        "files": receipt_files,
        "schema": RECEIPT_SCHEMA,
        "source_sha": source_sha,
    }

    write_list(output / "assets.list", assets)
    write_sized_list(
        output / "critical-assets.list", critical_assets, files
    )
    write_list(output / "cdn-refresh-paths.list", cdn_refresh_paths)
    write_list(output / "html-before-entry.list", html_before_entry)
    write_text_lf(
        output / "release.json",
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan(args.dist, args.sha, args.output)


if __name__ == "__main__":
    main()
