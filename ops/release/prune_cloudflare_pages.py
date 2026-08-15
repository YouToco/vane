#!/usr/bin/env python3
"""Fail-closed retention of old Cloudflare Pages production deployments.

This tool is intentionally separate from the Web release transaction. It plans
by default and only deletes when ``--execute`` is supplied together with an
exact, previously reviewed manifest digest.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Callable, Iterable, List, Mapping, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import uuid


API_BASE = "https://api.cloudflare.com/client/v4"
MANIFEST_SCHEMA = "vane.cloudflare-pages-prune/v1"
PROJECT = "vane-web"
CUSTOM_DOMAIN = "vane.zhuoqidev.com"
PRODUCTION_BRANCH = "main"
EXPECTED_DOMAINS = {f"{PROJECT}.pages.dev", CUSTOM_DOMAIN}
PER_PAGE = 25
SHA256_RE = re.compile(r"[0-9a-f]{64}")
REVISION_RE = re.compile(r"[0-9a-f]{40}")


class PruneError(RuntimeError):
    """Raised when a retention invariant does not hold."""


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def manifest_sha256(manifest: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json(manifest)).hexdigest()


def require_uuid(value: str, label: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise PruneError(f"{label} is not a UUID") from exc
    if str(parsed) != value.lower():
        raise PruneError(f"{label} is not a canonical UUID")


def require_revision(value: str) -> None:
    if not REVISION_RE.fullmatch(value):
        raise PruneError("expected revision is not an exact 40-character lowercase SHA")


def _cloudflare_error(payload: object) -> str:
    if not isinstance(payload, dict):
        return "Cloudflare API returned an invalid response"
    messages = []
    for item in payload.get("errors", []):
        if isinstance(item, dict):
            code = item.get("code")
            message = item.get("message")
            if isinstance(code, int) and isinstance(message, str):
                messages.append(f"{code}: {message}")
    return "; ".join(messages) or "Cloudflare API request failed"


class CloudflarePagesAPI:
    def __init__(
        self,
        account_id: str,
        token: str,
        *,
        project: str = PROJECT,
        custom_domain: str = CUSTOM_DOMAIN,
        api_base: str = API_BASE,
        timeout: int = 30,
        opener: Callable[..., object] = urlopen,
    ) -> None:
        if not account_id or not token:
            raise PruneError("Cloudflare account ID and API token are required")
        if project != PROJECT or custom_domain != CUSTOM_DOMAIN:
            raise PruneError(
                "Cloudflare Pages pruner is locked to vane-web and vane.zhuoqidev.com"
            )
        self.account_id = account_id
        self.token = token
        self.project = project
        self.custom_domain = custom_domain
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout
        self.opener = opener

    @property
    def project_path(self) -> str:
        return (
            f"/accounts/{quote(self.account_id, safe='')}/pages/projects/"
            f"{quote(self.project, safe='')}"
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Mapping[str, object]] = None,
    ) -> object:
        url = f"{self.api_base}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        request = Request(
            url,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "vane-cloudflare-pages-pruner/1",
            },
        )
        try:
            with self.opener(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            try:
                payload = json.loads(exc.read().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                payload = None
            raise PruneError(
                f"Cloudflare API HTTP {exc.code}: {_cloudflare_error(payload)}"
            ) from exc
        except URLError as exc:
            raise PruneError("Cloudflare API network request failed") from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise PruneError("Cloudflare API returned invalid JSON") from exc
        if not isinstance(payload, dict) or payload.get("success") is not True:
            raise PruneError(_cloudflare_error(payload))
        return payload

    def get_project(self) -> dict:
        payload = self._request("GET", self.project_path)
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            raise PruneError("Cloudflare project response is invalid")
        return result

    def get_custom_domain(self) -> dict:
        payload = self._request(
            "GET",
            f"{self.project_path}/domains/{quote(self.custom_domain, safe='')}",
        )
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            raise PruneError("Cloudflare custom-domain response is invalid")
        return result

    def list_deployments(self) -> List[dict]:
        deployments: List[dict] = []
        expected_total: Optional[int] = None
        page = 1
        while True:
            payload = self._request(
                "GET",
                f"{self.project_path}/deployments",
                query={"page": page, "per_page": PER_PAGE},
            )
            result = payload.get("result") if isinstance(payload, dict) else None
            info = payload.get("result_info") if isinstance(payload, dict) else None
            if not isinstance(result, list) or not all(
                isinstance(item, dict) for item in result
            ):
                raise PruneError("Cloudflare deployment-list response is invalid")
            deployments.extend(result)
            if isinstance(info, dict):
                total_count = info.get("total_count")
                total_pages = info.get("total_pages")
                if not isinstance(total_count, int) or total_count < 0:
                    raise PruneError("Cloudflare pagination total_count is invalid")
                if not isinstance(total_pages, int) or total_pages < 0:
                    raise PruneError("Cloudflare pagination total_pages is invalid")
                if expected_total is None:
                    expected_total = total_count
                elif expected_total != total_count:
                    raise PruneError("Cloudflare deployment list changed while paginating")
                if page >= total_pages:
                    break
            elif len(result) < PER_PAGE:
                break
            if not result:
                break
            page += 1
            if page > 10000:
                raise PruneError("Cloudflare deployment pagination did not terminate")
        if expected_total is not None and len(deployments) != expected_total:
            raise PruneError("Cloudflare deployment pagination was incomplete")
        ids = [item.get("id") for item in deployments]
        if any(not isinstance(item, str) for item in ids) or len(set(ids)) != len(ids):
            raise PruneError("Cloudflare deployment IDs are missing or duplicated")
        return deployments

    def delete_deployment(self, deployment_id: str) -> None:
        require_uuid(deployment_id, "candidate deployment ID")
        # Deliberately no force query parameter: aliased deployments must fail.
        self._request(
            "DELETE",
            f"{self.project_path}/deployments/{quote(deployment_id, safe='')}",
        )


def _deployment_status(deployment: Mapping[str, object]) -> object:
    latest_stage = deployment.get("latest_stage")
    return latest_stage.get("status") if isinstance(latest_stage, dict) else None


def _safe_deployment(deployment: Mapping[str, object]) -> dict:
    trigger = deployment.get("deployment_trigger")
    metadata = trigger.get("metadata") if isinstance(trigger, dict) else None
    aliases = deployment.get("aliases")
    if aliases is None:
        aliases = []
    if not isinstance(aliases, list) or not all(
        isinstance(alias, str) for alias in aliases
    ):
        raise PruneError("Cloudflare deployment aliases are invalid")
    return {
        "id": deployment.get("id"),
        "environment": deployment.get("environment"),
        "created_on": deployment.get("created_on"),
        "modified_on": deployment.get("modified_on"),
        "status": _deployment_status(deployment),
        "aliases": sorted(aliases),
        "branch": metadata.get("branch") if isinstance(metadata, dict) else None,
        "commit_hash": (
            metadata.get("commit_hash") if isinstance(metadata, dict) else None
        ),
        "commit_dirty": (
            metadata.get("commit_dirty") if isinstance(metadata, dict) else None
        ),
    }


def _require_successful_production(deployment: Mapping[str, object], label: str) -> None:
    if deployment.get("environment") != "production":
        raise PruneError(f"{label} is not a production deployment")
    if _deployment_status(deployment) != "success":
        raise PruneError(f"{label} is not a successful deployment")


def _require_deletable_candidate(deployment: Mapping[str, object]) -> None:
    candidate_id = deployment.get("id")
    if not isinstance(candidate_id, str):
        raise PruneError("candidate deployment ID is missing")
    require_uuid(candidate_id, "candidate deployment ID")
    if _deployment_status(deployment) == "active":
        raise PruneError(f"candidate deployment {candidate_id} is active")
    _require_successful_production(deployment, f"candidate deployment {candidate_id}")
    aliases = deployment.get("aliases")
    if aliases not in (None, []):
        raise PruneError(f"candidate deployment {candidate_id} is aliased")


def _authority_ids(project: Mapping[str, object]) -> Tuple[object, object]:
    canonical = project.get("canonical_deployment")
    latest = project.get("latest_deployment")
    canonical_id = canonical.get("id") if isinstance(canonical, dict) else None
    latest_id = latest.get("id") if isinstance(latest, dict) else None
    return canonical_id, latest_id


def _validate_new_canonical(
    deployment: object,
    *,
    custom_domain: str,
    new_canonical_id: str,
    expected_revision: str,
    label: str,
) -> None:
    if not isinstance(deployment, dict) or deployment.get("id") != new_canonical_id:
        raise PruneError(f"{label} is not the expected new canonical deployment")
    _require_successful_production(deployment, label)
    if deployment.get("aliases") != [f"https://{custom_domain}"]:
        raise PruneError(f"{label} does not have the exact custom-domain alias")
    trigger = deployment.get("deployment_trigger")
    metadata = trigger.get("metadata") if isinstance(trigger, dict) else None
    if not isinstance(metadata, dict):
        raise PruneError(f"{label} deployment metadata is missing")
    if metadata.get("branch") != PRODUCTION_BRANCH:
        raise PruneError(f"{label} is not from the production branch")
    if metadata.get("commit_hash") != expected_revision:
        raise PruneError(f"{label} is not bound to the expected revision")
    if metadata.get("commit_dirty") is not False:
        raise PruneError(f"{label} is not a clean exact deployment")


def _validate_authority(
    project: Mapping[str, object],
    custom_domain: str,
    new_canonical_id: str,
    expected_revision: str,
) -> None:
    if project.get("name") != PROJECT:
        raise PruneError("Cloudflare project identity differs from vane-web")
    if "source" not in project or project.get("source") is not None:
        raise PruneError("Cloudflare project is not an unbound direct-upload project")
    if project.get("production_branch") != PRODUCTION_BRANCH:
        raise PruneError("Cloudflare project production branch is not main")
    domains = project.get("domains")
    if (
        not isinstance(domains, list)
        or not all(isinstance(domain, str) for domain in domains)
        or len(domains) != len(EXPECTED_DOMAINS)
        or set(domains) != EXPECTED_DOMAINS
    ):
        raise PruneError("Cloudflare project domains differ from the exact allowlist")
    canonical_id, latest_id = _authority_ids(project)
    if canonical_id != new_canonical_id or latest_id != new_canonical_id:
        raise PruneError("Cloudflare canonical/latest deployment is not the expected new ID")
    _validate_new_canonical(
        project.get("canonical_deployment"),
        custom_domain=custom_domain,
        new_canonical_id=new_canonical_id,
        expected_revision=expected_revision,
        label="project canonical deployment",
    )
    _validate_new_canonical(
        project.get("latest_deployment"),
        custom_domain=custom_domain,
        new_canonical_id=new_canonical_id,
        expected_revision=expected_revision,
        label="project latest deployment",
    )


def _validate_domain(domain: Mapping[str, object], custom_domain: str) -> None:
    if domain.get("name") != custom_domain or domain.get("status") != "active":
        raise PruneError("Cloudflare custom domain is not active")
    for field in ("validation_data", "verification_data"):
        evidence = domain.get(field)
        if not isinstance(evidence, dict) or evidence.get("status") != "active":
            raise PruneError(
                f"Cloudflare custom-domain {field} is missing or not active"
            )


def build_manifest(
    api: CloudflarePagesAPI,
    *,
    new_canonical_id: str,
    previous_canonical_id: str,
    expected_revision: str,
    expected_total: int,
    expected_candidate_count: int,
) -> Tuple[dict, str]:
    require_uuid(new_canonical_id, "new canonical deployment ID")
    require_uuid(previous_canonical_id, "previous canonical deployment ID")
    require_revision(expected_revision)
    if new_canonical_id == previous_canonical_id:
        raise PruneError("new and previous canonical deployment IDs must differ")
    if expected_total < 2 or expected_candidate_count < 0:
        raise PruneError("expected deployment counts are invalid")
    if expected_total != expected_candidate_count + 2:
        raise PruneError("expected total must equal candidate count plus two keeps")

    project = api.get_project()
    domain = api.get_custom_domain()
    deployments = api.list_deployments()
    _validate_authority(
        project, api.custom_domain, new_canonical_id, expected_revision
    )
    _validate_domain(domain, api.custom_domain)
    if len(deployments) != expected_total:
        raise PruneError("Cloudflare deployment total differs from the expected total")

    by_id = {item["id"]: item for item in deployments}
    keep_ids = {new_canonical_id, previous_canonical_id}
    if not keep_ids.issubset(by_id):
        raise PruneError("one or more keep deployments are missing")
    for keep_id in sorted(keep_ids):
        _require_successful_production(by_id[keep_id], f"keep deployment {keep_id}")

    _validate_new_canonical(
        by_id[new_canonical_id],
        custom_domain=api.custom_domain,
        new_canonical_id=new_canonical_id,
        expected_revision=expected_revision,
        label="listed new canonical deployment",
    )

    candidates = [item for item in deployments if item["id"] not in keep_ids]
    if len(candidates) != expected_candidate_count:
        raise PruneError("Cloudflare candidate count differs from the expected count")
    for candidate in candidates:
        candidate_id = candidate["id"]
        _require_deletable_candidate(candidate)
        if candidate_id in _authority_ids(project):
            raise PruneError(f"candidate deployment {candidate_id} owns project authority")

    safe_keeps = sorted(
        (_safe_deployment(by_id[keep_id]) for keep_id in keep_ids),
        key=lambda item: item["id"],
    )
    safe_candidates = sorted(
        (_safe_deployment(item) for item in candidates), key=lambda item: item["id"]
    )
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "project": api.project,
        "custom_domain": api.custom_domain,
        "new_canonical_id": new_canonical_id,
        "previous_canonical_id": previous_canonical_id,
        "expected_revision": expected_revision,
        "expected_total": expected_total,
        "expected_candidate_count": expected_candidate_count,
        "project_authority": {
            "canonical_deployment_id": new_canonical_id,
            "latest_deployment_id": new_canonical_id,
            "production_branch": PRODUCTION_BRANCH,
            "source": None,
            "domains": sorted(EXPECTED_DOMAINS),
        },
        "custom_domain_status": "active",
        "keeps": safe_keeps,
        "candidates": safe_candidates,
    }
    return manifest, manifest_sha256(manifest)


def _assert_runtime_state(
    api: CloudflarePagesAPI,
    *,
    expected_ids: Iterable[str],
    keep_ids: Iterable[str],
    new_canonical_id: str,
    expected_revision: str,
) -> None:
    project = api.get_project()
    _validate_authority(
        project, api.custom_domain, new_canonical_id, expected_revision
    )
    domain = api.get_custom_domain()
    _validate_domain(domain, api.custom_domain)
    deployments = api.list_deployments()
    actual = {item["id"]: item for item in deployments}
    expected = set(expected_ids)
    if set(actual) != expected:
        raise PruneError("Cloudflare deployments changed during pruning")
    for keep_id in keep_ids:
        if keep_id not in actual:
            raise PruneError("a keep deployment disappeared during pruning")
        _require_successful_production(actual[keep_id], f"keep deployment {keep_id}")
    _validate_new_canonical(
        actual.get(new_canonical_id),
        custom_domain=api.custom_domain,
        new_canonical_id=new_canonical_id,
        expected_revision=expected_revision,
        label="listed new canonical deployment",
    )
    for deployment_id, deployment in actual.items():
        if deployment_id not in keep_ids:
            _require_deletable_candidate(deployment)


def run_prune(
    api: CloudflarePagesAPI,
    *,
    new_canonical_id: str,
    previous_canonical_id: str,
    expected_revision: str,
    expected_total: int,
    expected_candidate_count: int,
    expected_manifest_sha256: Optional[str],
    execute: bool = False,
    emit: Callable[[dict], None],
) -> dict:
    if expected_manifest_sha256 is not None and not SHA256_RE.fullmatch(
        expected_manifest_sha256
    ):
        raise PruneError("expected manifest SHA-256 is invalid")
    if execute and expected_manifest_sha256 is None:
        raise PruneError("execute requires the reviewed manifest SHA-256")
    manifest, digest = build_manifest(
        api,
        new_canonical_id=new_canonical_id,
        previous_canonical_id=previous_canonical_id,
        expected_revision=expected_revision,
        expected_total=expected_total,
        expected_candidate_count=expected_candidate_count,
    )
    summary = {
        "mode": "execute" if execute else "dry-run",
        "manifest_sha256": digest,
        "expected_revision": expected_revision,
        "deployment_total": expected_total,
        "candidate_count": expected_candidate_count,
        "keep_count": 2,
    }
    emit({"event": "cloudflare_pages_prune_plan", "manifest": manifest, "summary": summary})
    if expected_manifest_sha256 is not None and digest != expected_manifest_sha256:
        raise PruneError("Cloudflare prune manifest SHA-256 differs from expectation")
    if not execute:
        return {**summary, "deleted_count": 0, "status": "planned"}

    keep_ids = {new_canonical_id, previous_canonical_id}
    all_ids = keep_ids | {item["id"] for item in manifest["candidates"]}
    _assert_runtime_state(
        api,
        expected_ids=all_ids,
        keep_ids=keep_ids,
        new_canonical_id=new_canonical_id,
        expected_revision=expected_revision,
    )
    deleted: List[str] = []
    remaining = set(all_ids)
    for candidate in manifest["candidates"]:
        candidate_id = candidate["id"]
        api.delete_deployment(candidate_id)
        deleted.append(candidate_id)
        remaining.remove(candidate_id)
        _assert_runtime_state(
            api,
            expected_ids=remaining,
            keep_ids=keep_ids,
            new_canonical_id=new_canonical_id,
            expected_revision=expected_revision,
        )
        emit({
            "event": "cloudflare_pages_deployment_deleted",
            "deployment_id": candidate_id,
            "deleted_count": len(deleted),
            "remaining_candidate_count": expected_candidate_count - len(deleted),
        })

    _assert_runtime_state(
        api,
        expected_ids=keep_ids,
        keep_ids=keep_ids,
        new_canonical_id=new_canonical_id,
        expected_revision=expected_revision,
    )
    final = api.list_deployments()
    if len(final) != 2 or {item["id"] for item in final} != keep_ids:
        raise PruneError("Cloudflare pruning did not finish with exactly two keeps")
    for deployment in final:
        _require_successful_production(deployment, f"final keep {deployment['id']}")
    result = {**summary, "deleted_count": len(deleted), "status": "complete"}
    emit({"event": "cloudflare_pages_prune_complete", "result": result})
    return result

