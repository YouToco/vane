from __future__ import annotations

import copy
from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import unittest
from unittest import mock

from ops.cli import controller


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "prune_cloudflare_pages", ROOT / "ops/release/prune_cloudflare_pages.py"
)
assert SPEC and SPEC.loader
prune = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prune)

NEW_ID = "00000000-0000-0000-0000-000000000001"
PREVIOUS_ID = "00000000-0000-0000-0000-000000000002"
DOMAIN = "vane.zhuoqidev.com"
REVISION = "0123456789abcdef0123456789abcdef01234567"


def deployment(
    deployment_id: str,
    *,
    environment: str = "production",
    status: str = "success",
    aliases: object = None,
    ordinal: int = 1,
    revision: object = None,
    dirty: bool = False,
) -> dict:
    return {
        "id": deployment_id,
        "environment": environment,
        "created_on": f"2026-08-15T00:00:{ordinal % 60:02d}Z",
        "modified_on": f"2026-08-15T00:01:{ordinal % 60:02d}Z",
        "aliases": aliases,
        "latest_stage": {"name": "deploy", "status": status},
        "deployment_trigger": {
            "type": "ad_hoc",
            "metadata": {
                "branch": "main",
                "commit_hash": revision or f"{ordinal:040x}"[-40:],
                "commit_dirty": dirty,
            },
        },
        # This field deliberately contains a credential-shaped value. The safe
        # manifest projection must never copy arbitrary API response fields.
        "env_vars": {"SECRET": {"type": "secret_text", "value": "do-not-leak"}},
    }


def candidate_id(ordinal: int) -> str:
    return f"00000000-0000-0000-0000-{ordinal:012d}"


class FakeAPI:
    project = "vane-web"
    custom_domain = DOMAIN

    def __init__(self, candidate_count: int = 3) -> None:
        new_deployment = deployment(
            NEW_ID, aliases=[f"https://{DOMAIN}"], ordinal=1, revision=REVISION
        )
        self.deployments = [
            new_deployment,
            deployment(PREVIOUS_ID, aliases=None, ordinal=2),
        ] + [
            deployment(candidate_id(100 + index), aliases=None, ordinal=100 + index)
            for index in range(candidate_count)
        ]
        self.project_value = {
            "name": self.project,
            "domains": ["vane-web.pages.dev", DOMAIN],
            "source": None,
            "production_branch": "main",
            "canonical_deployment": copy.deepcopy(new_deployment),
            "latest_deployment": copy.deepcopy(new_deployment),
        }
        self.domain_value = {
            "name": DOMAIN,
            "status": "active",
            "validation_data": {"status": "active"},
            "verification_data": {"status": "active"},
        }
        self.deleted = []
        self.fail_on = None
        self.sticky_delete = False
        self.drift_after_delete = False
        self.alias_candidate_after_delete = False
        self.dirty_new_after_delete = False
        self.domain_pending_after_delete = False

    def get_project(self) -> dict:
        return copy.deepcopy(self.project_value)

    def get_custom_domain(self) -> dict:
        return copy.deepcopy(self.domain_value)

    def list_deployments(self) -> list:
        return copy.deepcopy(self.deployments)

    def delete_deployment(self, deployment_id: str) -> None:
        if deployment_id == self.fail_on:
            raise prune.PruneError("fixture delete failed")
        self.deleted.append(deployment_id)
        if not self.sticky_delete:
            self.deployments = [
                item for item in self.deployments if item["id"] != deployment_id
            ]
        if self.drift_after_delete:
            self.project_value["latest_deployment"] = {"id": PREVIOUS_ID}
        if self.alias_candidate_after_delete:
            for item in self.deployments:
                if item["id"] not in {NEW_ID, PREVIOUS_ID}:
                    item["aliases"] = ["https://concurrent.example"]
                    break
        if self.dirty_new_after_delete:
            self.deployments[0]["deployment_trigger"]["metadata"][
                "commit_dirty"
            ] = True
        if self.domain_pending_after_delete:
            self.domain_value["verification_data"]["status"] = "pending"


def plan(api: FakeAPI) -> tuple:
    return prune.build_manifest(
        api,
        new_canonical_id=NEW_ID,
        previous_canonical_id=PREVIOUS_ID,
        expected_revision=REVISION,
        expected_total=len(api.deployments),
        expected_candidate_count=len(api.deployments) - 2,
    )


class CloudflarePagesPruneTest(unittest.TestCase):
    def test_default_mode_is_dry_run(self) -> None:
        args = controller.parser().parse_args([
            "prune-cloudflare-pages",
            "--sha", REVISION,
            "--release-root", "/tmp/release",
            "--expected-total", "52",
            "--expected-candidate-count", "50",
        ])
        self.assertFalse(args.execute)
        self.assertIsNone(args.expected_manifest_sha256)
        self.assertFalse(hasattr(args, "previous_canonical_id"))
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            controller.parser().parse_args([
                "prune-cloudflare-pages",
                "--sha", REVISION,
                "--release-root", "/tmp/release",
                "--previous-canonical-id", candidate_id(999),
                "--expected-total", "52",
                "--expected-candidate-count", "50",
            ])

    def test_api_is_locked_to_the_vane_production_project_and_domain(self) -> None:
        with self.assertRaisesRegex(prune.PruneError, "locked to vane-web"):
            prune.CloudflarePagesAPI("account", "token", project="other-project")
        with self.assertRaisesRegex(prune.PruneError, "locked to vane-web"):
            prune.CloudflarePagesAPI(
                "account", "token", custom_domain="other.example.com"
            )

    def test_controller_dry_run_derives_canonical_from_combined_receipt(self) -> None:
        with self._controller_release() as (release_root, gate, args):
            api = FakeAPI()
            with (
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=REVISION),
                mock.patch.object(controller, "prune_authority_paths", return_value=(gate,)),
                mock.patch.object(
                    controller, "capture_release_authority",
                    return_value={str(gate): controller.sha256_file(gate)},
                ),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(
                    controller.subprocess, "run",
                    return_value=mock.Mock(returncode=0, stdout=""),
                ),
                mock.patch.object(
                    controller, "validate_resume_release_root",
                    return_value=release_root,
                ),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "validate_existing_web_result"),
                mock.patch.object(
                    controller, "verify_production_revision",
                    return_value="f" * 64,
                ),
                mock.patch.object(controller, "verify_web_after_server"),
                mock.patch.object(controller, "preflight_web_publication"),
                mock.patch.object(
                    controller.cloudflare_pruner, "CloudflarePagesAPI",
                    return_value=api,
                ),
                mock.patch.dict(controller.os.environ, {
                    "CLOUDFLARE_ACCOUNT_ID": "account",
                    "CLOUDFLARE_API_TOKEN": "token",
                }),
            ):
                self.assertEqual(controller.command_prune_cloudflare_pages(args), 0)
            self.assertEqual(api.deleted, [])

    def test_controller_refuses_untrusted_or_drifted_evidence_before_delete(self) -> None:
        cases = (
            "dirty", "wrong-gate", "tampered-provider", "server-drift",
            "dns-drift", "authority-drift",
        )
        for case in cases:
            with self.subTest(case=case), self._controller_release() as (
                release_root, gate, args
            ):
                api = FakeAPI()
                clean = mock.Mock(
                    returncode=0, stdout=" M ops/cli/controller.py\n" if case == "dirty" else ""
                )
                with ExitStack() as stack:
                    stack.enter_context(mock.patch.object(controller, "assert_origin_main"))
                    stack.enter_context(mock.patch.object(
                        controller, "git_revision", return_value=REVISION
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "prune_authority_paths", return_value=(gate,)
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "capture_release_authority",
                        return_value={str(gate): controller.sha256_file(gate)},
                    ))
                    stack.enter_context(mock.patch.object(
                        controller.subprocess, "run", return_value=clean
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "validate_resume_release_root",
                        return_value=release_root,
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "signed_web_gate",
                        side_effect=(
                            controller.PolicyError("signed Gate differs")
                            if case == "wrong-gate" else None
                        ),
                        return_value=gate,
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "validate_existing_web_result",
                        side_effect=(
                            controller.PolicyError("provider receipt digest differs")
                            if case == "tampered-provider" else None
                        ),
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "verify_production_revision",
                        side_effect=(
                            ["1" * 64, "2" * 64]
                            if case == "server-drift" else None
                        ),
                        return_value="f" * 64,
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "verify_web_after_server"
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "preflight_web_publication",
                        side_effect=(
                            controller.PolicyError("GeoDNS route contract differs")
                            if case == "dns-drift" else None
                        ),
                    ))
                    stack.enter_context(mock.patch.object(
                        controller, "validate_release_authority_after_gate",
                        side_effect=(
                            controller.PolicyError("prune authority changed")
                            if case == "authority-drift" else None
                        ),
                    ))
                    stack.enter_context(mock.patch.object(
                        controller.cloudflare_pruner, "CloudflarePagesAPI",
                        return_value=api,
                    ))
                    run_prune = stack.enter_context(mock.patch.object(
                        controller.cloudflare_pruner, "run_prune",
                        wraps=prune.run_prune,
                    ))
                    with self.assertRaises(controller.PolicyError):
                        controller.command_prune_cloudflare_pages(args)
                self.assertEqual(api.deleted, [])
                run_prune.assert_not_called()

    def test_controller_execute_requires_darwin_arm64_before_delete(self) -> None:
        with self._controller_release(execute=True) as (release_root, gate, args):
            api = FakeAPI()
            with (
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=REVISION),
                mock.patch.object(controller, "prune_authority_paths", return_value=(gate,)),
                mock.patch.object(
                    controller, "capture_release_authority",
                    return_value={str(gate): controller.sha256_file(gate)},
                ),
                mock.patch.object(controller, "validate_release_authority_after_gate"),
                mock.patch.object(
                    controller.subprocess, "run",
                    return_value=mock.Mock(returncode=0, stdout=""),
                ),
                mock.patch.object(
                    controller, "validate_resume_release_root",
                    return_value=release_root,
                ),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "validate_existing_web_result"),
                mock.patch.object(
                    controller, "verify_production_revision",
                    return_value="f" * 64,
                ),
                mock.patch.object(controller, "verify_web_after_server"),
                mock.patch.object(controller, "preflight_web_publication"),
                mock.patch.object(
                    controller.web_publisher, "machine_arch",
                    side_effect=RuntimeError("wrong platform"),
                ),
                mock.patch.object(
                    controller.cloudflare_pruner, "CloudflarePagesAPI",
                    return_value=api,
                ) as api_constructor,
            ):
                with self.assertRaisesRegex(controller.PolicyError, "darwin-arm64"):
                    controller.command_prune_cloudflare_pages(args)
            api_constructor.assert_not_called()
            self.assertEqual(api.deleted, [])

    def test_controller_refuses_prune_when_publication_could_not_prove_previous(self) -> None:
        with self._controller_release() as (release_root, gate, args):
            publication_path = release_root / "web-publication.json"
            publication = json.loads(publication_path.read_text(encoding="utf-8"))
            publication["providers"]["cloudflare_pages"][
                "previous_canonical_deployment_id"
            ] = None
            publication_path.write_text(json.dumps(publication), encoding="utf-8")
            with (
                mock.patch.object(controller, "assert_origin_main"),
                mock.patch.object(controller, "git_revision", return_value=REVISION),
                mock.patch.object(
                    controller.subprocess, "run",
                    return_value=mock.Mock(returncode=0, stdout=""),
                ),
                mock.patch.object(controller, "prune_authority_paths", return_value=(gate,)),
                mock.patch.object(
                    controller, "capture_release_authority",
                    return_value={str(gate): controller.sha256_file(gate)},
                ),
                mock.patch.object(
                    controller, "validate_resume_release_root",
                    return_value=release_root,
                ),
                mock.patch.object(controller, "signed_web_gate", return_value=gate),
                mock.patch.object(controller, "validate_existing_web_result"),
                mock.patch.object(
                    controller.cloudflare_pruner, "CloudflarePagesAPI"
                ) as api_constructor,
            ):
                with self.assertRaisesRegex(
                    controller.PolicyError, "does not prove a previous canonical"
                ):
                    controller.command_prune_cloudflare_pages(args)
            api_constructor.assert_not_called()

    def test_combined_result_cannot_substitute_an_arbitrary_previous_id(self) -> None:
        import tempfile
        import hashlib

        with tempfile.TemporaryDirectory() as temporary:
            release_root = Path(temporary)
            dist = release_root / "dist"
            dist.mkdir()
            marker = {"source_revision": REVISION}
            (dist / "vane-release.json").write_text(
                json.dumps(marker), encoding="utf-8"
            )
            artifact = release_root / "web-release-receipt.json"
            artifact.write_text("artifact\n", encoding="utf-8")
            artifact_digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            cf_receipt = {
                "deployment_id": NEW_ID,
                "previous_canonical_deployment_id": PREVIOUS_ID,
                "source_sha": REVISION,
            }
            ali_receipt = {"source_sha": REVISION}
            for filename, value in (
                ("web-cloudflare-receipt.json", cf_receipt),
                ("web-aliyun-receipt.json", ali_receipt),
            ):
                (release_root / filename).write_text(
                    json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
                    encoding="utf-8",
                )
            cf_digest = controller.sha256_file(
                release_root / "web-cloudflare-receipt.json"
            )
            ali_digest = controller.sha256_file(
                release_root / "web-aliyun-receipt.json"
            )
            result_path = release_root / "web-publication.json"
            result = {
                "schema": "vane.web-publication/v2",
                "revision": REVISION,
                "artifact_receipt_sha256": artifact_digest,
                "marker": marker,
                "providers": {
                    "cloudflare_pages": {
                        **cf_receipt, "receipt_sha256": cf_digest,
                    },
                    "aliyun": {**ali_receipt, "receipt_sha256": ali_digest},
                },
                "status": "published",
            }
            result_path.write_text(json.dumps(result), encoding="utf-8")
            gate = release_root / "gate.json"
            gate.write_text("{}\n", encoding="utf-8")
            with mock.patch.object(
                controller, "validated_web_dist", return_value=dist
            ):
                controller.validate_existing_web_result(
                    revision=REVISION,
                    release_root=release_root,
                    gate_evidence=gate,
                )
                result["providers"]["cloudflare_pages"][
                    "previous_canonical_deployment_id"
                ] = candidate_id(999)
                result_path.write_text(json.dumps(result), encoding="utf-8")
                with self.assertRaisesRegex(
                    controller.PolicyError, "differs from its exact receipt"
                ):
                    controller.validate_existing_web_result(
                        revision=REVISION,
                        release_root=release_root,
                        gate_evidence=gate,
                    )

    def _controller_release(self, *, execute: bool = False):
        class ReleaseFixture:
            def __init__(fixture_self, owner: "CloudflarePagesPruneTest") -> None:
                fixture_self.directory = None

            def __enter__(fixture_self):
                import tempfile
                fixture_self.directory = tempfile.TemporaryDirectory()
                root = Path(fixture_self.directory.name) / f"release-{REVISION}"
                root.mkdir()
                gate = root / "full-gate.json"
                gate.write_text("{}\n", encoding="utf-8")
                (root / "web-publication.json").write_text(json.dumps({
                    "providers": {"cloudflare_pages": {
                        "deployment_id": NEW_ID,
                        "previous_canonical_deployment_id": PREVIOUS_ID,
                        "source_sha": REVISION,
                    }}
                }), encoding="utf-8")
                args = controller.parser().parse_args([
                    "prune-cloudflare-pages",
                    "--sha", REVISION,
                    "--release-root", str(root),
                    "--expected-total", "5",
                    "--expected-candidate-count", "3",
                    *(["--expected-manifest-sha256", "a" * 64, "--execute"] if execute else []),
                ])
                return root, gate, args

            def __exit__(fixture_self, *_: object) -> None:
                fixture_self.directory.cleanup()

        return ReleaseFixture(self)

    def test_api_lists_every_page_and_rejects_incomplete_results(self) -> None:
        api = prune.CloudflarePagesAPI("account", "token")
        all_deployments = [
            deployment(candidate_id(1000 + index), ordinal=index)
            for index in range(51)
        ]
        pages = [all_deployments[:25], all_deployments[25:50], all_deployments[50:]]
        seen_pages = []

        def request(_method, _path, *, query=None):
            page = query["page"]
            seen_pages.append(page)
            return {
                "success": True,
                "result": copy.deepcopy(pages[page - 1]),
                "result_info": {
                    "page": page,
                    "per_page": 25,
                    "count": len(pages[page - 1]),
                    "total_count": 51,
                    "total_pages": 3,
                },
            }

        api._request = request
        self.assertEqual(len(api.list_deployments()), 51)
        self.assertEqual(seen_pages, [1, 2, 3])

        def incomplete(_method, _path, *, query=None):
            return {
                "success": True,
                "result": [],
                "result_info": {
                    "page": 1, "per_page": 25, "count": 0,
                    "total_count": 1, "total_pages": 1,
                },
            }

        api._request = incomplete
        with self.assertRaisesRegex(prune.PruneError, "pagination was incomplete"):
            api.list_deployments()

    def test_manifest_is_deterministic_and_does_not_copy_secrets(self) -> None:
        api = FakeAPI(candidate_count=50)
        first, first_digest = plan(api)
        api.deployments.reverse()
        second, second_digest = plan(api)
        self.assertEqual(first, second)
        self.assertEqual(first_digest, second_digest)
        self.assertEqual(len(first["candidates"]), 50)
        self.assertEqual(first["expected_total"], 52)
        self.assertEqual(first["expected_revision"], REVISION)
        self.assertEqual(first["project_authority"]["source"], None)
        self.assertEqual(
            first["project_authority"]["domains"],
            ["vane-web.pages.dev", DOMAIN],
        )
        serialized = json.dumps(first, sort_keys=True)
        self.assertNotIn("do-not-leak", serialized)
        self.assertNotIn("env_vars", serialized)
        self.assertEqual(first_digest, prune.manifest_sha256(first))

    def test_accepts_cloudflare_direct_upload_shape_with_source_omitted(self) -> None:
        api = FakeAPI()
        del api.project_value["source"]

        manifest, digest = plan(api)

        self.assertEqual(manifest["project_authority"]["source"], None)
        self.assertEqual(digest, prune.manifest_sha256(manifest))

    def test_dry_run_emits_reviewable_plan_without_delete(self) -> None:
        api = FakeAPI()
        _, digest = plan(api)
        events = []
        result = prune.run_prune(
            api,
            new_canonical_id=NEW_ID,
            previous_canonical_id=PREVIOUS_ID,
            expected_revision=REVISION,
            expected_total=5,
            expected_candidate_count=3,
            expected_manifest_sha256=None,
            emit=events.append,
        )
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["deleted_count"], 0)
        self.assertEqual(api.deleted, [])
        self.assertEqual(events[0]["event"], "cloudflare_pages_prune_plan")
        self.assertEqual(events[0]["summary"]["manifest_sha256"], digest)

    def test_execute_requires_reviewed_manifest_digest_before_delete(self) -> None:
        api = FakeAPI()
        with self.assertRaisesRegex(
            prune.PruneError, "execute requires the reviewed manifest"
        ):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=None,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(api.deleted, [])

    def test_manifest_mismatch_emits_plan_then_fails_before_delete(self) -> None:
        api = FakeAPI()
        events = []
        with self.assertRaisesRegex(prune.PruneError, "differs from expectation"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256="f" * 64,
                execute=True,
                emit=events.append,
            )
        self.assertEqual(len(events), 1)
        self.assertEqual(api.deleted, [])

    def test_rejects_wrong_authority_or_inactive_domain(self) -> None:
        api = FakeAPI()
        api.project_value["latest_deployment"] = {"id": PREVIOUS_ID}
        with self.assertRaisesRegex(prune.PruneError, "canonical/latest"):
            plan(api)
        api = FakeAPI()
        api.domain_value["status"] = "pending"
        with self.assertRaisesRegex(prune.PruneError, "not active"):
            plan(api)

    def test_rejects_non_exact_revision_and_project_authority(self) -> None:
        api = FakeAPI()
        with self.assertRaisesRegex(prune.PruneError, "exact 40-character"):
            prune.build_manifest(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision="A" * 40,
                expected_total=5,
                expected_candidate_count=3,
            )
        cases = {
            "git-bound": lambda api: api.project_value.update(
                source={"type": "github"}
            ),
            "wrong production branch": lambda api: api.project_value.update(
                production_branch="release"
            ),
            "extra domain": lambda api: api.project_value["domains"].append(
                "unexpected.example.com"
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                api = FakeAPI()
                mutate(api)
                with self.assertRaises(prune.PruneError):
                    plan(api)

    def test_rejects_non_exact_new_canonical_metadata_everywhere(self) -> None:
        cases = {
            "project dirty": lambda api: api.project_value[
                "canonical_deployment"
            ]["deployment_trigger"]["metadata"].update(commit_dirty=True),
            "project wrong sha": lambda api: api.project_value[
                "latest_deployment"
            ]["deployment_trigger"]["metadata"].update(commit_hash="f" * 40),
            "project wrong branch": lambda api: api.project_value[
                "canonical_deployment"
            ]["deployment_trigger"]["metadata"].update(branch="release"),
            "project extra alias": lambda api: api.project_value[
                "latest_deployment"
            ]["aliases"].append("https://extra.example.com"),
            "listed dirty": lambda api: api.deployments[0][
                "deployment_trigger"
            ]["metadata"].update(commit_dirty=True),
            "listed wrong sha": lambda api: api.deployments[0][
                "deployment_trigger"
            ]["metadata"].update(commit_hash="f" * 40),
            "listed wrong branch": lambda api: api.deployments[0][
                "deployment_trigger"
            ]["metadata"].update(branch="release"),
            "listed extra alias": lambda api: api.deployments[0]["aliases"].append(
                "https://extra.example.com"
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                api = FakeAPI()
                mutate(api)
                with self.assertRaises(prune.PruneError):
                    plan(api)

    def test_domain_evidence_is_required_and_fail_closed(self) -> None:
        for field in ("validation_data", "verification_data"):
            with self.subTest(field=field, state="missing"):
                api = FakeAPI()
                del api.domain_value[field]
                with self.assertRaisesRegex(prune.PruneError, field):
                    plan(api)
            with self.subTest(field=field, state="pending"):
                api = FakeAPI()
                api.domain_value[field]["status"] = "pending"
                with self.assertRaisesRegex(prune.PruneError, field):
                    plan(api)

    def test_rejects_invalid_keep_and_candidate_states(self) -> None:
        cases = {
            "keep preview": lambda api: api.deployments[1].update(environment="preview"),
            "candidate preview": lambda api: api.deployments[2].update(environment="preview"),
            "candidate failure": lambda api: api.deployments[2]["latest_stage"].update(status="failure"),
            "candidate alias": lambda api: api.deployments[2].update(aliases=["https://old.example"]),
            "candidate active": lambda api: api.deployments[2]["latest_stage"].update(status="active"),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                api = FakeAPI()
                mutate(api)
                with self.assertRaises(prune.PruneError):
                    plan(api)

    def test_rejects_missing_custom_alias_on_new_canonical(self) -> None:
        api = FakeAPI()
        api.deployments[0]["aliases"] = None
        with self.assertRaisesRegex(prune.PruneError, "custom-domain alias"):
            plan(api)

    def test_execute_deletes_each_candidate_and_finishes_with_two_keeps(self) -> None:
        api = FakeAPI(candidate_count=50)
        manifest, digest = plan(api)
        events = []
        result = prune.run_prune(
            api,
            new_canonical_id=NEW_ID,
            previous_canonical_id=PREVIOUS_ID,
            expected_revision=REVISION,
            expected_total=52,
            expected_candidate_count=50,
            expected_manifest_sha256=digest,
            execute=True,
            emit=events.append,
        )
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["deleted_count"], 50)
        self.assertEqual(api.deleted, [item["id"] for item in manifest["candidates"]])
        self.assertEqual({item["id"] for item in api.deployments}, {NEW_ID, PREVIOUS_ID})
        self.assertEqual(events[-1]["event"], "cloudflare_pages_prune_complete")

    def test_delete_failure_stops_before_later_candidates(self) -> None:
        api = FakeAPI()
        manifest, digest = plan(api)
        api.fail_on = manifest["candidates"][1]["id"]
        with self.assertRaisesRegex(prune.PruneError, "fixture delete failed"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=digest,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(api.deleted, [manifest["candidates"][0]["id"]])

    def test_execute_stops_if_deleted_id_remains_visible(self) -> None:
        api = FakeAPI()
        _, digest = plan(api)
        api.sticky_delete = True
        with self.assertRaisesRegex(prune.PruneError, "changed during pruning"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=digest,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(len(api.deleted), 1)

    def test_execute_stops_if_canonical_or_latest_drifts(self) -> None:
        api = FakeAPI()
        _, digest = plan(api)
        api.drift_after_delete = True
        with self.assertRaisesRegex(prune.PruneError, "canonical/latest"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=digest,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(len(api.deleted), 1)

    def test_execute_revalidates_remaining_candidates_after_each_delete(self) -> None:
        api = FakeAPI()
        _, digest = plan(api)
        api.alias_candidate_after_delete = True
        with self.assertRaisesRegex(prune.PruneError, "is aliased"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=digest,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(len(api.deleted), 1)

    def test_execute_revalidates_exact_new_revision_after_each_delete(self) -> None:
        api = FakeAPI()
        _, digest = plan(api)
        api.dirty_new_after_delete = True
        with self.assertRaisesRegex(prune.PruneError, "clean exact"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=digest,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(len(api.deleted), 1)

    def test_execute_revalidates_domain_evidence_after_each_delete(self) -> None:
        api = FakeAPI()
        _, digest = plan(api)
        api.domain_pending_after_delete = True
        with self.assertRaisesRegex(prune.PruneError, "verification_data"):
            prune.run_prune(
                api,
                new_canonical_id=NEW_ID,
                previous_canonical_id=PREVIOUS_ID,
                expected_revision=REVISION,
                expected_total=5,
                expected_candidate_count=3,
                expected_manifest_sha256=digest,
                execute=True,
                emit=lambda _event: None,
            )
        self.assertEqual(len(api.deleted), 1)

    def test_delete_request_has_no_force_query(self) -> None:
        api = prune.CloudflarePagesAPI("account", "token")
        calls = []

        def request(method, path, *, query=None):
            calls.append((method, path, query))
            return {"success": True, "result": {}}

        api._request = request
        api.delete_deployment(candidate_id(999))
        self.assertEqual(calls, [(
            "DELETE",
            f"{api.project_path}/deployments/{candidate_id(999)}",
            None,
        )])


if __name__ == "__main__":
    unittest.main()
