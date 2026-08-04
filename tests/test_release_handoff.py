import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
CACHE_SHA = "55cc8345863c7cc4c66a329aec7e433d2d1c52a9"


class ReleaseHandoffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_release_does_not_depend_on_github_artifact_quota(self) -> None:
        self.assertNotIn("actions/upload-artifact", self.workflow)
        self.assertNotIn("actions/download-artifact", self.workflow)
        self.assertEqual(
            self.workflow.count(f"actions/cache/save@{CACHE_SHA}"), 2
        )
        self.assertEqual(
            self.workflow.count(f"actions/cache/restore@{CACHE_SHA}"), 4
        )

    def test_handoffs_are_exact_to_run_component_and_source(self) -> None:
        for component in ("backend", "frontend"):
            key = (
                "vane-release-v1-${{ github.run_id }}-"
                f"{component}-${{{{ needs.plan.outputs.{component}_sha }}}}"
            )
            self.assertEqual(self.workflow.count(f"key: {key}"), 3)
            self.assertEqual(self.workflow.count(
                f"path: release-handoff/{component}"
            ), 3)

        self.assertNotIn("restore-keys:", self.workflow)
        self.assertNotIn(
            "vane-release-v1-${{ github.run_id }}-${{ github.run_attempt }}",
            self.workflow,
        )
        self.assertEqual(self.workflow.count("lookup-only: true"), 2)
        self.assertEqual(self.workflow.count("fail-on-cache-miss: true"), 4)

    def test_restored_bytes_still_cross_strict_validation(self) -> None:
        for component in ("backend", "frontend"):
            pattern = re.compile(
                rf"artifact\.py\" validate[\s\S]*?"
                rf"--component {component}[\s\S]*?"
                rf"--input \"\$HANDOFF_ROOT/{component}\"[\s\S]*?"
                rf"--output \"\$DEPLOY_ROOT/verified/{component}\""
            )
            self.assertRegex(self.workflow, pattern)

    def test_deploy_fails_fast_when_cache_codec_is_missing(self) -> None:
        prereq = "- name: Verify release handoff cache prerequisites"
        restore = "- name: Restore backend release handoff from this run"
        self.assertIn(prereq, self.workflow)
        self.assertLess(self.workflow.index(prereq), self.workflow.index(restore))
        self.assertIn("command -v tar >/dev/null", self.workflow)
        self.assertIn("command -v zstd >/dev/null", self.workflow)
        self.assertIn("zstd --version", self.workflow)

    def test_self_hosted_handoff_roots_are_recreated_private(self) -> None:
        self.assertEqual(
            self.workflow.count('rm -rf -- "$HANDOFF_ROOT"'), 4
        )
        self.assertEqual(
            self.workflow.count('install -d -m 0700 "$HANDOFF_ROOT"'), 2
        )

    def test_untrusted_build_job_never_receives_production_secrets(self) -> None:
        build, deploy = self.workflow.split("\n  deploy:\n", maxsplit=1)
        for secret in (
            "VPS_SSH_KEY",
            "VPS_HOST",
            "CLOUDFLARE_API_TOKEN",
            "ALIYUN_ACCESS_KEY_SECRET",
        ):
            self.assertNotIn(secret, build)
            self.assertIn(secret, deploy)


if __name__ == "__main__":
    unittest.main()
