import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
CERT_WORKFLOW = ROOT / ".github" / "workflows" / "cert-renew.yml"
ACTIONLINT_CONFIG = ROOT / ".github" / "actionlint.yaml"

EXPECTED_LABELS = {"self-hosted", "Linux", "vps-primary", "vane-deploy"}


def job_block(workflow: str, job_name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(job_name)}:\n(.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)",
        workflow,
    )
    if match is None:
        raise AssertionError(f"job not found: {job_name}")
    return match.group(1)


def runner_labels(block: str) -> set[str]:
    match = re.search(r"(?m)^    runs-on:\s*\[([^\]]+)\]\s*$", block)
    if match is None:
        raise AssertionError("job must use an inline, auditable runs-on label list")
    return {label.strip().strip("\"'") for label in match.group(1).split(",")}


class DeployStateOwnerTests(unittest.TestCase):
    def test_plan_and_deploy_share_one_durable_state_owner(self) -> None:
        workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

        self.assertEqual(runner_labels(job_block(workflow, "plan")), EXPECTED_LABELS)
        self.assertEqual(
            runner_labels(job_block(workflow, "deploy")), EXPECTED_LABELS
        )

    def test_certificate_state_uses_the_same_owner(self) -> None:
        workflow = CERT_WORKFLOW.read_text(encoding="utf-8")

        self.assertEqual(runner_labels(job_block(workflow, "renew")), EXPECTED_LABELS)

    def test_build_uses_the_isolated_build_role(self) -> None:
        workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
        build = job_block(workflow, "build")

        self.assertIn(
            "    runs-on: [self-hosted, Linux, vane-build]\n", build
        )
        self.assertNotIn("ubuntu-24.04", build)

    def test_control_plane_pr_and_main_trust_domains_are_separate(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn('["self-hosted","Linux","vane-test"]', workflow)
        self.assertIn('["self-hosted","Linux","vane-build"]', workflow)
        self.assertNotIn("ubuntu-24.04", workflow)
        self.assertNotIn("hosted-runner-smoke", workflow)

    def test_actionlint_knows_the_primary_runner_label(self) -> None:
        config = ACTIONLINT_CONFIG.read_text(encoding="utf-8")

        self.assertIn("    - vps-primary\n", config)


if __name__ == "__main__":
    unittest.main()
