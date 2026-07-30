import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"
CERT_WORKFLOW = ROOT / ".github" / "workflows" / "cert-renew.yml"
ACTIONLINT_CONFIG = ROOT / ".github" / "actionlint.yaml"

EXPECTED_LABELS = {"self-hosted", "Linux", "mac-mini", "vane-deploy"}


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

    def test_actionlint_knows_the_primary_runner_label(self) -> None:
        config = ACTIONLINT_CONFIG.read_text(encoding="utf-8")

        self.assertIn("    - mac-mini\n", config)


if __name__ == "__main__":
    unittest.main()
