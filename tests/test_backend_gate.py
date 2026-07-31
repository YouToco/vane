import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"


class BackendGateTests(unittest.TestCase):
    def test_shared_database_race_gate_is_not_randomized(self) -> None:
        workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn(
            "go test -race -count=1 -timeout=25m \\\n", workflow
        )
        self.assertNotIn("-shuffle", workflow)
        self.assertIn(
            "-coverprofile=coverage.txt -covermode=atomic ./...", workflow
        )

    def test_postgres_uses_an_ephemeral_host_port(self) -> None:
        workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("- 5432/tcp", workflow)
        self.assertNotIn("- 5432:5432", workflow)
        self.assertIn("job.services.postgres.ports['5432']", workflow)


if __name__ == "__main__":
    unittest.main()
