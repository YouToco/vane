import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"


class BackendGateTests(unittest.TestCase):
    def test_store_gate_uses_source_authoritative_isolated_shards(self) -> None:
        workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn(
            "go run ./cmd/storetestshard run \\\n",
            workflow,
        )
        for shard in range(3):
            self.assertIn(
                f"job.services.postgres_{shard}.ports['5432']", workflow
            )
        self.assertIn("job.services.postgres_rest.ports['5432']", workflow)
        self.assertIn(
            'store_package="$(go list ./store)"', workflow
        )
        self.assertIn(
            'go list ./... | grep -Fvx "$store_package"', workflow
        )
        self.assertNotIn(
            "-coverprofile=coverage.txt -covermode=atomic ./...", workflow
        )

    def test_each_postgres_shard_uses_an_ephemeral_host_port(self) -> None:
        workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")

        self.assertEqual(workflow.count("- 5432/tcp"), 4)
        self.assertNotIn("- 5432:5432", workflow)


if __name__ == "__main__":
    unittest.main()
