import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from deploy.config import load_target
from deploy.render import render_deployment


ROOT = Path(__file__).resolve().parents[2]


def manifest_env(path: Path) -> dict[str, str]:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {entry["name"]: str(entry["value"]) for entry in manifest["env"]}


class DeploymentRenderTest(unittest.TestCase):
    def test_renders_each_target_and_state_without_mutating_source(self):
        status_before = subprocess.run(
            ["git", "status", "--short"], cwd=ROOT, check=True, text=True, capture_output=True
        ).stdout

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            state_root = temporary_root / "state"
            state_root.mkdir()
            (state_root / "field_eng.json").write_text(
                json.dumps(
                    {
                        "warehouse_id": "field-eng-warehouse-id",
                        "genie_space_id": "field-eng-genie-id",
                        "storetime_mcp_url": "https://field-eng-storetime.example/mcp",
                        "opstask_mcp_url": "https://field-eng-opstask.example/mcp",
                        "lakebase": {
                            "validated": True,
                            "project": "gurary-bobabricks",
                            "branch": "production",
                            "endpoint": "primary",
                            "database": "databricks_postgres",
                            "schema": "gurary_bobabricks_app",
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", state_root
            ):
                rendered = {
                    (target_key, state_key): render_deployment(target_key, state_key)
                    for target_key in ("fevm", "field_eng")
                    for state_key in ("baseline", "upgraded")
                }

                self.assertEqual(
                    rendered[("fevm", "baseline")], temporary_root / "build" / "fevm" / "baseline"
                )

                fevm_baseline = rendered[("fevm", "baseline")]
                fevm_baseline_before = (fevm_baseline / "app.yaml").read_bytes()
                render_deployment("fevm", "upgraded")
                self.assertEqual((fevm_baseline / "app.yaml").read_bytes(), fevm_baseline_before)

                for (target_key, state_key), build_path in rendered.items():
                    with self.subTest(target=target_key, state=state_key):
                        target = load_target(target_key)
                        root_env = manifest_env(build_path / "app.yaml")
                        self.assertEqual(root_env, manifest_env(build_path / "app.yml"))
                        self.assertEqual(root_env["AGENT_MODEL"], "databricks-gpt-5")
                        self.assertEqual(root_env["MLFLOW_TRACE_UC_TABLE_PREFIX"], "gurary_bobabricks")
                        self.assertEqual(root_env["DATABRICKS_CATALOG"], target.catalog)
                        self.assertIn("DATABRICKS_TABLE_PREFIX", root_env)
                        self.assertIn("DATABRICKS_WAREHOUSE_ID", root_env)
                        self.assertIn("GENIE_SPACE_ID", root_env)
                        self.assertEqual(
                            root_env["SHARED_MCP_READ_ONLY"], str(target.shared_mcp_read_only).lower()
                        )

                        for relative_path in (
                            "app.yaml",
                            "app.yml",
                            "mcp-apps/storetime/app.yaml",
                            "mcp-apps/storetime/app.yml",
                            "mcp-apps/opstask/app.yaml",
                            "mcp-apps/opstask/app.yml",
                        ):
                            rendered_text = (build_path / relative_path).read_text(encoding="utf-8")
                            self.assertNotIn("ad341da9-d12e-4688-ad1c-3c049cf70486", rendered_text)
                            self.assertNotIn("bobabricks-store-ops-demo", rendered_text)

                        if state_key == "baseline":
                            self.assertNotIn("CONFLUENCE_MCP_ENABLED", root_env)
                            self.assertNotIn("ATLASSIAN_MCP_URL", root_env)
                        else:
                            self.assertEqual(root_env["CONFLUENCE_MCP_ENABLED"], "true")
                            self.assertTrue(
                                root_env["ATLASSIAN_MCP_URL"].endswith(
                                    "/api/2.0/mcp/external/system_ai_agent_atlassian_mcp"
                                )
                            )
                            self.assertTrue(root_env["ATLASSIAN_MCP_URL"].startswith(target.host))

                        if target_key == "field_eng":
                            self.assertEqual(root_env["DATABRICKS_WAREHOUSE_ID"], "field-eng-warehouse-id")
                            self.assertEqual(root_env["GENIE_SPACE_ID"], "field-eng-genie-id")
                            self.assertEqual(
                                root_env["STORETIME_MCP_URL"], "https://field-eng-storetime.example/mcp"
                            )
                            self.assertIn("LAKEBASE_ENDPOINT", root_env)

        status_after = subprocess.run(
            ["git", "status", "--short"], cwd=ROOT, check=True, text=True, capture_output=True
        ).stdout
        self.assertEqual(status_after, status_before)


if __name__ == "__main__":
    unittest.main()
