import json
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from deploy.config import load_target
from deploy.render import render_deployment


ROOT = Path(__file__).resolve().parents[2]
MANIFESTS = (
    "app.yaml",
    "app.yml",
    "mcp-apps/storetime/app.yaml",
    "mcp-apps/storetime/app.yml",
    "mcp-apps/opstask/app.yaml",
    "mcp-apps/opstask/app.yml",
)


def manifest_env(path: Path) -> dict[str, str]:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {entry["name"]: str(entry["value"]) for entry in manifest["env"]}


def write_source_templates(source_root: Path) -> None:
    for relative_path in MANIFESTS:
        path = source_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('command: ["python", "-m", "scripts.start_app"]\nenv: []\n', encoding="utf-8")


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
                        "target": "field_eng",
                        "warehouse_id": "field-eng-warehouse-id",
                        "genie_space_id": "field-eng-genie-id",
                        "storetime_mcp_url": (
                            "https://gurary-bobabricks-storetime-mcp-"
                            "1444828305810485.aws.databricksapps.com/mcp"
                        ),
                        "opstask_mcp_url": (
                            "https://gurary-bobabricks-opstask-mcp-"
                            "1444828305810485.aws.databricksapps.com/mcp"
                        ),
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
                                root_env["STORETIME_MCP_URL"],
                                "https://gurary-bobabricks-storetime-mcp-"
                                "1444828305810485.aws.databricksapps.com/mcp",
                            )
                            self.assertIn("LAKEBASE_ENDPOINT", root_env)

        status_after = subprocess.run(
            ["git", "status", "--short"], cwd=ROOT, check=True, text=True, capture_output=True
        ).stdout
        self.assertEqual(status_after, status_before)

    def test_rejects_untrusted_or_incomplete_generated_state(self):
        invalid_states = (
            ("fevm", {"target": "fevm", "warehouse_id": "attacker-warehouse"}),
            ("field_eng", {"target": "fevm"}),
            ("field_eng", {"target": "field_eng", "warehouse_id": []}),
            ("field_eng", {"target": "field_eng", "unexpected": "value"}),
            ("field_eng", {"target": "field_eng", "lakebase": {"validated": True}}),
            (
                "fevm",
                {
                    "target": "fevm",
                    "lakebase": {
                        "validated": True,
                        "project": "attacker-project",
                        "branch": "attacker-branch",
                        "endpoint": "attacker-endpoint",
                        "database": "attacker-database",
                        "schema": "attacker-schema",
                    },
                },
            ),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            state_root = temporary_root / "state"
            state_root.mkdir()
            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", state_root
            ):
                for target_key, generated_state in invalid_states:
                    with self.subTest(target=target_key, state=generated_state):
                        (state_root / f"{target_key}.json").write_text(
                            json.dumps(generated_state), encoding="utf-8"
                        )
                        with self.assertRaises(ValueError):
                            render_deployment(target_key, "baseline")

    def test_scans_all_rendered_text_and_rejects_source_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            source_root = temporary_root / "source"
            source_root.mkdir()
            write_source_templates(source_root)
            state_root = temporary_root / "state"
            state_root.mkdir()
            build_root = temporary_root / "build"

            (source_root / "notes.txt").write_text(
                "bobabricks-store-ops-demo", encoding="utf-8"
            )
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", build_root
            ), patch("deploy.render.STATE_ROOT", state_root):
                with self.assertRaises(ValueError):
                    render_deployment("fevm", "baseline")

            (source_root / "notes.txt").unlink()
            outside_file = temporary_root / "outside.txt"
            outside_file.write_text("outside", encoding="utf-8")
            (source_root / "linked.txt").symlink_to(outside_file)
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", build_root
            ), patch("deploy.render.STATE_ROOT", state_root):
                with self.assertRaises(ValueError):
                    render_deployment("fevm", "baseline")

    def test_replaces_build_pointer_without_a_missing_path(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", temporary_root / "state"
            ):
                build_path = render_deployment("fevm", "baseline")
                self.assertTrue(build_path.is_symlink())
                missing_path_observed = []
                stop_reading = threading.Event()

                def read_build_repeatedly():
                    while not stop_reading.is_set():
                        try:
                            (build_path / "app.yaml").read_text(encoding="utf-8")
                        except OSError as error:
                            missing_path_observed.append(error)

                reader = threading.Thread(target=read_build_repeatedly)
                reader.start()
                try:
                    render_deployment("fevm", "baseline")
                finally:
                    stop_reading.set()
                    reader.join()
                self.assertEqual(missing_path_observed, [])


if __name__ == "__main__":
    unittest.main()
