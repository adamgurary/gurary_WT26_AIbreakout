import json
from hashlib import sha256
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from deploy.config import load_target
from deploy import render
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


def field_eng_state(storetime_url: str | None = None) -> dict:
    return {
        "target": "field_eng",
        "presenter_app_id": "field-eng-presenter-id",
        "presenter_app_url": "https://gurary-bobabricks-store-ops-1444828305810485.aws.databricksapps.com",
        "presenter_service_principal_client_id": "field-eng-presenter-client-id",
        "storetime_app_id": "field-eng-storetime-id",
        "storetime_service_principal_client_id": "field-eng-storetime-client-id",
        "opstask_app_id": "field-eng-opstask-id",
        "opstask_service_principal_client_id": "field-eng-opstask-client-id",
        "warehouse_id": "field-eng-warehouse-id",
        "genie_space_id": "field-eng-genie-id",
        "storetime_mcp_url": storetime_url
        or "https://gurary-bobabricks-storetime-mcp-1444828305810485.aws.databricksapps.com/mcp",
        "opstask_mcp_url": "https://gurary-bobabricks-opstask-mcp-1444828305810485.aws.databricksapps.com/mcp",
        "mlflow_experiment_name": "/Shared/gurary-bobabricks-store-ops-uc",
        "lakebase": {
            "validated": True,
            "enabled": True,
            "project": "gurary-bobabricks",
            "branch": "production",
            "endpoint": "primary",
            "database": "databricks_postgres",
            "host": "field-eng-lakebase.database.databricks.com",
            "schema": "gurary_bobabricks_app",
        },
    }


def enabled_fevm_state() -> dict:
    return {
        "target": "fevm",
        "mlflow_experiment_name": "/Shared/gurary-bobabricks-store-ops-uc",
        "lakebase": {
            "validated": True,
            "enabled": True,
            "workspace_id": "7474657163903557",
            "workspace_host": "https://fevm-worldtour-ai.cloud.databricks.com",
            "project": "existing-fevm-project",
            "branch": "production",
            "endpoint": "primary",
            "database": "databricks_postgres",
            "host": "fevm-lakebase.database.databricks.com",
            "schema": "gurary_bobabricks_app",
        },
    }


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
                    field_eng_state()
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

            (source_root / "linked.txt").unlink()
            for relative_path in ("deploy/unsafe.txt", "tests/unsafe.txt"):
                unsafe_file = source_root / relative_path
                unsafe_file.parent.mkdir(parents=True, exist_ok=True)
                unsafe_file.write_text("bobabricks-store-ops-demo", encoding="utf-8")
                with patch("deploy.render.ROOT", source_root), patch(
                    "deploy.render.BUILD_ROOT", build_root
                ), patch("deploy.render.STATE_ROOT", state_root):
                    with self.assertRaises(ValueError):
                        render_deployment("fevm", "baseline")
                unsafe_file.unlink()

    def test_excludes_generated_live_inventory_from_rendered_source(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            source_root = temporary_root / "source"
            source_root.mkdir()
            write_source_templates(source_root)
            inventory_path = source_root / "deploy" / "inventory" / "live-fevm-before.json"
            inventory_path.parent.mkdir(parents=True)
            inventory_path.write_text(
                json.dumps(
                    {"shared_principal": "ad341da9-d12e-4688-ad1c-" "3c049cf70486"}
                ),
                encoding="utf-8",
            )

            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", temporary_root / "build"
            ), patch("deploy.render.STATE_ROOT", temporary_root / "state"):
                rendered = render_deployment("fevm", "baseline")

            self.assertFalse((rendered / "deploy" / "inventory").exists())

    def test_rejects_new_forbidden_occurrence_in_safe_reference_files(self):
        for relative_path in ("README.md", "deploy/render.py", "tests/deploy/test_render.py"):
            with self.subTest(path=relative_path), tempfile.TemporaryDirectory() as temporary_directory:
                temporary_root = Path(temporary_directory)
                source_root = temporary_root / "source"
                source_root.mkdir()
                write_source_templates(source_root)
                unsafe_file = source_root / relative_path
                unsafe_file.parent.mkdir(parents=True, exist_ok=True)
                unsafe_file.write_text(
                    "ACTIVE_APP_NAME=bobabricks-store-ops-demo\n", encoding="utf-8"
                )
                with patch("deploy.render.ROOT", source_root), patch(
                    "deploy.render.BUILD_ROOT", temporary_root / "build"
                ), patch("deploy.render.STATE_ROOT", temporary_root / "state"):
                    with self.assertRaises(ValueError):
                        render_deployment("fevm", "baseline")

    def test_rejects_duplicate_of_bounded_safe_reference_occurrence(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            source_root = temporary_root / "source"
            source_root.mkdir()
            write_source_templates(source_root)
            renderer = source_root / "deploy" / "render.py"
            renderer.parent.mkdir(parents=True)
            allowed_guard_line = '    "bobabricks-store-ops-demo",\n'
            renderer.write_text(allowed_guard_line * 2, encoding="utf-8")
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", temporary_root / "build"
            ), patch("deploy.render.STATE_ROOT", temporary_root / "state"):
                with self.assertRaises(ValueError):
                    render_deployment("fevm", "baseline")

    def test_multi_identifier_reference_requires_each_exact_policy_key(self):
        first_identifier = "bobabricks-store-ops-" "demo"
        second_identifier = "ad341da9-d12e-4688-ad1c-" "3c049cf70486"
        relative_path = Path("historical-reference.txt")
        line = f"Historical values: {first_identifier} and {second_identifier}"
        fingerprint = sha256(line.encode("utf-8")).hexdigest()

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            source_root = temporary_root / "source"
            source_root.mkdir()
            write_source_templates(source_root)
            (source_root / relative_path).write_text(f"{line}\n", encoding="utf-8")
            first_only_policy = {
                (relative_path, first_identifier, fingerprint): 1,
            }
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", temporary_root / "build"
            ), patch("deploy.render.STATE_ROOT", temporary_root / "state"), patch(
                "deploy.render._SAFE_REFERENCE_LINES", first_only_policy
            ):
                with self.assertRaises(ValueError) as error:
                    render_deployment("fevm", "baseline")
                self.assertIn(second_identifier, str(error.exception))

            complete_policy = {
                **first_only_policy,
                (relative_path, second_identifier, fingerprint): 1,
            }
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", temporary_root / "build"
            ), patch("deploy.render.STATE_ROOT", temporary_root / "state"), patch(
                "deploy.render._SAFE_REFERENCE_LINES", complete_policy
            ):
                render_deployment("fevm", "baseline")

    def test_safe_reference_count_is_bounded_per_exact_policy_key(self):
        first_identifier = "bobabricks-store-ops-" "demo"
        second_identifier = "ad341da9-d12e-4688-ad1c-" "3c049cf70486"
        relative_path = Path("historical-reference.txt")
        line = f"Historical values: {first_identifier} and {second_identifier}"
        fingerprint = sha256(line.encode("utf-8")).hexdigest()

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            source_root = temporary_root / "source"
            source_root.mkdir()
            write_source_templates(source_root)
            (source_root / relative_path).write_text(f"{line}\n{line}\n", encoding="utf-8")
            mismatched_bounds = {
                (relative_path, first_identifier, fingerprint): 2,
                (relative_path, second_identifier, fingerprint): 1,
            }
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", temporary_root / "build"
            ), patch("deploy.render.STATE_ROOT", temporary_root / "state"), patch(
                "deploy.render._SAFE_REFERENCE_LINES", mismatched_bounds
            ):
                with self.assertRaises(ValueError) as error:
                    render_deployment("fevm", "baseline")
                self.assertIn(second_identifier, str(error.exception))

            complete_bounds = {
                key: 2 for key in mismatched_bounds
            }
            with patch("deploy.render.ROOT", source_root), patch(
                "deploy.render.BUILD_ROOT", temporary_root / "build"
            ), patch("deploy.render.STATE_ROOT", temporary_root / "state"), patch(
                "deploy.render._SAFE_REFERENCE_LINES", complete_bounds
            ):
                render_deployment("fevm", "baseline")

    def test_replaces_build_pointer_without_a_missing_path(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", temporary_root / "state"
            ):
                build_path = render_deployment("fevm", "baseline")
                self.assertTrue(build_path.is_symlink())
                initial_version = build_path.resolve()
                versions_root = build_path.parent / ".versions"
                alternate_version = versions_root / "atomicity-alternate"
                alternate_version.mkdir()
                (alternate_version / "app.yaml").write_text("alternate complete tree", encoding="utf-8")
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
                    for publication in range(1_000):
                        render._install_pointer(build_path, build_path.parent, alternate_version)
                        render._install_pointer(build_path, build_path.parent, initial_version)
                finally:
                    stop_reading.set()
                    reader.join()
                self.assertEqual(missing_path_observed, [])

    def test_migrates_legacy_directory_and_replaces_its_pointer_atomically(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            build_root = temporary_root / "build"
            legacy_path = build_root / "fevm" / "baseline"
            legacy_path.mkdir(parents=True)
            (legacy_path / "app.yaml").write_text("legacy complete tree", encoding="utf-8")
            with patch("deploy.render.BUILD_ROOT", build_root), patch(
                "deploy.render.STATE_ROOT", temporary_root / "state"
            ):
                migrated = render_deployment("fevm", "baseline")
                self.assertTrue(migrated.is_symlink())
                self.assertEqual((migrated / "app.yaml").read_text(encoding="utf-8").splitlines()[0], "command:")
                versions = migrated.parent / ".versions"
                self.assertIn("legacy complete tree", [path.read_text(encoding="utf-8") for path in versions.rglob("app.yaml")])
                previous_target = migrated.readlink()
                render_deployment("fevm", "baseline")
                self.assertNotEqual(migrated.readlink(), previous_target)

    def test_generated_state_must_be_complete_and_urls_exact(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            state_root = temporary_root / "state"
            state_root.mkdir()
            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", state_root
            ):
                for state in (
                    {"target": "field_eng"},
                    field_eng_state(field_eng_state()["storetime_mcp_url"] + "?unsafe=true"),
                    field_eng_state(field_eng_state()["storetime_mcp_url"] + "#unsafe"),
                ):
                    (state_root / "field_eng.json").write_text(json.dumps(state), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        render_deployment("field_eng", "baseline")

    def test_preserves_copied_source_and_supports_fevm_memory_outcomes(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            state_root = temporary_root / "state"
            state_root.mkdir()
            disabled = {
                "target": "fevm",
                "mlflow_experiment_name": "/Shared/gurary-bobabricks-store-ops-uc",
                "lakebase": {"validated": True, "enabled": False, "schema": "gurary_bobabricks_app"},
            }
            enabled = enabled_fevm_state()
            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", state_root
            ):
                (state_root / "fevm.json").write_text(json.dumps(disabled), encoding="utf-8")
                disabled_build = render_deployment("fevm", "baseline")
                self.assertEqual(manifest_env(disabled_build / "app.yaml")["BOBABRICKS_DISABLE_LAKEBASE"], "1")
                (state_root / "fevm.json").write_text(json.dumps(enabled), encoding="utf-8")
                enabled_build = render_deployment("fevm", "baseline")
                enabled_env = manifest_env(enabled_build / "app.yaml")
                self.assertEqual(enabled_env["LAKEBASE_HOST"], "fevm-lakebase.database.databricks.com")
                self.assertNotIn("BOBABRICKS_DISABLE_LAKEBASE", enabled_env)
                for relative_path in (
                    "deploy/safety.py",
                    "deploy/render.py",
                    "agent_server/agent.py",
                    "tests/deploy/test_render.py",
                ):
                    self.assertEqual((enabled_build / relative_path).read_bytes(), (ROOT / relative_path).read_bytes())

    def test_enabled_fevm_lakebase_must_be_complete_and_target_affine(self):
        invalid_lakebases = []
        for field, value in (
            ("workspace_id", "1444828305810485"),
            ("workspace_host", "https://e2-demo-field-eng.cloud.databricks.com"),
            ("project", "../escape"),
            ("branch", "bad branch"),
            ("endpoint", "https://endpoint"),
            ("database", "database/name"),
            ("host", "https://attacker.example"),
        ):
            state = enabled_fevm_state()
            state["lakebase"][field] = value
            invalid_lakebases.append(state)
        missing_workspace = enabled_fevm_state()
        del missing_workspace["lakebase"]["workspace_id"]
        invalid_lakebases.append(missing_workspace)
        extra_field = enabled_fevm_state()
        extra_field["lakebase"]["unexpected"] = "value"
        invalid_lakebases.append(extra_field)

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            state_root = temporary_root / "state"
            state_root.mkdir()
            with patch("deploy.render.BUILD_ROOT", temporary_root / "build"), patch(
                "deploy.render.STATE_ROOT", state_root
            ):
                for state in invalid_lakebases:
                    (state_root / "fevm.json").write_text(json.dumps(state), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        render_deployment("fevm", "baseline")


if __name__ == "__main__":
    unittest.main()
