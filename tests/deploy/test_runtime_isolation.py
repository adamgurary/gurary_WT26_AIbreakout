from __future__ import annotations

import ast
import contextlib
import importlib.util
import io
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
PRIVATE_CATALOG = "gurary_" "catalog"
PRIVATE_SCHEMA = "gurary_bobabricks_store_" "ops"
PRIVATE_NAMESPACE = f"{PRIVATE_CATALOG}.{PRIVATE_SCHEMA}"
PRIVATE_PREFIX = "gurary_"
SHARED_CATALOG = "bobabricks_" "demo"
SHARED_SCHEMA = "store_" "ops"

# Importing the presenter constructs a Databricks OpenAI client. Keep that
# construction local and deterministic; no test in this module calls a remote
# service.
os.environ.setdefault(
    "DATABRICKS_HOST", "https://fevm-worldtour-" "ai.cloud.databricks.com"
)
os.environ.setdefault("DATABRICKS_TOKEN", "runtime-isolation-test-token")
os.environ.setdefault("MLFLOW_TRACKING_URI", "file:///tmp/bobabricks-runtime-isolation-mlruns")

from agent_server import agent
from scripts import provision_databricks_assets as provision


def load_server_module(relative_path: str, module_name: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_streamlit_table_client():
    path = ROOT / "app" / "app.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = []
    names = {"DATABRICKS_CATALOG", "DATABRICKS_SCHEMA", "DATABRICKS_TABLE_PREFIX"}
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in names for target in node.targets
        ):
            selected.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "DatabricksSqlResourceClient":
            selected.append(node)
    namespace = {"os": os, "get_workspace_client": lambda: None}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["DatabricksSqlResourceClient"]


class FakeDirectClient:
    def __init__(self, url: str, _token: str):
        self.url = url

    async def list_tools(self) -> list[dict]:
        if "opstask" in self.url:
            return [
                {
                    "name": "list_existing_ops_tasks",
                    "description": "lookup",
                    "inputSchema": {"type": "object", "properties": {}},
                },
                {
                    "name": "create_ops_task",
                    "description": "write",
                    "inputSchema": {"type": "object", "properties": {}},
                },
            ]
        if "/genie/" in self.url:
            return [
                {
                    "name": "query_space_genie-space-id",
                    "description": "query governed metrics",
                    "inputSchema": {"type": "object", "properties": {}},
                },
                {
                    "name": "poll_response_genie-space-id",
                    "description": "poll governed metrics",
                    "inputSchema": {"type": "object", "properties": {}},
                },
            ]
        return [
            {
                "name": "inspect_schedule",
                "description": "inspect",
                "inputSchema": {"type": "object", "properties": {}},
            }
        ]


class FakeAsyncExitStack:
    async def enter_async_context(self, context):
        return context


class FakeSdkMcpServer:
    def __init__(self, *, url, name, workspace_client):
        self.url = url
        self.name = name
        self.workspace_client = workspace_client


class RuntimeToolIsolationTest(unittest.IsolatedAsyncioTestCase):
    def test_utils_import_is_silent(self):
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("LAKEBASE_") and key != "BOBABRICKS_DISABLE_LAKEBASE"
        }
        result = subprocess.run(
            [sys.executable, "-c", "import agent_server.utils"],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_unconfigured_lakebase_is_diagnosed_when_session_requested(self):
        with patch.dict(os.environ, {"BOBABRICKS_DISABLE_LAKEBASE": "0"}):
            with self.assertLogs("agent_server.utils", level="WARNING") as captured:
                session = agent.maybe_create_session("runtime-diagnostic")

        self.assertIsNone(session)
        self.assertEqual(
            captured.output,
            [
                "WARNING:agent_server.utils:Lakebase is not configured; "
                "running without persistent session memory."
            ],
        )

    def test_fevm_read_only_filters_only_opstask_write(self):
        tools = [{"name": "list_existing_ops_tasks"}, {"name": "create_ops_task"}]
        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "true"}):
            self.assertEqual(
                [item["name"] for item in agent.filter_mcp_tool_metadata("opstask", tools)],
                ["list_existing_ops_tasks"],
            )
            self.assertEqual(agent.filter_mcp_tool_metadata("storetime", tools), tools)

    def test_field_eng_keeps_private_opstask_write(self):
        tools = [{"name": "list_existing_ops_tasks"}, {"name": "create_ops_task"}]
        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "false"}):
            self.assertEqual(len(agent.filter_mcp_tool_metadata("opstask", tools)), 2)

    async def test_direct_tool_discovery_applies_fevm_filter(self):
        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "true"}), patch.object(
            agent, "DirectMcpClient", FakeDirectClient
        ), patch.object(agent, "_databricks_mcp_token", return_value="token"):
            tools, unavailable = await agent.build_direct_databricks_mcp_tools(FakeAsyncExitStack())

        self.assertEqual(
            [tool.name for tool in tools],
            [
                "query_space_genie-space-id",
                "poll_response_genie-space-id",
                "inspect_schedule",
                "list_existing_ops_tasks",
            ],
        )
        self.assertEqual(unavailable, [])

    def test_default_direct_transport_does_not_duplicate_genie_as_an_sdk_server(self):
        with patch.object(agent, "USE_SDK_MCP_SERVERS", False):
            servers = agent.build_mcp_servers(object())

        self.assertEqual(servers, [])

    def test_fevm_sdk_path_omits_raw_opstask_server(self):
        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "true"}), patch.object(
            agent, "USE_SDK_MCP_SERVERS", True
        ), patch.object(agent, "McpServer", FakeSdkMcpServer):
            servers = agent.build_mcp_servers(object())

        self.assertEqual([server.name for server in servers], ["bobabricks_genie", "storetime"])

    def test_field_eng_sdk_path_keeps_raw_opstask_server(self):
        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "false"}), patch.object(
            agent, "USE_SDK_MCP_SERVERS", True
        ), patch.object(agent, "McpServer", FakeSdkMcpServer):
            servers = agent.build_mcp_servers(object())

        self.assertEqual(
            [server.name for server in servers],
            ["bobabricks_genie", "storetime", "opstask"],
        )

    def test_generated_instructions_match_runtime_write_policy(self):
        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "true"}):
            read_only = agent.create_agent(context_tools_enabled=True).instructions
        normalized_read_only = " ".join(read_only.split())
        self.assertIn("inspect follow-ups", read_only)
        self.assertIn("cannot create tasks", read_only)
        self.assertIn("Never create an OpsTask", normalized_read_only)
        self.assertNotIn("create tasks after approval", read_only)
        self.assertNotIn("task creation tools", read_only)
        self.assertNotIn(
            "unless the user explicitly asks to create a follow-up task",
            normalized_read_only,
        )
        self.assertIn(
            "official company training goal is unavailable",
            normalized_read_only,
        )
        self.assertIn(
            "Never cite a tool that was unavailable, failed, or was only used in a prior turn",
            normalized_read_only,
        )
        self.assertIn(
            "Source lines must name every tool that returned evidence in the current turn",
            normalized_read_only,
        )

        with patch.dict(os.environ, {"SHARED_MCP_READ_ONLY": "false"}):
            writable = agent.create_agent().instructions
        self.assertIn("create tasks after approval", writable)
        self.assertIn("explicit approval", writable)


class TablePrefixIsolationTest(unittest.TestCase):
    def test_every_runtime_table_resolver_honors_private_prefix_once(self):
        modules = [
            load_server_module(
                "mcp-apps/shared/bobabricks_mcp/server.py", "runtime_isolation_shared_server"
            ),
            load_server_module(
                "mcp-apps/storetime/bobabricks_mcp/server.py", "runtime_isolation_storetime_server"
            ),
            load_server_module(
                "mcp-apps/opstask/bobabricks_mcp/server.py", "runtime_isolation_opstask_server"
            ),
        ]
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_CATALOG": PRIVATE_CATALOG,
                "DATABRICKS_SCHEMA": PRIVATE_SCHEMA,
                "DATABRICKS_TABLE_PREFIX": PRIVATE_PREFIX,
            },
        ):
            expected = f"{PRIVATE_NAMESPACE}.{PRIVATE_PREFIX}ops_tasks"
            for module in modules:
                with self.subTest(module=module.__name__):
                    self.assertEqual(module.DatabricksTableClient.table_name("ops_tasks"), expected)

            streamlit_client = load_streamlit_table_client()
            self.assertEqual(streamlit_client.table("ops_tasks"), expected)


class ProvisioningIsolationTest(unittest.TestCase):
    def test_qualified_table_accepts_only_validated_private_names(self):
        helper = getattr(provision, "qualified_table", None)
        self.assertIsNotNone(helper)
        if helper is None:
            return
        self.assertEqual(
            helper(PRIVATE_NAMESPACE, PRIVATE_PREFIX, "ops_tasks"),
            f"{PRIVATE_NAMESPACE}.{PRIVATE_PREFIX}ops_tasks",
        )
        for namespace, prefix, table in (
            (f"{PRIVATE_CATALOG}.shared; DROP SCHEMA shared", PRIVATE_PREFIX, "ops_tasks"),
            (PRIVATE_NAMESPACE, f"{PRIVATE_PREFIX}.shared_", "ops_tasks"),
            (PRIVATE_NAMESPACE, PRIVATE_PREFIX, "ops_tasks; DROP TABLE x"),
        ):
            with self.subTest(namespace=namespace, prefix=prefix, table=table):
                with self.assertRaises(ValueError):
                    helper(namespace, prefix, table)

    def test_cli_rejects_empty_prefix_before_any_sql(self):
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["provision_databricks_assets.py"]), patch.object(
            provision, "execute_sql"
        ) as execute_sql:
            with contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(SystemExit, "2"):
                    provision.main()
        execute_sql.assert_not_called()
        self.assertIn("--table-prefix must be non-empty", stderr.getvalue())

    def test_private_cli_plan_replaces_only_prefixed_tables(self):
        statements = []
        inserted_tables = []

        def capture_insert(_profile, _warehouse_id, table, _columns, _rows):
            inserted_tables.append(table)

        argv = [
            "provision_databricks_assets.py",
            "--profile",
            "dbc-f7444b38",
            "--warehouse-id",
            "warehouse-id",
            "--catalog",
            PRIVATE_CATALOG,
            "--schema",
            PRIVATE_SCHEMA,
            "--table-prefix",
            PRIVATE_PREFIX,
        ]
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            with patch.object(sys, "argv", argv), patch.object(
                provision, "execute_sql", side_effect=lambda _p, _w, sql: statements.append(sql) or {}
            ), patch.object(
                provision,
                "build_seed_data",
                return_value={"stores": (["store_id"], [(101,)])},
            ), patch.object(provision, "insert_rows", side_effect=capture_insert):
                provision.main()

        table_ddl = [statement for statement in statements if " TABLE " in statement]
        self.assertTrue(table_ddl)
        self.assertFalse(any("DROP TABLE" in statement for statement in statements))
        self.assertTrue(all("CREATE OR REPLACE TABLE" in statement for statement in table_ddl))
        self.assertTrue(
            all(
                f"{PRIVATE_NAMESPACE}.{PRIVATE_PREFIX}" in statement
                for statement in table_ddl
            )
        )
        self.assertEqual(
            inserted_tables,
            [f"{PRIVATE_NAMESPACE}.{PRIVATE_PREFIX}stores"],
        )
        final_insert = statements[-1]
        for table in ("store_metrics", "store_weekly_metrics", "stores"):
            self.assertIn(
                f"{PRIVATE_NAMESPACE}.{PRIVATE_PREFIX}{table}", final_insert
            )
        self.assertEqual(
            stdout.getvalue(),
            f"Provisioned Bobabricks store-ops data in {PRIVATE_NAMESPACE} "
            f"with prefix {PRIVATE_PREFIX!r}\n",
        )

    def test_allow_shared_source_is_the_only_empty_prefix_escape_hatch(self):
        statements = []
        argv = ["provision_databricks_assets.py", "--allow-shared-source"]
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            with patch.object(sys, "argv", argv), patch.object(
                provision, "execute_sql", side_effect=lambda _p, _w, sql: statements.append(sql) or {}
            ), patch.object(provision, "build_seed_data", return_value={}), patch.object(
                provision, "insert_rows"
            ):
                provision.main()

        self.assertFalse(any("DROP TABLE" in statement for statement in statements))
        self.assertTrue(
            any(
                f"CREATE OR REPLACE TABLE {SHARED_CATALOG}.{SHARED_SCHEMA}.stores" in statement
                for statement in statements
            )
        )
        self.assertEqual(
            stdout.getvalue(),
            f"Provisioned Bobabricks store-ops data in {SHARED_CATALOG}.{SHARED_SCHEMA} "
            "with prefix ''\n",
        )


if __name__ == "__main__":
    unittest.main()
