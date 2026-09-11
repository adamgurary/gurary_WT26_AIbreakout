from __future__ import annotations

import importlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from unittest.mock import patch

import yaml


PROFILE = "e2-demo-field-eng"
HOST = "https://e2-demo-field-eng.cloud.databricks.com"
WORKSPACE_ID = "1444828305810485"
CATALOG = "gurary_" "catalog"
SCHEMA = "gurary_bobabricks_store_" "ops"
TABLE_PREFIX = "gurary_"
WAREHOUSE_ID = "a1b2c3d4e5f60718"
STORETIME_APP = "gurary-bobabricks-store" "time"
OPSTASK_APP = "gurary-bobabricks-ops" "task-mcp"
WORKSPACE_PARENT = "/Workspace/Users/adam.gurary@databricks.com"


class PlanBoundary:
    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        if profile != PROFILE or payload is not None:
            raise AssertionError("MCP dry run crossed the read-only field-eng boundary")
        if args == ["apps", "list"]:
            return {"apps": []}
        raise AssertionError(f"unexpected MCP plan request: {args}")


def sql_response(columns: list[str], rows: list[list[str]]) -> dict:
    return {
        "statement_id": "01f123456789abcdef0123456789abcd",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "schema": {"columns": [{"name": name} for name in columns]},
            "total_row_count": len(rows),
        },
        "result": {"data_array": rows},
    }


class MutableMcpBoundary:
    def __init__(self):
        self.events: list[tuple] = []
        self.apps: dict[str, dict] = {}
        self.grants: dict[tuple[str, str], set[str]] = {}
        self.workspace_directories: set[str] = set()
        self.workspace_files: set[str] = set()
        self.workspace_permissions: dict[str, set[str]] = {}
        self.warehouse_permissions: set[str] = set()
        self.deployments: dict[tuple[str, str], dict] = {}
        self.ops_rows = [
            {"task_id": "OPS-2194", "store_id": "104", "title": "Review training shift protection"},
            {"task_id": "OPS-2201", "store_id": "122", "title": "Investigate peak wait times"},
            {"task_id": "OPS-2208", "store_id": "117", "title": "Expedite oat milk transfer"},
            {"task_id": "OPS-2210", "store_id": "117", "title": "Confirm espresso reorder ETA"},
        ]

    def verify_profile(self, profile: str, host: str) -> dict:
        self.events.append(("binding", profile, host))
        return {"host": host, "current_user": {"userName": "adam.gurary@databricks.com"}}

    def workspace_id(self, profile: str) -> str:
        self.events.append(("workspace", profile))
        return WORKSPACE_ID

    def _app(self, name: str) -> dict:
        suffix = "11111111-1111-4111-8111-111111111111" if name == STORETIME_APP else "22222222-2222-4222-8222-222222222222"
        principal = "33333333-3333-4333-8333-333333333333" if name == STORETIME_APP else "44444444-4444-4444-8444-444444444444"
        return {
            "id": suffix,
            "name": name,
            "url": f"https://{name}-{WORKSPACE_ID}.aws.databricksapps.com",
            "service_principal_client_id": principal,
            "creator": "adam.gurary@databricks.com",
            "app_status": {"state": "RUNNING"},
            "compute_status": {"state": "ACTIVE"},
        }

    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        if profile != PROFILE:
            raise AssertionError(f"unexpected profile: {profile}")
        key = tuple(args)
        if key == ("apps", "list"):
            self.events.append(("inspection", "apps"))
            return {"apps": list(self.apps.values())}
        if key[:2] == ("apps", "get") and len(key) == 3:
            self.events.append(("inspection", "app", key[2]))
            return dict(self.apps[key[2]])
        if key[:2] == ("apps", "create"):
            self.events.append(("mutation", "create_app", key[2]))
            if len(key) != 3 or payload is not None or key[2] not in {STORETIME_APP, OPSTASK_APP}:
                raise AssertionError(f"unsafe app create: {args}, {payload}")
            app = self._app(key[2])
            self.apps[app["name"]] = app
            return dict(app)
        if key[:2] == ("catalogs", "get"):
            self.events.append(("inspection", "catalog", key[2]))
            return {"name": key[2], "owner": "adam.gurary@databricks.com"}
        if key[:2] == ("schemas", "get"):
            self.events.append(("inspection", "schema", key[2]))
            return {"full_name": key[2], "owner": "adam.gurary@databricks.com"}
        if key[:2] == ("tables", "get"):
            self.events.append(("inspection", "table", key[2]))
            return {"full_name": key[2], "owner": "adam.gurary@databricks.com", "table_type": "MANAGED"}
        if key[:2] == ("grants", "get"):
            self.events.append(("inspection", "grants", key[2], key[3]))
            principal_rows = []
            for app in self.apps.values():
                principal = app["service_principal_client_id"]
                privileges = sorted(self.grants.get((principal, key[3]), set()))
                if privileges:
                    principal_rows.append({"principal": principal, "privileges": privileges})
            return {"privilege_assignments": principal_rows}
        if key[:2] == ("grants", "update"):
            change = payload["changes"][0]
            self.events.append(("mutation", "grant", key[2], key[3], tuple(change["add"])))
            self.grants.setdefault((change["principal"], key[3]), set()).update(change["add"])
            return {}
        if key == ("warehouses", "get", WAREHOUSE_ID):
            self.events.append(("inspection", "warehouse", WAREHOUSE_ID))
            return {"id": WAREHOUSE_ID, "name": "gurary_bobabricks_" "warehouse", "creator_name": "adam.gurary@databricks.com"}
        if key == ("permissions", "get", "warehouses", WAREHOUSE_ID):
            self.events.append(("inspection", "warehouse_permissions", WAREHOUSE_ID))
            return {
                "access_control_list": [
                    {
                        "service_principal_name": principal,
                        "all_permissions": [{"permission_level": "CAN_USE"}],
                    }
                    for principal in sorted(self.warehouse_permissions)
                ]
            }
        if key == ("permissions", "update", "warehouses", WAREHOUSE_ID):
            principal = payload["access_control_list"][0]["service_principal_name"]
            self.events.append(("mutation", "warehouse_can_use", WAREHOUSE_ID, principal))
            self.warehouse_permissions.add(principal)
            return {}
        if key == ("workspace", "list", WORKSPACE_PARENT):
            self.events.append(("inspection", "workspace_parent"))
            return {
                "objects": [
                    {"path": path, "object_type": "DIRECTORY", "object_id": str(index)}
                    for index, path in enumerate(sorted(self.workspace_directories), 10)
                ]
            }
        if key[:2] == ("workspace", "list") and len(key) == 3:
            self.events.append(("inspection", "workspace_directory", key[2]))
            return {
                "objects": [
                    {"path": path, "object_type": "FILE"}
                    for path in sorted(self.workspace_files)
                    if path.startswith(key[2] + "/")
                ]
            }
        if key[:2] == ("workspace", "get-status"):
            self.events.append(("inspection", "workspace_status", key[2]))
            return {"path": key[2], "object_type": "DIRECTORY", "object_id": key[2]}
        if key[:2] == ("workspace", "get-permissions"):
            self.events.append(("inspection", "workspace_permissions", key[3]))
            return {
                "access_control_list": [
                    {
                        "service_principal_name": principal,
                        "all_permissions": [{"permission_level": "CAN_READ"}],
                    }
                    for principal in sorted(self.workspace_permissions.get(key[3], set()))
                ]
            }
        if key[:2] == ("workspace", "update-permissions"):
            principal = payload["access_control_list"][0]["service_principal_name"]
            self.events.append(("mutation", "workspace_acl", key[3], principal))
            self.workspace_permissions.setdefault(key[3], set()).add(principal)
            return {}
        if key[:2] == ("apps", "deploy"):
            name = key[2]
            source = key[4]
            deployment_id = f"deployment-{len(self.deployments) + 1}"
            self.events.append(("mutation", "deploy", name, source))
            deployment = {
                "deployment_id": deployment_id,
                "source_code_path": source,
                "status": {"state": "SUCCEEDED"},
            }
            self.deployments[(name, deployment_id)] = deployment
            return dict(deployment)
        if key[:2] == ("apps", "get-deployment"):
            self.events.append(("inspection", "deployment", key[2], key[3]))
            return dict(self.deployments[(key[2], key[3])])
        if key[:3] == ("api", "post", "/api/2.0/sql/statements"):
            statement = payload["statement"]
            table = f"`{CATALOG}`.`{SCHEMA}`.`gurary_ops_tasks`"
            if table not in statement:
                raise AssertionError(f"SQL escaped private table boundary: {statement}")
            if statement.lstrip().startswith("DELETE"):
                self.events.append(("mutation", "cleanup_row"))
                self.ops_rows = [row for row in self.ops_rows if row["task_id"] != "GURARY-T10-MCP-VALIDATION"]
                return sql_response([], [])
            self.events.append(("inspection", "validation_row_sql"))
            if "GURARY-T10-MCP-VALIDATION" in statement:
                rows = [row for row in self.ops_rows if row["task_id"] == "GURARY-T10-MCP-VALIDATION"]
            else:
                rows = self.ops_rows
            return sql_response(["task_id"], [[row["task_id"]] for row in rows])
        raise AssertionError(f"unexpected apply boundary: {args}, {payload}")

    def sync(self, command: list[str]) -> None:
        workspace_path = command[-3]
        self.events.append(("mutation", "sync", workspace_path))
        self.workspace_directories.add(workspace_path)
        self.workspace_files.add(f"{workspace_path}/app.yaml")

    def invoke(self, profile: str, url: str, payload: dict) -> dict:
        name = STORETIME_APP if STORETIME_APP in url else OPSTASK_APP
        method = payload["method"]
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}},
            }
        if method == "tools/list":
            tools = (
                ["inspect_schedule", "list_training_events"]
                if name == STORETIME_APP
                else ["list_existing_ops_tasks", "create_ops_task"]
            )
            return {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {"tools": [{"name": tool} for tool in tools]},
            }
        tool = payload["params"]["name"]
        if tool == "inspect_schedule":
            result = [{"store_id": "104", "week": "2026-W27", "scheduled_training_hours": "42", "completed_training_hours": "28", "converted_training_hours": "14"}]
        elif tool == "list_existing_ops_tasks":
            result = list(self.ops_rows)
        elif tool == "create_ops_task":
            self.events.append(("mutation", "create_validation_row"))
            arguments = payload["params"]["arguments"]
            self.ops_rows.append({"task_id": arguments["task_id"], "store_id": str(arguments["store_id"]), "title": arguments["title"]})
            result = {"task_id": arguments["task_id"], "status": "open"}
        else:
            raise AssertionError(f"unexpected tool: {tool}")
        return {
            "jsonrpc": "2.0",
            "id": payload["id"],
            "result": {"content": [{"type": "text", "text": json.dumps(result)}]},
        }


def verified_identity(profile: str, host: str) -> dict:
    if (profile, host) != (PROFILE, HOST):
        raise AssertionError("wrong field-eng binding")
    return {
        "host": host,
        "current_user": {"userName": "adam.gurary@databricks.com"},
    }


class McpDeploymentPlanTest(unittest.TestCase):
    def test_plan_binds_exact_private_apps_manifests_and_grants(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        build_plan = getattr(field_eng, "build_mcp_deployment_plan", None)
        self.assertIsNotNone(build_plan, "MCP deployment planning is missing")
        if build_plan is None:
            return

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "field_eng.json"
            state_path.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": WAREHOUSE_ID,
                        "genie_space_id": "01f123456789abcdef0123456789abcd",
                        "lakebase": {
                            "validated": True,
                            "enabled": True,
                            "project": "gurary-bobabricks",
                            "branch": "production",
                            "endpoint": "primary",
                            "database": "databricks_postgres",
                            "host": "private.database.databricks.com",
                            "schema": "gurary_bobabricks_app",
                        },
                    }
                ),
                encoding="utf-8",
            )
            plan = build_plan(
                run_cli=PlanBoundary(),
                verify_profile=verified_identity,
                workspace_id_provider=lambda _profile: WORKSPACE_ID,
                state_path=state_path,
            )

        self.assertEqual(
            tuple(app.app_name for app in plan.apps),
            (STORETIME_APP, OPSTASK_APP),
        )
        expected_environment = {
            "DATABRICKS_WAREHOUSE_ID": WAREHOUSE_ID,
            "DATABRICKS_CATALOG": CATALOG,
            "DATABRICKS_SCHEMA": SCHEMA,
            "DATABRICKS_TABLE_PREFIX": TABLE_PREFIX,
        }
        self.assertEqual(plan.environment, expected_environment)
        self.assertEqual(
            tuple(app.workspace_path for app in plan.apps),
            (
                f"{WORKSPACE_PARENT}/{STORETIME_APP}",
                f"{WORKSPACE_PARENT}/{OPSTASK_APP}",
            ),
        )
        self.assertEqual(
            tuple(app.source_relative for app in plan.apps),
            ("mcp-apps/storetime", "mcp-apps/opstask"),
        )

        namespace = f"{CATALOG}.{SCHEMA}"
        grants = {
            app.app_name: {(grant.securable_type, grant.full_name): grant.privileges for grant in app.grants}
            for app in plan.apps
        }
        self.assertEqual(
            grants[STORETIME_APP],
            {
                ("catalog", CATALOG): frozenset({"USE_CATALOG"}),
                ("schema", namespace): frozenset({"USE_SCHEMA"}),
                ("table", f"{namespace}.gurary_schedules"): frozenset({"SELECT"}),
                ("table", f"{namespace}.gurary_training_events"): frozenset({"SELECT"}),
                ("table", f"{namespace}.gurary_storetime_shifts"): frozenset({"SELECT"}),
            },
        )
        self.assertEqual(
            grants[OPSTASK_APP],
            {
                ("catalog", CATALOG): frozenset({"USE_CATALOG"}),
                ("schema", namespace): frozenset({"USE_SCHEMA"}),
                ("table", f"{namespace}.gurary_ops_tasks"): frozenset({"SELECT", "MODIFY"}),
            },
        )
        self.assertEqual(
            [full_name for (_kind, full_name) in grants[OPSTASK_APP] if full_name.count(".") == 2],
            [f"{namespace}.gurary_ops_tasks"],
        )

    def test_plan_resolves_authoritative_metadata_when_app_list_is_sparse(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        boundary = MutableMcpBoundary()
        boundary.apps = {
            STORETIME_APP: boundary._app(STORETIME_APP),
            OPSTASK_APP: boundary._app(OPSTASK_APP),
        }

        def sparse_cli(profile: str, args: list[str], payload: dict | None = None):
            if args == ["apps", "list"]:
                boundary.events.append(("inspection", "apps"))
                return {"apps": [{"name": name} for name in boundary.apps]}
            return boundary(profile, args, payload)

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "field_eng.json"
            state_path.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": WAREHOUSE_ID}),
                encoding="utf-8",
            )
            try:
                plan = field_eng.build_mcp_deployment_plan(
                    run_cli=sparse_cli,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                    state_path=state_path,
                )
            except RuntimeError as error:
                self.fail(f"sparse app inventory was not resolved through authoritative readback: {error}")

        self.assertEqual(
            [app.current_app["id"] for app in plan.apps],
            [boundary.apps[STORETIME_APP]["id"], boundary.apps[OPSTASK_APP]["id"]],
        )


class McpDeploymentApplyTest(unittest.TestCase):
    def _planned_apply(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "field_eng.json"
            state_path.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": WAREHOUSE_ID}),
                encoding="utf-8",
            )
            plan = field_eng.build_mcp_deployment_plan(
                run_cli=PlanBoundary(),
                verify_profile=verified_identity,
                workspace_id_provider=lambda _profile: WORKSPACE_ID,
                state_path=state_path,
            )
        return field_eng, plan

    def test_apply_rejects_wrong_workspace_path_before_first_remote_inspection(self):
        field_eng, plan = self._planned_apply()
        boundary = MutableMcpBoundary()
        wrong = replace(
            plan,
            apps=(
                replace(plan.apps[0], workspace_path=f"{WORKSPACE_PARENT}/gurary-wrong-source"),
                plan.apps[1],
            ),
        )

        with self.assertRaisesRegex(RuntimeError, "exact private deployment contract"):
            field_eng.apply_mcp_deployment_plan(
                wrong,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
            )

        self.assertEqual(boundary.events, [])

    def test_apply_rejects_duplicate_app_names_before_first_remote_inspection(self):
        field_eng, plan = self._planned_apply()
        boundary = MutableMcpBoundary()
        duplicate = replace(
            plan,
            apps=(plan.apps[0], replace(plan.apps[1], app_name=STORETIME_APP)),
        )

        with self.assertRaisesRegex(RuntimeError, "exact private deployment contract"):
            field_eng.apply_mcp_deployment_plan(
                duplicate,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
            )

        self.assertEqual(boundary.events, [])

    def test_apply_rejects_missing_grant_before_first_remote_inspection(self):
        field_eng, plan = self._planned_apply()
        boundary = MutableMcpBoundary()
        incomplete = replace(
            plan,
            apps=(replace(plan.apps[0], grants=plan.apps[0].grants[:-1]), plan.apps[1]),
        )

        with self.assertRaisesRegex(RuntimeError, "exact private deployment contract"):
            field_eng.apply_mcp_deployment_plan(
                incomplete,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
            )

        self.assertEqual(boundary.events, [])

    def test_target_contract_pins_distinct_controller_approved_app_names(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        target = field_eng.load_target("field_eng")

        for unsafe in (
            replace(target, storetime_app_name="gurary-unreviewed-storetime"),
            replace(target, opstask_app_name="gurary-unreviewed-opstask"),
            replace(target, opstask_app_name=target.storetime_app_name),
        ):
            with self.subTest(unsafe=unsafe):
                with self.assertRaisesRegex(RuntimeError, "safety contract"):
                    field_eng._require_target_contract(unsafe)

    def test_apply_mcp_cli_mode_reaches_reconciliation_and_emits_evidence(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        plan = object()

        class Evidence:
            @staticmethod
            def as_dict():
                return {"mode": "apply-mcp", "ops_baseline_count": 4}

        output = io.StringIO()
        with patch.object(field_eng, "build_mcp_deployment_plan", return_value=plan) as build, patch.object(
            field_eng, "apply_mcp_deployment_plan", return_value=Evidence()
        ) as apply, patch("sys.argv", ["provision_field_eng.py", "--apply-mcp"]), redirect_stdout(output):
            try:
                field_eng.main()
            except SystemExit as error:
                self.fail(f"supported MCP CLI mode was rejected: {error}")

        build.assert_called_once_with()
        apply.assert_called_once_with(plan)
        self.assertEqual(
            json.loads(output.getvalue()),
            {"mode": "apply-mcp", "ops_baseline_count": 4},
        )

    def test_app_create_uses_installed_cli_positional_name_shape(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")

        class PositionalCreateBoundary(MutableMcpBoundary):
            def __call__(self, profile: str, args: list[str], payload: dict | None = None):
                if args[:2] == ["apps", "create"]:
                    if args != ["apps", "create", STORETIME_APP] or payload is not None:
                        raise RuntimeError("installed CLI requires positional app NAME")
                    app = self._app(STORETIME_APP)
                    self.apps[STORETIME_APP] = app
                    self.events.append(("mutation", "create_app", STORETIME_APP))
                    return dict(app)
                return super().__call__(profile, args, payload)

        boundary = PositionalCreateBoundary()
        app_plan = importlib.import_module("scripts.provision_field_eng").McpAppPlan(
            app_name=STORETIME_APP,
            source_relative="mcp-apps/storetime",
            workspace_path=f"{WORKSPACE_PARENT}/{STORETIME_APP}",
            grants=(),
            current_app=None,
        )
        try:
            created = field_eng._ensure_mcp_app(
                field_eng.load_target("field_eng"),
                app_plan,
                boundary,
                boundary.verify_profile,
                boundary.workspace_id,
            )
        except RuntimeError as error:
            self.fail(f"app create did not use the installed positional CLI shape: {error}")

        self.assertEqual(created["name"], STORETIME_APP)

    def test_apply_reconciles_deploys_validates_and_restores_the_private_baseline(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        apply_plan = getattr(field_eng, "apply_mcp_deployment_plan", None)
        self.assertIsNotNone(apply_plan, "MCP deployment reconciliation is missing")
        if apply_plan is None:
            return

        boundary = MutableMcpBoundary()
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            state_path = temporary / "field_eng.json"
            original_state = {
                "target": "field_eng",
                "warehouse_id": WAREHOUSE_ID,
                "genie_space_id": "01f123456789abcdef0123456789abcd",
                "lakebase": {
                    "validated": True,
                    "enabled": True,
                    "project": "gurary-bobabricks",
                    "branch": "production",
                    "endpoint": "primary",
                    "database": "databricks_postgres",
                    "host": "private.database.databricks.com",
                    "schema": "gurary_bobabricks_app",
                },
            }
            state_path.write_text(json.dumps(original_state), encoding="utf-8")
            plan = field_eng.build_mcp_deployment_plan(
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                state_path=state_path,
            )
            evidence = apply_plan(
                plan,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                state_path=state_path,
                build_root=temporary / "rendered",
                sync_runner=boundary.sync,
                mcp_invoker=boundary.invoke,
                waiter=lambda _seconds: None,
            )

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            for key, value in original_state.items():
                self.assertEqual(saved[key], value)
            self.assertEqual(
                set(saved) - set(original_state),
                {
                    "storetime_app_id",
                    "storetime_service_principal_client_id",
                    "storetime_mcp_url",
                    "opstask_app_id",
                    "opstask_service_principal_client_id",
                    "opstask_mcp_url",
                },
            )
            self.assertEqual(
                saved["storetime_mcp_url"],
                f"https://{STORETIME_APP}-{WORKSPACE_ID}.aws.databricksapps.com/mcp",
            )
            self.assertEqual(
                saved["opstask_mcp_url"],
                f"https://{OPSTASK_APP}-{WORKSPACE_ID}.aws.databricksapps.com/mcp",
            )

            for app_name, source in (
                (STORETIME_APP, "storetime"),
                (OPSTASK_APP, "opstask"),
            ):
                manifest = yaml.safe_load(
                    (temporary / "rendered" / app_name / "app.yaml").read_text(encoding="utf-8")
                )
                self.assertEqual(
                    {entry["name"]: str(entry["value"]) for entry in manifest["env"]},
                    {
                        "DATABRICKS_WAREHOUSE_ID": WAREHOUSE_ID,
                        "DATABRICKS_CATALOG": CATALOG,
                        "DATABRICKS_SCHEMA": SCHEMA,
                        "DATABRICKS_TABLE_PREFIX": TABLE_PREFIX,
                    },
                )
                self.assertTrue((temporary / "rendered" / app_name / "app.py").is_file(), source)
                self.assertFalse(
                    any(
                        "__pycache__" in path.parts or path.suffix == ".pyc"
                        for path in (temporary / "rendered" / app_name).rglob("*")
                    )
                )

        self.assertEqual(evidence.app_states, {STORETIME_APP: "RUNNING", OPSTASK_APP: "RUNNING"})
        self.assertEqual(evidence.store_104_storetime, (42, 28, 14))
        self.assertEqual(evidence.ops_baseline_count, 4)
        self.assertEqual({row["task_id"] for row in boundary.ops_rows}, {"OPS-2194", "OPS-2201", "OPS-2208", "OPS-2210"})
        warehouse_permissions = [
            event for event in boundary.events if event[:2] == ("mutation", "warehouse_can_use")
        ]
        self.assertEqual(
            warehouse_permissions,
            [
                ("mutation", "warehouse_can_use", WAREHOUSE_ID, boundary.apps[STORETIME_APP]["service_principal_client_id"]),
                ("mutation", "warehouse_can_use", WAREHOUSE_ID, boundary.apps[OPSTASK_APP]["service_principal_client_id"]),
            ],
        )

        previous_mutation = -1
        for mutation_index in [
            index for index, event in enumerate(boundary.events) if event[0] == "mutation"
        ]:
            preflight = boundary.events[previous_mutation + 1 : mutation_index]
            self.assertTrue(any(event[0] == "binding" for event in preflight), boundary.events[mutation_index])
            self.assertTrue(any(event[0] == "workspace" for event in preflight), boundary.events[mutation_index])
            self.assertTrue(any(event[0] == "inspection" for event in preflight), boundary.events[mutation_index])
            previous_mutation = mutation_index

    def test_validation_failure_still_deletes_only_the_owned_row_and_restores_four_rows(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")

        class FailingValidationBoundary(MutableMcpBoundary):
            def invoke(self, profile: str, url: str, payload: dict) -> dict:
                if (
                    payload.get("method") == "tools/call"
                    and payload.get("params", {}).get("name") == "list_existing_ops_tasks"
                    and any(row["task_id"] == "GURARY-T10-MCP-VALIDATION" for row in self.ops_rows)
                ):
                    raise RuntimeError("forced post-create validation failure")
                return super().invoke(profile, url, payload)

        boundary = FailingValidationBoundary()
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            state_path = temporary / "field_eng.json"
            state_path.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": WAREHOUSE_ID}),
                encoding="utf-8",
            )
            plan = field_eng.build_mcp_deployment_plan(
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                state_path=state_path,
            )
            with self.assertRaisesRegex(RuntimeError, "forced post-create validation failure"):
                field_eng.apply_mcp_deployment_plan(
                    plan,
                    run_cli=boundary,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                    state_path=state_path,
                    build_root=temporary / "rendered",
                    sync_runner=boundary.sync,
                    mcp_invoker=boundary.invoke,
                    waiter=lambda _seconds: None,
                )

        self.assertEqual(
            {row["task_id"] for row in boundary.ops_rows},
            {"OPS-2194", "OPS-2201", "OPS-2208", "OPS-2210"},
        )
        self.assertEqual(
            [event for event in boundary.events if event[:2] == ("mutation", "cleanup_row")],
            [("mutation", "cleanup_row")],
        )


if __name__ == "__main__":
    unittest.main()
