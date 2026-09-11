import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from deploy.config import load_target
from deploy.databricks_cli import assert_profile
from scripts.provision_fevm_fallback import (
    apply_provision_plan,
    build_provision_plan,
    canonical_ops_snapshot,
)


SHARED_PRESENTER = "bobabricks-store-ops-" "demo"
PROBE_SESSION = "fresh-probe-session"
PROBE_STARTED_AT_MS = 1_789_000_000_000
PROBE_EXPERIMENT = (
    "/Users/gurary-bobabricks-store-ops-principal/"
    "gurary_bobabricks_store_ops_uc"
)
PROBE_TRACE_LOCATION = (
    "worldtour_ai_" "catalog.ai_gateway_" "demo.gurary_bobabricks"
)


def successful_probe_invocation(profile: str, app_url: str, payload: dict) -> dict:
    return {
        "output": [{"role": "assistant", "content": "Available tools."}],
        "custom_outputs": {"session_id": PROBE_SESSION},
    }


def successful_trace_proof(request: dict) -> dict:
    return {
        "experiment_name": PROBE_EXPERIMENT,
        "experiment_tags": {
            "mlflow.experiment.databricksTraceDestinationPath": PROBE_TRACE_LOCATION,
            "mlflow.experiment.databricksTraceSpanStorageTable": (
                "worldtour_ai_" "catalog.ai_gateway_" "demo.gurary_bobabricks_otel_spans"
            ),
        },
        "trace_id": "trace-123",
        "trace_request_time_ms": PROBE_STARTED_AT_MS + 1,
        "trace_session_id": PROBE_SESSION,
        "trace_state": "OK",
        "spans": [{"name": "ResponsesAgent", "status": "OK"}],
    }


def probe_kwargs() -> dict:
    return {
        "app_invoker": successful_probe_invocation,
        "trace_proof_provider": successful_trace_proof,
        "session_id_provider": lambda: PROBE_SESSION,
        "now_ms": lambda: PROBE_STARTED_AT_MS,
    }


def app_response(name: str, workspace_id: str) -> dict:
    return {
        "active_deployment": {
            "deployment_id": "deployment-123",
            "source_code_path": "/Workspace/Users/owner/source",
            "status": {"message": "Deployment succeeded", "state": "SUCCEEDED"},
        },
        "app_status": {"message": "App is running", "state": "RUNNING"},
        "compute_status": {"message": "Compute is active", "state": "ACTIVE"},
        "create_time": "2026-09-10T12:00:00Z",
        "creator": "owner@example.com",
        "default_source_code_path": "/Workspace/Users/owner/source",
        "description": "Bobabricks dependency",
        "effective_user_api_scopes": ["ai-gateway", "genie", "mcp.external"],
        "id": f"{name}-id",
        "name": name,
        "oauth2_app_client_id": "oauth-client-123",
        "oauth2_app_integration_id": "oauth-integration-123",
        "service_principal_client_id": f"{name}-principal",
        "service_principal_id": "123456789",
        "service_principal_name": name,
        "update_time": "2026-09-10T12:00:00Z",
        "updater": "owner@example.com",
        "url": f"https://{name}-{workspace_id}.aws.databricksapps.com",
        "user_api_scopes": ["ai-gateway", "genie", "mcp.external"],
    }


def statement_response(rows: list[list[str | None]]) -> dict:
    return {
        "manifest": {
            "format": "JSON_ARRAY",
            "schema": {
                "column_count": 3,
                "columns": [
                    {"name": "task_id", "position": 0, "type_name": "STRING", "type_text": "STRING"},
                    {"name": "store_id", "position": 1, "type_name": "STRING", "type_text": "STRING"},
                    {"name": "status", "position": 2, "type_name": "STRING", "type_text": "STRING"},
                ],
            },
            "total_chunk_count": 1,
            "total_row_count": len(rows),
        },
        "result": {
            "byte_count": 64,
            "chunk_index": 0,
            "data_array": rows,
            "row_count": len(rows),
            "row_offset": 0,
        },
        "statement_id": "statement-123",
        "status": {"state": "SUCCEEDED"},
    }


class FakeReadOnlyCli:
    def __init__(
        self,
        *,
        owned_app: dict | None = None,
        change_second_snapshot: bool = False,
        apps_top_level_list: bool = False,
    ):
        self.target = load_target("fevm")
        self.owned_app = owned_app
        self.change_second_snapshot = change_second_snapshot
        self.apps_top_level_list = apps_top_level_list
        self.snapshot_reads = 0

    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        self.assert_read_only(profile, args, payload)
        key = tuple(args)
        shared_apps = {
            SHARED_PRESENTER,
            self.target.storetime_app_name,
            self.target.opstask_app_name,
        }
        if key[:2] == ("apps", "get") and key[2] in shared_apps:
            return app_response(key[2], self.target.workspace_id)
        if key == ("apps", "list"):
            apps = [self.owned_app] if self.owned_app else []
            return apps if self.apps_top_level_list else {"apps": apps}
        if key == ("genie", "get-space", self.target.genie_space_id):
            return {"space_id": self.target.genie_space_id, "title": "Shared Store Operations"}
        if key == ("warehouses", "get", self.target.warehouse_id):
            return {
                "auto_stop_mins": 10,
                "cluster_size": "Small",
                "creator_name": "owner@example.com",
                "enable_photon": True,
                "enable_serverless_compute": True,
                "id": self.target.warehouse_id,
                "max_num_clusters": 1,
                "min_num_clusters": 1,
                "name": "Shared Warehouse",
                "num_active_sessions": 0,
                "num_clusters": 1,
                "state": "RUNNING",
                "warehouse_type": "PRO",
            }
        if key == ("catalogs", "get", self.target.catalog):
            return {
                "catalog_type": "MANAGED_CATALOG",
                "created_at": 1,
                "created_by": "owner@example.com",
                "full_name": self.target.catalog,
                "isolation_mode": "OPEN",
                "name": self.target.catalog,
                "owner": "owner@example.com",
                "securable_type": "CATALOG",
                "updated_at": 1,
                "updated_by": "owner@example.com",
            }
        schema_name = f"{self.target.catalog}.{self.target.data_schema}"
        if key == ("schemas", "get", schema_name):
            return {
                "catalog_name": self.target.catalog,
                "catalog_type": "MANAGED_CATALOG",
                "created_at": 1,
                "created_by": "owner@example.com",
                "full_name": schema_name,
                "name": self.target.data_schema,
                "owner": "owner@example.com",
                "schema_id": "schema-123",
                "updated_at": 1,
                "updated_by": "owner@example.com",
            }
        connection_name = self.target.confluence_connection
        if key == ("connections", "get", connection_name):
            return {
                "comment": "Managed read-only dependency",
                "connection_id": "connection-123",
                "connection_type": "HTTP",
                "created_at": 1,
                "created_by": "owner@example.com",
                "credential_type": "OAUTH_U2M",
                "full_name": connection_name,
                "metastore_id": "metastore-123",
                "name": connection_name,
                "options": {"authorization": "[REDACTED]"},
                "owner": "owner@example.com",
                "read_only": True,
                "securable_type": "CONNECTION",
                "updated_at": 1,
                "updated_by": "owner@example.com",
                "url": "https://example.invalid",
            }
        table_name = f"{schema_name}.ops_tasks"
        if key == ("tables", "get", table_name):
            return {
                "catalog_name": self.target.catalog,
                "columns": [],
                "created_at": 1,
                "created_by": "owner@example.com",
                "data_source_format": "DELTA",
                "full_name": table_name,
                "name": "ops_tasks",
                "owner": "owner@example.com",
                "schema_name": self.target.data_schema,
                "table_id": "table-123",
                "table_type": "MANAGED",
                "updated_at": 1,
                "updated_by": "owner@example.com",
            }
        if key == ("api", "post", "/api/2.0/sql/statements"):
            self.snapshot_reads += 1
            rows = [["task-2", "104", "open"], ["task-1", "101", "closed"]]
            if self.change_second_snapshot and self.snapshot_reads == 2:
                rows.append(["task-3", "105", "open"])
            return statement_response(rows)
        self.fail_unknown(key)

    def assert_read_only(self, profile: str, args: list[str], payload: dict | None):
        if profile != self.target.profile:
            raise AssertionError(f"unexpected profile: {profile}")
        if args[:2] == ["api", "post"]:
            if not payload or not str(payload.get("statement", "")).lstrip().upper().startswith("SELECT"):
                raise AssertionError("SQL boundary was not read-only")
            return
        if args[:2] in (["apps", "get"], ["apps", "list"], ["genie", "get-space"],
                        ["warehouses", "get"], ["catalogs", "get"], ["schemas", "get"],
                        ["connections", "get"], ["tables", "get"]):
            if payload is not None:
                raise AssertionError("read operation unexpectedly received a payload")
            return
        raise AssertionError(f"unexpected mutating command: {args}")

    def fail_unknown(self, key: tuple[str, ...]):
        raise AssertionError(f"unhandled CLI request: {key}")


class FakeMutableCli:
    def __init__(
        self,
        *,
        forbid_grant_calls: bool = False,
        require_source_acl: bool = False,
        require_manifest_check: bool = False,
    ):
        self.target = load_target("fevm")
        self.app = None
        self.grants: dict[tuple[str, str], set[str]] = {}
        self.forbid_grant_calls = forbid_grant_calls
        self.require_source_acl = require_source_acl
        self.require_manifest_check = require_manifest_check
        self.workspace_manifest_checked = False
        self.workspace_acl: dict[str, str] = {}

    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        if profile != self.target.profile:
            raise AssertionError(f"unexpected profile: {profile}")
        key = tuple(args)
        if key == ("apps", "list"):
            return [dict(self.app)] if self.app else []
        if key == ("apps", "create"):
            if payload != {
                "name": self.target.presenter_app_name,
                "forward_user_access_token": True,
            }:
                raise AssertionError("unsafe app create payload")
            self.app = app_response(self.target.presenter_app_name, self.target.workspace_id)
            self.app["user_api_scopes"] = []
            self.app["effective_user_api_scopes"] = []
            return dict(self.app)
        if key == ("apps", "get", self.target.presenter_app_name):
            if not self.app:
                raise AssertionError("owned app was read before creation")
            return dict(self.app)
        if key == ("apps", "update", self.target.presenter_app_name):
            if not self.app or payload != {"user_api_scopes": ["ai-gateway", "genie", "mcp.external"]}:
                raise AssertionError("unsafe app update payload")
            self.app["user_api_scopes"] = list(payload["user_api_scopes"])
            self.app["effective_user_api_scopes"] = list(payload["user_api_scopes"])
            return dict(self.app)
        if key[:1] == ("grants",) and self.forbid_grant_calls:
            raise AssertionError("preauthorized mode accessed the grants API")
        if key[:2] == ("grants", "get"):
            securable_type, full_name = key[2], key[3]
            privileges = sorted(self.grants.get((securable_type, full_name), set()))
            assignments = []
            if privileges:
                assignments.append(
                    {
                        "principal": self.app["service_principal_client_id"],
                        "privileges": privileges,
                    }
                )
            return {"privilege_assignments": assignments}
        if key[:2] == ("grants", "update"):
            securable_type, full_name = key[2], key[3]
            expected_principal = self.app["service_principal_client_id"]
            changes = payload.get("changes") if isinstance(payload, dict) else None
            if (
                not isinstance(changes, list)
                or len(changes) != 1
                or changes[0].get("principal") != expected_principal
                or not isinstance(changes[0].get("add"), list)
                or "remove" in changes[0]
            ):
                raise AssertionError("unsafe grant payload")
            self.grants.setdefault((securable_type, full_name), set()).update(changes[0]["add"])
            return {
                "privilege_assignments": [
                    {
                        "principal": expected_principal,
                        "privileges": sorted(self.grants[(securable_type, full_name)]),
                    }
                ]
            }
        if key[:2] == ("workspace", "list"):
            if key[2].endswith("/gurary_WT26_AIbreakout"):
                self.workspace_manifest_checked = True
                return [
                    {
                        "object_id": "manifest-123",
                        "object_type": "FILE",
                        "path": f"{key[2]}/app.yaml",
                        "resource_id": "manifest-resource-123",
                    }
                ]
            return []
        if key[:2] == ("workspace", "get-status"):
            return {
                "object_id": "directory-123",
                "object_type": "DIRECTORY",
                "path": key[2],
                "resource_id": "resource-123",
            }
        if key == ("workspace", "get-permissions", "directories", "directory-123"):
            entries = [
                {
                    "all_permissions": [
                        {"inherited": False, "permission_level": permission}
                    ],
                    "display_name": principal,
                    "service_principal_name": principal,
                }
                for principal, permission in self.workspace_acl.items()
            ]
            return {
                "access_control_list": entries,
                "object_id": "directory-123",
                "object_type": "directory",
            }
        if key == ("workspace", "update-permissions", "directories", "directory-123"):
            principal = self.app["service_principal_client_id"]
            expected = {
                "access_control_list": [
                    {
                        "service_principal_name": principal,
                        "permission_level": "CAN_READ",
                    }
                ]
            }
            if payload != expected:
                raise AssertionError("workspace source ACL was not narrowly additive")
            self.workspace_acl[principal] = "CAN_READ"
            return {
                "access_control_list": [
                    {
                        "all_permissions": [
                            {"inherited": False, "permission_level": "CAN_READ"}
                        ],
                        "display_name": principal,
                        "service_principal_name": principal,
                    }
                ],
                "object_id": "directory-123",
                "object_type": "directory",
            }
        if key[:3] == ("apps", "deploy", self.target.presenter_app_name):
            if "--auto-approve" in args:
                raise AssertionError("auto approval is forbidden")
            principal = self.app["service_principal_client_id"]
            if self.require_source_acl and self.workspace_acl.get(principal) != "CAN_READ":
                raise AssertionError("owned app cannot read the synced source directory")
            if self.require_manifest_check and not self.workspace_manifest_checked:
                raise AssertionError("synced root manifest was not checked before deployment")
            self.app["active_deployment"] = {
                "deployment_id": "deployment-456",
                "source_code_path": args[args.index("--source-code-path") + 1],
                "status": {"message": "Deployment succeeded", "state": "SUCCEEDED"},
            }
            self.app["app_status"] = {"message": "App is running", "state": "RUNNING"}
            self.app["compute_status"] = {"message": "Compute is active", "state": "ACTIVE"}
            return dict(self.app["active_deployment"])
        raise AssertionError(f"unexpected CLI mutation boundary: {key}")


class FevmProvisionPlanTest(unittest.TestCase):
    def test_dry_run_plans_only_owned_mutations_and_preserves_shared_ops(self):
        target = load_target("fevm")
        cli = FakeReadOnlyCli()

        plan = build_provision_plan(
            run_cli=cli,
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [
                {"name": "projects/shared-project", "project_id": "shared-project", "status": "ACTIVE"}
            ],
        )

        actions = plan.actions
        self.assertEqual(actions[0].kind, "create_app")
        self.assertEqual(actions[0].name, "gurary-bobabricks-store-ops")
        self.assertNotIn(SHARED_PRESENTER, json.dumps([action.as_dict() for action in actions]))
        self.assertTrue(all(action.name.startswith(("gurary-", "gurary_")) for action in actions))
        self.assertNotIn("create_ops_task", plan.effective_tools)
        self.assertEqual(plan.shared_ops_before, plan.shared_ops_after_dry_run)
        self.assertTrue(all(not item["mutation_allowed"] for item in plan.shared_inventory.values()))

    def test_dry_run_fails_closed_if_shared_ops_changes_during_planning(self):
        with self.assertRaisesRegex(RuntimeError, "changed during dry run"):
            build_provision_plan(
                run_cli=FakeReadOnlyCli(change_second_snapshot=True),
                verify_profile=lambda profile, host: {
                    "host": host,
                    "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
                },
                workspace_id_provider=lambda profile: load_target("fevm").workspace_id,
                lakebase_inventory_provider=lambda profile: [],
            )

    def test_existing_owned_app_is_reconciled_without_a_create_action(self):
        target = load_target("fevm")
        owned = app_response(target.presenter_app_name, target.workspace_id)

        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(owned_app=owned),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
        )

        self.assertNotIn("create_app", [action.kind for action in plan.actions])
        self.assertEqual(plan.actions[0].kind, "update_app_scopes")

    def test_preauthorized_plan_records_external_grants_without_local_reconciliation(self):
        target = load_target("fevm")
        try:
            plan = build_provision_plan(
                run_cli=FakeReadOnlyCli(),
                verify_profile=lambda profile, host: {
                    "host": host,
                    "current_user": {
                        "user_name": "adam.gurary@databricks.com",
                        "id": "user-123",
                    },
                },
                workspace_id_provider=lambda profile: target.workspace_id,
                lakebase_inventory_provider=lambda profile: [],
                preauthorized_trace_grants=True,
            )
        except TypeError as error:
            self.fail(f"preauthorized trace grants mode is unavailable: {error}")

        action_kinds = [action.kind for action in plan.actions]
        self.assertNotIn("grant_trace_catalog", action_kinds)
        self.assertNotIn("grant_trace_schema", action_kinds)
        self.assertEqual(
            action_kinds[2:4],
            [
                "accept_preauthorized_trace_catalog",
                "accept_preauthorized_trace_schema",
            ],
        )
        output = plan.as_dict()
        self.assertEqual(output["trace_grant_mode"], "preauthorized_external_confirmation")
        self.assertEqual(
            output["trace_grant_proof"],
            "required_live_trace_write_tags_and_spans",
        )

    def test_default_plan_still_reconciles_trace_grants_locally(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
        )

        self.assertIn("grant_trace_catalog", [action.kind for action in plan.actions])
        self.assertIn("grant_trace_schema", [action.kind for action in plan.actions])
        self.assertEqual(plan.as_dict()["trace_grant_mode"], "reconcile_locally")

    def test_accepts_the_cli_top_level_apps_list_response(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(apps_top_level_list=True),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
        )

        self.assertEqual(plan.actions[0].kind, "create_app")

    def test_canonical_snapshot_is_independent_of_sql_row_order(self):
        rows_a = [
            {"task_id": "task-2", "store_id": "104", "status": "open"},
            {"task_id": "task-1", "store_id": "101", "status": "closed"},
        ]
        rows_b = list(reversed(rows_a))

        self.assertEqual(canonical_ops_snapshot(rows_a), canonical_ops_snapshot(rows_b))


class FevmReconciliationTest(unittest.TestCase):
    def test_apply_uses_discovered_principal_and_deploys_rendered_disabled_memory_state(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
        )
        cli = FakeMutableCli()

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_root = root / "state"
            build_root = root / "build"
            sync_commands = []

            def run_sync(command: list[str]):
                sync_commands.append(command)

            with patch("deploy.render.STATE_ROOT", state_root), patch(
                "deploy.render.BUILD_ROOT", build_root
            ):
                evidence = apply_provision_plan(
                    plan,
                    run_cli=cli,
                    verify_profile=lambda profile, host: {
                        "host": host,
                        "current_user": {
                            "user_name": "adam.gurary@databricks.com",
                            "id": "user-123",
                        },
                    },
                    workspace_id_provider=lambda profile: target.workspace_id,
                    state_path=state_root / "fevm.json",
                    inventory_path=root / "live-fevm-before.json",
                    sync_runner=run_sync,
                )

            state = json.loads((state_root / "fevm.json").read_text(encoding="utf-8"))
            principal = cli.app["service_principal_client_id"]
            self.assertEqual(state["presenter_service_principal_client_id"], principal)
            self.assertIn(principal, state["mlflow_experiment_name"])
            self.assertEqual(
                state["lakebase"],
                {"enabled": False, "schema": "gurary_bobabricks_app", "validated": True},
            )

            root_env = {
                entry["name"]: str(entry["value"])
                for entry in yaml.safe_load(
                    (build_root / "fevm" / "baseline" / "app.yaml").read_text(encoding="utf-8")
                )["env"]
            }
            self.assertEqual(root_env["BOBABRICKS_DISABLE_LAKEBASE"], "1")
            self.assertNotIn("CONFLUENCE_MCP_ENABLED", root_env)
            self.assertEqual(evidence.deployment_state, "SUCCEEDED")
            self.assertEqual(evidence.app_state, "RUNNING")
            self.assertEqual(evidence.compute_state, "ACTIVE")
            self.assertTrue(
                all(name.startswith(("gurary-", "gurary_")) for name in evidence.mutated_resources)
            )
            self.assertEqual(len(sync_commands), 1)
            self.assertNotIn("--auto-approve", sync_commands[0])
            self.assertIn("--include", sync_commands[0])
            self.assertEqual(sync_commands[0][sync_commands[0].index("--include") + 1], "**")
            sync_source = Path(sync_commands[0][sync_commands[0].index("**") + 1])
            self.assertFalse(sync_source.is_symlink())
            self.assertEqual(
                sync_source,
                (build_root / "fevm" / "baseline").resolve(),
            )

            expected_grants = {
                ("catalog", target.catalog): {"USE_CATALOG"},
                ("schema", f"{target.catalog}.{target.trace_schema}"): {
                    "CREATE_TABLE",
                    "USE_SCHEMA",
                },
            }
            self.assertEqual(cli.grants, expected_grants)

    def test_preauthorized_apply_skips_grants_api_and_records_live_proof_requirement(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
            preauthorized_trace_grants=True,
        )
        cli = FakeMutableCli(forbid_grant_calls=True)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_root = root / "state"
            build_root = root / "build"
            with patch("deploy.render.STATE_ROOT", state_root), patch(
                "deploy.render.BUILD_ROOT", build_root
            ):
                evidence = apply_provision_plan(
                    plan,
                    run_cli=cli,
                    verify_profile=lambda profile, host: {
                        "host": host,
                        "current_user": {
                            "user_name": "adam.gurary@databricks.com",
                            "id": "user-123",
                        },
                    },
                    workspace_id_provider=lambda profile: target.workspace_id,
                    state_path=state_root / "fevm.json",
                    inventory_path=root / "live-fevm-before.json",
                    sync_runner=lambda command: None,
                    **probe_kwargs(),
                )

            inventory = json.loads(
                (root / "live-fevm-before.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                evidence.trace_grant_mode,
                "preauthorized_external_confirmation",
            )
            self.assertEqual(
                inventory["trace_grants"],
                {
                    "authoritative_proof_required": "required_live_trace_write_tags_and_spans",
                    "external_confirmation_recorded": True,
                    "mode": "preauthorized_external_confirmation",
                },
            )
            self.assertEqual(cli.grants, {})

    def test_apply_grants_owned_app_read_only_access_to_synced_source(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"user_name": "adam.gurary@databricks.com", "id": "user-123"},
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
            preauthorized_trace_grants=True,
        )
        cli = FakeMutableCli(
            forbid_grant_calls=True,
            require_source_acl=True,
            require_manifest_check=True,
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_root = root / "state"
            with patch("deploy.render.STATE_ROOT", state_root), patch(
                "deploy.render.BUILD_ROOT", root / "build"
            ):
                evidence = apply_provision_plan(
                    plan,
                    run_cli=cli,
                    verify_profile=lambda profile, host: {
                        "host": host,
                        "current_user": {
                            "user_name": "adam.gurary@databricks.com",
                            "id": "user-123",
                        },
                    },
                    workspace_id_provider=lambda profile: target.workspace_id,
                    state_path=state_root / "fevm.json",
                    inventory_path=root / "live-fevm-before.json",
                    sync_runner=lambda command: None,
                    **probe_kwargs(),
                )

        principal = cli.app["service_principal_client_id"]
        self.assertEqual(cli.workspace_acl, {principal: "CAN_READ"})
        self.assertEqual(evidence.deployment_state, "SUCCEEDED")

    def test_preauthorized_apply_invokes_probe_and_accepts_complete_live_trace_proof(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {
                    "user_name": "adam.gurary@databricks.com",
                    "id": "user-123",
                },
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
            preauthorized_trace_grants=True,
        )
        cli = FakeMutableCli(forbid_grant_calls=True)
        observed = []

        def invoke(profile: str, app_url: str, payload: dict) -> dict:
            observed.append(("invoke", profile, app_url, payload))
            return successful_probe_invocation(profile, app_url, payload)

        def prove(request: dict) -> dict:
            observed.append(("prove", request))
            return successful_trace_proof(request)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with patch("deploy.render.STATE_ROOT", root / "state"), patch(
                "deploy.render.BUILD_ROOT", root / "build"
            ):
                evidence = apply_provision_plan(
                    plan,
                    run_cli=cli,
                    verify_profile=lambda profile, host: {
                        "host": host,
                        "current_user": {
                            "user_name": "adam.gurary@databricks.com",
                            "id": "user-123",
                        },
                    },
                    workspace_id_provider=lambda profile: target.workspace_id,
                    state_path=root / "state" / "fevm.json",
                    inventory_path=root / "live-fevm-before.json",
                    sync_runner=lambda command: None,
                    app_invoker=invoke,
                    trace_proof_provider=prove,
                    session_id_provider=lambda: PROBE_SESSION,
                    now_ms=lambda: PROBE_STARTED_AT_MS,
                )

        self.assertEqual(evidence.trace_proof_state, "PASS")
        self.assertEqual([item[0] for item in observed], ["invoke", "prove"])
        self.assertEqual(
            observed[0][3],
            {
                "input": [{"role": "user", "content": "What tools do you have?"}],
                "custom_inputs": {"session_id": PROBE_SESSION},
            },
        )
        self.assertEqual(
            observed[1][1],
            {
                "profile": "fevm-worldtour-ai",
                "experiment_name": PROBE_EXPERIMENT,
                "trace_location": PROBE_TRACE_LOCATION,
                "warehouse_id": target.warehouse_id,
                "session_id": PROBE_SESSION,
                "not_before_ms": PROBE_STARTED_AT_MS,
            },
        )

    def test_preauthorized_apply_fails_closed_for_incomplete_live_trace_proof(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {
                    "user_name": "adam.gurary@databricks.com",
                    "id": "user-123",
                },
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
            preauthorized_trace_grants=True,
        )
        cases = {
            "wrong experiment tags": (
                {"experiment_tags": {}},
                "trace tags",
            ),
            "no persisted trace": ({"trace_id": ""}, "trace write"),
            "stale trace": (
                {"trace_request_time_ms": PROBE_STARTED_AT_MS - 1},
                "fresh trace",
            ),
            "wrong session": ({"trace_session_id": "another-session"}, "session"),
            "failed trace": ({"trace_state": "ERROR"}, "trace did not succeed"),
            "no retrieved spans": ({"spans": []}, "span retrieval"),
        }

        for label, (replacement, expected_error) in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                cli = FakeMutableCli(forbid_grant_calls=True)

                def incomplete_proof(request: dict, replacement=replacement) -> dict:
                    proof = successful_trace_proof(request)
                    proof.update(replacement)
                    return proof

                with patch("deploy.render.STATE_ROOT", root / "state"), patch(
                    "deploy.render.BUILD_ROOT", root / "build"
                ), self.assertRaisesRegex(RuntimeError, expected_error):
                    apply_provision_plan(
                        plan,
                        run_cli=cli,
                        verify_profile=lambda profile, host: {
                            "host": host,
                            "current_user": {
                                "user_name": "adam.gurary@databricks.com",
                                "id": "user-123",
                            },
                        },
                        workspace_id_provider=lambda profile: target.workspace_id,
                        state_path=root / "state" / "fevm.json",
                        inventory_path=root / "live-fevm-before.json",
                        sync_runner=lambda command: None,
                        app_invoker=successful_probe_invocation,
                        trace_proof_provider=incomplete_proof,
                        session_id_provider=lambda: PROBE_SESSION,
                        now_ms=lambda: PROBE_STARTED_AT_MS,
                    )

    def test_preauthorized_apply_fails_closed_if_probe_invocation_has_no_output(self):
        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {
                    "user_name": "adam.gurary@databricks.com",
                    "id": "user-123",
                },
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
            preauthorized_trace_grants=True,
        )
        cli = FakeMutableCli(forbid_grant_calls=True)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with patch("deploy.render.STATE_ROOT", root / "state"), patch(
                "deploy.render.BUILD_ROOT", root / "build"
            ), self.assertRaisesRegex(RuntimeError, "probe invocation"):
                apply_provision_plan(
                    plan,
                    run_cli=cli,
                    verify_profile=lambda profile, host: {
                        "host": host,
                        "current_user": {
                            "user_name": "adam.gurary@databricks.com",
                            "id": "user-123",
                        },
                    },
                    workspace_id_provider=lambda profile: target.workspace_id,
                    state_path=root / "state" / "fevm.json",
                    inventory_path=root / "live-fevm-before.json",
                    sync_runner=lambda command: None,
                    app_invoker=lambda profile, app_url, payload: {"output": []},
                    trace_proof_provider=successful_trace_proof,
                    session_id_provider=lambda: PROBE_SESSION,
                    now_ms=lambda: PROBE_STARTED_AT_MS,
                )

    def test_preauthorized_apply_retries_transient_post_deploy_app_readiness(self):
        from scripts.provision_fevm_fallback import TransientAppInvocationError

        target = load_target("fevm")
        plan = build_provision_plan(
            run_cli=FakeReadOnlyCli(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {
                    "user_name": "adam.gurary@databricks.com",
                    "id": "user-123",
                },
            },
            workspace_id_provider=lambda profile: target.workspace_id,
            lakebase_inventory_provider=lambda profile: [],
            preauthorized_trace_grants=True,
        )
        cli = FakeMutableCli(forbid_grant_calls=True)
        attempts = []
        waits = []

        def invoke(profile: str, app_url: str, payload: dict) -> dict:
            attempts.append(payload)
            if len(attempts) == 1:
                raise TransientAppInvocationError("app ingress is not ready")
            return successful_probe_invocation(profile, app_url, payload)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with patch("deploy.render.STATE_ROOT", root / "state"), patch(
                "deploy.render.BUILD_ROOT", root / "build"
            ):
                evidence = apply_provision_plan(
                    plan,
                    run_cli=cli,
                    verify_profile=lambda profile, host: {
                        "host": host,
                        "current_user": {
                            "user_name": "adam.gurary@databricks.com",
                            "id": "user-123",
                        },
                    },
                    workspace_id_provider=lambda profile: target.workspace_id,
                    state_path=root / "state" / "fevm.json",
                    inventory_path=root / "live-fevm-before.json",
                    sync_runner=lambda command: None,
                    app_invoker=invoke,
                    trace_proof_provider=successful_trace_proof,
                    session_id_provider=lambda: PROBE_SESSION,
                    now_ms=lambda: PROBE_STARTED_AT_MS,
                    app_ready_waiter=waits.append,
                )

        self.assertEqual(evidence.trace_proof_state, "PASS")
        self.assertEqual(len(attempts), 2)
        self.assertEqual(waits, [2.0])

    def test_disabled_lakebase_panel_is_not_rendered_green(self):
        from scripts.start_app import lakebase_panel_source

        with patch.dict(os.environ, {"BOBABRICKS_DISABLE_LAKEBASE": "1"}, clear=False):
            panel = lakebase_panel_source()

        self.assertIn("Memory disabled", panel)
        self.assertNotIn("bg-emerald-500", panel)


class CliCompatibilityTest(unittest.TestCase):
    def test_explicit_preauthorized_apply_mode_reaches_planning(self):
        try:
            from scripts.provision_fevm_fallback import _parse_arguments
        except ImportError as error:
            self.fail(f"preauthorized CLI mode is unavailable: {error}")

        arguments = _parse_arguments(["--apply", "--preauthorized-trace-grants"])

        self.assertTrue(arguments.apply)
        self.assertTrue(arguments.preauthorized_trace_grants)

    def test_profile_accepts_current_cli_camel_case_user_name(self):
        responses = iter([
            {"env": {"host": "https://fevm-worldtour-ai.cloud.databricks.com"}},
            {"userName": "adam.gurary@databricks.com", "id": "user-123", "active": True},
        ])

        from unittest.mock import patch

        with patch("deploy.databricks_cli.run_json", side_effect=lambda *_args: next(responses)):
            identity = assert_profile(
                "fevm-worldtour-ai", "https://fevm-worldtour-ai.cloud.databricks.com"
            )

        self.assertEqual(identity["current_user"]["userName"], "adam.gurary@databricks.com")


class LiveTraceProofProviderTest(unittest.TestCase):
    def test_uc_trace_read_uses_explicit_profile_without_default_warehouse_autostart(self):
        from scripts.provision_fevm_fallback import _default_trace_proof_provider

        request = {
            "profile": "fevm-worldtour-ai",
            "experiment_name": PROBE_EXPERIMENT,
            "trace_location": PROBE_TRACE_LOCATION,
            "warehouse_id": "warehouse-123",
            "session_id": PROBE_SESSION,
            "not_before_ms": PROBE_STARTED_AT_MS,
        }
        trace_info = SimpleNamespace(
            trace_id="trace-123",
            request_time=PROBE_STARTED_AT_MS + 1,
            trace_metadata={"mlflow.trace.session": PROBE_SESSION},
            state=SimpleNamespace(value="OK"),
        )
        trace = SimpleNamespace(
            info=trace_info,
            data=SimpleNamespace(
                spans=[
                    SimpleNamespace(
                        name="ResponsesAgent",
                        status=SimpleNamespace(value="OK"),
                    )
                ]
            ),
        )

        class FakeMlflowClient:
            def get_experiment_by_name(self, name):
                return SimpleNamespace(
                    name=PROBE_EXPERIMENT,
                    tags=successful_trace_proof(request)["experiment_tags"],
                )

            def search_traces(self, **kwargs):
                if os.environ.get("MLFLOW_SQL_WAREHOUSE_AUTO_START") != "false":
                    raise AssertionError("trace search attempted default-profile warehouse auth")
                return [trace]

            def get_trace(self, trace_id, display):
                return trace

        with patch.dict(
            os.environ,
            {
                "MLFLOW_TRACING_SQL_WAREHOUSE_ID": "previous-warehouse",
                "MLFLOW_SQL_WAREHOUSE_AUTO_START": "original-setting",
            },
            clear=False,
        ), patch("mlflow.MlflowClient", return_value=FakeMlflowClient()), patch(
            "scripts.provision_fevm_fallback.time.monotonic",
            side_effect=[0.0, 0.0, 121.0],
        ), patch("scripts.provision_fevm_fallback.time.sleep"):
            proof = _default_trace_proof_provider(request)

            self.assertEqual(
                os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"],
                "previous-warehouse",
            )
            self.assertEqual(
                os.environ["MLFLOW_SQL_WAREHOUSE_AUTO_START"],
                "original-setting",
            )

        self.assertEqual(proof["trace_session_id"], PROBE_SESSION)
        self.assertEqual(len(proof["spans"]), 1)


if __name__ == "__main__":
    unittest.main()
