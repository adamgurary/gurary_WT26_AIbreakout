#!/usr/bin/env python3
"""Plan and reconcile the private field-engineering data foundation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.config import TargetConfig, load_target
from deploy.databricks_cli import REQUIRED_USER, assert_profile, run_json
from deploy.lakebase import LakebaseState, ensure_lakebase
from deploy.safety import assert_safe_mutation


EXPECTED_COUNTS = {
    "stores": 14,
    "training_courses": 5,
    "employees": 168,
    "training_progress": 504,
    "store_weekly_metrics": 112,
    "schedules": 14,
    "training_events": 14,
    "storetime_shifts": 196,
    "inventory": 70,
    "customer_feedback": 252,
    "ops_tasks": 4,
    "store_metrics": 14,
}
TABLE_NAMES = tuple(EXPECTED_COUNTS)
FIELD_PROFILE = "e2-demo-field-eng"
FIELD_HOST = "https://e2-demo-field-eng.cloud.databricks.com"
FIELD_WORKSPACE_ID = "1444828305810485"
FIELD_WAREHOUSE_NAME = "gurary_bobabricks_" "warehouse"
FIELD_CATALOG = "gurary_" "catalog"
FIELD_DATA_SCHEMA = "gurary_bobabricks_store_" "ops"
FIELD_TRACE_SCHEMA = "gurary_ai_gate" "way_demo"
FIELD_TABLE_PREFIX = "gurary_"
FIELD_STORETIME_APP = "gurary-bobabricks-store" "time"
FIELD_OPSTASK_APP = "gurary-bobabricks-ops" "task-mcp"
WAREHOUSE_CONFIG = {
    "name": FIELD_WAREHOUSE_NAME,
    "cluster_size": "X-Small",
    "warehouse_type": "PRO",
    "enable_serverless_compute": True,
    "auto_stop_mins": 10,
    "min_num_clusters": 1,
    "max_num_clusters": 1,
}
DATA_PROVISIONER_ARGV = (
    "scripts/provision_databricks_assets.py",
    "--profile",
    FIELD_PROFILE,
    "--warehouse-id",
    "{warehouse_id}",
    "--catalog",
    FIELD_CATALOG,
    "--schema",
    FIELD_DATA_SCHEMA,
    "--table-prefix",
    FIELD_TABLE_PREFIX,
)
SCHEMA_COMMENT = "Private Bobabricks field-engineering demo resources"
SQL_STATEMENTS_ENDPOINT = "/api/2.0/sql/statements"
CONCRETE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]+")
MCP_STORETIME_TABLES = ("schedules", "training_events", "storetime_shifts")
MCP_OPSTASK_TABLE = "ops_tasks"
MCP_WORKSPACE_PARENT = "/Workspace/Users/adam.gurary@databricks.com"

RunCli = Callable[[str, list[str], dict | None], dict | list]
VerifyProfile = Callable[[str, str], dict]
WorkspaceIdProvider = Callable[[str], str]
ProcessRunner = Callable[[list[str]], None]
Waiter = Callable[[float], None]
WorkspaceFactory = Callable[[str], Any]
LakebaseEnsurer = Callable[[Any, TargetConfig], LakebaseState]
SyncRunner = Callable[[list[str]], None]
McpInvoker = Callable[[str, str, dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class Action:
    kind: str
    name: str
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProvisionPlan:
    target: TargetConfig
    actions: tuple[Action, ...]
    warehouse: dict[str, Any] | None
    existing_schemas: tuple[str, ...]
    existing_tables: tuple[str, ...]
    data_provisioner_argv: tuple[str, ...] = DATA_PROVISIONER_ARGV

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": "dry-run",
            "prechecks": {
                "profile_identity": "PASS",
                "workspace_id": "PASS",
                "catalog_access": "PASS",
            },
            "actions": [action.as_dict() for action in self.actions],
            "data_provisioner_argv": list(self.data_provisioner_argv),
        }


@dataclass(frozen=True)
class ApplyEvidence:
    warehouse_id: str
    schemas: tuple[str, ...]
    table_names: tuple[str, ...]
    counts: dict[str, int]
    store_104_training_completion: float
    store_104_storetime: tuple[int, int, int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": "apply",
            "warehouse_id": self.warehouse_id,
            "schemas": list(self.schemas),
            "table_names": list(self.table_names),
            "counts": dict(self.counts),
            "store_104": {
                "latest_training_completion": self.store_104_training_completion,
                "storetime": {
                    "scheduled": self.store_104_storetime[0],
                    "completed": self.store_104_storetime[1],
                    "converted": self.store_104_storetime[2],
                },
            },
        }


@dataclass(frozen=True)
class McpGrant:
    securable_type: str
    full_name: str
    privileges: frozenset[str]


@dataclass(frozen=True)
class McpAppPlan:
    app_name: str
    source_relative: str
    workspace_path: str
    grants: tuple[McpGrant, ...]
    current_app: dict[str, Any] | None


@dataclass(frozen=True)
class McpDeploymentPlan:
    target: TargetConfig
    environment: dict[str, str]
    apps: tuple[McpAppPlan, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": "dry-run",
            "prechecks": {"profile_identity": "PASS", "workspace_id": "PASS"},
            "environment": dict(self.environment),
            "apps": [
                {
                    "name": app.app_name,
                    "action": "reuse_app" if app.current_app else "create_app",
                    "source": app.source_relative,
                    "workspace_path": app.workspace_path,
                    "grants": [
                        {
                            "securable_type": grant.securable_type,
                            "full_name": grant.full_name,
                            "privileges": sorted(grant.privileges),
                        }
                        for grant in app.grants
                    ],
                }
                for app in self.apps
            ],
        }


@dataclass(frozen=True)
class McpDeploymentEvidence:
    app_states: dict[str, str]
    deployment_states: dict[str, str]
    tools: dict[str, tuple[str, ...]]
    store_104_storetime: tuple[int, int, int]
    ops_baseline_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": "apply-mcp",
            "app_states": dict(self.app_states),
            "deployment_states": dict(self.deployment_states),
            "tools": {name: list(tools) for name, tools in self.tools.items()},
            "store_104_storetime": list(self.store_104_storetime),
            "ops_baseline_count": self.ops_baseline_count,
        }


def _workspace_id_from_sdk(profile: str) -> str:
    from databricks.sdk import WorkspaceClient

    return str(WorkspaceClient(profile=profile).get_workspace_id())


def _records(response: dict | list, key: str, label: str) -> list[dict[str, Any]]:
    value = response if isinstance(response, list) else response.get(key, [])
    if not isinstance(value, list) or not all(isinstance(record, dict) for record in value):
        raise RuntimeError(f"Databricks {label} response is malformed")
    return [dict(record) for record in value]


def _authenticated_user(identity: dict[str, Any]) -> str:
    current_user = identity.get("current_user") if isinstance(identity, dict) else None
    if not isinstance(current_user, dict):
        raise RuntimeError("Databricks current-user identity is malformed")
    user_name = current_user.get("user_name", current_user.get("userName"))
    if user_name != REQUIRED_USER:
        raise RuntimeError("Databricks current user does not match the field-eng owner")
    return user_name


def _require_target_contract(target: TargetConfig) -> None:
    expected = {
        "key": "field_eng",
        "profile": FIELD_PROFILE,
        "host": FIELD_HOST,
        "workspace_id": FIELD_WORKSPACE_ID,
        "catalog": FIELD_CATALOG,
        "data_schema": FIELD_DATA_SCHEMA,
        "trace_schema": FIELD_TRACE_SCHEMA,
        "table_prefix": FIELD_TABLE_PREFIX,
        "warehouse_name": FIELD_WAREHOUSE_NAME,
        "storetime_app_name": FIELD_STORETIME_APP,
        "opstask_app_name": FIELD_OPSTASK_APP,
    }
    if (
        any(getattr(target, name) != value for name, value in expected.items())
        or target.storetime_app_name == target.opstask_app_name
    ):
        raise RuntimeError("Field-eng target configuration does not match the safety contract")


def _assert_target_binding(
    target: TargetConfig,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> str:
    _require_target_contract(target)
    identity = verify_profile(target.profile, target.host)
    user_name = _authenticated_user(identity)
    if str(workspace_id_provider(target.profile)) != target.workspace_id:
        raise RuntimeError("Databricks workspace ID does not match the field-eng target")
    return user_name


def _catalog_privileges(response: dict | list, principal: str) -> set[str]:
    if not isinstance(response, dict):
        raise RuntimeError("Databricks catalog grants response is malformed")
    assignments = response.get("privilege_assignments", [])
    if not isinstance(assignments, list) or not all(
        isinstance(assignment, dict) for assignment in assignments
    ):
        raise RuntimeError("Databricks catalog grants response is malformed")
    privileges: set[str] = set()
    for assignment in assignments:
        if assignment.get("principal") != principal:
            continue
        values = assignment.get("privileges", [])
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise RuntimeError("Databricks catalog privileges are malformed")
        privileges.update(values)
    return privileges


def _assert_catalog_access(
    run_cli: RunCli, target: TargetConfig, expected_owner: str
) -> dict[str, Any]:
    catalog = run_cli(target.profile, ["catalogs", "get", target.catalog])
    if not isinstance(catalog, dict) or catalog.get("name") != target.catalog:
        raise RuntimeError("Databricks catalog response did not match the configured catalog")
    if catalog.get("owner") == expected_owner:
        return dict(catalog)
    grants = run_cli(target.profile, ["grants", "get", "catalog", target.catalog])
    if not {"MANAGE", "ALL_PRIVILEGES"} & _catalog_privileges(grants, expected_owner):
        raise RuntimeError("Authenticated user cannot manage the configured catalog")
    return dict(catalog)


def _exact_schema_names(target: TargetConfig) -> tuple[str, str]:
    return (
        f"{target.catalog}.{target.data_schema}",
        f"{target.catalog}.{target.trace_schema}",
    )


def _exact_table_names(target: TargetConfig) -> tuple[str, ...]:
    namespace = f"{target.catalog}.{target.data_schema}"
    return tuple(f"{namespace}.{target.table_prefix}{table}" for table in TABLE_NAMES)


def _assert_exact_owned_name(target: TargetConfig, resource_type: str, name: str) -> None:
    allowed = {target.warehouse_name, *_exact_schema_names(target), *_exact_table_names(target)}
    if name not in allowed:
        raise RuntimeError(f"Refusing unexpected field-eng {resource_type}")
    assert_safe_mutation(target, resource_type, name)


def _warehouse_matches(response: dict[str, Any], target: TargetConfig) -> bool:
    return response.get("name") == target.warehouse_name


def _warehouse_id(warehouse: dict[str, Any]) -> str:
    warehouse_id = warehouse.get("id")
    if not isinstance(warehouse_id, str) or not CONCRETE_ID.fullmatch(warehouse_id):
        raise RuntimeError("Owned warehouse has no concrete ID")
    return warehouse_id


def _find_owned_warehouse(run_cli: RunCli, target: TargetConfig) -> dict[str, Any] | None:
    warehouses = _records(
        run_cli(target.profile, ["warehouses", "list"]), "warehouses", "warehouses list"
    )
    matches = [record for record in warehouses if _warehouse_matches(record, target)]
    if len(matches) > 1:
        raise RuntimeError("Databricks returned duplicate owned warehouse records")
    return dict(matches[0]) if matches else None


def _schema_records(run_cli: RunCli, target: TargetConfig) -> list[dict[str, Any]]:
    return _records(
        run_cli(target.profile, ["schemas", "list", target.catalog]),
        "schemas",
        "schemas list",
    )


def _table_records(run_cli: RunCli, target: TargetConfig) -> list[dict[str, Any]]:
    return _records(
        run_cli(target.profile, ["tables", "list", target.catalog, target.data_schema]),
        "tables",
        "tables list",
    )


def build_provision_plan(
    *,
    run_cli: RunCli = run_json,
    verify_profile: VerifyProfile = assert_profile,
    workspace_id_provider: WorkspaceIdProvider = _workspace_id_from_sdk,
) -> ProvisionPlan:
    target = load_target("field_eng")
    expected_owner = _assert_target_binding(target, verify_profile, workspace_id_provider)

    warehouses = _records(
        run_cli(target.profile, ["warehouses", "list"]), "warehouses", "warehouses list"
    )
    matches = [record for record in warehouses if record.get("name") == target.warehouse_name]
    if len(matches) > 1:
        raise RuntimeError("Databricks returned duplicate owned warehouse records")

    _assert_catalog_access(run_cli, target, expected_owner)

    schemas = _records(
        run_cli(target.profile, ["schemas", "list", target.catalog]),
        "schemas",
        "schemas list",
    )
    existing_schemas = tuple(
        sorted(
            str(record["full_name"])
            for record in schemas
            if record.get("full_name")
            in {
                f"{target.catalog}.{target.data_schema}",
                f"{target.catalog}.{target.trace_schema}",
            }
        )
    )
    data_schema_name = f"{target.catalog}.{target.data_schema}"
    existing_tables: tuple[str, ...] = ()
    if data_schema_name in existing_schemas:
        tables = _records(
            run_cli(target.profile, ["tables", "list", target.catalog, target.data_schema]),
            "tables",
            "tables list",
        )
        existing_tables = tuple(
            sorted(
                str(record["full_name"])
                for record in tables
                if record.get("full_name")
                in {
                    f"{data_schema_name}.{target.table_prefix}{table}"
                    for table in TABLE_NAMES
                }
            )
        )

    actions = [
        Action("reuse_warehouse" if matches else "create_warehouse", target.warehouse_name)
    ]
    for schema in (target.data_schema, target.trace_schema):
        full_name = f"{target.catalog}.{schema}"
        actions.append(
            Action("reuse_schema" if full_name in existing_schemas else "create_schema", full_name)
        )
    actions.extend(
        Action("replace_table", f"{data_schema_name}.{target.table_prefix}{table}")
        for table in TABLE_NAMES
    )
    return ProvisionPlan(
        target=target,
        actions=tuple(actions),
        warehouse=dict(matches[0]) if matches else None,
        existing_schemas=existing_schemas,
        existing_tables=existing_tables,
    )


def _ensure_warehouse(
    target: TargetConfig,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> dict[str, Any]:
    _assert_target_binding(target, verify_profile, workspace_id_provider)
    _assert_exact_owned_name(target, "warehouse", str(target.warehouse_name))
    warehouse = _find_owned_warehouse(run_cli, target)
    if warehouse is None:
        created = run_cli(target.profile, ["warehouses", "create"], dict(WAREHOUSE_CONFIG))
        if not isinstance(created, dict) or not _warehouse_matches(created, target):
            raise RuntimeError("Created warehouse did not match the exact owned name")
        warehouse = dict(created)
    warehouse_id = _warehouse_id(warehouse)
    current = run_cli(target.profile, ["warehouses", "get", warehouse_id])
    if (
        not isinstance(current, dict)
        or not _warehouse_matches(current, target)
        or str(current.get("id")) != warehouse_id
    ):
        raise RuntimeError("Owned warehouse readback did not match its exact identity")
    return dict(current)


def _ensure_schema(
    target: TargetConfig,
    schema: str,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> dict[str, Any]:
    expected_owner = _assert_target_binding(target, verify_profile, workspace_id_provider)
    full_name = f"{target.catalog}.{schema}"
    _assert_exact_owned_name(target, "schema", full_name)
    _assert_catalog_access(run_cli, target, expected_owner)
    matches = [record for record in _schema_records(run_cli, target) if record.get("full_name") == full_name]
    if len(matches) > 1:
        raise RuntimeError("Databricks returned duplicate owned schema records")
    if matches:
        return dict(matches[0])
    created = run_cli(
        target.profile,
        ["schemas", "create", schema, target.catalog, "--comment", SCHEMA_COMMENT],
    )
    if (
        not isinstance(created, dict)
        or created.get("full_name") != full_name
        or created.get("catalog_name") != target.catalog
        or created.get("name") != schema
    ):
        raise RuntimeError("Created schema did not match the exact owned name")
    return dict(created)


def _default_process_runner(command: list[str]) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as temporary:
        temporary.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def _read_generated_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("Field-eng generated state is unreadable") from error
    if not isinstance(value, dict):
        raise RuntimeError("Field-eng generated state must be an object")
    allowed = {
        "target",
        "warehouse_id",
        "genie_space_id",
        "lakebase",
        "storetime_app_id",
        "storetime_service_principal_client_id",
        "storetime_mcp_url",
        "opstask_app_id",
        "opstask_service_principal_client_id",
        "opstask_mcp_url",
    }
    if set(value) - allowed:
        raise RuntimeError("Field-eng generated state contains unexpected fields")
    if value.get("target") not in (None, "field_eng"):
        raise RuntimeError("Refusing to merge generated state for another target")
    return dict(value)


def _mcp_grants(target: TargetConfig, app_name: str) -> tuple[McpGrant, ...]:
    namespace = f"{target.catalog}.{target.data_schema}"
    common = (
        McpGrant("catalog", target.catalog, frozenset({"USE_CATALOG"})),
        McpGrant("schema", namespace, frozenset({"USE_SCHEMA"})),
    )
    if app_name == target.storetime_app_name:
        return common + tuple(
            McpGrant(
                "table",
                f"{namespace}.{target.table_prefix}{table}",
                frozenset({"SELECT"}),
            )
            for table in MCP_STORETIME_TABLES
        )
    if app_name == target.opstask_app_name:
        return common + (
            McpGrant(
                "table",
                f"{namespace}.{target.table_prefix}{MCP_OPSTASK_TABLE}",
                frozenset({"SELECT", "MODIFY"}),
            ),
        )
    raise RuntimeError("Refusing to plan an unexpected field-eng MCP app")


def build_mcp_deployment_plan(
    *,
    run_cli: RunCli = run_json,
    verify_profile: VerifyProfile = assert_profile,
    workspace_id_provider: WorkspaceIdProvider = _workspace_id_from_sdk,
    state_path: Path | None = None,
) -> McpDeploymentPlan:
    """Inspect and plan the two exact private field-eng MCP deployments."""
    target = load_target("field_eng")
    expected_owner = _assert_target_binding(target, verify_profile, workspace_id_provider)
    output_path = state_path or ROOT / "deploy" / "state" / "field_eng.json"
    state = _read_generated_state(output_path)
    warehouse_id = state.get("warehouse_id")
    if state.get("target") != target.key or not isinstance(warehouse_id, str):
        raise RuntimeError("Field-eng generated state is missing the private warehouse")

    app_records = _records(run_cli(target.profile, ["apps", "list"]), "apps", "apps list")
    apps: list[McpAppPlan] = []
    app_specs = (
        (target.storetime_app_name, "mcp-apps/storetime"),
        (target.opstask_app_name, "mcp-apps/opstask"),
    )
    for app_name, source_relative in app_specs:
        assert_safe_mutation(target, "app", app_name)
        matches = [dict(record) for record in app_records if record.get("name") == app_name]
        if len(matches) > 1:
            raise RuntimeError("Databricks returned duplicate owned MCP app records")
        current = None
        if matches:
            readback = run_cli(target.profile, ["apps", "get", app_name])
            if not isinstance(readback, dict):
                raise RuntimeError("Databricks MCP app response is malformed")
            current = _validate_mcp_app(target, app_name, readback, expected_owner)
        workspace_path = f"{MCP_WORKSPACE_PARENT}/{app_name}"
        assert_safe_mutation(target, "workspace directory", app_name)
        apps.append(
            McpAppPlan(
                app_name=app_name,
                source_relative=source_relative,
                workspace_path=workspace_path,
                grants=_mcp_grants(target, app_name),
                current_app=current,
            )
        )
    return McpDeploymentPlan(
        target=target,
        environment={
            "DATABRICKS_WAREHOUSE_ID": warehouse_id,
            "DATABRICKS_CATALOG": target.catalog,
            "DATABRICKS_SCHEMA": target.data_schema,
            "DATABRICKS_TABLE_PREFIX": target.table_prefix,
        },
        apps=tuple(apps),
    )


def _app_url(target: TargetConfig, app_name: str) -> str:
    return f"https://{app_name}-{target.workspace_id}.aws.databricksapps.com"


def _require_uuid(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"Owned MCP app is missing concrete {label}")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise RuntimeError(f"Owned MCP app is missing concrete {label}") from error
    if str(parsed) != value.lower():
        raise RuntimeError(f"Owned MCP app has non-canonical {label}")
    return value


def _validate_mcp_app(
    target: TargetConfig, app_name: str, app: dict[str, Any], expected_owner: str
) -> dict[str, Any]:
    if app.get("name") != app_name or app.get("url") != _app_url(target, app_name):
        raise RuntimeError("Owned MCP app readback did not match its exact name and URL")
    if expected_owner not in {app.get("creator"), app.get("owner")}:
        raise RuntimeError("Owned MCP app ownership does not match Adam")
    _require_uuid(app.get("id"), "app ID")
    _require_uuid(app.get("service_principal_client_id"), "service-principal client ID")
    return dict(app)


def _inspect_mcp_app(
    target: TargetConfig,
    app_name: str,
    run_cli: RunCli,
    expected_owner: str,
) -> dict[str, Any] | None:
    if app_name not in {target.storetime_app_name, target.opstask_app_name}:
        raise RuntimeError("Refusing to inspect an unexpected field-eng MCP app")
    assert_safe_mutation(target, "app", app_name)
    apps = _records(run_cli(target.profile, ["apps", "list"]), "apps", "apps list")
    matches = [record for record in apps if record.get("name") == app_name]
    if len(matches) > 1:
        raise RuntimeError("Databricks returned duplicate owned MCP app records")
    if not matches:
        return None
    response = run_cli(target.profile, ["apps", "get", app_name])
    if not isinstance(response, dict):
        raise RuntimeError("Databricks MCP app response is malformed")
    return _validate_mcp_app(target, app_name, response, expected_owner)


def _fresh_mcp_app_precheck(
    target: TargetConfig,
    app_name: str,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> tuple[str, dict[str, Any] | None]:
    owner = _assert_target_binding(target, verify_profile, workspace_id_provider)
    return owner, _inspect_mcp_app(target, app_name, run_cli, owner)


def _require_same_mcp_identity(
    app: dict[str, Any], app_id: str, principal: str
) -> None:
    if app.get("id") != app_id or app.get("service_principal_client_id") != principal:
        raise RuntimeError("Owned MCP app identity changed during reconciliation")


def _ensure_mcp_app(
    target: TargetConfig,
    app_plan: McpAppPlan,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> dict[str, Any]:
    owner, current = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if current is None:
        created = run_cli(
            target.profile,
            ["apps", "create", app_plan.app_name],
        )
        if not isinstance(created, dict) or created.get("name") != app_plan.app_name:
            raise RuntimeError("Created MCP app did not match the exact owned name")
    owner, app = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if app is None:
        raise RuntimeError("Owned MCP app was absent after reconciliation")
    return _validate_mcp_app(target, app_plan.app_name, app, owner)


def _principal_privileges(response: dict | list, principal: str) -> set[str]:
    if isinstance(response, list):
        assignments = response
    elif isinstance(response, dict):
        assignments = response.get("privilege_assignments", [])
    else:
        raise RuntimeError("Databricks grants response is malformed")
    if not isinstance(assignments, list) or not all(
        isinstance(assignment, dict) for assignment in assignments
    ):
        raise RuntimeError("Databricks grants response is malformed")
    privileges: set[str] = set()
    for assignment in assignments:
        if assignment.get("principal") != principal:
            continue
        values = assignment.get("privileges", [])
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise RuntimeError("Databricks grant privileges are malformed")
        privileges.update(values)
    return privileges


def _inspect_mcp_securable(
    target: TargetConfig,
    grant: McpGrant,
    run_cli: RunCli,
    expected_owner: str,
) -> None:
    assert_safe_mutation(target, grant.securable_type, grant.full_name)
    if grant.securable_type == "catalog":
        response = run_cli(target.profile, ["catalogs", "get", grant.full_name])
        identity = response.get("name") if isinstance(response, dict) else None
    elif grant.securable_type == "schema":
        response = run_cli(target.profile, ["schemas", "get", grant.full_name])
        identity = response.get("full_name") if isinstance(response, dict) else None
    elif grant.securable_type == "table":
        response = run_cli(target.profile, ["tables", "get", grant.full_name])
        identity = response.get("full_name") if isinstance(response, dict) else None
    else:
        raise RuntimeError("Refusing unsupported MCP grant securable type")
    if identity != grant.full_name:
        raise RuntimeError("MCP grant resource readback did not match its exact name")
    if expected_owner not in {response.get("owner"), response.get("created_by")}:
        raise RuntimeError("MCP grant resource ownership does not match Adam")


def _ensure_mcp_grant(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    grant: McpGrant,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> bool:
    allowed = {item for item in _mcp_grants(target, app_plan.app_name)}
    if grant not in allowed:
        raise RuntimeError("Refusing an MCP grant outside the exact private ledger")
    owner, current_app = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if current_app is None:
        raise RuntimeError("Owned MCP app disappeared before grant reconciliation")
    principal = app["service_principal_client_id"]
    _require_same_mcp_identity(current_app, app["id"], principal)
    _inspect_mcp_securable(target, grant, run_cli, owner)
    args = ["grants", "get", grant.securable_type, grant.full_name]
    current = run_cli(target.profile, args)
    missing = grant.privileges - _principal_privileges(current, principal)
    if missing:
        run_cli(
            target.profile,
            ["grants", "update", grant.securable_type, grant.full_name],
            {"changes": [{"principal": principal, "add": sorted(missing)}]},
        )
    effective = run_cli(target.profile, args)
    if not grant.privileges.issubset(_principal_privileges(effective, principal)):
        raise RuntimeError("Owned MCP app grant was not effective")
    return bool(missing)


def _merge_mcp_state(
    path: Path,
    target: TargetConfig,
    apps: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    state = _read_generated_state(path)
    fields: dict[str, Any] = {}
    for prefix, app_name in (
        ("storetime", target.storetime_app_name),
        ("opstask", target.opstask_app_name),
    ):
        app = apps[app_name]
        fields[f"{prefix}_app_id"] = app["id"]
        fields[f"{prefix}_service_principal_client_id"] = app[
            "service_principal_client_id"
        ]
        fields[f"{prefix}_mcp_url"] = f"{app['url']}/mcp"
    state.update(fields)
    _write_json_atomic(path, state)
    return state


def _render_mcp_source(
    plan: McpDeploymentPlan,
    app_plan: McpAppPlan,
    build_root: Path,
) -> Path:
    from deploy.render import _write_manifest

    source = (ROOT / app_plan.source_relative).resolve(strict=True)
    expected_source = (ROOT / "mcp-apps" / (
        "storetime" if app_plan.app_name == plan.target.storetime_app_name else "opstask"
    )).resolve(strict=True)
    if source != expected_source or not source.is_dir():
        raise RuntimeError("MCP source directory escaped its exact private deployment source")
    build_root.mkdir(parents=True, exist_ok=True)
    destination = build_root / app_plan.app_name
    with tempfile.TemporaryDirectory(prefix=f".{app_plan.app_name}-", dir=build_root) as temporary:
        staged = Path(temporary) / "source"
        shutil.copytree(
            source,
            staged,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        environment = list(plan.environment.items())
        for manifest_name in ("app.yaml", "app.yml"):
            _write_manifest(staged / manifest_name, environment)
        if destination.exists():
            if not destination.is_dir() or destination.is_symlink():
                raise RuntimeError("Refusing to replace an unsafe MCP render destination")
            shutil.rmtree(destination)
        os.replace(staged, destination)

    import yaml

    manifest = yaml.safe_load((destination / "app.yaml").read_text(encoding="utf-8"))
    entries = manifest.get("env") if isinstance(manifest, dict) else None
    if not isinstance(entries, list):
        raise RuntimeError("Rendered MCP manifest is malformed")
    effective = {
        entry.get("name"): str(entry.get("value"))
        for entry in entries
        if isinstance(entry, dict)
    }
    if effective != plan.environment:
        raise RuntimeError("Rendered MCP manifest escaped the private environment")
    return destination


def _workspace_objects(response: dict | list) -> list[dict[str, Any]]:
    if isinstance(response, list):
        objects = response
    elif isinstance(response, dict):
        objects = response.get("objects", [])
    else:
        raise RuntimeError("Databricks workspace response is malformed")
    if not isinstance(objects, list) or not all(isinstance(item, dict) for item in objects):
        raise RuntimeError("Databricks workspace response is malformed")
    return [dict(item) for item in objects]


def _precheck_workspace_mutation(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> None:
    _owner, current_app = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if current_app is None:
        raise RuntimeError("Owned MCP app disappeared before workspace mutation")
    _require_same_mcp_identity(
        current_app, app["id"], app["service_principal_client_id"]
    )
    assert_safe_mutation(target, "workspace directory", app_plan.app_name)
    objects = _workspace_objects(
        run_cli(target.profile, ["workspace", "list", MCP_WORKSPACE_PARENT])
    )
    matches = [item for item in objects if item.get("path") == app_plan.workspace_path]
    if len(matches) > 1:
        raise RuntimeError("Workspace returned duplicate owned MCP source directories")
    if matches and str(matches[0].get("object_type")).upper() != "DIRECTORY":
        raise RuntimeError("Owned MCP workspace path is not a directory")


def _sync_mcp_source(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    source: Path,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
    sync_runner: SyncRunner,
) -> None:
    _precheck_workspace_mutation(
        target, app_plan, app, run_cli, verify_profile, workspace_id_provider
    )
    sync_runner(
        [
            "databricks",
            "sync",
            "--full",
            "--include",
            "**",
            str(source.resolve(strict=True)),
            app_plan.workspace_path,
            "--profile",
            target.profile,
        ]
    )
    objects = _workspace_objects(
        run_cli(target.profile, ["workspace", "list", app_plan.workspace_path])
    )
    if not any(
        item.get("path") == f"{app_plan.workspace_path}/app.yaml"
        and str(item.get("object_type")).upper() == "FILE"
        for item in objects
    ):
        raise RuntimeError("MCP workspace sync did not produce the exact app manifest")


def _workspace_acl(response: dict | list) -> list[dict[str, Any]]:
    if not isinstance(response, dict):
        raise RuntimeError("Workspace permission response is malformed")
    entries = response.get("access_control_list", [])
    if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
        raise RuntimeError("Workspace permission response is malformed")
    return [dict(entry) for entry in entries]


def _workspace_can_read(response: dict | list, principal: str) -> bool:
    for entry in _workspace_acl(response):
        if entry.get("service_principal_name") != principal:
            continue
        permissions = entry.get("all_permissions", [])
        if not isinstance(permissions, list):
            raise RuntimeError("Workspace principal permissions are malformed")
        if any(
            isinstance(permission, dict)
            and permission.get("permission_level")
            in {"CAN_READ", "CAN_EDIT", "CAN_MANAGE", "CAN_RUN"}
            for permission in permissions
        ):
            return True
    return False


def _warehouse_can_use(response: dict | list, principal: str) -> bool:
    if not isinstance(response, dict):
        raise RuntimeError("Warehouse permission response is malformed")
    entries = response.get("access_control_list", [])
    if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
        raise RuntimeError("Warehouse permission response is malformed")
    for entry in entries:
        if entry.get("service_principal_name") != principal:
            continue
        permissions = entry.get("all_permissions", [])
        if not isinstance(permissions, list):
            raise RuntimeError("Warehouse principal permissions are malformed")
        if any(
            isinstance(permission, dict)
            and permission.get("permission_level") in {"CAN_USE", "CAN_MANAGE", "IS_OWNER"}
            for permission in permissions
        ):
            return True
    return False


def _ensure_private_warehouse_use(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    warehouse_id: str,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> bool:
    owner, current_app = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if current_app is None:
        raise RuntimeError("Owned MCP app disappeared before warehouse permission mutation")
    principal = app["service_principal_client_id"]
    _require_same_mcp_identity(current_app, app["id"], principal)
    assert_safe_mutation(target, "warehouse", str(target.warehouse_name))
    warehouse = run_cli(target.profile, ["warehouses", "get", warehouse_id])
    if (
        not isinstance(warehouse, dict)
        or warehouse.get("id") != warehouse_id
        or warehouse.get("name") != target.warehouse_name
        or owner not in {warehouse.get("creator_name"), warehouse.get("owner")}
    ):
        raise RuntimeError("Private MCP warehouse readback did not match its exact owned identity")
    args = ["permissions", "get", "warehouses", warehouse_id]
    current = run_cli(target.profile, args)
    changed = not _warehouse_can_use(current, principal)
    if changed:
        run_cli(
            target.profile,
            ["permissions", "update", "warehouses", warehouse_id],
            {
                "access_control_list": [
                    {
                        "service_principal_name": principal,
                        "permission_level": "CAN_USE",
                    }
                ]
            },
        )
    effective = run_cli(target.profile, args)
    if not _warehouse_can_use(effective, principal):
        raise RuntimeError("Owned MCP app cannot use the exact private warehouse")
    return changed


def _ensure_workspace_read(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> bool:
    _precheck_workspace_mutation(
        target, app_plan, app, run_cli, verify_profile, workspace_id_provider
    )
    status = run_cli(
        target.profile, ["workspace", "get-status", app_plan.workspace_path]
    )
    if (
        not isinstance(status, dict)
        or status.get("path") != app_plan.workspace_path
        or str(status.get("object_type")).upper() != "DIRECTORY"
    ):
        raise RuntimeError("Owned MCP workspace source directory readback did not match")
    object_id = status.get("object_id")
    if not isinstance(object_id, (str, int)) or not str(object_id):
        raise RuntimeError("Owned MCP workspace source directory has no concrete ID")
    args = ["workspace", "get-permissions", "directories", str(object_id)]
    principal = app["service_principal_client_id"]
    current = run_cli(target.profile, args)
    changed = not _workspace_can_read(current, principal)
    if changed:
        run_cli(
            target.profile,
            ["workspace", "update-permissions", "directories", str(object_id)],
            {
                "access_control_list": [
                    {
                        "service_principal_name": principal,
                        "permission_level": "CAN_READ",
                    }
                ]
            },
        )
    effective = run_cli(target.profile, args)
    if not _workspace_can_read(effective, principal):
        raise RuntimeError("Owned MCP app cannot read its exact workspace source")
    return changed


def _status_state(value: Any, label: str) -> str:
    state = value.get("state") if isinstance(value, dict) else value
    if not isinstance(state, str) or not state:
        raise RuntimeError(f"Owned MCP app {label} state is malformed")
    return state


def _deployment_id(response: dict[str, Any]) -> str:
    value = response.get("deployment_id")
    if not isinstance(value, str) or not value or "\x00" in value:
        raise RuntimeError("MCP app deployment has no concrete deployment ID")
    return value


def _deploy_mcp_app(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
    waiter: Waiter,
) -> tuple[str, str]:
    _precheck_workspace_mutation(
        target, app_plan, app, run_cli, verify_profile, workspace_id_provider
    )
    status = run_cli(
        target.profile, ["workspace", "get-status", app_plan.workspace_path]
    )
    if not isinstance(status, dict) or status.get("path") != app_plan.workspace_path:
        raise RuntimeError("MCP deployment source did not match the owned workspace path")
    response = run_cli(
        target.profile,
        [
            "apps",
            "deploy",
            app_plan.app_name,
            "--source-code-path",
            app_plan.workspace_path,
        ],
    )
    if not isinstance(response, dict):
        raise RuntimeError("Databricks MCP deployment response is malformed")
    deployment_id = _deployment_id(response)
    deployment: dict[str, Any] = {}
    for attempt in range(120):
        current = run_cli(
            target.profile,
            ["apps", "get-deployment", app_plan.app_name, deployment_id],
        )
        if not isinstance(current, dict):
            raise RuntimeError("Databricks MCP deployment readback is malformed")
        if (
            _deployment_id(current) != deployment_id
            or current.get("source_code_path") != app_plan.workspace_path
        ):
            raise RuntimeError("MCP deployment readback escaped its exact app or source")
        deployment = current
        state = _status_state(current.get("status"), "deployment")
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED", "ERROR"}:
            raise RuntimeError("Owned MCP app deployment did not succeed")
        if attempt < 119:
            waiter(2.0)
    else:
        raise RuntimeError("Owned MCP app deployment did not reach SUCCEEDED")

    owner, deployed = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if deployed is None:
        raise RuntimeError("Owned MCP app disappeared after deployment")
    deployed = _validate_mcp_app(target, app_plan.app_name, deployed, owner)
    _require_same_mcp_identity(
        deployed, app["id"], app["service_principal_client_id"]
    )
    deployment_state = _status_state(deployment.get("status"), "deployment")
    app_state = _status_state(deployed.get("app_status"), "application")
    compute_state = _status_state(deployed.get("compute_status"), "compute")
    if (deployment_state, app_state, compute_state) != ("SUCCEEDED", "RUNNING", "ACTIVE"):
        raise RuntimeError("Owned MCP app did not reach RUNNING/ACTIVE")
    return deployment_state, app_state


class TransientMcpInvocationError(RuntimeError):
    """The owned MCP app exists but its ingress is not ready."""


def _default_mcp_invoker(
    profile: str, app_url: str, payload: dict[str, Any]
) -> dict[str, Any]:
    import httpx
    from databricks.sdk import WorkspaceClient

    headers: dict[str, str] = {}
    try:
        headers.update(WorkspaceClient(profile=profile).config.authenticate())
        with httpx.Client(timeout=120.0) as client:
            response = client.post(app_url, headers=headers, json=payload)
            if response.status_code in {502, 503, 504}:
                raise TransientMcpInvocationError("Owned MCP ingress is not ready")
            if response.status_code < 200 or response.status_code >= 300:
                raise RuntimeError("Owned MCP protocol invocation failed")
            result = response.json()
    finally:
        headers.clear()
    if not isinstance(result, dict):
        raise RuntimeError("Owned MCP protocol invocation returned malformed JSON")
    return result


def _invoke_mcp_when_ready(
    invoker: McpInvoker,
    profile: str,
    url: str,
    payload: dict[str, Any],
    waiter: Waiter,
    fresh_precheck: Callable[[], None],
) -> dict[str, Any]:
    for attempt in range(60):
        fresh_precheck()
        try:
            return invoker(profile, url, payload)
        except TransientMcpInvocationError:
            if attempt == 59:
                raise RuntimeError("Owned MCP ingress did not become ready") from None
            waiter(2.0)
    raise RuntimeError("Owned MCP ingress did not become ready")


def _mcp_result(response: dict[str, Any], request_id: int) -> dict[str, Any]:
    if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
        raise RuntimeError("Owned MCP response did not match its request")
    if "error" in response:
        raise RuntimeError("Owned MCP response returned a protocol error")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Owned MCP result is malformed")
    return result


def _mcp_tool_rows(response: dict[str, Any], request_id: int) -> Any:
    result = _mcp_result(response, request_id)
    content = result.get("content")
    if (
        not isinstance(content, list)
        or len(content) != 1
        or not isinstance(content[0], dict)
        or content[0].get("type") != "text"
        or not isinstance(content[0].get("text"), str)
    ):
        raise RuntimeError("Owned MCP tool content is malformed")
    try:
        return json.loads(content[0]["text"])
    except json.JSONDecodeError as error:
        raise RuntimeError("Owned MCP tool content is not JSON") from error


def _call_mcp_tool(
    invoker: McpInvoker,
    profile: str,
    url: str,
    request_id: int,
    name: str,
    arguments: dict[str, Any],
) -> Any:
    return _mcp_tool_rows(
        invoker(
            profile,
            url,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
        ),
        request_id,
    )


MCP_BASELINE_TASK_IDS = frozenset({"OPS-2194", "OPS-2201", "OPS-2208", "OPS-2210"})
MCP_VALIDATION_TASK_ID = "GURARY-T10-MCP-VALIDATION"
MCP_VALIDATION_TASK_TITLE = "Gurary Task 10 private MCP validation"
MCP_VALIDATION_TASK_OWNER = "adam.gurary@databricks.com"


def _validate_ops_rows(rows: Any, expected_ids: set[str] | frozenset[str]) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise RuntimeError("Private OpsTask rows are malformed")
    ids = [row.get("task_id") for row in rows]
    if len(ids) != len(set(ids)) or set(ids) != set(expected_ids):
        raise RuntimeError("OpsTask MCP did not return the exact private rows")
    return [dict(row) for row in rows]


def _write_warehouse_state(path: Path, target: TargetConfig, warehouse_id: str) -> None:
    state = _read_generated_state(path)
    state.update({"target": target.key, "warehouse_id": warehouse_id})
    _write_json_atomic(path, state)


def reconcile_lakebase_state(
    *,
    state_path: Path | None = None,
    workspace_factory: WorkspaceFactory | None = None,
    lakebase_ensurer: LakebaseEnsurer = ensure_lakebase,
) -> LakebaseState:
    """Reconcile Lakebase and atomically merge its credential-free generated state."""
    from databricks.sdk import WorkspaceClient

    target = load_target("field_eng")
    _require_target_contract(target)
    output_path = state_path or ROOT / "deploy" / "state" / "field_eng.json"
    state = _read_generated_state(output_path)
    if state.get("target") != target.key:
        raise RuntimeError("Field-eng generated state is missing the exact target binding")
    for field_name in ("warehouse_id", "genie_space_id"):
        value = state.get(field_name)
        if not isinstance(value, str) or not CONCRETE_ID.fullmatch(value):
            raise RuntimeError(f"Field-eng generated state is missing concrete {field_name}")

    factory = workspace_factory or WorkspaceClient
    workspace = factory(profile=target.profile)
    lakebase = lakebase_ensurer(workspace, target)
    state["lakebase"] = lakebase.as_dict()
    _write_json_atomic(output_path, state)
    return lakebase


def _statement_rows(
    response: dict | list, run_cli: RunCli, profile: str
) -> list[dict[str, Any]]:
    if not isinstance(response, dict):
        raise RuntimeError("Databricks SQL statement response is malformed")
    status = response.get("status")
    if not isinstance(status, dict) or status.get("state") != "SUCCEEDED":
        raise RuntimeError("Databricks SQL statement did not succeed")
    manifest = response.get("manifest")
    schema = manifest.get("schema") if isinstance(manifest, dict) else None
    columns = schema.get("columns") if isinstance(schema, dict) else None
    if not isinstance(columns, list) or not all(isinstance(column, dict) for column in columns):
        raise RuntimeError("Databricks SQL statement columns are malformed")
    names = [column.get("name") for column in columns]
    if not all(isinstance(name, str) and name for name in names):
        raise RuntimeError("Databricks SQL statement contains an invalid column name")
    rows: list[list[Any]] = []
    result = response.get("result")
    while isinstance(result, dict):
        data = result.get("data_array", [])
        if not isinstance(data, list) or not all(isinstance(row, list) for row in data):
            raise RuntimeError("Databricks SQL statement rows are malformed")
        rows.extend(data)
        next_link = result.get("next_chunk_internal_link")
        if not next_link:
            break
        chunk = run_cli(profile, ["api", "get", str(next_link)])
        if not isinstance(chunk, dict):
            raise RuntimeError("Databricks SQL result chunk is malformed")
        result = chunk
    if any(len(row) != len(names) for row in rows):
        raise RuntimeError("Databricks SQL statement row width is malformed")
    if not isinstance(manifest, dict) or manifest.get("total_row_count") != len(rows):
        raise RuntimeError("Databricks SQL statement result is incomplete")
    return [dict(zip(names, row)) for row in rows]


def _run_statement(
    run_cli: RunCli,
    target: TargetConfig,
    warehouse_id: str,
    statement: str,
    waiter: Waiter,
) -> list[dict[str, Any]]:
    response = run_cli(
        target.profile,
        ["api", "post", SQL_STATEMENTS_ENDPOINT],
        {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
    )
    if not isinstance(response, dict):
        raise RuntimeError("Databricks SQL statement response is malformed")
    for _attempt in range(60):
        status = response.get("status")
        state = status.get("state") if isinstance(status, dict) else None
        if state not in {"PENDING", "RUNNING"}:
            break
        statement_id = response.get("statement_id")
        if not isinstance(statement_id, str) or not statement_id:
            raise RuntimeError("Pending SQL statement has no concrete ID")
        waiter(2.0)
        response = run_cli(
            target.profile, ["api", "get", f"{SQL_STATEMENTS_ENDPOINT}/{statement_id}"]
        )
        if not isinstance(response, dict):
            raise RuntimeError("Databricks SQL statement response is malformed")
    return _statement_rows(response, run_cli, target.profile)


def _verify_private_data(
    run_cli: RunCli,
    target: TargetConfig,
    warehouse_id: str,
    waiter: Waiter,
) -> tuple[dict[str, int], float, tuple[int, int, int]]:
    namespace = f"`{target.catalog}`.`{target.data_schema}`"
    count_sql = "\nUNION ALL\n".join(
        f"SELECT '{table}' AS table_name, COUNT(*) AS row_count "
        f"FROM {namespace}.`{target.table_prefix}{table}`"
        for table in TABLE_NAMES
    )
    count_rows = _run_statement(run_cli, target, warehouse_id, count_sql, waiter)
    try:
        counts = {str(row["table_name"]): int(row["row_count"]) for row in count_rows}
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Private table count evidence is malformed") from error
    if counts != EXPECTED_COUNTS or len(count_rows) != len(EXPECTED_COUNTS):
        raise RuntimeError("Private table counts did not match the deterministic seed")

    training_rows = _run_statement(
        run_cli,
        target,
        warehouse_id,
        f"SELECT training_completion_pct FROM {namespace}.`{target.table_prefix}store_weekly_metrics` "
        "WHERE store_id = 104 ORDER BY week_start_date DESC LIMIT 1",
        waiter,
    )
    if len(training_rows) != 1:
        raise RuntimeError("Store 104 latest training fact was not unique")
    try:
        training_completion = float(training_rows[0]["training_completion_pct"])
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Store 104 latest training fact is malformed") from error
    if training_completion != 81.8:
        raise RuntimeError("Store 104 latest training completion did not match 81.8")

    storetime_rows = _run_statement(
        run_cli,
        target,
        warehouse_id,
        "SELECT scheduled_training_hours, completed_training_hours, converted_training_hours "
        f"FROM {namespace}.`{target.table_prefix}schedules` "
        "WHERE store_id = 104 AND week = '2026-W27'",
        waiter,
    )
    if len(storetime_rows) != 1:
        raise RuntimeError("Store 104 StoreTime fact was not unique")
    try:
        storetime_values = tuple(
            Decimal(str(storetime_rows[0][field]))
            for field in (
                "scheduled_training_hours",
                "completed_training_hours",
                "converted_training_hours",
            )
        )
    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Store 104 StoreTime fact is malformed") from error
    if storetime_values != (42, 28, 14):
        raise RuntimeError("Store 104 StoreTime fact did not match 42/28/14")
    storetime = tuple(int(value) for value in storetime_values)
    return counts, training_completion, storetime


def _prepare_ops_mutation(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> None:
    owner, current = _fresh_mcp_app_precheck(
        target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
    )
    if current is None:
        raise RuntimeError("Owned OpsTask app disappeared before validation mutation")
    _require_same_mcp_identity(
        current, app["id"], app["service_principal_client_id"]
    )
    if (
        _status_state(current.get("app_status"), "application") != "RUNNING"
        or _status_state(current.get("compute_status"), "compute") != "ACTIVE"
    ):
        raise RuntimeError("Owned OpsTask app is not running before validation mutation")
    table_grant = next(
        grant for grant in app_plan.grants if grant.securable_type == "table"
    )
    _inspect_mcp_securable(target, table_grant, run_cli, owner)
    grants = run_cli(
        target.profile,
        ["grants", "get", table_grant.securable_type, table_grant.full_name],
    )
    if not table_grant.privileges.issubset(
        _principal_privileges(grants, app["service_principal_client_id"])
    ):
        raise RuntimeError("Owned OpsTask app lost its exact private-table grants")


def _cleanup_validation_row(
    target: TargetConfig,
    app_plan: McpAppPlan,
    app: dict[str, Any],
    warehouse_id: str,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
    waiter: Waiter,
) -> int:
    _prepare_ops_mutation(
        target, app_plan, app, run_cli, verify_profile, workspace_id_provider
    )
    table = f"`{target.catalog}`.`{target.data_schema}`.`{target.table_prefix}{MCP_OPSTASK_TABLE}`"
    validation_rows = _run_statement(
        run_cli,
        target,
        warehouse_id,
        f"SELECT task_id FROM {table} "
        f"WHERE task_id = '{MCP_VALIDATION_TASK_ID}' "
        f"AND title = '{MCP_VALIDATION_TASK_TITLE}' "
        f"AND owner = '{MCP_VALIDATION_TASK_OWNER}'",
        waiter,
    )
    if len(validation_rows) > 1:
        raise RuntimeError("Private validation row was not unique")
    if validation_rows:
        _prepare_ops_mutation(
            target, app_plan, app, run_cli, verify_profile, workspace_id_provider
        )
        _run_statement(
            run_cli,
            target,
            warehouse_id,
            f"DELETE FROM {table} "
            f"WHERE task_id = '{MCP_VALIDATION_TASK_ID}' "
            f"AND title = '{MCP_VALIDATION_TASK_TITLE}' "
            f"AND owner = '{MCP_VALIDATION_TASK_OWNER}'",
            waiter,
        )
    baseline_rows = _run_statement(
        run_cli,
        target,
        warehouse_id,
        f"SELECT task_id FROM {table} ORDER BY task_id",
        waiter,
    )
    try:
        baseline_ids = {str(row["task_id"]) for row in baseline_rows}
    except (KeyError, TypeError) as error:
        raise RuntimeError("Private OpsTask cleanup evidence is malformed") from error
    if len(baseline_rows) != 4 or baseline_ids != MCP_BASELINE_TASK_IDS:
        raise RuntimeError("Private OpsTask four-row baseline was not restored")
    return len(baseline_rows)


def _default_sync_runner(command: list[str]) -> None:
    subprocess.run(command, check=True)


def _validate_mcp_deployment_plan(
    plan: McpDeploymentPlan, generated_state: dict[str, Any]
) -> str:
    """Fail closed on any constructed plan that differs from the reviewed contract."""
    target = plan.target
    _require_target_contract(target)
    warehouse_id = plan.environment.get("DATABRICKS_WAREHOUSE_ID")
    state_warehouse_id = generated_state.get("warehouse_id")
    if (
        generated_state.get("target") != target.key
        or not isinstance(state_warehouse_id, str)
        or CONCRETE_ID.fullmatch(state_warehouse_id) is None
        or warehouse_id != state_warehouse_id
    ):
        raise RuntimeError("MCP plan does not match the exact private warehouse state")
    expected_environment = {
        "DATABRICKS_WAREHOUSE_ID": warehouse_id,
        "DATABRICKS_CATALOG": target.catalog,
        "DATABRICKS_SCHEMA": target.data_schema,
        "DATABRICKS_TABLE_PREFIX": target.table_prefix,
    }
    expected_specs = (
        (
            target.storetime_app_name,
            "mcp-apps/storetime",
            f"{MCP_WORKSPACE_PARENT}/{target.storetime_app_name}",
        ),
        (
            target.opstask_app_name,
            "mcp-apps/opstask",
            f"{MCP_WORKSPACE_PARENT}/{target.opstask_app_name}",
        ),
    )
    exact = (
        plan.environment == expected_environment
        and isinstance(warehouse_id, str)
        and CONCRETE_ID.fullmatch(warehouse_id) is not None
        and len(plan.apps) == len(expected_specs)
        and len({app.app_name for app in plan.apps}) == len(expected_specs)
    )
    if exact:
        for app_plan, (app_name, source_relative, workspace_path) in zip(
            plan.apps, expected_specs, strict=True
        ):
            if (
                app_plan.app_name != app_name
                or app_plan.source_relative != source_relative
                or app_plan.workspace_path != workspace_path
                or app_plan.grants != _mcp_grants(target, app_name)
            ):
                exact = False
                break
    if not exact:
        raise RuntimeError("MCP plan does not match the exact private deployment contract")
    return warehouse_id


def apply_mcp_deployment_plan(
    plan: McpDeploymentPlan,
    *,
    run_cli: RunCli = run_json,
    verify_profile: VerifyProfile = assert_profile,
    workspace_id_provider: WorkspaceIdProvider = _workspace_id_from_sdk,
    state_path: Path | None = None,
    build_root: Path | None = None,
    sync_runner: SyncRunner = _default_sync_runner,
    mcp_invoker: McpInvoker = _default_mcp_invoker,
    waiter: Waiter = time.sleep,
) -> McpDeploymentEvidence:
    """Reconcile, deploy, and live-validate the two private field-eng MCP apps."""
    target = plan.target
    output_path = state_path or ROOT / "deploy" / "state" / "field_eng.json"
    original_state = _read_generated_state(output_path)
    warehouse_id = _validate_mcp_deployment_plan(plan, original_state)
    apps: dict[str, dict[str, Any]] = {}
    for app_plan in plan.apps:
        app = _ensure_mcp_app(
            target, app_plan, run_cli, verify_profile, workspace_id_provider
        )
        prefix = "storetime" if app_plan.app_name == target.storetime_app_name else "opstask"
        saved_id = original_state.get(f"{prefix}_app_id")
        saved_principal = original_state.get(f"{prefix}_service_principal_client_id")
        saved_url = original_state.get(f"{prefix}_mcp_url")
        if any(value is not None for value in (saved_id, saved_principal, saved_url)):
            if (saved_id, saved_principal, saved_url) != (
                app["id"],
                app["service_principal_client_id"],
                f"{app['url']}/mcp",
            ):
                raise RuntimeError("Saved MCP app identity does not match live readback")
        apps[app_plan.app_name] = app

    for app_plan in plan.apps:
        app = apps[app_plan.app_name]
        for grant in app_plan.grants:
            _ensure_mcp_grant(
                target,
                app_plan,
                app,
                grant,
                run_cli,
                verify_profile,
                workspace_id_provider,
            )
        _ensure_private_warehouse_use(
            target,
            app_plan,
            app,
            warehouse_id,
            run_cli,
            verify_profile,
            workspace_id_provider,
        )

    for app_plan in plan.apps:
        _owner, current = _fresh_mcp_app_precheck(
            target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
        )
        if current is None:
            raise RuntimeError("Owned MCP app disappeared before state persistence")
        _require_same_mcp_identity(
            current,
            apps[app_plan.app_name]["id"],
            apps[app_plan.app_name]["service_principal_client_id"],
        )
    generated_state = _merge_mcp_state(output_path, target, apps)

    render_root = build_root or ROOT / ".build" / "field_eng" / "mcp"
    rendered: dict[str, Path] = {}
    for app_plan in plan.apps:
        _owner, current = _fresh_mcp_app_precheck(
            target, app_plan.app_name, run_cli, verify_profile, workspace_id_provider
        )
        if current is None:
            raise RuntimeError("Owned MCP app disappeared before source rendering")
        _require_same_mcp_identity(
            current,
            apps[app_plan.app_name]["id"],
            apps[app_plan.app_name]["service_principal_client_id"],
        )
        rendered[app_plan.app_name] = _render_mcp_source(plan, app_plan, render_root)

    deployment_states: dict[str, str] = {}
    app_states: dict[str, str] = {}
    for app_plan in plan.apps:
        app = apps[app_plan.app_name]
        _sync_mcp_source(
            target,
            app_plan,
            app,
            rendered[app_plan.app_name],
            run_cli,
            verify_profile,
            workspace_id_provider,
            sync_runner,
        )
        _ensure_workspace_read(
            target,
            app_plan,
            app,
            run_cli,
            verify_profile,
            workspace_id_provider,
        )
        deployment_state, app_state = _deploy_mcp_app(
            target,
            app_plan,
            app,
            run_cli,
            verify_profile,
            workspace_id_provider,
            waiter,
        )
        deployment_states[app_plan.app_name] = deployment_state
        app_states[app_plan.app_name] = app_state

    tools: dict[str, tuple[str, ...]] = {}
    expected_tools = {
        target.storetime_app_name: ("inspect_schedule", "list_training_events"),
        target.opstask_app_name: ("create_ops_task", "list_existing_ops_tasks"),
    }
    request_id = 1
    for app_plan in plan.apps:
        app = apps[app_plan.app_name]
        url = generated_state[
            "storetime_mcp_url"
            if app_plan.app_name == target.storetime_app_name
            else "opstask_mcp_url"
        ]

        def precheck(
            current_plan: McpAppPlan = app_plan,
            expected_app: dict[str, Any] = app,
        ) -> None:
            _owner, current = _fresh_mcp_app_precheck(
                target,
                current_plan.app_name,
                run_cli,
                verify_profile,
                workspace_id_provider,
            )
            if current is None:
                raise RuntimeError("Owned MCP app disappeared before protocol validation")
            _require_same_mcp_identity(
                current,
                expected_app["id"],
                expected_app["service_principal_client_id"],
            )

        initialized = _invoke_mcp_when_ready(
            mcp_invoker,
            target.profile,
            url,
            {"jsonrpc": "2.0", "id": request_id, "method": "initialize", "params": {}},
            waiter,
            precheck,
        )
        initialize_result = _mcp_result(initialized, request_id)
        if initialize_result.get("protocolVersion") != "2024-11-05":
            raise RuntimeError("Owned MCP initialize returned the wrong protocol version")
        request_id += 1
        precheck()
        listed = _mcp_result(
            mcp_invoker(
                target.profile,
                url,
                {"jsonrpc": "2.0", "id": request_id, "method": "tools/list", "params": {}},
            ),
            request_id,
        )
        specs = listed.get("tools")
        if not isinstance(specs, list) or not all(isinstance(spec, dict) for spec in specs):
            raise RuntimeError("Owned MCP tools/list result is malformed")
        names = tuple(sorted(str(spec.get("name")) for spec in specs))
        if names != expected_tools[app_plan.app_name]:
            raise RuntimeError("Owned MCP tools/list did not match the exact expectation")
        tools[app_plan.app_name] = names
        request_id += 1

    store_plan, ops_plan = plan.apps
    store_url = generated_state["storetime_mcp_url"]
    ops_url = generated_state["opstask_mcp_url"]
    store_rows = _call_mcp_tool(
        mcp_invoker,
        target.profile,
        store_url,
        request_id,
        "inspect_schedule",
        {"store_id": 104, "week": "2026-W27"},
    )
    request_id += 1
    if not isinstance(store_rows, list) or len(store_rows) != 1:
        raise RuntimeError("StoreTime MCP did not return one Store 104 schedule")
    try:
        storetime_values = tuple(
            Decimal(str(store_rows[0][field]))
            for field in (
                "scheduled_training_hours",
                "completed_training_hours",
                "converted_training_hours",
            )
        )
    except (InvalidOperation, KeyError, TypeError, ValueError) as error:
        raise RuntimeError("StoreTime MCP Store 104 result is malformed") from error
    if storetime_values != (42, 28, 14):
        raise RuntimeError("StoreTime MCP Store 104 result did not match 42/28/14")
    storetime = tuple(int(value) for value in storetime_values)

    ops_rows = _call_mcp_tool(
        mcp_invoker,
        target.profile,
        ops_url,
        request_id,
        "list_existing_ops_tasks",
        {},
    )
    request_id += 1
    _validate_ops_rows(ops_rows, MCP_BASELINE_TASK_IDS)

    validation_attempted = False
    validation_error: BaseException | None = None
    baseline_count = 0
    try:
        _prepare_ops_mutation(
            target,
            ops_plan,
            apps[ops_plan.app_name],
            run_cli,
            verify_profile,
            workspace_id_provider,
        )
        validation_attempted = True
        created = _call_mcp_tool(
            mcp_invoker,
            target.profile,
            ops_url,
            request_id,
            "create_ops_task",
            {
                "task_id": MCP_VALIDATION_TASK_ID,
                "store_id": 104,
                "title": MCP_VALIDATION_TASK_TITLE,
                "description": "Temporary private Task 10 protocol validation row",
                "category": "validation",
                "severity": "low",
                "status": "open",
                "owner": MCP_VALIDATION_TASK_OWNER,
                "created_date": "2026-09-11",
            },
        )
        request_id += 1
        if not isinstance(created, dict) or created.get("task_id") != MCP_VALIDATION_TASK_ID:
            raise RuntimeError("OpsTask MCP did not return the exact validation task")
        after_create = _call_mcp_tool(
            mcp_invoker,
            target.profile,
            ops_url,
            request_id,
            "list_existing_ops_tasks",
            {},
        )
        _validate_ops_rows(
            after_create, MCP_BASELINE_TASK_IDS | {MCP_VALIDATION_TASK_ID}
        )
    except BaseException as error:
        validation_error = error
    finally:
        if validation_attempted:
            try:
                baseline_count = _cleanup_validation_row(
                    target,
                    ops_plan,
                    apps[ops_plan.app_name],
                    warehouse_id,
                    run_cli,
                    verify_profile,
                    workspace_id_provider,
                    waiter,
                )
            except BaseException as cleanup_error:
                raise RuntimeError(
                    "Private MCP validation cleanup or four-row baseline proof failed"
                ) from cleanup_error
    if validation_error is not None:
        raise validation_error

    return McpDeploymentEvidence(
        app_states=app_states,
        deployment_states=deployment_states,
        tools=tools,
        store_104_storetime=storetime,
        ops_baseline_count=baseline_count,
    )


def apply_provision_plan(
    plan: ProvisionPlan,
    *,
    run_cli: RunCli = run_json,
    verify_profile: VerifyProfile = assert_profile,
    workspace_id_provider: WorkspaceIdProvider = _workspace_id_from_sdk,
    process_runner: ProcessRunner = _default_process_runner,
    state_path: Path | None = None,
    waiter: Waiter = time.sleep,
) -> ApplyEvidence:
    target = plan.target
    _require_target_contract(target)
    warehouse = _ensure_warehouse(
        target, run_cli, verify_profile, workspace_id_provider
    )
    warehouse_id = _warehouse_id(warehouse)
    schemas = tuple(
        _ensure_schema(
            target, schema, run_cli, verify_profile, workspace_id_provider
        )["full_name"]
        for schema in (target.data_schema, target.trace_schema)
    )

    expected_owner = _assert_target_binding(target, verify_profile, workspace_id_provider)
    for table_name in _exact_table_names(target):
        _assert_exact_owned_name(target, "table", table_name)
    _assert_catalog_access(run_cli, target, expected_owner)
    current_warehouse = run_cli(target.profile, ["warehouses", "get", warehouse_id])
    if not isinstance(current_warehouse, dict) or not _warehouse_matches(current_warehouse, target):
        raise RuntimeError("Data provisioner warehouse inspection failed")
    current_schema_names = {record.get("full_name") for record in _schema_records(run_cli, target)}
    if not set(_exact_schema_names(target)).issubset(current_schema_names):
        raise RuntimeError("Data provisioner schema inspection failed")
    _table_records(run_cli, target)

    argv = [value.format(warehouse_id=warehouse_id) for value in plan.data_provisioner_argv]
    if argv[-2:] != ["--table-prefix", target.table_prefix] or not target.table_prefix:
        raise RuntimeError("Private data provisioner table prefix is unsafe")
    if "--allow-shared-source" in argv:
        raise RuntimeError("Private data provisioner cannot use shared-source mode")
    process_runner([sys.executable, *argv])

    table_records = _table_records(run_cli, target)
    expected_table_names = set(_exact_table_names(target))
    table_names = tuple(
        sorted(
            str(record["full_name"])
            for record in table_records
            if record.get("full_name") in expected_table_names
        )
    )
    if set(table_names) != expected_table_names:
        raise RuntimeError("Private data provisioner did not create all exact owned tables")
    counts, training_completion, storetime = _verify_private_data(
        run_cli, target, warehouse_id, waiter
    )
    output_path = state_path or ROOT / "deploy" / "state" / "field_eng.json"
    _write_warehouse_state(output_path, target, warehouse_id)
    return ApplyEvidence(
        warehouse_id=warehouse_id,
        schemas=schemas,
        table_names=table_names,
        counts=counts,
        store_104_training_completion=training_completion,
        store_104_storetime=storetime,
    )


def _parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--dry-run-mcp", action="store_true")
    mode.add_argument("--apply-mcp", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    arguments = _parse_arguments()
    if arguments.dry_run_mcp:
        result = build_mcp_deployment_plan().as_dict()
    elif arguments.apply_mcp:
        mcp_plan = build_mcp_deployment_plan()
        result = apply_mcp_deployment_plan(mcp_plan).as_dict()
    elif arguments.dry_run:
        plan = build_provision_plan()
        result = plan.as_dict()
    else:
        plan = build_provision_plan()
        result = apply_provision_plan(plan).as_dict()
        reconcile_lakebase_state()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
