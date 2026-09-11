#!/usr/bin/env python3
"""Plan and reconcile the isolated FEVM presenter fallback."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, field
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Iterable
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.config import TargetConfig, load_state, load_target
from deploy.databricks_cli import assert_profile, run_json
from deploy.inventory import project_inventory_record
from deploy.safety import assert_safe_fevm_grant, assert_safe_mutation


SHARED_PRESENTER = "bobabricks-store-ops-" "demo"
OWNED_WORKSPACE_DIRECTORY = "gurary_WT26_AIbreakout"
ISOLATED_LAKEBASE_SCHEMA = "gurary_bobabricks_app"
APP_SCOPES = ("ai-gateway", "genie", "mcp.external")
TRACE_CATALOG_PRIVILEGES = frozenset({"USE_CATALOG"})
TRACE_SCHEMA_PRIVILEGES = frozenset({"USE_SCHEMA", "CREATE_TABLE"})
SQL_STATEMENTS_ENDPOINT = "/api/2.0/sql/statements"
LOCAL_GRANT_MODE = "reconcile_locally"
PREAUTHORIZED_GRANT_MODE = "preauthorized_external_confirmation"
PREAUTHORIZED_GRANT_PROOF = "required_live_trace_write_tags_and_spans"
TRACE_TABLE_PREFIX = "gurary_bobabricks"
TRACE_DESTINATION_TAG = "mlflow.experiment.databricksTraceDestinationPath"
TRACE_SPAN_TABLE_TAG = "mlflow.experiment.databricksTraceSpanStorageTable"
TRACE_PROBE_PROMPT = "What tools do you have?"


RunCli = Callable[[str, list[str], dict | None], dict | list]
VerifyProfile = Callable[[str, str], dict]
WorkspaceIdProvider = Callable[[str], str]
LakebaseInventoryProvider = Callable[[str], list[dict[str, Any]]]
SyncRunner = Callable[[list[str]], Any]
AppInvoker = Callable[[str, str, dict[str, Any]], dict[str, Any]]
TraceProofProvider = Callable[[dict[str, Any]], dict[str, Any]]
SessionIdProvider = Callable[[], str]
NowMs = Callable[[], int]
Waiter = Callable[[float], None]


class TransientAppInvocationError(RuntimeError):
    """The deployed app exists, but its ingress is not ready yet."""


@dataclass(frozen=True)
class Action:
    kind: str
    name: str
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SharedOpsSnapshot:
    row_count: int
    sha256: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProvisionPlan:
    actions: tuple[Action, ...]
    effective_tools: tuple[str, ...]
    shared_inventory: dict[str, dict[str, Any]]
    shared_ops_before: SharedOpsSnapshot
    shared_ops_after_dry_run: SharedOpsSnapshot
    owned_app: dict[str, Any] | None
    trace_grant_mode: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": "dry-run",
            "prechecks": {
                "profile_identity": "PASS",
                "workspace_id": "PASS",
                "shared_dependencies": "PASS",
                "shared_ops_unchanged": self.shared_ops_before == self.shared_ops_after_dry_run,
            },
            "actions": [action.as_dict() for action in self.actions],
            "effective_tools": list(self.effective_tools),
            "shared_ops_snapshot": self.shared_ops_before.as_dict(),
            "trace_grant_mode": self.trace_grant_mode,
            "trace_grant_proof": (
                PREAUTHORIZED_GRANT_PROOF
                if self.trace_grant_mode == PREAUTHORIZED_GRANT_MODE
                else "direct_grant_readback"
            ),
        }

    def inventory_dict(self) -> dict[str, Any]:
        return {
            "shared_dependencies": self.shared_inventory,
            "shared_ops_snapshot": self.shared_ops_before.as_dict(),
            "trace_grants": {
                "mode": self.trace_grant_mode,
                "external_confirmation_recorded": (
                    self.trace_grant_mode == PREAUTHORIZED_GRANT_MODE
                ),
                "authoritative_proof_required": (
                    PREAUTHORIZED_GRANT_PROOF
                    if self.trace_grant_mode == PREAUTHORIZED_GRANT_MODE
                    else "direct_grant_readback"
                ),
            },
        }


@dataclass(frozen=True)
class ApplyEvidence:
    deployment_state: str
    app_state: str
    compute_state: str
    mutated_resources: tuple[str, ...]
    trace_grant_mode: str
    trace_proof_state: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": "apply",
            "deployment_state": self.deployment_state,
            "app_state": self.app_state,
            "compute_state": self.compute_state,
            "mutated_resources": list(self.mutated_resources),
            "trace_grant_mode": self.trace_grant_mode,
            "trace_proof_state": self.trace_proof_state,
        }


def _workspace_id_from_sdk(profile: str) -> str:
    from databricks.sdk import WorkspaceClient

    return str(WorkspaceClient(profile=profile).get_workspace_id())


def _lakebase_inventory_from_sdk(profile: str) -> list[dict[str, Any]]:
    from databricks.sdk import WorkspaceClient

    projects = WorkspaceClient(profile=profile).postgres.list_projects()
    return [project.as_dict() for project in projects]


def _require_mapping(value: dict | list, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"Databricks {label} response must be an object")
    return dict(value)


def _require_exact(record: dict[str, Any], field_name: str, expected: str, label: str) -> None:
    if str(record.get(field_name)) != expected:
        raise RuntimeError(f"Databricks {label} response did not match the configured resource")


def _run(run_cli: RunCli, profile: str, args: list[str], payload: dict | None = None):
    return run_cli(profile, args, payload)


def _read_app(run_cli: RunCli, target: TargetConfig, name: str) -> dict[str, Any]:
    app = _require_mapping(_run(run_cli, target.profile, ["apps", "get", name]), f"app {name}")
    _require_exact(app, "name", name, f"app {name}")
    expected_url = f"https://{name}-{target.workspace_id}.aws.databricksapps.com"
    _require_exact(app, "url", expected_url, f"app {name}")
    return app


def _read_owned_app(
    run_cli: RunCli,
    target: TargetConfig,
    expected_owner: str,
    saved_identity: tuple[str, str] | None = None,
) -> dict[str, Any] | None:
    response = _run(run_cli, target.profile, ["apps", "list"])
    if isinstance(response, list):
        apps = response
    elif isinstance(response, dict):
        apps = response.get("apps", [])
    else:
        raise RuntimeError("Databricks apps list response is malformed")
    if not isinstance(apps, list) or not all(isinstance(item, dict) for item in apps):
        raise RuntimeError("Databricks apps list response is malformed")
    matches = [dict(app) for app in apps if app.get("name") == target.presenter_app_name]
    if len(matches) > 1:
        raise RuntimeError("Databricks returned duplicate owned app records")
    if not matches:
        return None
    app = matches[0]
    _require_exact(app, "name", target.presenter_app_name, "owned app")
    expected_url = (
        f"https://{target.presenter_app_name}-{target.workspace_id}.aws.databricksapps.com"
    )
    _require_exact(app, "url", expected_url, "owned app")
    _require_owned_app_metadata(target, app)
    if saved_identity is None:
        _require_authoritative_app_owner(app, expected_owner)
    elif (app["id"], app["service_principal_client_id"]) != saved_identity:
        raise RuntimeError("Owned app does not match the saved generated-state identity")
    return app


def _read_shared_inventory(
    run_cli: RunCli,
    target: TargetConfig,
    lakebase_inventory_provider: LakebaseInventoryProvider,
    expected_owner: str,
    saved_identity: tuple[str, str] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
    shared_apps = {
        "presenter_app": _read_app(run_cli, target, SHARED_PRESENTER),
        "storetime_app": _read_app(run_cli, target, target.storetime_app_name),
        "opstask_app": _read_app(run_cli, target, target.opstask_app_name),
    }

    genie = _require_mapping(
        _run(run_cli, target.profile, ["genie", "get-space", target.genie_space_id]),
        "Genie space",
    )
    _require_exact(genie, "space_id", str(target.genie_space_id), "Genie space")

    warehouse = _require_mapping(
        _run(run_cli, target.profile, ["warehouses", "get", target.warehouse_id]),
        "warehouse",
    )
    _require_exact(warehouse, "id", str(target.warehouse_id), "warehouse")

    catalog = _require_mapping(
        _run(run_cli, target.profile, ["catalogs", "get", target.catalog]), "catalog"
    )
    _require_exact(catalog, "name", target.catalog, "catalog")

    schema_name = f"{target.catalog}.{target.data_schema}"
    schema = _require_mapping(
        _run(run_cli, target.profile, ["schemas", "get", schema_name]), "schema"
    )
    _require_exact(schema, "full_name", schema_name, "schema")

    connection = _require_mapping(
        _run(run_cli, target.profile, ["connections", "get", target.confluence_connection]),
        "connection",
    )
    _require_exact(connection, "name", target.confluence_connection, "connection")

    table_name = f"{schema_name}.ops_tasks"
    ops_table = _require_mapping(
        _run(run_cli, target.profile, ["tables", "get", table_name]), "OpsTask table"
    )
    _require_exact(ops_table, "full_name", table_name, "OpsTask table")

    lakebase_projects = lakebase_inventory_provider(target.profile)
    if not isinstance(lakebase_projects, list) or not all(
        isinstance(project, dict) for project in lakebase_projects
    ):
        raise RuntimeError("Databricks Lakebase inventory response is malformed")

    records: dict[str, Any] = {
        **shared_apps,
        "genie_space": genie,
        "warehouse": warehouse,
        "catalog": catalog,
        "data_schema": schema,
        "managed_connection": connection,
        "ops_tasks_table": ops_table,
        "lakebase_projects": lakebase_projects,
    }
    resource_types = {
        "presenter_app": "apps",
        "storetime_app": "apps",
        "opstask_app": "apps",
        "genie_space": "genie_spaces",
        "warehouse": "warehouses",
        "catalog": "uc_objects",
        "data_schema": "uc_objects",
        "managed_connection": "connections",
        "ops_tasks_table": "uc_objects",
        "lakebase_projects": "lakebase",
    }
    inventory = {}
    for key, value in records.items():
        resource_type = resource_types[key]
        if isinstance(value, list):
            record = [project_inventory_record(resource_type, item) for item in value]
        else:
            record = project_inventory_record(resource_type, value)
        inventory[key] = {"mutation_allowed": False, "record": record}
    return inventory, _read_owned_app(run_cli, target, expected_owner, saved_identity)


def canonical_ops_snapshot(rows: Iterable[dict[str, Any]]) -> SharedOpsSnapshot:
    canonical_rows = [
        json.dumps(dict(row), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        for row in rows
    ]
    canonical_rows.sort()
    payload = "\n".join(canonical_rows).encode("utf-8")
    return SharedOpsSnapshot(row_count=len(canonical_rows), sha256=sha256(payload).hexdigest())


def _statement_rows(
    response: dict | list,
    run_cli: RunCli,
    profile: str,
) -> list[dict[str, Any]]:
    statement = _require_mapping(response, "SQL statement")
    status = statement.get("status")
    if not isinstance(status, dict):
        raise RuntimeError("Databricks SQL statement status is malformed")
    if status.get("state") != "SUCCEEDED":
        raise RuntimeError(f"Databricks SQL statement did not succeed: {status.get('state')}")

    manifest = statement.get("manifest")
    if not isinstance(manifest, dict):
        raise RuntimeError("Databricks SQL statement manifest is malformed")
    schema = manifest.get("schema")
    columns = schema.get("columns") if isinstance(schema, dict) else None
    if not isinstance(columns, list) or not all(isinstance(column, dict) for column in columns):
        raise RuntimeError("Databricks SQL statement columns are malformed")
    names = [column.get("name") for column in columns]
    if not all(isinstance(name, str) and name for name in names):
        raise RuntimeError("Databricks SQL statement contains an invalid column name")

    rows: list[list[Any]] = []
    result = statement.get("result")
    while isinstance(result, dict):
        data_array = result.get("data_array", [])
        if not isinstance(data_array, list) or not all(isinstance(row, list) for row in data_array):
            raise RuntimeError("Databricks SQL statement rows are malformed")
        rows.extend(data_array)
        next_link = result.get("next_chunk_internal_link")
        if not next_link:
            break
        result = _require_mapping(
            _run(run_cli, profile, ["api", "get", str(next_link)]), "SQL result chunk"
        )

    if any(len(row) != len(names) for row in rows):
        raise RuntimeError("Databricks SQL statement row width does not match its schema")
    expected_count = manifest.get("total_row_count")
    if not isinstance(expected_count, int) or expected_count != len(rows):
        raise RuntimeError("Databricks SQL statement row count is incomplete")
    return [dict(zip(names, row)) for row in rows]


def _read_shared_ops_snapshot(run_cli: RunCli, target: TargetConfig) -> SharedOpsSnapshot:
    warehouse = _require_mapping(
        _run(run_cli, target.profile, ["warehouses", "get", target.warehouse_id]),
        "warehouse",
    )
    _require_exact(warehouse, "id", str(target.warehouse_id), "warehouse")
    if warehouse.get("state") != "RUNNING":
        raise RuntimeError("Protected shared warehouse must already be RUNNING before SQL")

    full_name = f"{target.catalog}.{target.data_schema}.ops_tasks"
    payload = {
        "warehouse_id": target.warehouse_id,
        "statement": f"SELECT * FROM `{target.catalog}`.`{target.data_schema}`.`ops_tasks`",
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    response = _run(
        run_cli,
        target.profile,
        ["api", "post", SQL_STATEMENTS_ENDPOINT],
        payload,
    )
    rows = _statement_rows(response, run_cli, target.profile)
    if not full_name.endswith(".ops_tasks"):
        raise RuntimeError("Unexpected shared OpsTask table")
    return canonical_ops_snapshot(rows)


def _effective_baseline_tools(target: TargetConfig) -> tuple[str, ...]:
    tools_path = ROOT / "agent-store-ops" / "tools.yaml"
    import yaml

    document = yaml.safe_load(tools_path.read_text(encoding="utf-8"))
    tools = document.get("tools") if isinstance(document, dict) else None
    if not isinstance(tools, list) or not all(isinstance(tool, dict) for tool in tools):
        raise RuntimeError("Agent tool configuration is malformed")
    names = [tool.get("name") for tool in tools]
    if not all(isinstance(name, str) and name for name in names):
        raise RuntimeError("Agent tool configuration contains an invalid name")
    if target.shared_mcp_read_only:
        names = [name for name in names if name != "create_ops_task"]
    state = load_state("baseline")
    if state.confluence_enabled:
        raise RuntimeError("FEVM baseline unexpectedly enables Confluence")
    return tuple(names)


def _actions(
    target: TargetConfig,
    owned_app: dict[str, Any] | None,
    preauthorized_trace_grants: bool = False,
) -> tuple[Action, ...]:
    assert_safe_mutation(target, "app", target.presenter_app_name)
    assert_safe_mutation(target, "workspace directory", OWNED_WORKSPACE_DIRECTORY)
    assert_safe_mutation(target, "Lakebase schema", ISOLATED_LAKEBASE_SCHEMA)
    assert_safe_fevm_grant(
        target.presenter_app_name, target.catalog, set(TRACE_CATALOG_PRIVILEGES)
    )
    trace_schema = f"{target.catalog}.{target.trace_schema}"
    assert_safe_fevm_grant(
        target.presenter_app_name, trace_schema, set(TRACE_SCHEMA_PRIVILEGES)
    )

    actions: list[Action] = []
    if owned_app is None:
        actions.append(
            Action(
                "create_app",
                target.presenter_app_name,
                {"forward_user_access_token": True},
            )
        )
    actions.append(
        Action("update_app_scopes", target.presenter_app_name, {"user_api_scopes": list(APP_SCOPES)})
    )
    if preauthorized_trace_grants:
        actions.extend(
            [
                Action(
                    "accept_preauthorized_trace_catalog",
                    target.presenter_app_name,
                    {
                        "securable": target.catalog,
                        "privileges": sorted(TRACE_CATALOG_PRIVILEGES),
                        "source": "external_confirmation",
                        "proof_required": PREAUTHORIZED_GRANT_PROOF,
                    },
                ),
                Action(
                    "accept_preauthorized_trace_schema",
                    target.presenter_app_name,
                    {
                        "securable": trace_schema,
                        "privileges": sorted(TRACE_SCHEMA_PRIVILEGES),
                        "source": "external_confirmation",
                        "proof_required": PREAUTHORIZED_GRANT_PROOF,
                    },
                ),
            ]
        )
    else:
        actions.extend(
            [
            Action(
                "grant_trace_catalog",
                target.presenter_app_name,
                {"securable": target.catalog, "privileges": sorted(TRACE_CATALOG_PRIVILEGES)},
            ),
            Action(
                "grant_trace_schema",
                target.presenter_app_name,
                {"securable": trace_schema, "privileges": sorted(TRACE_SCHEMA_PRIVILEGES)},
            ),
            ]
        )
    actions.extend(
        [
            Action(
                "configure_lakebase_disabled",
                ISOLATED_LAKEBASE_SCHEMA,
                {"reason": "isolated schema/grant is not safely automatable"},
            ),
            Action(
                "sync_workspace",
                OWNED_WORKSPACE_DIRECTORY,
                {"target": "presenter baseline"},
            ),
            Action(
                "grant_workspace_source_read",
                OWNED_WORKSPACE_DIRECTORY,
                {"principal": "discovered_owned_app", "permission": "CAN_READ"},
            ),
            Action("deploy_app", target.presenter_app_name, {"state": "baseline"}),
        ]
    )
    return tuple(actions)


def _assert_target_binding(
    target: TargetConfig,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> dict[str, Any]:
    identity = verify_profile(target.profile, target.host)
    if str(workspace_id_provider(target.profile)) != target.workspace_id:
        raise RuntimeError("Databricks workspace ID does not match the FEVM target")
    if not isinstance(identity, dict):
        raise RuntimeError("Databricks profile identity is malformed")
    return identity


def _authenticated_user_name(identity: dict[str, Any]) -> str:
    current_user = identity.get("current_user")
    if not isinstance(current_user, dict):
        raise RuntimeError("Databricks current-user identity is malformed")
    user_name = current_user.get("user_name", current_user.get("userName"))
    if not isinstance(user_name, str) or not user_name:
        raise RuntimeError("Databricks current-user name is malformed")
    return user_name


def _require_authoritative_app_owner(app: dict[str, Any], expected_owner: str) -> None:
    if expected_owner not in {app.get("creator"), app.get("owner")}:
        raise RuntimeError("Owned app ownership does not match the authenticated user")


def _require_uuid(value: Any, label: str) -> str:
    try:
        canonical = str(uuid.UUID(value)) if isinstance(value, str) else ""
    except (ValueError, AttributeError):
        canonical = ""
    if not canonical or canonical != value.lower():
        raise RuntimeError(f"Owned app is missing concrete UUID {label}")
    return value


def _load_saved_owned_identity(
    target: TargetConfig, state_path: Path | None
) -> tuple[str, str] | None:
    if state_path is None or not state_path.exists():
        return None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("Generated FEVM state is unreadable") from error
    if not isinstance(state, dict) or state.get("target") != target.key:
        raise RuntimeError("Generated FEVM state does not match the target")
    return (
        _require_uuid(state.get("presenter_app_id"), "presenter_app_id"),
        _require_uuid(
            state.get("presenter_service_principal_client_id"),
            "presenter_service_principal_client_id",
        ),
    )


def _require_owned_app_metadata(target: TargetConfig, app: dict[str, Any]) -> dict[str, Any]:
    _require_exact(app, "name", target.presenter_app_name, "owned app")
    expected_url = (
        f"https://{target.presenter_app_name}-{target.workspace_id}.aws.databricksapps.com"
    )
    _require_exact(app, "url", expected_url, "owned app")
    for field_name in ("id", "service_principal_client_id"):
        _require_uuid(app.get(field_name), field_name)
    return app


def _require_owned_app_identity(
    app: dict[str, Any], expected_app_id: str, expected_principal: str
) -> None:
    if (
        app.get("id") != expected_app_id
        or app.get("service_principal_client_id") != expected_principal
    ):
        raise RuntimeError("Owned app identity changed during reconciliation")


def _ensure_owned_app(
    target: TargetConfig,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
    state_path: Path,
) -> tuple[dict[str, Any], bool]:
    identity = _assert_target_binding(target, verify_profile, workspace_id_provider)
    expected_owner = _authenticated_user_name(identity)
    assert_safe_mutation(target, "app", target.presenter_app_name)
    saved_identity = _load_saved_owned_identity(target, state_path)
    current = _read_owned_app(run_cli, target, expected_owner, saved_identity)
    created = current is None
    if created and saved_identity is not None:
        raise RuntimeError("Saved owned app identity is missing from the workspace")
    if created:
        _run(
            run_cli,
            target.profile,
            ["apps", "create"],
            {
                "name": target.presenter_app_name,
                "forward_user_access_token": True,
            },
        )
    app = _read_app(run_cli, target, target.presenter_app_name)
    app = _require_owned_app_metadata(target, app)
    if saved_identity is None:
        _require_authoritative_app_owner(app, expected_owner)
    elif (app["id"], app["service_principal_client_id"]) != saved_identity:
        raise RuntimeError("Owned app does not match the saved generated-state identity")
    return app, created


def _ensure_app_scopes(
    target: TargetConfig,
    expected_app_id: str,
    expected_principal: str,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> tuple[dict[str, Any], bool]:
    _assert_target_binding(target, verify_profile, workspace_id_provider)
    assert_safe_mutation(target, "app", target.presenter_app_name)
    current = _require_owned_app_metadata(
        target, _read_app(run_cli, target, target.presenter_app_name)
    )
    _require_owned_app_identity(current, expected_app_id, expected_principal)
    changed = tuple(current.get("user_api_scopes", ())) != APP_SCOPES
    if changed:
        _run(
            run_cli,
            target.profile,
            ["apps", "update", target.presenter_app_name],
            {"user_api_scopes": list(APP_SCOPES)},
        )
    app = _require_owned_app_metadata(
        target, _read_app(run_cli, target, target.presenter_app_name)
    )
    _require_owned_app_identity(app, expected_app_id, expected_principal)
    if set(app.get("user_api_scopes", ())) != set(APP_SCOPES):
        raise RuntimeError("Owned app OBO scopes were not reconciled")
    return app, changed


def _privilege_assignments(response: dict | list) -> list[dict[str, Any]]:
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
    return [dict(assignment) for assignment in assignments]


def _principal_privileges(response: dict | list, principal: str) -> set[str]:
    privileges: set[str] = set()
    for assignment in _privilege_assignments(response):
        if assignment.get("principal") != principal:
            continue
        values = assignment.get("privileges", [])
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise RuntimeError("Databricks grant privileges are malformed")
        privileges.update(values)
    return privileges


def _ensure_grant(
    target: TargetConfig,
    expected_app_id: str,
    expected_principal: str,
    securable_type: str,
    full_name: str,
    privileges: frozenset[str],
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> bool:
    _assert_target_binding(target, verify_profile, workspace_id_provider)
    assert_safe_fevm_grant(target.presenter_app_name, full_name, set(privileges))
    app = _require_owned_app_metadata(
        target, _read_app(run_cli, target, target.presenter_app_name)
    )
    _require_owned_app_identity(app, expected_app_id, expected_principal)

    args = ["grants", "get", securable_type, full_name]
    current = _run(run_cli, target.profile, args)
    missing = privileges - _principal_privileges(current, expected_principal)
    if missing:
        payload = {
            "changes": [
                {
                    "principal": expected_principal,
                    "add": sorted(missing),
                }
            ]
        }
        _run(
            run_cli,
            target.profile,
            ["grants", "update", securable_type, full_name],
            payload,
        )
    effective = _run(run_cli, target.profile, args)
    if not privileges.issubset(_principal_privileges(effective, expected_principal)):
        raise RuntimeError(f"Owned app trace grant was not effective on {full_name}")
    return bool(missing)


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as temporary:
        temporary.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def _generated_state(target: TargetConfig, app: dict[str, Any]) -> dict[str, Any]:
    principal = app["service_principal_client_id"]
    return {
        "target": target.key,
        "presenter_app_id": app["id"],
        "presenter_app_url": app["url"],
        "presenter_service_principal_client_id": principal,
        "mlflow_experiment_name": f"/Users/{principal}/gurary_bobabricks_store_ops_uc",
        "lakebase": {
            "validated": True,
            "enabled": False,
            "schema": ISOLATED_LAKEBASE_SCHEMA,
        },
    }


def _validate_rendered_baseline(build_path: Path) -> None:
    import yaml

    manifest = yaml.safe_load((build_path / "app.yaml").read_text(encoding="utf-8"))
    environment = {
        entry.get("name"): str(entry.get("value"))
        for entry in manifest.get("env", [])
        if isinstance(entry, dict)
    }
    if environment.get("BOBABRICKS_DISABLE_LAKEBASE") != "1":
        raise RuntimeError("Rendered FEVM baseline does not honestly disable Lakebase")
    if "CONFLUENCE_MCP_ENABLED" in environment or "ATLASSIAN_MCP_URL" in environment:
        raise RuntimeError("Rendered FEVM baseline unexpectedly includes Confluence")


def _workspace_listing(response: dict | list) -> list[dict[str, Any]]:
    if isinstance(response, list):
        objects = response
    elif isinstance(response, dict):
        objects = response.get("objects", [])
    else:
        raise RuntimeError("Databricks workspace list response is malformed")
    if not isinstance(objects, list) or not all(isinstance(item, dict) for item in objects):
        raise RuntimeError("Databricks workspace list response is malformed")
    return [dict(item) for item in objects]


def _workspace_acl(response: dict | list) -> list[dict[str, Any]]:
    permissions = _require_mapping(response, "workspace permissions")
    access_control_list = permissions.get("access_control_list", [])
    if not isinstance(access_control_list, list) or not all(
        isinstance(entry, dict) for entry in access_control_list
    ):
        raise RuntimeError("Databricks workspace permissions response is malformed")
    return [dict(entry) for entry in access_control_list]


def _workspace_principal_can_read(response: dict | list, principal: str) -> bool:
    sufficient = {"CAN_READ", "CAN_EDIT", "CAN_MANAGE", "CAN_RUN"}
    for entry in _workspace_acl(response):
        if entry.get("service_principal_name") != principal:
            continue
        permissions = entry.get("all_permissions", [])
        if not isinstance(permissions, list) or not all(
            isinstance(permission, dict) for permission in permissions
        ):
            raise RuntimeError("Databricks workspace principal permissions are malformed")
        if any(permission.get("permission_level") in sufficient for permission in permissions):
            return True
    return False


def _ensure_workspace_source_read(
    target: TargetConfig,
    workspace_path: str,
    principal: str,
    run_cli: RunCli,
    verify_profile: VerifyProfile,
    workspace_id_provider: WorkspaceIdProvider,
) -> bool:
    _assert_target_binding(target, verify_profile, workspace_id_provider)
    assert_safe_mutation(target, "workspace directory", OWNED_WORKSPACE_DIRECTORY)
    status = _require_mapping(
        _run(run_cli, target.profile, ["workspace", "get-status", workspace_path]),
        "workspace source directory",
    )
    if status.get("path") != workspace_path or str(status.get("object_type")).upper() != "DIRECTORY":
        raise RuntimeError("Workspace source directory does not match the owned sync target")
    object_id = status.get("object_id")
    if not isinstance(object_id, (str, int)) or not str(object_id):
        raise RuntimeError("Workspace source directory has no concrete object ID")
    args = ["workspace", "get-permissions", "directories", str(object_id)]
    current = _run(run_cli, target.profile, args)
    changed = not _workspace_principal_can_read(current, principal)
    if changed:
        _run(
            run_cli,
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
    effective = _run(run_cli, target.profile, args)
    if not _workspace_principal_can_read(effective, principal):
        raise RuntimeError("Owned app cannot read its synced workspace source directory")
    return changed


def _default_sync_runner(command: list[str]) -> None:
    subprocess.run(command, check=True)


def _default_app_invoker(
    profile: str,
    app_url: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Invoke the owned app with SDK authentication without retaining auth headers."""
    import httpx
    from databricks.sdk import WorkspaceClient

    headers: dict[str, str] = {}
    try:
        headers.update(WorkspaceClient(profile=profile).config.authenticate())
        with httpx.Client(timeout=360.0) as client:
            response = client.post(
                f"{app_url.rstrip('/')}/invocations",
                headers=headers,
                json=payload,
            )
            if response.status_code in {502, 503, 504}:
                raise TransientAppInvocationError("Owned app ingress is not ready")
            if response.status_code < 200 or response.status_code >= 300:
                raise RuntimeError("Owned app probe invocation failed")
            result = response.json()
    except RuntimeError:
        raise
    except Exception:
        raise RuntimeError("Owned app probe invocation failed") from None
    finally:
        headers.clear()
    if not isinstance(result, dict):
        raise RuntimeError("Owned app probe invocation returned a malformed response")
    return result


def _invoke_probe_when_ready(
    app_invoker: AppInvoker,
    profile: str,
    app_url: str,
    payload: dict[str, Any],
    waiter: Waiter,
    before_attempt: Callable[[], None],
) -> dict[str, Any]:
    for attempt in range(4):
        try:
            before_attempt()
            return app_invoker(profile, app_url, payload)
        except TransientAppInvocationError:
            if attempt == 3:
                raise RuntimeError("Owned app probe invocation failed after readiness wait") from None
            waiter(2.0 * (2**attempt))
    raise RuntimeError("Owned app probe invocation failed after readiness wait")


def _trace_state_name(value: Any) -> str:
    state = getattr(value, "value", value)
    return str(state)


def _default_trace_proof_provider(request: dict[str, Any]) -> dict[str, Any]:
    """Poll the private UC trace location until the probe trace and spans are readable."""
    from mlflow import MlflowClient

    profile = request["profile"]
    experiment_name = request["experiment_name"]
    trace_location = request["trace_location"]
    warehouse_id = request["warehouse_id"]
    session_id = request["session_id"]
    not_before_ms = request["not_before_ms"]
    client = MlflowClient(tracking_uri=f"databricks://{profile}")
    deadline = time.monotonic() + 120.0
    previous_warehouse_id = os.environ.get("MLFLOW_TRACING_SQL_WAREHOUSE_ID")
    previous_warehouse_auto_start = os.environ.get("MLFLOW_SQL_WAREHOUSE_AUTO_START")
    os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id
    os.environ["MLFLOW_SQL_WAREHOUSE_AUTO_START"] = "false"
    try:
        while time.monotonic() < deadline:
            try:
                experiment = client.get_experiment_by_name(experiment_name)
                if experiment is None:
                    time.sleep(2.0)
                    continue
                traces = client.search_traces(
                    locations=[trace_location],
                    max_results=100,
                    order_by=["timestamp_ms DESC"],
                    include_spans=False,
                )
                for candidate in traces:
                    info = candidate.info
                    metadata = info.trace_metadata
                    if (
                        metadata.get("mlflow.trace.session") != session_id
                        or info.request_time < not_before_ms
                    ):
                        continue
                    trace = client.get_trace(info.trace_id, display=False)
                    spans = trace.data.spans
                    if not spans:
                        continue
                    return {
                        "experiment_name": experiment.name,
                        "experiment_tags": dict(experiment.tags),
                        "trace_id": info.trace_id,
                        "trace_request_time_ms": info.request_time,
                        "trace_session_id": metadata.get("mlflow.trace.session"),
                        "trace_state": _trace_state_name(info.state),
                        "spans": [
                            {
                                "name": str(getattr(span, "name", "")),
                                "status": _trace_state_name(getattr(span, "status", "")),
                            }
                            for span in spans
                        ],
                    }
            except Exception:
                pass
            time.sleep(2.0)
    finally:
        if previous_warehouse_id is None:
            os.environ.pop("MLFLOW_TRACING_SQL_WAREHOUSE_ID", None)
        else:
            os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = previous_warehouse_id
        if previous_warehouse_auto_start is None:
            os.environ.pop("MLFLOW_SQL_WAREHOUSE_AUTO_START", None)
        else:
            os.environ["MLFLOW_SQL_WAREHOUSE_AUTO_START"] = previous_warehouse_auto_start
    raise RuntimeError("Fresh owned-app trace and spans were not readable")


def _validate_probe_invocation(response: dict[str, Any]) -> None:
    output = response.get("output") if isinstance(response, dict) else None
    if not isinstance(output, list) or not output:
        raise RuntimeError("Owned app probe invocation returned no output")


def _validate_live_trace_proof(
    request: dict[str, Any], proof: dict[str, Any]
) -> None:
    if not isinstance(proof, dict):
        raise RuntimeError("Live trace proof is malformed")
    if proof.get("experiment_name") != request["experiment_name"]:
        raise RuntimeError("Live trace proof did not resolve the owned app experiment")
    tags = proof.get("experiment_tags")
    expected_tags = {
        TRACE_DESTINATION_TAG: request["trace_location"],
        TRACE_SPAN_TABLE_TAG: f"{request['trace_location']}_otel_spans",
    }
    if not isinstance(tags, dict) or any(tags.get(key) != value for key, value in expected_tags.items()):
        raise RuntimeError("Live trace tags did not match the isolated UC destination")
    trace_id = proof.get("trace_id")
    if not isinstance(trace_id, str) or not trace_id:
        raise RuntimeError("Live trace write was not proven")
    request_time = proof.get("trace_request_time_ms")
    if (
        not isinstance(request_time, int)
        or isinstance(request_time, bool)
        or request_time < request["not_before_ms"]
    ):
        raise RuntimeError("Live proof did not contain a fresh trace")
    if proof.get("trace_session_id") != request["session_id"]:
        raise RuntimeError("Live trace session did not match the probe session")
    if proof.get("trace_state") != "OK":
        raise RuntimeError("Live trace did not succeed")
    spans = proof.get("spans")
    if not isinstance(spans, list) or not spans:
        raise RuntimeError("Live trace span retrieval was not proven")


def _status_state(value: Any, label: str) -> str:
    state = value.get("state") if isinstance(value, dict) else value
    if not isinstance(state, str) or not state:
        raise RuntimeError(f"Owned app {label} state is malformed")
    return state


def _deployment_id(deployment: dict[str, Any]) -> str:
    deployment_id = deployment.get("deployment_id")
    if not isinstance(deployment_id, str) or not deployment_id or "\x00" in deployment_id:
        raise RuntimeError("Requested deployment response has no concrete deployment ID")
    return deployment_id


def _wait_for_exact_deployment(
    run_cli: RunCli,
    target: TargetConfig,
    deployment_id: str,
    workspace_path: str,
    waiter: Waiter,
) -> dict[str, Any]:
    for attempt in range(60):
        deployment = _require_mapping(
            _run(
                run_cli,
                target.profile,
                ["apps", "get-deployment", target.presenter_app_name, deployment_id],
            ),
            "requested deployment",
        )
        if _deployment_id(deployment) != deployment_id:
            raise RuntimeError("Polled deployment did not match the requested deployment ID")
        if deployment.get("source_code_path") != workspace_path:
            raise RuntimeError("Requested deployment source path did not match the owned sync path")
        state = _status_state(deployment.get("status"), "deployment")
        if state == "SUCCEEDED":
            return deployment
        if state in {"FAILED", "CANCELLED", "ERROR"}:
            raise RuntimeError("Requested deployment did not succeed")
        if attempt < 59:
            waiter(2.0)
    raise RuntimeError("Requested deployment did not reach SUCCEEDED")


def apply_provision_plan(
    plan: ProvisionPlan,
    *,
    run_cli: RunCli = run_json,
    verify_profile: VerifyProfile = assert_profile,
    workspace_id_provider: WorkspaceIdProvider = _workspace_id_from_sdk,
    state_path: Path = ROOT / "deploy" / "state" / "fevm.json",
    inventory_path: Path = ROOT / "deploy" / "inventory" / "live-fevm-before.json",
    sync_runner: SyncRunner = _default_sync_runner,
    app_invoker: AppInvoker = _default_app_invoker,
    trace_proof_provider: TraceProofProvider = _default_trace_proof_provider,
    session_id_provider: SessionIdProvider = lambda: uuid.uuid4().hex,
    now_ms: NowMs = lambda: time.time_ns() // 1_000_000,
    app_ready_waiter: Waiter = time.sleep,
    deployment_waiter: Waiter = time.sleep,
) -> ApplyEvidence:
    if plan.shared_ops_before != plan.shared_ops_after_dry_run:
        raise RuntimeError("Refusing apply with a changed shared OpsTask snapshot")
    target = load_target("fevm")
    _assert_target_binding(target, verify_profile, workspace_id_provider)
    _write_json_atomic(inventory_path, plan.inventory_dict())

    app, created = _ensure_owned_app(
        target, run_cli, verify_profile, workspace_id_provider, state_path
    )
    app_id = app["id"]
    principal = app["service_principal_client_id"]
    app, scopes_changed = _ensure_app_scopes(
        target,
        app_id,
        principal,
        run_cli,
        verify_profile,
        workspace_id_provider,
    )

    trace_schema = f"{target.catalog}.{target.trace_schema}"
    if plan.trace_grant_mode == PREAUTHORIZED_GRANT_MODE:
        catalog_changed = False
        schema_changed = False
    elif plan.trace_grant_mode == LOCAL_GRANT_MODE:
        catalog_changed = _ensure_grant(
            target,
            app_id,
            principal,
            "catalog",
            target.catalog,
            TRACE_CATALOG_PRIVILEGES,
            run_cli,
            verify_profile,
            workspace_id_provider,
        )
        schema_changed = _ensure_grant(
            target,
            app_id,
            principal,
            "schema",
            trace_schema,
            TRACE_SCHEMA_PRIVILEGES,
            run_cli,
            verify_profile,
            workspace_id_provider,
        )
    else:
        raise RuntimeError(f"Unknown trace grant mode: {plan.trace_grant_mode}")

    _assert_target_binding(target, verify_profile, workspace_id_provider)
    assert_safe_mutation(target, "Lakebase schema", ISOLATED_LAKEBASE_SCHEMA)
    current_app = _require_owned_app_metadata(
        target, _read_app(run_cli, target, target.presenter_app_name)
    )
    _require_owned_app_identity(current_app, app_id, principal)
    _write_json_atomic(state_path, _generated_state(target, current_app))

    from deploy.render import render_deployment

    build_path = render_deployment("fevm", "baseline")
    _validate_rendered_baseline(build_path)
    sync_source_path = build_path.resolve(strict=True)
    if not sync_source_path.is_dir():
        raise RuntimeError("Rendered FEVM baseline did not resolve to a directory")

    _assert_target_binding(target, verify_profile, workspace_id_provider)
    assert_safe_mutation(target, "workspace directory", OWNED_WORKSPACE_DIRECTORY)
    workspace_parent = "/Workspace/Users/adam.gurary@databricks.com"
    _workspace_listing(_run(run_cli, target.profile, ["workspace", "list", workspace_parent]))
    workspace_path = f"{workspace_parent}/{OWNED_WORKSPACE_DIRECTORY}"
    sync_runner(
        [
            "databricks",
            "sync",
            "--full",
            "--include",
            "**",
            str(sync_source_path),
            workspace_path,
            "--profile",
            target.profile,
        ]
    )
    synced_objects = _workspace_listing(
        _run(run_cli, target.profile, ["workspace", "list", workspace_path])
    )
    expected_manifest = f"{workspace_path}/app.yaml"
    if not any(
        item.get("path") == expected_manifest
        and str(item.get("object_type")).upper() == "FILE"
        for item in synced_objects
    ):
        raise RuntimeError("Owned workspace sync did not produce the root app manifest")

    workspace_acl_changed = _ensure_workspace_source_read(
        target,
        workspace_path,
        principal,
        run_cli,
        verify_profile,
        workspace_id_provider,
    )

    _assert_target_binding(target, verify_profile, workspace_id_provider)
    assert_safe_mutation(target, "app", target.presenter_app_name)
    before_deploy = _require_owned_app_metadata(
        target, _read_app(run_cli, target, target.presenter_app_name)
    )
    _require_owned_app_identity(before_deploy, app_id, principal)
    deploy_response = _require_mapping(
        _run(
            run_cli,
            target.profile,
            [
                "apps",
                "deploy",
                target.presenter_app_name,
                "--source-code-path",
                workspace_path,
            ],
        ),
        "requested deployment",
    )
    requested_deployment = _wait_for_exact_deployment(
        run_cli,
        target,
        _deployment_id(deploy_response),
        workspace_path,
        deployment_waiter,
    )
    deployed = _require_owned_app_metadata(
        target, _read_app(run_cli, target, target.presenter_app_name)
    )
    _require_owned_app_identity(deployed, app_id, principal)
    deployment_state = _status_state(
        requested_deployment.get("status"),
        "deployment",
    )
    app_state = _status_state(deployed.get("app_status"), "application")
    compute_state = _status_state(deployed.get("compute_status"), "compute")
    expected = ("SUCCEEDED", "RUNNING", "ACTIVE")
    if (deployment_state, app_state, compute_state) != expected:
        raise RuntimeError(
            "Owned app did not reach the required deployment/application/compute states"
        )

    session_id = session_id_provider()
    if not isinstance(session_id, str) or not session_id or "\x00" in session_id:
        raise RuntimeError("Trace probe session identifier is invalid")
    probe_started_at_ms = now_ms()
    if not isinstance(probe_started_at_ms, int) or isinstance(probe_started_at_ms, bool):
        raise RuntimeError("Trace probe start time is invalid")
    probe_payload = {
        "input": [{"role": "user", "content": TRACE_PROBE_PROMPT}],
        "custom_inputs": {"session_id": session_id},
    }
    invocation = _invoke_probe_when_ready(
        app_invoker,
        target.profile,
        deployed["url"],
        probe_payload,
        app_ready_waiter,
        lambda: _assert_target_binding(target, verify_profile, workspace_id_provider),
    )
    _validate_probe_invocation(invocation)
    trace_request = {
        "profile": target.profile,
        "experiment_name": _generated_state(target, deployed)["mlflow_experiment_name"],
        "trace_location": f"{target.catalog}.{target.trace_schema}.{TRACE_TABLE_PREFIX}",
        "warehouse_id": target.warehouse_id,
        "session_id": session_id,
        "not_before_ms": probe_started_at_ms,
    }
    proof = trace_proof_provider(trace_request)
    _validate_live_trace_proof(trace_request, proof)
    trace_proof_state = "PASS"

    mutated: list[str] = []
    if created:
        mutated.append(target.presenter_app_name)
    if scopes_changed:
        mutated.append(target.presenter_app_name)
    if catalog_changed:
        mutated.append(target.presenter_app_name)
    if schema_changed:
        mutated.append(target.presenter_app_name)
    if workspace_acl_changed:
        mutated.append(OWNED_WORKSPACE_DIRECTORY)
    mutated.extend(
        [ISOLATED_LAKEBASE_SCHEMA, OWNED_WORKSPACE_DIRECTORY, target.presenter_app_name]
    )
    return ApplyEvidence(
        deployment_state=deployment_state,
        app_state=app_state,
        compute_state=compute_state,
        mutated_resources=tuple(mutated),
        trace_grant_mode=plan.trace_grant_mode,
        trace_proof_state=trace_proof_state,
    )


def build_provision_plan(
    *,
    run_cli: RunCli = run_json,
    verify_profile: VerifyProfile = assert_profile,
    workspace_id_provider: WorkspaceIdProvider = _workspace_id_from_sdk,
    lakebase_inventory_provider: LakebaseInventoryProvider = _lakebase_inventory_from_sdk,
    preauthorized_trace_grants: bool = False,
    state_path: Path | None = None,
) -> ProvisionPlan:
    target = load_target("fevm")
    identity = _assert_target_binding(target, verify_profile, workspace_id_provider)
    expected_owner = _authenticated_user_name(identity)
    saved_identity = _load_saved_owned_identity(target, state_path)

    shared_inventory, owned_app = _read_shared_inventory(
        run_cli,
        target,
        lakebase_inventory_provider,
        expected_owner,
        saved_identity,
    )
    before = _read_shared_ops_snapshot(run_cli, target)
    actions = _actions(target, owned_app, preauthorized_trace_grants)
    after = _read_shared_ops_snapshot(run_cli, target)
    if before != after:
        raise RuntimeError("Shared OpsTask table changed during dry run")
    effective_tools = _effective_baseline_tools(target)
    if "create_ops_task" in effective_tools:
        raise RuntimeError("FEVM effective tools unexpectedly expose create_ops_task")
    return ProvisionPlan(
        actions=actions,
        effective_tools=effective_tools,
        shared_inventory=shared_inventory,
        shared_ops_before=before,
        shared_ops_after_dry_run=after,
        owned_app=owned_app,
        trace_grant_mode=(
            PREAUTHORIZED_GRANT_MODE if preauthorized_trace_grants else LOCAL_GRANT_MODE
        ),
    )


def _parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Inspect and print the action plan")
    mode.add_argument("--apply", action="store_true", help="Reconcile and deploy the reviewed plan")
    parser.add_argument(
        "--preauthorized-trace-grants",
        action="store_true",
        help=(
            "Record externally confirmed trace grants and require live trace proof "
            "instead of reading or updating grants"
        ),
    )
    return parser.parse_args(argv)


def main() -> None:
    arguments = _parse_arguments()
    state_path = ROOT / "deploy" / "state" / "fevm.json"
    plan = build_provision_plan(
        preauthorized_trace_grants=arguments.preauthorized_trace_grants,
        state_path=state_path,
    )
    result = (
        plan.as_dict()
        if arguments.dry_run
        else apply_provision_plan(plan, state_path=state_path).as_dict()
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
