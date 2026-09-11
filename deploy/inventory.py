"""Read-only, credential-safe workspace inventory."""

from __future__ import annotations

from typing import Any

from .config import TargetConfig, load_target
from .databricks_cli import assert_profile, run_json


_RESOURCE_FIELDS = {
    "current_users": ("id", "user_name", "userName", "active", "display_name"),
    "apps": (
        "app_id",
        "id",
        "name",
        "url",
        "owner",
        "creator",
        "app_status",
        "compute_status",
        "active_deployment",
        "service_principal_client_id",
    ),
    "warehouses": ("id", "name", "state", "creator_name"),
    "genie_spaces": ("space_id", "title", "owner"),
    "experiments": ("experiment_id", "name", "lifecycle_stage", "tags"),
    "uc_objects": (
        "full_name",
        "name",
        "owner",
        "catalog_name",
        "schema_name",
        "table_id",
        "schema_id",
        "table_type",
    ),
    "service_principals": ("id", "application_id", "display_name", "active"),
    "effective_grants": ("principal", "privileges"),
    "lakebase": ("name", "project_id", "status", "owner"),
    "connections": (
        "connection_id",
        "connection_type",
        "full_name",
        "name",
        "owner",
        "read_only",
    ),
}
_EXPERIMENT_TAG_KEYS = frozenset(
    {
        "team",
        "mlflow.experiment.databricksTraceDestinationPath",
        "mlflow.experiment.databricksTraceSpanStorageTable",
    }
)


def _project_status(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    return {"state": value["state"]} if "state" in value else {}


def _project_experiment_tags(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: value[key] for key in _EXPERIMENT_TAG_KEYS if key in value}
    if isinstance(value, list):
        return [
            {"key": item["key"], "value": item.get("value")}
            for item in value
            if isinstance(item, dict) and item.get("key") in _EXPERIMENT_TAG_KEYS
        ]
    return []


def _project_active_deployment(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    projected = {
        key: value[key]
        for key in ("deployment_id", "source_code_path")
        if key in value
    }
    if "status" in value:
        projected["status"] = _project_status(value["status"])
    return projected


def project_inventory_record(resource_type: str, record: dict[str, Any]) -> dict[str, Any]:
    """Return only allowlisted identity, state, and ownership evidence."""
    try:
        allowed_fields = _RESOURCE_FIELDS[resource_type]
    except KeyError as error:
        raise ValueError(f"Unknown inventory resource type: {resource_type}") from error

    projected: dict[str, Any] = {}
    for field_name in allowed_fields:
        if field_name not in record:
            continue
        value = record[field_name]
        if field_name in {"app_status", "compute_status", "status"}:
            value = _project_status(value)
        elif field_name == "active_deployment":
            value = _project_active_deployment(value)
        elif field_name == "tags":
            value = _project_experiment_tags(value)
        elif field_name == "privileges":
            value = [item for item in value if isinstance(item, str)] if isinstance(value, list) else []
        projected[field_name] = value
    return projected


def _records(response: dict | list, key: str) -> list[dict[str, Any]]:
    records = response if isinstance(response, list) else response.get(key, [])
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise RuntimeError(f"Databricks {key} response is malformed")
    return [dict(record) for record in records]


def _mark_records(
    target: TargetConfig,
    resource_type: str,
    records: list[dict[str, Any]],
    owned_names: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    projected_records = [project_inventory_record(resource_type, record) for record in records]
    for record in projected_records:
        if target.shared_mcp_read_only:
            record["mutation_allowed"] = record.get("name") in owned_names
    return projected_records


def _inventory_records(profile: str, args: list[str], response_key: str) -> list[dict[str, Any]]:
    return _records(run_json(profile, args), response_key)


def inventory_target(target_key: str) -> dict:
    """Inventory the configured workspace using only Databricks read operations."""
    target = load_target(target_key)
    identity = assert_profile(target.profile, target.host)
    profile = target.profile

    apps = _inventory_records(profile, ["apps", "list"], "apps")
    warehouses = _inventory_records(profile, ["warehouses", "list"], "warehouses")
    genie_spaces = _inventory_records(profile, ["genie", "list-spaces"], "spaces")
    experiments = _inventory_records(profile, ["experiments", "list"], "experiments")
    uc_objects = _inventory_records(
        profile, ["tables", "list", target.catalog, target.data_schema], "tables"
    )
    service_principals = _inventory_records(
        profile, ["service-principals", "list"], "service_principals"
    )
    effective_grants = _inventory_records(
        profile, ["grants", "get", "schema", f"{target.catalog}.{target.data_schema}"],
        "privilege_assignments",
    )
    lakebase = _inventory_records(profile, ["database", "projects", "list"], "projects")
    connections = _inventory_records(profile, ["connections", "list"], "connections")

    inventory = {
        "workspace": {"key": target.key, "host": identity["host"], "workspace_id": target.workspace_id},
        "current_user": project_inventory_record("current_users", identity["current_user"]),
        "apps": _mark_records(target, "apps", apps, (target.presenter_app_name,)),
        "warehouses": _mark_records(target, "warehouses", warehouses),
        "genie_spaces": _mark_records(target, "genie_spaces", genie_spaces),
        "experiments": _mark_records(target, "experiments", experiments),
        "uc_objects": _mark_records(target, "uc_objects", uc_objects),
        "service_principals": _mark_records(
            target, "service_principals", service_principals
        ),
        "effective_grants": _mark_records(target, "effective_grants", effective_grants),
        "lakebase": _mark_records(target, "lakebase", lakebase),
        "connections": _mark_records(target, "connections", connections),
    }
    return inventory
