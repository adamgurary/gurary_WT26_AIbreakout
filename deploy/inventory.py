"""Read-only, credential-safe workspace inventory."""

from __future__ import annotations

from typing import Any

from .config import TargetConfig, load_target
from .databricks_cli import assert_profile, run_json


_SENSITIVE_KEY_PARTS = ("token", "authorization", "secret")


def sanitize_for_json(value: Any) -> Any:
    """Copy JSON data while dropping credentials and redacted connection options."""
    if isinstance(value, list):
        return [sanitize_for_json(item) for item in value]
    if not isinstance(value, dict):
        return value

    sanitized: dict[str, Any] = {}
    for key, item in value.items():
        normalized_key = str(key).lower().replace("-", "_")
        if any(part in normalized_key for part in _SENSITIVE_KEY_PARTS):
            continue
        if normalized_key in {"connection_options", "options"} and _contains_redacted(item):
            continue
        sanitized[str(key)] = sanitize_for_json(item)
    return sanitized


def _contains_redacted(value: Any) -> bool:
    if isinstance(value, str):
        return "redacted" in value.lower()
    if isinstance(value, list):
        return any(_contains_redacted(item) for item in value)
    if isinstance(value, dict):
        return any(_contains_redacted(item) for item in value.values())
    return False


def _records(response: dict | list, key: str) -> list[dict[str, Any]]:
    records = response if isinstance(response, list) else response.get(key, [])
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise RuntimeError(f"Databricks {key} response is malformed")
    return [dict(record) for record in records]


def _shared_for_fevm(target: TargetConfig, record: dict[str, Any]) -> bool:
    if not target.shared_mcp_read_only:
        return False
    ownership_fields = (
        "name", "display_name", "full_name", "app_name", "url", "experiment_name",
    )
    return not any(
        isinstance(record.get(field), str)
        and ("gurary_" in record[field].lower() or "gurary-" in record[field].lower())
        for field in ownership_fields
    )


def _mark_records(target: TargetConfig, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for record in records:
        if _shared_for_fevm(target, record):
            record["mutation_allowed"] = False
    return records


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
        "current_user": identity["current_user"],
        "apps": _mark_records(target, apps),
        "warehouses": _mark_records(target, warehouses),
        "genie_spaces": _mark_records(target, genie_spaces),
        "experiments": _mark_records(target, experiments),
        "uc_objects": _mark_records(target, uc_objects),
        "service_principals": _mark_records(target, service_principals),
        "effective_grants": _mark_records(target, effective_grants),
        "lakebase": _mark_records(target, lakebase),
        "connections": _mark_records(target, connections),
    }
    return sanitize_for_json(inventory)
