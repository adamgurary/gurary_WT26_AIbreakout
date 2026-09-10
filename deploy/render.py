"""Render an isolated, target-specific deployment tree without editing source."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

import yaml

from .config import DemoState, TargetConfig, load_state, load_target


ROOT = Path(__file__).resolve().parents[1]
BUILD_ROOT = ROOT / ".build"
STATE_ROOT = ROOT / "deploy" / "state"
ROOT_MANIFESTS = (Path("app.yaml"), Path("app.yml"))
MCP_MANIFESTS = (
    Path("mcp-apps/storetime/app.yaml"),
    Path("mcp-apps/storetime/app.yml"),
    Path("mcp-apps/opstask/app.yaml"),
    Path("mcp-apps/opstask/app.yml"),
)
RENDERED_MANIFESTS = ROOT_MANIFESTS + MCP_MANIFESTS

_AMBER_IDENTIFIERS = {
    "ad341da9-d12e-4688-ad1c-3c049cf70486",
    "bobabricks-store-ops-demo",
    "/Users/ad341da9-d12e-4688-ad1c-3c049cf70486/bobabricks-store-ops-demo-uc",
}


def _load_generated_state(target_key: str) -> dict[str, Any]:
    path = STATE_ROOT / f"{target_key}.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid generated state: {path}") from error
    if not isinstance(data, dict):
        raise ValueError(f"Expected generated-state object in {path}")
    return data


def _state_value(state: dict[str, Any], key: str, default: str | None) -> str | None:
    value = state.get(key, default)
    return str(value) if value is not None else None


def _mcp_url(app_name: str, target: TargetConfig) -> str:
    return f"https://{app_name}-{target.workspace_id}.aws.databricksapps.com/mcp"


def _lakebase_environment(target: TargetConfig, generated_state: dict[str, Any]) -> list[tuple[str, str]]:
    lakebase = generated_state.get("lakebase")
    if not isinstance(lakebase, dict) or lakebase.get("validated") is not True:
        return []

    values = {
        "project": lakebase.get("project", target.lakebase_project),
        "branch": lakebase.get("branch", target.lakebase_branch),
        "endpoint": lakebase.get("endpoint", target.lakebase_endpoint),
        "database": lakebase.get("database", target.lakebase_database),
        "schema": lakebase.get("schema", target.lakebase_schema),
    }
    if not all(isinstance(value, str) and value for value in values.values()):
        raise ValueError("Validated Lakebase state is missing an isolated destination")

    environment = [
        (
            "LAKEBASE_ENDPOINT",
            f"projects/{values['project']}/branches/{values['branch']}/endpoints/{values['endpoint']}",
        ),
        ("LAKEBASE_AUTOSCALING_PROJECT", values["project"]),
        ("LAKEBASE_AUTOSCALING_BRANCH", values["branch"]),
        ("LAKEBASE_DATABASE_NAME", values["database"]),
        ("LAKEBASE_MEMORY_SCHEMA", values["schema"]),
        ("LAKEBASE_AGENT_MEMORY_SCHEMA", values["schema"]),
    ]
    host = lakebase.get("host")
    if isinstance(host, str) and host:
        environment.append(("LAKEBASE_HOST", host))
    return environment


def _presenter_environment(
    target: TargetConfig, state: DemoState, generated_state: dict[str, Any]
) -> list[tuple[str, str]]:
    warehouse = _state_value(generated_state, "warehouse_id", target.warehouse_id or target.warehouse_name)
    genie = _state_value(generated_state, "genie_space_id", target.genie_space_id or target.genie_space_name)
    storetime_name = _state_value(generated_state, "storetime_app_name", target.storetime_app_name)
    opstask_name = _state_value(generated_state, "opstask_app_name", target.opstask_app_name)
    if not warehouse or not genie or not storetime_name or not opstask_name:
        raise ValueError(f"Target {target.key} is missing a required deployment identifier")

    storetime_url = _state_value(
        generated_state,
        "storetime_mcp_url",
        target.storetime_mcp_url or _mcp_url(storetime_name, target),
    )
    opstask_url = _state_value(
        generated_state,
        "opstask_mcp_url",
        target.opstask_mcp_url or _mcp_url(opstask_name, target),
    )
    experiment = _state_value(
        generated_state,
        "mlflow_experiment_name",
        _state_value(generated_state, "experiment_name", f"/Shared/{target.presenter_app_name}-uc"),
    )
    if not storetime_url or not opstask_url or not experiment:
        raise ValueError(f"Target {target.key} has incomplete generated deployment state")

    environment = [
        ("CHAT_APP_PORT", "3000"),
        ("AGENT_MODEL", "databricks-gpt-5"),
        ("MLFLOW_TRACKING_URI", "databricks"),
        ("MLFLOW_REGISTRY_URI", "databricks-uc"),
        ("MLFLOW_EXPERIMENT_NAME", experiment),
        ("MLFLOW_TRACE_UC_CATALOG", target.catalog),
        ("MLFLOW_TRACE_UC_SCHEMA", target.trace_schema),
        ("MLFLOW_TRACE_UC_TABLE_PREFIX", "gurary_bobabricks"),
        ("GENIE_SPACE_ID", genie),
        ("ENABLE_GENIE_MCP", "true"),
        ("DISABLE_MCP_SERVERS", "false"),
        ("DATABRICKS_WAREHOUSE_ID", warehouse),
        ("DATABRICKS_CATALOG", target.catalog),
        ("DATABRICKS_SCHEMA", target.data_schema),
        ("DATABRICKS_TABLE_PREFIX", target.table_prefix),
        ("SHARED_MCP_READ_ONLY", str(target.shared_mcp_read_only).lower()),
        ("STORETIME_APP_NAME", storetime_name),
        ("STORETIME_MCP_URL", storetime_url),
        ("OPSTASK_APP_NAME", opstask_name),
        ("OPSTASK_MCP_URL", opstask_url),
    ]
    environment.extend(_lakebase_environment(target, generated_state))
    if state.confluence_enabled:
        environment.extend(
            [
                ("CONFLUENCE_MCP_ENABLED", "true"),
                (
                    "ATLASSIAN_MCP_URL",
                    f"{target.host.rstrip('/')}/api/2.0/mcp/external/{target.confluence_connection}",
                ),
            ]
        )
    return environment


def _mcp_environment(target: TargetConfig, generated_state: dict[str, Any]) -> list[tuple[str, str]]:
    warehouse = _state_value(generated_state, "warehouse_id", target.warehouse_id or target.warehouse_name)
    if not warehouse:
        raise ValueError(f"Target {target.key} is missing a warehouse identifier")
    return [
        ("DATABRICKS_WAREHOUSE_ID", warehouse),
        ("DATABRICKS_CATALOG", target.catalog),
        ("DATABRICKS_SCHEMA", target.data_schema),
        ("DATABRICKS_TABLE_PREFIX", target.table_prefix),
    ]


def _write_manifest(path: Path, environment: list[tuple[str, str]]) -> None:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or "command" not in manifest:
        raise ValueError(f"Invalid renderer-owned manifest: {path}")
    manifest["env"] = [{"name": name, "value": value} for name, value in environment]
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _copy_source(destination: Path) -> None:
    def ignore(directory: str, names: list[str]) -> set[str]:
        relative = Path(directory).relative_to(ROOT)
        ignored = {
            name
            for name in names
            if name in {".git", ".build", ".venv", "__pycache__", ".pytest_cache"}
        }
        if relative == Path("deploy"):
            ignored.add("state")
        return ignored

    shutil.copytree(ROOT, destination, ignore=ignore)


def _forbidden_identifiers(target: TargetConfig) -> set[str]:
    identifiers = set(_AMBER_IDENTIFIERS)
    for candidate_key in ("fevm", "field_eng"):
        candidate = load_target(candidate_key)
        for value in (
            candidate.catalog,
            candidate.data_schema,
            candidate.trace_schema,
            candidate.warehouse_id,
            candidate.warehouse_name,
            candidate.genie_space_id,
            candidate.genie_space_name,
            candidate.storetime_app_name,
            candidate.storetime_mcp_url,
            candidate.opstask_app_name,
            candidate.opstask_mcp_url,
        ):
            if value and value not in {
                target.catalog,
                target.data_schema,
                target.trace_schema,
                target.warehouse_id,
                target.warehouse_name,
                target.genie_space_id,
                target.genie_space_name,
                target.storetime_app_name,
                target.storetime_mcp_url,
                target.opstask_app_name,
                target.opstask_mcp_url,
            }:
                identifiers.add(str(value))
    return identifiers


def _scan_rendered_manifests(root: Path, target: TargetConfig) -> None:
    forbidden = _forbidden_identifiers(target)
    for relative_path in RENDERED_MANIFESTS:
        text = (root / relative_path).read_text(encoding="utf-8")
        for identifier in forbidden:
            if identifier in _AMBER_IDENTIFIERS:
                unsafe = identifier in text
            else:
                unsafe = bool(re.search(rf"(?<![\w-]){re.escape(identifier)}(?![\w-])", text))
            if unsafe:
                raise ValueError(f"Unsafe rendered identifier in {relative_path}: {identifier}")


def render_deployment(target_key: str, state_key: str) -> Path:
    """Render one target/state deployment under ``.build/{target}/{state}``."""

    target = load_target(target_key)
    state = load_state(state_key)
    generated_state = _load_generated_state(target_key)
    destination = BUILD_ROOT / target.key / state.key
    destination_parent = destination.parent
    destination_parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f".{state.key}-render-", dir=destination_parent) as temporary:
        staged_root = Path(temporary) / "deployment"
        _copy_source(staged_root)
        presenter_environment = _presenter_environment(target, state, generated_state)
        for relative_path in ROOT_MANIFESTS:
            _write_manifest(staged_root / relative_path, presenter_environment)
        mcp_environment = _mcp_environment(target, generated_state)
        for relative_path in MCP_MANIFESTS:
            _write_manifest(staged_root / relative_path, mcp_environment)
        _scan_rendered_manifests(staged_root, target)

        backup = Path(temporary) / "previous"
        if destination.exists():
            os.replace(destination, backup)
        try:
            os.replace(staged_root, destination)
        except Exception:
            if backup.exists():
                os.replace(backup, destination)
            raise

    return destination
