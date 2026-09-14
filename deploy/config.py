"""Typed, allowlisted deployment configuration loaders."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


_DEPLOY_DIRECTORY = Path(__file__).resolve().parent
_TARGET_FILES = {
    "fevm": _DEPLOY_DIRECTORY / "targets" / "fevm.yaml",
    "field_eng": _DEPLOY_DIRECTORY / "targets" / "field_eng.yaml",
}
_STATE_FILES = {
    "baseline": _DEPLOY_DIRECTORY / "states" / "baseline.yaml",
    "upgraded": _DEPLOY_DIRECTORY / "states" / "upgraded.yaml",
}


@dataclass(frozen=True)
class TargetConfig:
    key: str
    profile: str
    host: str
    workspace_id: str
    presenter_app_name: str
    catalog: str
    data_schema: str
    trace_schema: str
    table_prefix: str
    warehouse_id: str | None = None
    warehouse_name: str | None = None
    genie_space_id: str | None = None
    genie_space_name: str | None = None
    storetime_app_name: str = ""
    storetime_mcp_url: str | None = None
    opstask_app_name: str = ""
    opstask_mcp_url: str | None = None
    lakebase_project: str | None = None
    lakebase_branch: str | None = None
    lakebase_endpoint: str | None = None
    lakebase_database: str | None = None
    lakebase_schema: str | None = None
    confluence_connection: str = ""
    shared_mcp_read_only: bool = False
    apps_domain: str = "aws.databricksapps.com"


@dataclass(frozen=True)
class DemoState:
    key: str
    confluence_enabled: bool
    extra_tools: tuple[str, ...]
    required_tools: tuple[str, ...]


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as config_file:
        data = yaml.safe_load(config_file)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def load_target(key: str) -> TargetConfig:
    try:
        path = _TARGET_FILES[key]
    except KeyError as error:
        raise ValueError(f"Unknown deployment target: {key}") from error
    return TargetConfig(**_load_yaml(path))


def load_state(key: str) -> DemoState:
    try:
        path = _STATE_FILES[key]
    except KeyError as error:
        raise ValueError(f"Unknown demo state: {key}") from error
    data = _load_yaml(path)
    return DemoState(
        key=data["key"],
        confluence_enabled=data["confluence_enabled"],
        extra_tools=tuple(data["extra_tools"]),
        required_tools=tuple(data["required_tools"]),
    )
