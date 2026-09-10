"""Render isolated target deployments without mutating tracked source files."""

from __future__ import annotations

import ctypes
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import yaml

from .config import DemoState, TargetConfig, load_state, load_target


ROOT = Path(__file__).resolve().parents[1]
BUILD_ROOT = ROOT / ".build"
STATE_ROOT = ROOT / "deploy" / "state"
ROOT_MANIFESTS = (Path("app.yaml"), Path("app.yml"))
MCP_MANIFESTS = (
    Path("mcp-apps/storetime/app.yaml"), Path("mcp-apps/storetime/app.yml"),
    Path("mcp-apps/opstask/app.yaml"), Path("mcp-apps/opstask/app.yml"),
)
_AMBER_IDENTIFIERS = {
    "ad341da9-d12e-4688-ad1c-3c049cf70486",
    "bobabricks-store-ops-demo",
    "/Users/ad341da9-d12e-4688-ad1c-3c049cf70486/bobabricks-store-ops-demo-uc",
}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
_OWNED_METADATA = {
    "presenter_app_id", "presenter_app_url", "presenter_service_principal_client_id",
    "storetime_app_id", "storetime_app_url", "storetime_service_principal_client_id",
    "opstask_app_id", "opstask_app_url", "opstask_service_principal_client_id",
}
_FIELD_REQUIRED = {
    "target", "warehouse_id", "genie_space_id", "storetime_mcp_url", "opstask_mcp_url",
    "mlflow_experiment_name", "lakebase",
}
_SAFE_REFERENCE_FILES = {
    Path(path) for path in (
        ".env.databricks.example", "AGENTS.md", "CODEX_UPGRADE_PROMPT.txt", "DATABRICKS_BUILD_PLAN.md",
        "PRESENTER_SETUP.md", "README.md", "databricks.yml", "script.md", "app/app.yaml",
        "mcp-apps/README.md", "mcp-apps/inventory/app.yaml", "mcp-apps/inventory/app.yml",
        "mcp-apps/opstask/databricks.yml", "mcp-apps/storetime/databricks.yml", "scripts/render_deployment.py",
    )
}
_SAFE_REFERENCE_PREFIXES = (Path("archive"), Path("deploy"), Path("tests"))
_RUNTIME_REFERENCE_FILES = {
    Path(path) for path in (
        "agent-store-ops/mcp_servers.yaml", "agent_server/agent.py", "app/app.py",
        "app/bobabricks_agent.py", "app/services/lakebase_memory.py", "mcp-apps/opstask/app.py", "mcp-apps/storetime/app.py",
        "scripts/provision_databricks_assets.py", "scripts/start_app.py",
    )
}


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"Generated state {field} must be a non-empty string")
    return value


def _require_identifier(value: Any, field: str) -> str:
    value = _require_string(value, field)
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Generated state {field} is not a concrete identifier")
    return value


def _app_url(app_name: str, target: TargetConfig) -> str:
    return f"https://{app_name}-{target.workspace_id}.aws.databricksapps.com"


def _mcp_url(app_name: str, target: TargetConfig) -> str:
    return f"{_app_url(app_name, target)}/mcp"


def _require_exact_url(value: Any, field: str, expected: str) -> str:
    value = _require_string(value, field)
    parsed = urlparse(value)
    if (
        value != expected
        or parsed.scheme != "https"
        or parsed.query
        or parsed.fragment
        or parsed.params
    ):
        raise ValueError(f"Generated state {field} is not the exact target URL")
    return value


def _validate_metadata(target: TargetConfig, data: dict[str, Any]) -> None:
    for field in _OWNED_METADATA & set(data):
        if field.endswith("_url"):
            app_name = {
                "presenter_app_url": target.presenter_app_name,
                "storetime_app_url": target.storetime_app_name,
                "opstask_app_url": target.opstask_app_name,
            }[field]
            _require_exact_url(data[field], field, _app_url(app_name, target))
        else:
            _require_identifier(data[field], field)


def _validate_lakebase(target: TargetConfig, lakebase: Any) -> dict[str, Any]:
    if not isinstance(lakebase, dict) or lakebase.get("validated") is not True:
        raise ValueError("Generated Lakebase state must be a validated object")
    disabled = {"validated", "enabled", "schema"}
    enabled = disabled | {"project", "branch", "endpoint", "database", "host"}
    if lakebase.get("enabled") is False:
        if set(lakebase) != disabled or lakebase["schema"] != "gurary_bobabricks_app":
            raise ValueError("Disabled Lakebase state must name only the isolated schema")
        return lakebase
    if lakebase.get("enabled") is not True or set(lakebase) != enabled:
        raise ValueError("Enabled Lakebase state must be complete")
    for field in ("project", "branch", "endpoint", "database", "host", "schema"):
        _require_string(lakebase[field], f"lakebase.{field}")
    if lakebase["schema"] != "gurary_bobabricks_app":
        raise ValueError("Lakebase state must use the isolated schema")
    if target.key == "field_eng":
        expected = {
            "project": target.lakebase_project, "branch": target.lakebase_branch,
            "endpoint": target.lakebase_endpoint, "database": target.lakebase_database,
        }
        if any(lakebase[field] != value for field, value in expected.items()):
            raise ValueError("Field Eng Lakebase state is not target-safe")
    return lakebase


def _validate_generated_state(target: TargetConfig, data: dict[str, Any]) -> dict[str, Any]:
    allowed = {"target", "mlflow_experiment_name", "lakebase"} | _OWNED_METADATA
    if target.key == "field_eng":
        allowed |= {"warehouse_id", "genie_space_id", "storetime_mcp_url", "opstask_mcp_url"}
    unknown = set(data) - allowed
    if unknown:
        raise ValueError(f"Unknown generated-state fields: {sorted(unknown)}")
    if data.get("target") != target.key:
        raise ValueError(f"Generated state is not for target {target.key}")
    _validate_metadata(target, data)

    experiment = _require_string(data.get("mlflow_experiment_name"), "mlflow_experiment_name")
    if not experiment.startswith("/") or "gurary" not in experiment:
        raise ValueError("Generated state experiment must be an owned MLflow path")
    _validate_lakebase(target, data.get("lakebase"))

    if target.key == "field_eng":
        missing = _FIELD_REQUIRED - set(data)
        if missing:
            raise ValueError(f"Field Eng state is not render-ready: {sorted(missing)}")
        _require_identifier(data["warehouse_id"], "warehouse_id")
        _require_identifier(data["genie_space_id"], "genie_space_id")
        _require_exact_url(data["storetime_mcp_url"], "storetime_mcp_url", _mcp_url(target.storetime_app_name, target))
        _require_exact_url(data["opstask_mcp_url"], "opstask_mcp_url", _mcp_url(target.opstask_app_name, target))
    return data


def _load_generated_state(target: TargetConfig) -> dict[str, Any]:
    path = STATE_ROOT / f"{target.key}.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid generated state: {path}") from error
    if not isinstance(data, dict):
        raise ValueError(f"Expected generated-state object in {path}")
    return _validate_generated_state(target, data)


def _lakebase_environment(target: TargetConfig, state: dict[str, Any]) -> list[tuple[str, str]]:
    lakebase = state.get("lakebase")
    if lakebase is None:
        return []
    if lakebase["enabled"] is False:
        return [("BOBABRICKS_DISABLE_LAKEBASE", "1")]
    return [
        ("LAKEBASE_ENDPOINT", f"projects/{lakebase['project']}/branches/{lakebase['branch']}/endpoints/{lakebase['endpoint']}"),
        ("LAKEBASE_AUTOSCALING_PROJECT", lakebase["project"]),
        ("LAKEBASE_AUTOSCALING_BRANCH", lakebase["branch"]),
        ("LAKEBASE_HOST", lakebase["host"]),
        ("LAKEBASE_DATABASE_NAME", lakebase["database"]),
        ("LAKEBASE_MEMORY_SCHEMA", lakebase["schema"]),
        ("LAKEBASE_AGENT_MEMORY_SCHEMA", lakebase["schema"]),
    ]


def _presenter_environment(target: TargetConfig, state: DemoState, generated: dict[str, Any]) -> list[tuple[str, str]]:
    warehouse = generated.get("warehouse_id", target.warehouse_id or target.warehouse_name)
    genie = generated.get("genie_space_id", target.genie_space_id or target.genie_space_name)
    if not isinstance(warehouse, str) or not isinstance(genie, str):
        raise ValueError(f"Target {target.key} is missing a deployment identifier")
    storetime_url = generated.get("storetime_mcp_url", target.storetime_mcp_url or _mcp_url(target.storetime_app_name, target))
    opstask_url = generated.get("opstask_mcp_url", target.opstask_mcp_url or _mcp_url(target.opstask_app_name, target))
    experiment = generated.get("mlflow_experiment_name", f"/Shared/{target.presenter_app_name}-uc")
    environment = [
        ("CHAT_APP_PORT", "3000"), ("AGENT_MODEL", "databricks-gpt-5"),
        ("MLFLOW_TRACKING_URI", "databricks"), ("MLFLOW_REGISTRY_URI", "databricks-uc"),
        ("MLFLOW_EXPERIMENT_NAME", experiment), ("MLFLOW_TRACE_UC_CATALOG", target.catalog),
        ("MLFLOW_TRACE_UC_SCHEMA", target.trace_schema), ("MLFLOW_TRACE_UC_TABLE_PREFIX", "gurary_bobabricks"),
        ("GENIE_SPACE_ID", genie), ("ENABLE_GENIE_MCP", "true"), ("DISABLE_MCP_SERVERS", "false"),
        ("DATABRICKS_WAREHOUSE_ID", warehouse), ("DATABRICKS_CATALOG", target.catalog),
        ("DATABRICKS_SCHEMA", target.data_schema), ("DATABRICKS_TABLE_PREFIX", target.table_prefix),
        ("SHARED_MCP_READ_ONLY", str(target.shared_mcp_read_only).lower()),
        ("STORETIME_APP_NAME", target.storetime_app_name), ("STORETIME_MCP_URL", storetime_url),
        ("OPSTASK_APP_NAME", target.opstask_app_name), ("OPSTASK_MCP_URL", opstask_url),
    ]
    environment.extend(_lakebase_environment(target, generated))
    if state.confluence_enabled:
        environment.extend([
            ("CONFLUENCE_MCP_ENABLED", "true"),
            ("ATLASSIAN_MCP_URL", f"{target.host.rstrip('/')}/api/2.0/mcp/external/{target.confluence_connection}"),
        ])
    return environment


def _mcp_environment(target: TargetConfig, generated: dict[str, Any]) -> list[tuple[str, str]]:
    warehouse = generated.get("warehouse_id", target.warehouse_id or target.warehouse_name)
    if not isinstance(warehouse, str):
        raise ValueError(f"Target {target.key} is missing a warehouse identifier")
    return [
        ("DATABRICKS_WAREHOUSE_ID", warehouse), ("DATABRICKS_CATALOG", target.catalog),
        ("DATABRICKS_SCHEMA", target.data_schema), ("DATABRICKS_TABLE_PREFIX", target.table_prefix),
    ]


def _write_manifest(path: Path, environment: list[tuple[str, str]]) -> None:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or "command" not in manifest:
        raise ValueError(f"Invalid renderer-owned manifest: {path}")
    manifest["env"] = [{"name": name, "value": value} for name, value in environment]
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _ignored_names(directory: Path, names: list[str]) -> set[str]:
    relative = directory.relative_to(ROOT)
    ignored = {name for name in names if name in {".git", ".build", ".venv", ".superpowers", "__pycache__", ".pytest_cache"}}
    if relative == Path("deploy"):
        ignored.add("state")
    return ignored


def _copy_source(destination: Path) -> None:
    for directory, directories, files in os.walk(ROOT, followlinks=False):
        path = Path(directory)
        ignored = _ignored_names(path, directories + files)
        directories[:] = [name for name in directories if name not in ignored]
        if any(name not in ignored and (path / name).is_symlink() for name in directories + files):
            raise ValueError(f"Refusing source symlink below {path}")
    shutil.copytree(ROOT, destination, ignore=lambda directory, names: _ignored_names(Path(directory), names))


def _all_declared_identifiers() -> set[str]:
    values = set()
    for key in ("fevm", "field_eng"):
        target = load_target(key)
        values.update(
            str(value)
            for value in (
                target.presenter_app_name, target.catalog, target.data_schema, target.trace_schema,
                target.warehouse_id, target.warehouse_name, target.genie_space_id, target.genie_space_name,
                target.storetime_app_name, target.storetime_mcp_url, target.opstask_app_name,
                target.opstask_mcp_url, target.confluence_connection,
            )
            if value
        )
    return values


def _is_safe_reference(relative: Path, identifier: str) -> bool:
    if relative in _SAFE_REFERENCE_FILES or any(relative.is_relative_to(prefix) for prefix in _SAFE_REFERENCE_PREFIXES):
        return True
    return relative in _RUNTIME_REFERENCE_FILES and identifier not in _AMBER_IDENTIFIERS and identifier in _all_declared_identifiers()


def _scan_rendered_text(root: Path, target: TargetConfig) -> None:
    target_values = {
        str(value)
        for value in (
            target.presenter_app_name, target.catalog, target.data_schema, target.trace_schema,
            target.warehouse_id, target.warehouse_name, target.genie_space_id, target.genie_space_name,
            target.storetime_app_name, target.storetime_mcp_url, target.opstask_app_name,
            target.opstask_mcp_url, target.confluence_connection,
        )
        if value
    }
    foreign = _all_declared_identifiers() - target_values
    forbidden = _AMBER_IDENTIFIERS | foreign
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        content = path.read_bytes()
        if b"\0" in content:
            continue
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            continue
        relative = path.relative_to(root)
        for identifier in forbidden:
            if _is_safe_reference(relative, identifier):
                continue
            if identifier in _AMBER_IDENTIFIERS or re.search(rf"(?<![\w-]){re.escape(identifier)}(?![\w-])", text):
                if identifier in text:
                    raise ValueError(f"Unsafe rendered identifier in {relative}: {identifier}")


def _atomic_swap(first: Path, second: Path) -> None:
    if sys.platform != "darwin":
        raise RuntimeError("Atomic legacy-build migration requires macOS renamex_np")
    renamex_np = ctypes.CDLL(None, use_errno=True).renamex_np
    renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
    renamex_np.restype = ctypes.c_int
    if renamex_np(os.fsencode(first), os.fsencode(second), 0x00000002) != 0:
        raise OSError(ctypes.get_errno(), f"Could not atomically swap {first} and {second}")


def _install_pointer(destination: Path, target_root: Path, version_path: Path) -> None:
    temporary_link = target_root / f".{destination.name}-{uuid4().hex}"
    temporary_link.symlink_to(Path(".versions") / version_path.name)
    if destination.exists() and not destination.is_symlink():
        _atomic_swap(destination, temporary_link)
        os.replace(temporary_link, version_path.parent / f"legacy-{destination.name}-{uuid4().hex}")
    else:
        os.replace(temporary_link, destination)


def render_deployment(target_key: str, state_key: str) -> Path:
    """Render one target/state deployment under ``.build/{target}/{state}``."""
    target = load_target(target_key)
    state = load_state(state_key)
    generated = _load_generated_state(target)
    target_root = BUILD_ROOT / target.key
    destination = target_root / state.key
    versions_root = target_root / ".versions"
    target_root.mkdir(parents=True, exist_ok=True)
    versions_root.mkdir(exist_ok=True)
    version_name = f"{state.key}-{uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix=f".{state.key}-render-", dir=versions_root) as temporary:
        staged_root = Path(temporary) / "deployment"
        _copy_source(staged_root)
        presenter_environment = _presenter_environment(target, state, generated)
        for relative in ROOT_MANIFESTS:
            _write_manifest(staged_root / relative, presenter_environment)
        mcp_environment = _mcp_environment(target, generated)
        for relative in MCP_MANIFESTS:
            _write_manifest(staged_root / relative, mcp_environment)
        _scan_rendered_text(staged_root, target)
        version_path = versions_root / version_name
        os.replace(staged_root, version_path)
        _install_pointer(destination, target_root, version_path)
    return destination
