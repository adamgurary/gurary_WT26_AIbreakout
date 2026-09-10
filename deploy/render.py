"""Render an isolated, target-specific deployment tree without editing source."""

from __future__ import annotations

import json
import os
import re
import shutil
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
    Path("mcp-apps/storetime/app.yaml"),
    Path("mcp-apps/storetime/app.yml"),
    Path("mcp-apps/opstask/app.yaml"),
    Path("mcp-apps/opstask/app.yml"),
)

_AMBER_IDENTIFIERS = {
    "ad341da9-d12e-4688-ad1c-3c049cf70486",
    "bobabricks-store-ops-demo",
    "/Users/ad341da9-d12e-4688-ad1c-3c049cf70486/bobabricks-store-ops-demo-uc",
}
_FIELD_ENG_STATE_FIELDS = {
    "target", "warehouse_id", "genie_space_id", "storetime_mcp_url",
    "opstask_mcp_url", "mlflow_experiment_name", "lakebase",
}
_FEVM_STATE_FIELDS = {"target", "mlflow_experiment_name", "lakebase"}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
_LEGACY_TEXT_FILES = {
    Path(path)
    for path in (
        ".env.databricks.example", "AGENTS.md", "CODEX_UPGRADE_PROMPT.txt", "DATABRICKS_BUILD_PLAN.md",
        "PRESENTER_SETUP.md", "README.md", "databricks.yml", "script.md",
        "agent-store-ops/mcp_servers.yaml", "agent_server/agent.py", "app/app.py", "app/bobabricks_agent.py",
        "app/app.yaml", "archive/long-version/BOBABRICKS_DEMO_RUNBOOK.md",
        "deploy/render.py", "deploy/safety.py", "deploy/targets/fevm.yaml",
        "deploy/targets/field_eng.yaml", "mcp-apps/README.md",
        "mcp-apps/inventory/app.yaml", "mcp-apps/inventory/app.yml",
        "mcp-apps/opstask/app.py", "mcp-apps/opstask/databricks.yml",
        "mcp-apps/storetime/app.py", "mcp-apps/storetime/databricks.yml",
        "scripts/provision_databricks_assets.py", "scripts/start_app.py", "tests/deploy/test_config.py", "tests/deploy/test_render.py",
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


def _expected_mcp_url(app_name: str, target: TargetConfig) -> str:
    return f"https://{app_name}-{target.workspace_id}.aws.databricksapps.com/mcp"


def _validate_url(value: Any, field: str, expected: str) -> str:
    value = _require_string(value, field)
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.netloc != urlparse(expected).netloc or parsed.path != "/mcp":
        raise ValueError(f"Generated state {field} is not target-affine")
    return value


def _validate_lakebase(target: TargetConfig, lakebase: Any) -> dict[str, Any]:
    if not isinstance(lakebase, dict):
        raise ValueError("Generated state lakebase must be an object")
    if target.key == "fevm":
        if set(lakebase) != {"validated", "enabled", "schema"}:
            raise ValueError("FEVM Lakebase state must use the disabled-memory contract")
        if lakebase["validated"] is not True or lakebase["enabled"] is not False:
            raise ValueError("FEVM Lakebase state must be validated and disabled")
        if lakebase["schema"] != "gurary_bobabricks_app":
            raise ValueError("FEVM Lakebase state must use the isolated schema")
        return lakebase

    expected = {
        "project": target.lakebase_project,
        "branch": target.lakebase_branch,
        "endpoint": target.lakebase_endpoint,
        "database": target.lakebase_database,
        "schema": target.lakebase_schema,
    }
    if set(lakebase) != {"validated", *expected} or lakebase["validated"] is not True:
        raise ValueError("Field Eng Lakebase state must be complete and validated")
    for field, expected_value in expected.items():
        if lakebase[field] != expected_value:
            raise ValueError(f"Field Eng Lakebase {field} is not target-safe")
    return lakebase


def _validate_generated_state(target: TargetConfig, data: dict[str, Any]) -> dict[str, Any]:
    allowed = _FEVM_STATE_FIELDS if target.key == "fevm" else _FIELD_ENG_STATE_FIELDS
    unknown = set(data) - allowed
    if unknown:
        raise ValueError(f"Unknown generated-state fields: {sorted(unknown)}")
    if data.get("target") != target.key:
        raise ValueError(f"Generated state is not for target {target.key}")
    if "mlflow_experiment_name" in data:
        experiment = _require_string(data["mlflow_experiment_name"], "mlflow_experiment_name")
        if not experiment.startswith("/") or "gurary" not in experiment:
            raise ValueError("Generated state experiment must be an owned MLflow path")
    if target.key == "field_eng":
        for field in ("warehouse_id", "genie_space_id"):
            if field in data:
                _require_identifier(data[field], field)
        if "storetime_mcp_url" in data:
            _validate_url(data["storetime_mcp_url"], "storetime_mcp_url", _expected_mcp_url(target.storetime_app_name, target))
        if "opstask_mcp_url" in data:
            _validate_url(data["opstask_mcp_url"], "opstask_mcp_url", _expected_mcp_url(target.opstask_app_name, target))
    elif any(field in data for field in ("warehouse_id", "genie_space_id", "storetime_mcp_url", "opstask_mcp_url")):
        raise ValueError("FEVM shared read-only dependencies cannot be overridden")
    if "lakebase" in data:
        _validate_lakebase(target, data["lakebase"])
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


def _state_value(state: dict[str, Any], key: str, default: str | None) -> str | None:
    value = state.get(key, default)
    return str(value) if value is not None else None


def _lakebase_environment(target: TargetConfig, state: dict[str, Any]) -> list[tuple[str, str]]:
    lakebase = state.get("lakebase")
    if lakebase is None or target.key == "fevm":
        return []
    return [
        ("LAKEBASE_ENDPOINT", f"projects/{lakebase['project']}/branches/{lakebase['branch']}/endpoints/{lakebase['endpoint']}"),
        ("LAKEBASE_AUTOSCALING_PROJECT", lakebase["project"]),
        ("LAKEBASE_AUTOSCALING_BRANCH", lakebase["branch"]),
        ("LAKEBASE_DATABASE_NAME", lakebase["database"]),
        ("LAKEBASE_MEMORY_SCHEMA", lakebase["schema"]),
        ("LAKEBASE_AGENT_MEMORY_SCHEMA", lakebase["schema"]),
    ]


def _presenter_environment(target: TargetConfig, state: DemoState, generated: dict[str, Any]) -> list[tuple[str, str]]:
    warehouse = _state_value(generated, "warehouse_id", target.warehouse_id or target.warehouse_name)
    genie = _state_value(generated, "genie_space_id", target.genie_space_id or target.genie_space_name)
    if not warehouse or not genie:
        raise ValueError(f"Target {target.key} is missing a required deployment identifier")
    storetime_url = _state_value(generated, "storetime_mcp_url", target.storetime_mcp_url or _expected_mcp_url(target.storetime_app_name, target))
    opstask_url = _state_value(generated, "opstask_mcp_url", target.opstask_mcp_url or _expected_mcp_url(target.opstask_app_name, target))
    experiment = _state_value(generated, "mlflow_experiment_name", f"/Shared/{target.presenter_app_name}-uc")
    if not storetime_url or not opstask_url or not experiment:
        raise ValueError(f"Target {target.key} has incomplete generated deployment state")
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
    warehouse = _state_value(generated, "warehouse_id", target.warehouse_id or target.warehouse_name)
    if not warehouse:
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
    ignored = {
        name
        for name in names
        if name in {".git", ".build", ".venv", ".superpowers", "__pycache__", ".pytest_cache"}
    }
    if relative == Path("deploy"):
        ignored.add("state")
    return ignored


def _assert_source_has_no_symlinks() -> None:
    for directory, directories, files in os.walk(ROOT, followlinks=False):
        path = Path(directory)
        ignored = _ignored_names(path, directories + files)
        directories[:] = [name for name in directories if name not in ignored]
        for name in directories + files:
            if name not in ignored and (path / name).is_symlink():
                raise ValueError(f"Refusing source symlink: {path / name}")


def _copy_source(destination: Path) -> None:
    _assert_source_has_no_symlinks()
    shutil.copytree(ROOT, destination, ignore=lambda directory, names: _ignored_names(Path(directory), names))


def _replace_exact(text: str, original: str, replacement: str) -> str:
    return re.sub(rf"(?<![\w-]){re.escape(original)}(?![\w-])", replacement, text)


def _render_legacy_text(root: Path, target: TargetConfig) -> None:
    replacements = {identifier: target.presenter_app_name for identifier in _AMBER_IDENTIFIERS}
    for candidate_key in ("fevm", "field_eng"):
        candidate = load_target(candidate_key)
        if candidate.key == target.key:
            continue
        replacements.update({
            candidate.catalog: target.catalog,
            candidate.data_schema: target.data_schema,
            candidate.trace_schema: target.trace_schema,
            str(candidate.warehouse_id or candidate.warehouse_name): str(target.warehouse_id or target.warehouse_name),
            str(candidate.genie_space_id or candidate.genie_space_name): str(target.genie_space_id or target.genie_space_name),
            candidate.storetime_app_name: target.storetime_app_name,
            candidate.storetime_mcp_url or _expected_mcp_url(candidate.storetime_app_name, candidate): target.storetime_mcp_url or _expected_mcp_url(target.storetime_app_name, target),
            candidate.opstask_app_name: target.opstask_app_name,
            candidate.opstask_mcp_url or _expected_mcp_url(candidate.opstask_app_name, candidate): target.opstask_mcp_url or _expected_mcp_url(target.opstask_app_name, target),
        })
    for relative in _LEGACY_TEXT_FILES:
        path = root / relative
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for original in sorted(replacements, key=len, reverse=True):
            replacement = replacements[original]
            if original:
                text = _replace_exact(text, original, replacement)
        path.write_text(text, encoding="utf-8")


def _forbidden_identifiers(target: TargetConfig) -> set[str]:
    identifiers = set(_AMBER_IDENTIFIERS)
    for candidate_key in ("fevm", "field_eng"):
        candidate = load_target(candidate_key)
        for value in (candidate.catalog, candidate.data_schema, candidate.trace_schema, candidate.warehouse_id,
                      candidate.warehouse_name, candidate.genie_space_id, candidate.genie_space_name,
                      candidate.storetime_app_name, candidate.storetime_mcp_url, candidate.opstask_app_name,
                      candidate.opstask_mcp_url):
            if value and value not in (target.catalog, target.data_schema, target.trace_schema, target.warehouse_id,
                                       target.warehouse_name, target.genie_space_id, target.genie_space_name,
                                       target.storetime_app_name, target.storetime_mcp_url, target.opstask_app_name,
                                       target.opstask_mcp_url):
                identifiers.add(str(value))
    return identifiers


def _scan_rendered_text(root: Path, target: TargetConfig) -> None:
    forbidden = _forbidden_identifiers(target)
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
        for identifier in forbidden:
            unsafe = identifier in text if identifier in _AMBER_IDENTIFIERS else bool(
                re.search(rf"(?<![\w-]){re.escape(identifier)}(?![\w-])", text)
            )
            if unsafe:
                raise ValueError(f"Unsafe rendered identifier in {path.relative_to(root)}: {identifier}")


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
    if destination.exists() and not destination.is_symlink():
        raise ValueError(f"Refusing to replace legacy non-atomic build path: {destination}")

    version_name = f"{state.key}-{uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix=f".{state.key}-render-", dir=versions_root) as temporary:
        staged_root = Path(temporary) / "deployment"
        _copy_source(staged_root)
        presenter_environment = _presenter_environment(target, state, generated)
        for relative in ROOT_MANIFESTS:
            _write_manifest(staged_root / relative, presenter_environment)
        for relative in MCP_MANIFESTS:
            _write_manifest(staged_root / relative, _mcp_environment(target, generated))
        _render_legacy_text(staged_root, target)
        _scan_rendered_text(staged_root, target)

        version_path = versions_root / version_name
        os.replace(staged_root, version_path)
        temporary_link = target_root / f".{state.key}-{uuid4().hex}"
        temporary_link.symlink_to(Path(".versions") / version_name)
        os.replace(temporary_link, destination)
    return destination
