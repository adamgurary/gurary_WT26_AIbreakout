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
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable


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

RunCli = Callable[[str, list[str], dict | None], dict | list]
VerifyProfile = Callable[[str, str], dict]
WorkspaceIdProvider = Callable[[str], str]
ProcessRunner = Callable[[list[str]], None]
Waiter = Callable[[float], None]
WorkspaceFactory = Callable[[str], Any]
LakebaseEnsurer = Callable[[Any, TargetConfig], LakebaseState]


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
    }
    if any(getattr(target, name) != value for name, value in expected.items()):
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
    allowed = {"target", "warehouse_id", "genie_space_id", "lakebase"}
    if set(value) - allowed:
        raise RuntimeError("Field-eng generated state contains unexpected fields")
    if value.get("target") not in (None, "field_eng"):
        raise RuntimeError("Refusing to merge generated state for another target")
    return dict(value)


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
    return parser.parse_args(argv)


def main() -> None:
    arguments = _parse_arguments()
    plan = build_provision_plan()
    if arguments.dry_run:
        result = plan.as_dict()
    else:
        result = apply_provision_plan(plan).as_dict()
        reconcile_lakebase_state()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
