"""Clone the Bobabricks Genie definition into the private field-eng namespace."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any

from .config import TargetConfig, load_target
from .databricks_cli import REQUIRED_USER, assert_profile, run_json
from .safety import assert_safe_mutation


SOURCE_CATALOG = "worldtour_ai_" "catalog"
SOURCE_SCHEMA = "bobabricks_store_" "ops"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}"
SOURCE_WAREHOUSE_ID = "88fd32ee6d94" "38ac"
SOURCE_TABLE_NAMES = (
    "stores",
    "training_courses",
    "employees",
    "training_progress",
    "store_weekly_metrics",
    "schedules",
    "training_events",
    "storetime_shifts",
    "inventory",
    "customer_feedback",
    "ops_tasks",
    "store_metrics",
)
FIELD_PROFILE = "e2-demo-field-eng"
FIELD_HOST = "https://e2-demo-field-eng.cloud.databricks.com"
FIELD_WORKSPACE_ID = "1444828305810485"
FIELD_WAREHOUSE_NAME = "gurary_bobabricks_" "warehouse"
FIELD_SPACE_NAME = "gurary_bobabricks_store_" "operations"
FIELD_PARENT_PATH = "/Users/adam.gurary@databricks.com"
FIELD_STATE_KEYS = {"target", "warehouse_id"}
ACCEPTANCE_STATE_KEYS = {"target", "warehouse_id", "genie_space_id"}
ACCEPTANCE_QUESTIONS = (
    "How many scheduled, completed, and converted training hours does the Pacific region have for week 2026-W27?",
    "What is Store 104's latest training completion percentage?",
)
_SQL_RELATION = re.compile(
    r"\b(?:FROM|JOIN)\s+((?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)){0,2})",
    re.IGNORECASE,
)
_SQL_FROM_CLAUSE = re.compile(
    r"\bFROM\b(?P<body>.*?)(?=\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|"
    r"\bORDER\s+BY\b|\bLIMIT\b|\bUNION\b|;|$)",
    re.IGNORECASE | re.DOTALL,
)
_SQL_COMMA_RELATION = re.compile(
    r",(?:\s|/\*.*?\*/|--[^\r\n]*(?:\r?\n|$))*"
    r"((?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)){0,2})",
    re.IGNORECASE | re.DOTALL,
)
_SQL_QUALIFIED_IDENTIFIER = re.compile(
    r"(?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)){2}",
    re.IGNORECASE,
)


def _table_mapping(target: TargetConfig) -> dict[str, str]:
    target_namespace = f"{target.catalog}.{target.data_schema}"
    return {
        f"{SOURCE_NAMESPACE}.{table}": f"{target_namespace}.{target.table_prefix}{table}"
        for table in SOURCE_TABLE_NAMES
    }


def _rewrite_values(value: Any, mapping: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: _rewrite_values(child, mapping) for key, child in value.items()}
    if isinstance(value, list):
        return [_rewrite_values(child, mapping) for child in value]
    if isinstance(value, str):
        return mapping.get(value, value)
    return value


def rewrite_serialized_space(serialized: str, target: TargetConfig) -> str:
    """Rewrite only exact source table values and validate the private result."""
    parsed = json.loads(serialized)
    rewritten = _rewrite_values(parsed, _table_mapping(target))
    expected_prefix = f"{target.catalog}.{target.data_schema}.{target.table_prefix}"
    data_sources = rewritten.get("data_sources") if isinstance(rewritten, dict) else None
    tables = data_sources.get("tables") if isinstance(data_sources, dict) else None
    if not isinstance(tables, list) or len(tables) != len(SOURCE_TABLE_NAMES):
        raise ValueError("Serialized Genie space must contain all 12 table data sources")
    identifiers = [table.get("identifier") if isinstance(table, dict) else None for table in tables]
    expected_identifiers = set(_table_mapping(target).values())
    if set(identifiers) != expected_identifiers or len(identifiers) != len(expected_identifiers):
        raise ValueError("Serialized Genie space must contain the exact private tables")
    if any(not isinstance(identifier, str) or not identifier.startswith(expected_prefix) for identifier in identifiers):
        raise ValueError("Serialized Genie space contains a non-private table data source")
    result = json.dumps(rewritten, separators=(",", ":"), sort_keys=True)
    protected = (SOURCE_CATALOG, SOURCE_SCHEMA, SOURCE_WAREHOUSE_ID)
    if any(
        re.search(rf"(?<![A-Za-z0-9_]){re.escape(identifier)}(?![A-Za-z0-9_])", result)
        for identifier in protected
    ):
        raise ValueError("Serialized Genie space retains a protected source reference")
    return result


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as temporary:
        temporary.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def export_source(
    *,
    profile: str,
    space_id: str,
    output: Path,
    run_cli=run_json,
) -> dict[str, Any]:
    """Read a source Genie export and persist only its definition and title."""
    response = run_cli(
        profile,
        ["api", "get", f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true"],
    )
    if not isinstance(response, dict):
        raise RuntimeError("Source Genie response is malformed")
    title = response.get("title")
    serialized = response.get("serialized_space")
    if not isinstance(title, str) or not title or not isinstance(serialized, str):
        raise RuntimeError("Source Genie export is incomplete")
    parsed = json.loads(serialized)
    data_sources = parsed.get("data_sources") if isinstance(parsed, dict) else None
    tables = data_sources.get("tables") if isinstance(data_sources, dict) else None
    identifiers = [table.get("identifier") if isinstance(table, dict) else None for table in tables or []]
    expected = {f"{SOURCE_NAMESPACE}.{table}" for table in SOURCE_TABLE_NAMES}
    if set(identifiers) != expected or len(identifiers) != len(expected):
        raise RuntimeError("Source Genie export does not contain the exact expected tables")
    saved = {"serialized_space": serialized, "title": title}
    _write_json_atomic(output, saved)
    return saved


def _workspace_id_from_sdk(profile: str) -> str:
    from databricks.sdk import WorkspaceClient

    return str(WorkspaceClient(profile=profile).get_workspace_id())


def _require_field_target(target: TargetConfig) -> None:
    expected = {
        "key": "field_eng",
        "profile": FIELD_PROFILE,
        "host": FIELD_HOST,
        "workspace_id": FIELD_WORKSPACE_ID,
        "catalog": "gurary_" "catalog",
        "data_schema": "gurary_bobabricks_store_" "ops",
        "table_prefix": "gurary_",
        "warehouse_name": FIELD_WAREHOUSE_NAME,
        "genie_space_name": FIELD_SPACE_NAME,
    }
    if any(getattr(target, key) != value for key, value in expected.items()):
        raise RuntimeError("Field-eng target configuration does not match the Genie safety contract")


def _assert_fresh_binding(target, verify_profile, workspace_id_provider) -> None:
    identity = verify_profile(target.profile, target.host)
    current_user = identity.get("current_user") if isinstance(identity, dict) else None
    if not isinstance(current_user, dict):
        raise RuntimeError("Databricks current-user identity is malformed")
    user_name = current_user.get("user_name", current_user.get("userName"))
    if user_name != REQUIRED_USER:
        raise RuntimeError("Databricks current user does not match the Genie owner")
    if str(workspace_id_provider(target.profile)) != target.workspace_id:
        raise RuntimeError("Databricks workspace ID does not match the field-eng target")


def _space_records(response: dict | list) -> list[dict[str, Any]]:
    records = response if isinstance(response, list) else response.get("spaces", [])
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise RuntimeError("Databricks Genie list response is malformed")
    return [dict(record) for record in records]


def _private_identifiers(serialized: str, target: TargetConfig) -> set[str]:
    parsed = json.loads(serialized)
    data_sources = parsed.get("data_sources") if isinstance(parsed, dict) else None
    tables = data_sources.get("tables") if isinstance(data_sources, dict) else None
    identifiers = {
        table.get("identifier")
        for table in tables or []
        if isinstance(table, dict) and isinstance(table.get("identifier"), str)
    }
    expected = set(_table_mapping(target).values())
    if identifiers != expected or len(tables or []) != len(expected):
        raise RuntimeError("Target Genie does not contain the exact private table sources")
    return identifiers


def _inspect_target_space(
    *,
    target: TargetConfig,
    warehouse_id: str,
    run_cli,
    verify_profile,
    workspace_id_provider,
) -> dict[str, Any] | None:
    """Refresh identity, workspace, warehouse, exact title, and current space state."""
    _require_field_target(target)
    _assert_fresh_binding(target, verify_profile, workspace_id_provider)
    assert_safe_mutation(target, "warehouse", FIELD_WAREHOUSE_NAME)
    if warehouse_id == SOURCE_WAREHOUSE_ID:
        raise RuntimeError("Refusing the protected shared warehouse")
    warehouse = run_cli(target.profile, ["warehouses", "get", warehouse_id])
    if (
        not isinstance(warehouse, dict)
        or warehouse.get("id") != warehouse_id
        or warehouse.get("name") != FIELD_WAREHOUSE_NAME
    ):
        raise RuntimeError("Private warehouse readback did not match the exact owned resource")
    assert_safe_mutation(target, "genie", FIELD_SPACE_NAME)
    matches = [
        record
        for record in _space_records(run_cli(target.profile, ["genie", "list-spaces"]))
        if record.get("title") == FIELD_SPACE_NAME
    ]
    if len(matches) > 1:
        raise RuntimeError("Databricks returned duplicate exact owned Genie spaces")
    if not matches:
        return None
    space_id = matches[0].get("space_id")
    if not isinstance(space_id, str) or not space_id:
        raise RuntimeError("Owned Genie space has no concrete ID")
    current = run_cli(
        target.profile,
        ["api", "get", f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true"],
    )
    if (
        not isinstance(current, dict)
        or current.get("space_id") != space_id
        or current.get("title") != FIELD_SPACE_NAME
        or current.get("warehouse_id") != warehouse_id
    ):
        raise RuntimeError("Owned Genie readback did not match its exact identity")
    serialized = current.get("serialized_space")
    if not isinstance(serialized, str):
        raise RuntimeError("Owned Genie readback omitted its serialized definition")
    _private_identifiers(serialized, target)
    return dict(current)


def _default_update_space(
    profile: str, space_id: str, payload: dict[str, Any], etag: str
) -> dict[str, Any]:
    """Use the SDK transport so the optimistic-concurrency ETag is sent as a header."""
    from databricks.sdk import WorkspaceClient

    response = WorkspaceClient(profile=profile).api_client.do(
        "PATCH",
        f"/api/2.0/genie/spaces/{space_id}",
        body=payload,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "If-Match": etag,
        },
    )
    if not isinstance(response, dict):
        raise RuntimeError("Databricks Genie update response is malformed")
    return response


def clone_target(
    *,
    source_path: Path,
    state_path: Path,
    run_cli=run_json,
    verify_profile=assert_profile,
    workspace_id_provider=_workspace_id_from_sdk,
    update_space=_default_update_space,
    waiter=time.sleep,
) -> dict[str, Any]:
    """Create/update only the exact private Genie and grant its owner CAN_MANAGE."""
    target = load_target("field_eng")
    _require_field_target(target)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    valid_state_keys = {frozenset(FIELD_STATE_KEYS), frozenset(ACCEPTANCE_STATE_KEYS)}
    if (
        not isinstance(state, dict)
        or frozenset(state) not in valid_state_keys
        or state.get("target") != target.key
    ):
        raise RuntimeError("Generated field-eng state is not an expected Task 7/8 state")
    warehouse_id = state.get("warehouse_id")
    if not isinstance(warehouse_id, str) or not warehouse_id or warehouse_id == SOURCE_WAREHOUSE_ID:
        raise RuntimeError("Generated field-eng state has no safe private warehouse ID")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    serialized = source.get("serialized_space") if isinstance(source, dict) else None
    if not isinstance(serialized, str):
        raise RuntimeError("Sanitized source Genie export is incomplete")
    rewritten = rewrite_serialized_space(serialized, target)
    payload = {
        "parent_path": FIELD_PARENT_PATH,
        "serialized_space": rewritten,
        "title": FIELD_SPACE_NAME,
        "warehouse_id": warehouse_id,
    }

    current = _inspect_target_space(
        target=target,
        warehouse_id=warehouse_id,
        run_cli=run_cli,
        verify_profile=verify_profile,
        workspace_id_provider=workspace_id_provider,
    )
    saved_space_id = state.get("genie_space_id")
    if current is None and saved_space_id is not None:
        raise RuntimeError("Saved Genie space ID is missing from the exact live title")
    if current is not None and saved_space_id not in {None, current.get("space_id")}:
        raise RuntimeError("Saved Genie space ID does not match the exact live title")
    if current is None:
        response = run_cli(target.profile, ["api", "post", "/api/2.0/genie/spaces"], payload)
        action = "created"
    else:
        etag = current.get("etag")
        space_id = current.get("space_id")
        if not isinstance(etag, str) or not etag:
            raise RuntimeError("Owned Genie update requires its live ETag")
        response = update_space(target.profile, space_id, payload, etag)
        action = "updated"
    if not isinstance(response, dict):
        raise RuntimeError("Databricks Genie mutation response is malformed")
    space_id = response.get("space_id")
    if not isinstance(space_id, str) or not space_id:
        raise RuntimeError("Databricks Genie mutation returned no concrete ID")

    current = None
    for attempt in range(10):
        current = _inspect_target_space(
            target=target,
            warehouse_id=warehouse_id,
            run_cli=run_cli,
            verify_profile=verify_profile,
            workspace_id_provider=workspace_id_provider,
        )
        if current is not None:
            break
        if attempt < 9:
            waiter(1.0)
    if current is None or current.get("space_id") != space_id:
        raise RuntimeError("Databricks Genie post-mutation readback changed identity")
    permissions = run_cli(target.profile, ["permissions", "get", "genie", space_id])
    if not isinstance(permissions, dict):
        raise RuntimeError("Databricks Genie permissions response is malformed")
    permission_payload = {
        "access_control_list": [
            {"user_name": REQUIRED_USER, "permission_level": "CAN_MANAGE"}
        ]
    }
    permission_result = run_cli(
        target.profile,
        ["permissions", "update", "genie", space_id],
        permission_payload,
    )
    if not isinstance(permission_result, dict):
        raise RuntimeError("Databricks Genie permission update response is malformed")
    access_control = permission_result.get("access_control_list")
    if not isinstance(access_control, list):
        raise RuntimeError("Databricks Genie permission readback omitted owner CAN_MANAGE")
    owner_can_manage = False
    for entry in access_control:
        if not isinstance(entry, dict) or entry.get("user_name") != REQUIRED_USER:
            continue
        levels = {
            permission.get("permission_level")
            for permission in entry.get("all_permissions", [])
            if isinstance(permission, dict)
        }
        if entry.get("permission_level") == "CAN_MANAGE" or "CAN_MANAGE" in levels:
            owner_can_manage = True
    if not owner_can_manage:
        raise RuntimeError("Databricks Genie permission readback omitted owner CAN_MANAGE")

    _write_json_atomic(
        state_path,
        {**state, "genie_space_id": space_id},
    )
    return {
        "action": action,
        "space_id": space_id,
        "title": FIELD_SPACE_NAME,
        "warehouse_id": warehouse_id,
        "private_table_count": len(_private_identifiers(rewritten, target)),
        "permission": "CAN_MANAGE",
    }


def _query_evidence(response: dict | list, target: TargetConfig) -> dict[str, Any]:
    if not isinstance(response, dict) or response.get("status") != "COMPLETED":
        raise RuntimeError("Genie acceptance message did not complete")
    attachments = response.get("attachments")
    if not isinstance(attachments, list):
        raise RuntimeError("Genie acceptance message omitted attachments")
    query_attachments = [
        attachment.get("query")
        for attachment in attachments
        if isinstance(attachment, dict) and isinstance(attachment.get("query"), dict)
    ]
    sql_values = [query.get("query") for query in query_attachments if isinstance(query.get("query"), str)]
    if not sql_values:
        raise RuntimeError("Genie acceptance message omitted SQL evidence")
    references: list[str] = []
    expected_prefix = f"{target.catalog}.{target.data_schema}.{target.table_prefix}"
    allowed = set(_table_mapping(target).values())
    for sql in sql_values:
        qualified = [
            re.sub(r"[`\s]", "", match.group(0))
            for match in _SQL_QUALIFIED_IDENTIFIER.finditer(sql)
        ]
        if any(identifier not in allowed for identifier in qualified):
            raise RuntimeError("Genie acceptance SQL referenced a non-private table")
        relation_values = [match.group(1) for match in _SQL_RELATION.finditer(sql)]
        relation_values.extend(
            match.group(1)
            for clause in _SQL_FROM_CLAUSE.finditer(sql)
            for match in _SQL_COMMA_RELATION.finditer(clause.group("body"))
        )
        for value in relation_values:
            reference = re.sub(r"[`\s]", "", value)
            if not reference.startswith(expected_prefix) or reference not in allowed:
                raise RuntimeError("Genie acceptance SQL referenced a non-private table")
        references.extend(qualified)
    if not references:
        raise RuntimeError("Genie acceptance SQL contained no table-reference evidence")
    row_counts = [
        metadata.get("row_count")
        for query in query_attachments
        for metadata in [query.get("query_result_metadata")]
        if isinstance(metadata, dict) and isinstance(metadata.get("row_count"), int)
    ]
    return {
        "status": "COMPLETED",
        "sql": sql_values,
        "table_references": references,
        "result_row_counts": row_counts,
    }


def run_acceptance(
    *,
    state_path: Path,
    run_cli=run_json,
    verify_profile=assert_profile,
    workspace_id_provider=_workspace_id_from_sdk,
) -> list[dict[str, Any]]:
    """Ask both acceptance questions and require exact private table evidence."""
    target = load_target("field_eng")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    if (
        not isinstance(state, dict)
        or set(state) != ACCEPTANCE_STATE_KEYS
        or state.get("target") != target.key
    ):
        raise RuntimeError("Generated field-eng state is incomplete for Genie acceptance")
    warehouse_id = state.get("warehouse_id")
    space_id = state.get("genie_space_id")
    if not isinstance(warehouse_id, str) or not isinstance(space_id, str):
        raise RuntimeError("Generated field-eng state has invalid concrete IDs")
    evidence = []
    for question in ACCEPTANCE_QUESTIONS:
        current = _inspect_target_space(
            target=target,
            warehouse_id=warehouse_id,
            run_cli=run_cli,
            verify_profile=verify_profile,
            workspace_id_provider=workspace_id_provider,
        )
        if current is None or current.get("space_id") != space_id:
            raise RuntimeError("Genie acceptance target changed identity")
        response = run_cli(
            target.profile,
            ["genie", "start-conversation", space_id, question],
        )
        evidence.append({"question": question, **_query_evidence(response, target)})
    return evidence


def _parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export-source")
    export.add_argument("--profile", required=True)
    export.add_argument("--space-id", required=True)
    export.add_argument("--output", required=True, type=Path)
    clone = commands.add_parser("clone-target")
    clone.add_argument(
        "--source",
        type=Path,
        default=Path("deploy/inventory/source-genie-space.json"),
    )
    clone.add_argument(
        "--state",
        type=Path,
        default=Path("deploy/state/field_eng.json"),
    )
    acceptance = commands.add_parser("acceptance")
    acceptance.add_argument(
        "--state",
        type=Path,
        default=Path("deploy/state/field_eng.json"),
    )
    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
    *,
    run_cli=run_json,
    verify_profile=assert_profile,
    workspace_id_provider=_workspace_id_from_sdk,
    update_space=_default_update_space,
) -> None:
    arguments = _parse_arguments(argv)
    if arguments.command == "export-source":
        result = export_source(
            profile=arguments.profile,
            space_id=arguments.space_id,
            output=arguments.output,
            run_cli=run_cli,
        )
        output = {
            "output": str(arguments.output),
            "serialized_space_chars": len(result["serialized_space"]),
            "title": result["title"],
        }
    elif arguments.command == "clone-target":
        output = clone_target(
            source_path=arguments.source,
            state_path=arguments.state,
            run_cli=run_cli,
            verify_profile=verify_profile,
            workspace_id_provider=workspace_id_provider,
            update_space=update_space,
        )
    else:
        output = run_acceptance(
            state_path=arguments.state,
            run_cli=run_cli,
            verify_profile=verify_profile,
            workspace_id_provider=workspace_id_provider,
        )
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
