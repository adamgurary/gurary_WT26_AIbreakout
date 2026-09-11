#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import random
import re
import sys
import time
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.config import load_target
from deploy.databricks_cli import assert_profile, run_json


IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
TABLE_REFERENCE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\b"
)
FIELD_PROFILE = "e2-demo-field-eng"
FIELD_WORKSPACE_ID = "1444828305810485"
FIELD_WAREHOUSE_NAME = "gurary_bobabricks_" "warehouse"
FIELD_CATALOG = "gurary_" "catalog"
FIELD_SCHEMA = "gurary_bobabricks_store_" "ops"
FIELD_PREFIX = "gurary_"
TABLE_DDL = {
    "store_metrics": """region STRING, store_id STRING, store_name STRING, sales_vs_plan_pct DOUBLE, training_completion_pct DOUBLE, avg_wait_minutes DOUBLE, customer_satisfaction DOUBLE""",
    "stores": """store_id INT, store_name STRING, region STRING, district STRING, city STRING, state STRING, area_leader STRING, opened_date DATE, seat_count INT, weekly_sales_target DOUBLE""",
    "training_courses": """course_id STRING, course_name STRING, category STRING, required_for_roles STRING, target_hours DOUBLE, recurrence STRING, is_mandatory BOOLEAN""",
    "employees": """employee_id STRING, store_id INT, full_name STRING, role STRING, hire_date DATE, status STRING, employment_type STRING""",
    "training_progress": """record_id STRING, store_id INT, employee_id STRING, course_id STRING, status STRING, assigned_date DATE, due_date DATE, completed_date DATE, completed_hours DOUBLE, target_hours DOUBLE""",
    "store_weekly_metrics": """store_id INT, week_start_date DATE, region STRING, net_sales DOUBLE, weekly_sales_target DOUBLE, sales_vs_plan_pct DOUBLE, training_completion_pct DOUBLE, avg_wait_minutes DOUBLE, customer_satisfaction DOUBLE, scheduled_training_hours DOUBLE, actual_training_hours DOUBLE, converted_training_hours DOUBLE, conversion_reason STRING""",
    "schedules": """store_id INT, week STRING, scheduled_training_hours DOUBLE, completed_training_hours DOUBLE, converted_training_hours DOUBLE, conversion_reason STRING""",
    "training_events": """store_id INT, week STRING, scheduled_training_hours DOUBLE, completed_training_hours DOUBLE, converted_training_hours DOUBLE, conversion_reason STRING""",
    "storetime_shifts": """shift_id STRING, store_id INT, shift_date DATE, daypart STRING, scheduled_partners INT, actual_partners INT, coverage_area STRING""",
    "inventory": """store_id INT, ingredient STRING, on_hand_units DOUBLE, forecast_7d_units DOUBLE, reorder_eta_days DOUBLE, stockout_risk STRING, supplier STRING""",
    "customer_feedback": """feedback_id STRING, store_id INT, feedback_date DATE, topic STRING, rating INT, comment_summary STRING""",
    "ops_tasks": """task_id STRING, store_id INT, title STRING, description STRING, category STRING, severity STRING, status STRING, created_date DATE, owner STRING""",
}
FIELD_TABLES = frozenset(
    f"{FIELD_CATALOG}.{FIELD_SCHEMA}.{FIELD_PREFIX}{table}" for table in TABLE_DDL
)


def qualified_table(namespace: str, prefix: str, table: str) -> str:
    namespace_parts = namespace.split(".")
    if len(namespace_parts) != 2 or any(
        not IDENTIFIER.fullmatch(part) for part in namespace_parts
    ):
        raise ValueError(f"Invalid table namespace: {namespace!r}")
    if prefix and not IDENTIFIER.fullmatch(prefix):
        raise ValueError(f"Invalid table prefix: {prefix!r}")
    if not IDENTIFIER.fullmatch(table):
        raise ValueError(f"Invalid table name: {table!r}")
    qualified_name = f"{prefix}{table}"
    if not IDENTIFIER.fullmatch(qualified_name):
        raise ValueError(f"Invalid qualified table name: {qualified_name!r}")
    return f"{namespace}.{qualified_name}"


def run_databricks(profile: str, args: list[str], payload: dict | None = None) -> dict:
    response = run_json(profile, args, payload)
    if not isinstance(response, dict):
        raise RuntimeError("Databricks CLI response must be an object")
    return response


def _workspace_id_from_sdk(profile: str) -> str:
    from databricks.sdk import WorkspaceClient

    return str(WorkspaceClient(profile=profile).get_workspace_id())


def _records(response: dict | list, key: str) -> list[dict]:
    value = response if isinstance(response, list) else response.get(key, [])
    if not isinstance(value, list) or not all(isinstance(record, dict) for record in value):
        raise RuntimeError(f"Databricks {key} response is malformed")
    return [dict(record) for record in value]


def _guard_private_sql_mutation(profile: str, warehouse_id: str, statement: str) -> None:
    if profile != FIELD_PROFILE:
        return
    references = TABLE_REFERENCE.findall(statement)
    if not references or references[0] not in FIELD_TABLES:
        raise ValueError("Refusing mutation of a non-owned private table")
    if any(reference not in FIELD_TABLES for reference in references):
        raise ValueError("Refusing SQL that references a non-owned private table")

    target = load_target("field_eng")
    expected_contract = (
        target.profile == FIELD_PROFILE
        and target.workspace_id == FIELD_WORKSPACE_ID
        and target.catalog == FIELD_CATALOG
        and target.data_schema == FIELD_SCHEMA
        and target.table_prefix == FIELD_PREFIX
        and target.warehouse_name == FIELD_WAREHOUSE_NAME
    )
    if not expected_contract:
        raise RuntimeError("Field-eng target configuration does not match the SQL safety contract")
    assert_profile(target.profile, target.host)
    if _workspace_id_from_sdk(target.profile) != target.workspace_id:
        raise RuntimeError("Databricks workspace ID does not match the field-eng target")

    warehouse = run_json(target.profile, ["warehouses", "get", warehouse_id])
    if (
        not isinstance(warehouse, dict)
        or str(warehouse.get("id")) != warehouse_id
        or warehouse.get("name") != FIELD_WAREHOUSE_NAME
    ):
        raise RuntimeError("SQL warehouse does not match the exact owned warehouse")
    schema_name = f"{FIELD_CATALOG}.{FIELD_SCHEMA}"
    schema = run_json(target.profile, ["schemas", "get", schema_name])
    if not isinstance(schema, dict) or schema.get("full_name") != schema_name:
        raise RuntimeError("SQL schema does not match the exact owned schema")
    _records(
        run_json(target.profile, ["tables", "list", FIELD_CATALOG, FIELD_SCHEMA]),
        "tables",
    )


def execute_sql(profile: str, warehouse_id: str, statement: str) -> dict:
    _guard_private_sql_mutation(profile, warehouse_id, statement)
    response = run_databricks(
        profile,
        ["api", "post", "/api/2.0/sql/statements"],
        {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s", "on_wait_timeout": "CONTINUE"},
    )
    statement_id = response.get("statement_id")
    state = response.get("status", {}).get("state")
    while statement_id and state in {"PENDING", "RUNNING"}:
        time.sleep(2)
        response = run_databricks(profile, ["api", "get", f"/api/2.0/sql/statements/{statement_id}"])
        state = response.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL failed with state {state}: {response}")
    return response


def sql_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dt.date):
        return f"DATE'{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


def insert_rows(profile: str, warehouse_id: str, table: str, columns: list[str], rows: Iterable[tuple], batch_size: int = 200) -> None:
    rows = list(rows)
    if not rows:
        return
    for index in range(0, len(rows), batch_size):
        chunk = rows[index : index + batch_size]
        values = ",\n".join("(" + ", ".join(sql_value(value) for value in row) + ")" for row in chunk)
        execute_sql(profile, warehouse_id, f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n{values}")
    print(f"inserted {len(rows):>4} rows into {table}")


def build_seed_data() -> dict[str, tuple[list[str], list[tuple]]]:
    random.seed(20260708)
    base_week = dt.date(2026, 6, 29)
    stores = [
        (101, "Seattle Capitol Hill", "Pacific", "PNW North", "Seattle", "WA", "Maria Chen", dt.date(2020, 5, 12), 64, 88000),
        (102, "Bellevue Commons", "Pacific", "PNW North", "Bellevue", "WA", "Maria Chen", dt.date(2021, 3, 18), 58, 81000),
        (103, "Tacoma Narrows", "Pacific", "PNW South", "Tacoma", "WA", "Maria Chen", dt.date(2019, 9, 4), 52, 72000),
        (104, "Seattle Pike Place", "Pacific", "PNW North", "Seattle", "WA", "Maria Chen", dt.date(2018, 4, 20), 72, 95000),
        (117, "Portland Pearl", "Pacific", "PNW South", "Portland", "OR", "Maria Chen", dt.date(2022, 7, 11), 60, 84000),
        (122, "San Francisco Market", "Pacific", "Bay Area", "San Francisco", "CA", "Maria Chen", dt.date(2021, 11, 2), 68, 102000),
        (130, "San Diego Harbor", "Pacific", "SoCal", "San Diego", "CA", "Maria Chen", dt.date(2020, 2, 28), 62, 90000),
        (201, "Denver Union", "Mountain", "Front Range", "Denver", "CO", "Noah Patel", dt.date(2019, 8, 15), 56, 76000),
        (205, "Boulder Pearl", "Mountain", "Front Range", "Boulder", "CO", "Noah Patel", dt.date(2022, 1, 9), 46, 61000),
        (214, "Salt Lake Central", "Mountain", "Wasatch", "Salt Lake City", "UT", "Noah Patel", dt.date(2021, 5, 17), 54, 69000),
        (301, "Boston Seaport", "Atlantic", "New England", "Boston", "MA", "Priya Morgan", dt.date(2018, 10, 3), 66, 92000),
        (309, "Brooklyn Heights", "Atlantic", "NY Metro", "Brooklyn", "NY", "Priya Morgan", dt.date(2020, 6, 22), 58, 87000),
        (318, "Philadelphia Rittenhouse", "Atlantic", "Mid-Atlantic", "Philadelphia", "PA", "Priya Morgan", dt.date(2021, 2, 14), 55, 74000),
        (327, "Washington Union", "Atlantic", "Mid-Atlantic", "Washington", "DC", "Priya Morgan", dt.date(2019, 12, 8), 61, 86000),
    ]
    store_by_id = {store[0]: store for store in stores}

    courses = [
        ("BB-FOOD-101", "Food Safety Foundations", "Food Safety", "Team Member,Shift Lead,Assistant Manager,General Manager", 2.0, "Annual", True),
        ("BB-SVC-201", "Rush Window Service Flow", "Service", "Team Member,Shift Lead", 1.5, "Quarterly", True),
        ("BB-INV-210", "Inventory Rotation and Transfer Playbook", "Inventory", "Shift Lead,Assistant Manager,General Manager", 1.0, "Quarterly", True),
        ("BB-LDR-301", "Manager Escalation Standards", "Leadership", "Assistant Manager,General Manager", 1.5, "Annual", True),
        ("BB-NEW-001", "Bobabricks Barista Onboarding", "Onboarding", "Team Member", 3.0, "Onboarding", True),
    ]

    employees = []
    progress = []
    roles = ["Team Member", "Team Member", "Team Member", "Shift Lead", "Assistant Manager", "General Manager"]
    employee_number = 1000
    for store_id, *_ in stores:
        for offset in range(12):
            employee_number += 1
            role = roles[offset % len(roles)]
            employee_id = f"E{employee_number}"
            employees.append((employee_id, store_id, f"Partner {employee_number}", role, dt.date(2024 + offset % 3, 1 + offset % 11, 3 + offset % 20), "Active", "Full-Time" if offset % 3 else "Part-Time"))
            for course_id, _, _, required_roles, target_hours, _, _ in courses:
                if role not in required_roles.split(","):
                    continue
                engineered_gap = store_id == 104 and course_id in {"BB-SVC-201", "BB-INV-210"} and offset < 8
                status = "In Progress" if engineered_gap else ("Completed" if random.random() > 0.13 else "Not Started")
                completed_date = None if status != "Completed" else base_week - dt.timedelta(days=random.randint(3, 45))
                completed_hours = 0 if status == "Not Started" else (target_hours * (0.55 if status == "In Progress" else 1.0))
                progress.append((f"TP-{employee_id}-{course_id}", store_id, employee_id, course_id, status, base_week - dt.timedelta(days=35), base_week + dt.timedelta(days=7), completed_date, round(completed_hours, 2), target_hours))

    weekly_metrics = []
    for week_offset in range(8):
        week = base_week - dt.timedelta(days=7 * (7 - week_offset))
        for store_id, store_name, region, *_rest in stores:
            target = store_by_id[store_id][-1]
            sales_factor = 1 + random.uniform(-0.05, 0.06)
            training_completion = random.uniform(91, 99)
            wait = random.uniform(3.8, 5.8)
            csat = random.uniform(4.2, 4.8)
            converted_training_hours = random.choice([0, 0, 1, 2, 3])
            if store_id == 104:
                training_completion = 79 + week_offset * 0.4
                wait = 6.4 + random.uniform(0.1, 0.7)
                converted_training_hours = 12 + week_offset % 4
                sales_factor = 0.96
            if store_id == 117:
                sales_factor = 0.935
                wait = 5.2
            if store_id == 122:
                wait = 8.1 + random.uniform(0, 0.5)
                csat = 3.75 + random.uniform(0, 0.2)
            weekly_metrics.append((store_id, week, region, round(target * sales_factor, 2), target, round((sales_factor - 1) * 100, 2), round(training_completion, 1), round(wait, 1), round(csat, 2), 40, round(40 - converted_training_hours, 1), converted_training_hours, "rush window service coverage" if converted_training_hours else None))

    schedules = []
    shifts = []
    for store_id, *_ in stores:
        scheduled = 42 if store_id == 104 else random.choice([34, 36, 38, 40])
        converted = 14 if store_id == 104 else random.choice([0, 0, 1, 2, 3])
        schedules.append((store_id, "2026-W27", scheduled, scheduled - converted, converted, "rush window service coverage" if converted else None))
        for day in range(7):
            date = base_week + dt.timedelta(days=day)
            shifts.append((f"S-{store_id}-{day}-AM", store_id, date, "AM", 5, 4 if store_id in {104, 122} and day in {4, 5} else 5, "front-of-house"))
            shifts.append((f"S-{store_id}-{day}-PM", store_id, date, "PM", 6, 5 if store_id == 104 and day in {3, 4, 5} else 6, "bar and handoff"))

    inventory = []
    ingredients = ["espresso beans", "oat milk", "cold brew concentrate", "vanilla syrup", "breakfast sandwich eggs"]
    for store_id, *_ in stores:
        for ingredient in ingredients:
            forecast = random.randint(60, 160)
            on_hand = random.randint(60, 210)
            eta = random.choice([1, 2, 3, 4])
            risk = "low"
            if on_hand < forecast * 0.75 and eta >= 3:
                risk = "medium"
            if store_id == 117 and ingredient in {"oat milk", "espresso beans"}:
                on_hand = 42 if ingredient == "oat milk" else 80
                forecast = 96 if ingredient == "oat milk" else 135
                eta = 5 if ingredient == "oat milk" else 4
                risk = "high"
            if store_id == 122 and ingredient == "cold brew concentrate":
                on_hand, forecast, eta, risk = 70, 82, 2, "medium"
            inventory.append((store_id, ingredient, on_hand, forecast, eta, risk, "Pacific Foods" if ingredient != "espresso beans" else "Cascade Roasters"))

    feedback = []
    topics = ["wait_time", "mobile_order", "drink_quality", "staff_friendliness", "cleanliness"]
    for store_id, *_ in stores:
        for index in range(18):
            topic = random.choice(topics)
            rating = random.choice([4, 5, 5, 4, 3])
            if store_id == 122 and topic in {"wait_time", "mobile_order"}:
                rating = random.choice([2, 3, 3])
            feedback.append((f"CF-{store_id}-{index}", store_id, base_week - dt.timedelta(days=random.randint(0, 20)), topic, rating, "Peak wait time called out" if topic == "wait_time" and rating <= 3 else "Routine feedback"))

    ops_tasks = [
        ("OPS-2194", 104, "Review training shift protection", "Protect training blocks and reduce conversion during rush windows.", "training", "high", "open", base_week - dt.timedelta(days=5), "Maria Chen"),
        ("OPS-2201", 122, "Investigate peak wait times", "Analyze mobile-order queue staffing for peak windows.", "customer_experience", "high", "open", base_week - dt.timedelta(days=3), "Maria Chen"),
        ("OPS-2208", 117, "Expedite oat milk transfer", "Move oat milk from Store 130 before weekend demand spike.", "inventory", "high", "open", base_week - dt.timedelta(days=1), "Maria Chen"),
        ("OPS-2210", 117, "Confirm espresso reorder ETA", "Escalate Cascade Roasters delivery for Portland Pearl.", "inventory", "high", "draft", base_week, "Maria Chen"),
    ]

    store_metrics = [
        (
            row[2],
            str(row[0]),
            store_by_id[row[0]][1],
            row[5],
            row[6],
            row[7],
            row[8],
        )
        for row in weekly_metrics
        if row[1] == base_week
    ]

    return {
        "stores": (["store_id", "store_name", "region", "district", "city", "state", "area_leader", "opened_date", "seat_count", "weekly_sales_target"], stores),
        "training_courses": (["course_id", "course_name", "category", "required_for_roles", "target_hours", "recurrence", "is_mandatory"], courses),
        "employees": (["employee_id", "store_id", "full_name", "role", "hire_date", "status", "employment_type"], employees),
        "training_progress": (["record_id", "store_id", "employee_id", "course_id", "status", "assigned_date", "due_date", "completed_date", "completed_hours", "target_hours"], progress),
        "store_weekly_metrics": (["store_id", "week_start_date", "region", "net_sales", "weekly_sales_target", "sales_vs_plan_pct", "training_completion_pct", "avg_wait_minutes", "customer_satisfaction", "scheduled_training_hours", "actual_training_hours", "converted_training_hours", "conversion_reason"], weekly_metrics),
        "schedules": (["store_id", "week", "scheduled_training_hours", "completed_training_hours", "converted_training_hours", "conversion_reason"], schedules),
        "training_events": (["store_id", "week", "scheduled_training_hours", "completed_training_hours", "converted_training_hours", "conversion_reason"], schedules),
        "storetime_shifts": (["shift_id", "store_id", "shift_date", "daypart", "scheduled_partners", "actual_partners", "coverage_area"], shifts),
        "inventory": (["store_id", "ingredient", "on_hand_units", "forecast_7d_units", "reorder_eta_days", "stockout_risk", "supplier"], inventory),
        "customer_feedback": (["feedback_id", "store_id", "feedback_date", "topic", "rating", "comment_summary"], feedback),
        "ops_tasks": (["task_id", "store_id", "title", "description", "category", "severity", "status", "created_date", "owner"], ops_tasks),
        "store_metrics": (["region", "store_id", "store_name", "sales_vs_plan_pct", "training_completion_pct", "avg_wait_minutes", "customer_satisfaction"], store_metrics),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision Bobabricks store-ops Databricks demo assets.")
    parser.add_argument("--profile", default="fevm-worldtour-ai")
    parser.add_argument("--warehouse-id", default="88fd32ee6d9438ac")
    parser.add_argument("--catalog", default="bobabricks_demo")
    parser.add_argument("--schema", default="store_ops")
    parser.add_argument("--table-prefix", default="")
    parser.add_argument("--allow-shared-source", action="store_true")
    args = parser.parse_args()

    if not args.table_prefix and not args.allow_shared_source:
        parser.error("--table-prefix must be non-empty unless --allow-shared-source is set")
    if not args.allow_shared_source and (
        args.profile,
        args.catalog,
        args.schema,
        args.table_prefix,
    ) != (FIELD_PROFILE, FIELD_CATALOG, FIELD_SCHEMA, FIELD_PREFIX):
        parser.error("private provisioning arguments do not match the field-eng safety contract")

    namespace = f"{args.catalog}.{args.schema}"
    table_names = {
        table: qualified_table(namespace, args.table_prefix, table) for table in TABLE_DDL
    }

    for table, columns in TABLE_DDL.items():
        execute_sql(args.profile, args.warehouse_id, f"CREATE OR REPLACE TABLE {table_names[table]} ({columns}) USING DELTA COMMENT 'Bobabricks {table.replace('_', ' ')} demo table'")

    data = build_seed_data()
    for table, (columns, rows) in data.items():
        if table == "store_metrics":
            continue
        insert_rows(args.profile, args.warehouse_id, table_names[table], columns, rows)

    execute_sql(
        args.profile,
        args.warehouse_id,
        f"""
        INSERT INTO {table_names['store_metrics']}
        SELECT s.region, CAST(s.store_id AS STRING), s.store_name, m.sales_vs_plan_pct,
               m.training_completion_pct, m.avg_wait_minutes, m.customer_satisfaction
        FROM {table_names['store_weekly_metrics']} m
        JOIN {table_names['stores']} s ON m.store_id = s.store_id
        WHERE m.week_start_date = (SELECT max(week_start_date) FROM {table_names['store_weekly_metrics']})
        """,
    )

    print(f"Provisioned Bobabricks store-ops data in {namespace} with prefix {args.table_prefix!r}")


if __name__ == "__main__":
    main()
