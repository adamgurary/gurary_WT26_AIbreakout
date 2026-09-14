from __future__ import annotations

import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import provision_databricks_assets as data_provisioner
from scripts.provision_databricks_assets import build_seed_data


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
PROFILE = "dbc-f7444b38"
WORKSPACE_ID = "715783009495722"
WAREHOUSE_NAME = "gurary_bobabricks_" "warehouse"
CATALOG = "gurary_" "catalog"
DATA_SCHEMA = "gurary_bobabricks_store_" "ops"
TRACE_SCHEMA = "gurary_ai_gate" "way_demo"
TABLE_PREFIX = "gurary_"
DATA_NAMESPACE = f"{CATALOG}.{DATA_SCHEMA}"
TRACE_NAMESPACE = f"{CATALOG}.{TRACE_SCHEMA}"
EXPECTED_ACTION_NAMES = {
    WAREHOUSE_NAME,
    DATA_NAMESPACE,
    TRACE_NAMESPACE,
    *{
        f"{DATA_NAMESPACE}.{TABLE_PREFIX}{table}"
        for table in EXPECTED_COUNTS
    },
}
EXPECTED_WAREHOUSE_PAYLOAD = {
    "name": WAREHOUSE_NAME,
    "cluster_size": "X-Small",
    "warehouse_type": "PRO",
    "enable_serverless_compute": True,
    "auto_stop_mins": 10,
    "min_num_clusters": 1,
    "max_num_clusters": 1,
}
WAREHOUSE_ID = "a1b2c3d4e5f60718"


def sql_response(columns: list[str], rows: list[list[str]]) -> dict:
    return {
        "statement_id": "01f123456789abcdef0123456789abcd",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "format": "JSON_ARRAY",
            "schema": {
                "column_count": len(columns),
                "columns": [
                    {"name": name, "position": index, "type_name": "STRING"}
                    for index, name in enumerate(columns)
                ],
            },
            "total_row_count": len(rows),
            "total_chunk_count": 1,
        },
        "result": {"chunk_index": 0, "row_count": len(rows), "data_array": rows},
    }


class PlanBoundary:
    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        self.assert_request(profile, payload)
        if args == ["warehouses", "list"]:
            return {"warehouses": []}
        if args == ["catalogs", "get", CATALOG]:
            return {
                "name": CATALOG,
                "owner": "adam.gurary@databricks.com",
                "catalog_type": "MANAGED_CATALOG",
            }
        if args == ["schemas", "list", CATALOG]:
            return {"schemas": []}
        raise AssertionError(f"unexpected read boundary: {args}")

    def assert_request(self, profile: str, payload: dict | None) -> None:
        if profile != PROFILE:
            raise AssertionError(f"unexpected profile: {profile}")
        if payload is not None:
            raise AssertionError("dry run attempted a mutation")


class MutableBoundary:
    def __init__(self):
        self.events: list[tuple] = []
        self.warehouse: dict | None = None
        self.schemas: set[str] = set()
        self.tables: set[str] = set()
        self.warehouse_create_payloads: list[dict] = []
        self.schema_creates: list[str] = []
        self.process_commands: list[list[str]] = []
        self.storetime_values = ["42", "28", "14"]

    def verify_profile(self, profile: str, host: str) -> dict:
        self.events.append(("binding", profile, host))
        return {
            "host": host,
            "current_user": {
                "id": "1000000000000000",
                "userName": "adam.gurary@databricks.com",
                "active": True,
                "displayName": "Adam Gurary",
            },
        }

    def workspace_id(self, profile: str) -> str:
        self.events.append(("workspace", profile))
        return WORKSPACE_ID

    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        if profile != PROFILE:
            raise AssertionError(f"unexpected profile: {profile}")
        key = tuple(args)
        if key == ("warehouses", "list"):
            self.events.append(("inspection", "warehouse"))
            return {"warehouses": [dict(self.warehouse)] if self.warehouse else []}
        if key == ("catalogs", "get", CATALOG):
            self.events.append(("inspection", "catalog"))
            return {
                "name": CATALOG,
                "owner": "adam.gurary@databricks.com",
                "catalog_type": "MANAGED_CATALOG",
                "metastore_id": "11111111-2222-3333-4444-555555555555",
            }
        if key == ("schemas", "list", CATALOG):
            self.events.append(("inspection", "schemas"))
            return {
                "schemas": [
                    {
                        "name": name.split(".", 1)[1],
                        "catalog_name": CATALOG,
                        "full_name": name,
                        "owner": "adam.gurary@databricks.com",
                        "schema_id": f"schema-{index}",
                    }
                    for index, name in enumerate(sorted(self.schemas), 1)
                ]
            }
        if key == (
            "tables",
            "list",
            CATALOG,
            DATA_SCHEMA,
        ):
            self.events.append(("inspection", "tables"))
            return {
                "tables": [
                    {
                        "name": name.rsplit(".", 1)[1],
                        "catalog_name": CATALOG,
                        "schema_name": DATA_SCHEMA,
                        "full_name": name,
                        "table_type": "MANAGED",
                        "table_id": f"table-{index}",
                    }
                    for index, name in enumerate(sorted(self.tables), 1)
                ]
            }
        if key == ("warehouses", "create"):
            self.events.append(("mutation", "warehouse"))
            if payload != EXPECTED_WAREHOUSE_PAYLOAD:
                raise AssertionError(f"unsafe warehouse payload: {payload}")
            self.warehouse_create_payloads.append(dict(payload))
            self.warehouse = {
                "id": WAREHOUSE_ID,
                **EXPECTED_WAREHOUSE_PAYLOAD,
                "creator_name": "adam.gurary@databricks.com",
                "state": "RUNNING",
                "health": {"status": "HEALTHY"},
            }
            return dict(self.warehouse)
        if key == ("warehouses", "get", WAREHOUSE_ID):
            self.events.append(("inspection", "warehouse_by_id"))
            if self.warehouse is None:
                raise AssertionError("warehouse read before create")
            return dict(self.warehouse)
        if key[:2] == ("schemas", "create"):
            self.events.append(("mutation", "schema"))
            schema = key[2]
            if key[3:] != (
                CATALOG,
                "--comment",
                "Private Bobabricks field-engineering demo resources",
            ):
                raise AssertionError(f"unsafe schema create argv: {args}")
            if payload is not None:
                raise AssertionError(f"unsafe schema create payload: {payload}")
            full_name = f"{CATALOG}.{schema}"
            self.schema_creates.append(full_name)
            self.schemas.add(full_name)
            return {
                "name": schema,
                "catalog_name": CATALOG,
                "full_name": full_name,
                "owner": "adam.gurary@databricks.com",
                "schema_id": f"schema-{len(self.schemas)}",
            }
        if key[:3] == ("api", "post", "/api/2.0/sql/statements"):
            if not isinstance(payload, dict):
                raise AssertionError("SQL statement payload is missing")
            statement = payload.get("statement", "")
            if "UNION ALL" in statement:
                return sql_response(
                    ["table_name", "row_count"],
                    [[table, str(count)] for table, count in EXPECTED_COUNTS.items()],
                )
            if "training_completion_pct" in statement:
                return sql_response(["training_completion_pct"], [["81.8"]])
            if "scheduled_training_hours" in statement:
                return sql_response(
                    [
                        "scheduled_training_hours",
                        "completed_training_hours",
                        "converted_training_hours",
                    ],
                    [self.storetime_values],
                )
            raise AssertionError(f"unexpected verification SQL: {statement}")
        raise AssertionError(f"unexpected CLI boundary: {args}")

    def run_process(self, command: list[str]) -> None:
        self.events.append(("mutation", "tables"))
        self.process_commands.append(list(command))
        self.tables.update(
            f"{DATA_NAMESPACE}.{TABLE_PREFIX}{table}"
            for table in EXPECTED_COUNTS
        )


class FieldEngPlanAndSeedTest(unittest.TestCase):
    def test_deterministic_seed_has_all_exact_counts_and_store_104_facts(self):
        seed = build_seed_data()

        self.assertEqual(
            {table: len(rows) for table, (_columns, rows) in seed.items()},
            EXPECTED_COUNTS,
        )
        metric_columns, metric_rows = seed["store_weekly_metrics"]
        latest_store_104 = [
            dict(zip(metric_columns, row)) for row in metric_rows if row[0] == 104
        ][-1]
        self.assertEqual(latest_store_104["training_completion_pct"], 81.8)

        schedule_columns, schedule_rows = seed["schedules"]
        store_104_schedule = [
            dict(zip(schedule_columns, row)) for row in schedule_rows if row[0] == 104
        ]
        self.assertEqual(
            store_104_schedule,
            [
                {
                    "store_id": 104,
                    "week": "2026-W27",
                    "scheduled_training_hours": 42,
                    "completed_training_hours": 28,
                    "converted_training_hours": 14,
                    "conversion_reason": "rush window service coverage",
                }
            ],
        )

    def test_dry_run_plans_only_the_exact_owned_resources(self):
        try:
            field_eng = importlib.import_module("scripts.provision_field_eng")
        except ModuleNotFoundError:
            field_eng = None
        self.assertIsNotNone(field_eng, "field-eng provisioner is missing")
        if field_eng is None:
            return

        plan = field_eng.build_provision_plan(
            run_cli=PlanBoundary(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"userName": "adam.gurary@databricks.com"},
            },
            workspace_id_provider=lambda profile: WORKSPACE_ID,
        )

        self.assertEqual({action.name for action in plan.actions}, EXPECTED_ACTION_NAMES)


class FieldEngReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.field_eng = importlib.import_module("scripts.provision_field_eng")

    def test_plan_records_the_exact_private_data_provisioner_argv(self):
        plan = self.field_eng.build_provision_plan(
            run_cli=PlanBoundary(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"userName": "adam.gurary@databricks.com"},
            },
            workspace_id_provider=lambda profile: WORKSPACE_ID,
        )

        self.assertEqual(
            plan.data_provisioner_argv,
            (
                "scripts/provision_databricks_assets.py",
                "--profile",
                PROFILE,
                "--warehouse-id",
                "{warehouse_id}",
                "--catalog",
                CATALOG,
                "--schema",
                DATA_SCHEMA,
                "--table-prefix",
                TABLE_PREFIX,
            ),
        )
        self.assertNotIn("--allow-shared-source", plan.data_provisioner_argv)

    def test_storetime_verification_rejects_fractional_values(self):
        boundary = MutableBoundary()
        boundary.storetime_values = ["42.9", "28", "14"]

        with self.assertRaisesRegex(RuntimeError, "did not match 42/28/14"):
            self.field_eng._verify_private_data(
                boundary,
                self.field_eng.load_target("field_eng"),
                WAREHOUSE_ID,
                lambda _seconds: None,
            )

    def test_storetime_verification_rejects_fraction_below_float_precision(self):
        boundary = MutableBoundary()
        boundary.storetime_values = ["42.000000000000000001", "28", "14"]

        with self.assertRaisesRegex(RuntimeError, "did not match 42/28/14"):
            self.field_eng._verify_private_data(
                boundary,
                self.field_eng.load_target("field_eng"),
                WAREHOUSE_ID,
                lambda _seconds: None,
            )

    def test_schema_create_uses_the_installed_cli_positional_flag_shape(self):
        boundary = MutableBoundary()

        def cli_shape(profile: str, args: list[str], payload: dict | None = None):
            if args[:2] != ["schemas", "create"]:
                return boundary(profile, args, payload)
            if payload is not None:
                raise RuntimeError("installed CLI rejects a JSON schema-create body")
            expected = [
                "schemas",
                "create",
                DATA_SCHEMA,
                CATALOG,
                "--comment",
                "Private Bobabricks field-engineering demo resources",
            ]
            if args != expected:
                raise AssertionError(f"unexpected schema-create shape: {args}")
            return {
                "name": DATA_SCHEMA,
                "catalog_name": CATALOG,
                "full_name": DATA_NAMESPACE,
                "owner": "adam.gurary@databricks.com",
                "schema_id": "schema-1",
            }

        created = self.field_eng._ensure_schema(
            self.field_eng.load_target("field_eng"),
            DATA_SCHEMA,
            cli_shape,
            boundary.verify_profile,
            boundary.workspace_id,
        )

        self.assertEqual(
            created["full_name"],
            DATA_NAMESPACE,
        )

    def test_apply_reconciles_exact_resources_and_is_idempotent(self):
        boundary = MutableBoundary()
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "field_eng.json"
            plan = self.field_eng.build_provision_plan(
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
            )
            first = self.field_eng.apply_provision_plan(
                plan,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                process_runner=boundary.run_process,
                state_path=state_path,
            )

            first_names = first.table_names
            first_counts = first.counts
            second_plan = self.field_eng.build_provision_plan(
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
            )
            second = self.field_eng.apply_provision_plan(
                second_plan,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                process_runner=boundary.run_process,
                state_path=state_path,
            )

            self.assertEqual(boundary.warehouse_create_payloads, [EXPECTED_WAREHOUSE_PAYLOAD])
            self.assertEqual(
                boundary.schema_creates,
                [
                    DATA_NAMESPACE,
                    TRACE_NAMESPACE,
                ],
            )
            self.assertEqual(len(boundary.process_commands), 2)
            for command in boundary.process_commands:
                self.assertEqual(command[1:4], ["scripts/provision_databricks_assets.py", "--profile", PROFILE])
                self.assertIn("--warehouse-id", command)
                self.assertEqual(command[command.index("--table-prefix") + 1], TABLE_PREFIX)
                self.assertNotIn("--allow-shared-source", command)
            self.assertEqual(first_names, second.table_names)
            self.assertEqual(first_counts, second.counts)
            self.assertEqual(first_counts, EXPECTED_COUNTS)
            self.assertEqual(first.store_104_training_completion, 81.8)
            self.assertEqual(first.store_104_storetime, (42, 28, 14))
            self.assertEqual(
                json.loads(state_path.read_text(encoding="utf-8")),
                {"target": "field_eng", "warehouse_id": WAREHOUSE_ID},
            )

        previous_mutation = -1
        for mutation_index in [
            index for index, event in enumerate(boundary.events) if event[0] == "mutation"
        ]:
            preflight = boundary.events[previous_mutation + 1 : mutation_index]
            self.assertTrue(any(event[0] == "binding" for event in preflight))
            self.assertTrue(any(event[0] == "workspace" for event in preflight))
            self.assertTrue(any(event[0] == "inspection" for event in preflight))
            previous_mutation = mutation_index

    def test_non_owner_must_have_manage_on_the_catalog(self):
        class ManagedCatalogBoundary(PlanBoundary):
            def __call__(self, profile, args, payload=None):
                if args == ["catalogs", "get", CATALOG]:
                    return {
                        "name": CATALOG,
                        "owner": "catalog-owner@databricks.com",
                        "catalog_type": "MANAGED_CATALOG",
                    }
                if args == ["grants", "get", "catalog", CATALOG]:
                    return {
                        "privilege_assignments": [
                            {
                                "principal": "adam.gurary@databricks.com",
                                "privileges": ["USE_CATALOG", "MANAGE"],
                            }
                        ]
                    }
                return super().__call__(profile, args, payload)

        plan = self.field_eng.build_provision_plan(
            run_cli=ManagedCatalogBoundary(),
            verify_profile=lambda profile, host: {
                "host": host,
                "current_user": {"userName": "adam.gurary@databricks.com"},
            },
            workspace_id_provider=lambda profile: WORKSPACE_ID,
        )
        self.assertEqual({action.name for action in plan.actions}, EXPECTED_ACTION_NAMES)


class PrivateDataProvisionerSafetyTest(unittest.TestCase):
    def test_private_mutation_refreshes_binding_and_inspects_exact_state(self):
        events: list[tuple] = []
        statement = (
            "CREATE OR REPLACE TABLE "
            f"{DATA_NAMESPACE}.{TABLE_PREFIX}stores "
            "(store_id INT) USING DELTA"
        )

        def run_json_boundary(profile, args, payload=None):
            events.append(("inspection", tuple(args)))
            if args == ["warehouses", "get", WAREHOUSE_ID]:
                return {"id": WAREHOUSE_ID, "name": WAREHOUSE_NAME, "state": "RUNNING"}
            if args == ["schemas", "get", DATA_NAMESPACE]:
                return {"full_name": DATA_NAMESPACE, "owner": "adam.gurary@databricks.com"}
            if args == ["tables", "list", CATALOG, DATA_SCHEMA]:
                return {"tables": []}
            raise AssertionError(f"unexpected private inspection: {args}")

        def sql_boundary(profile, args, payload=None):
            events.append(("sql", tuple(args)))
            return {"statement_id": "01f123456789abcdef0123456789abcd", "status": {"state": "SUCCEEDED"}}

        with patch.object(
            data_provisioner,
            "assert_profile",
            create=True,
            side_effect=lambda profile, host: events.append(("binding", profile, host))
            or {"host": host, "current_user": {"userName": "adam.gurary@databricks.com"}},
        ), patch.object(
            data_provisioner,
            "_workspace_id_from_sdk",
            create=True,
            side_effect=lambda profile: events.append(("workspace", profile)) or WORKSPACE_ID,
        ), patch.object(
            data_provisioner, "run_json", create=True, side_effect=run_json_boundary
        ), patch.object(data_provisioner, "run_databricks", side_effect=sql_boundary):
            data_provisioner.execute_sql(PROFILE, WAREHOUSE_ID, statement)

        sql_index = next(index for index, event in enumerate(events) if event[0] == "sql")
        self.assertTrue(any(event[0] == "binding" for event in events[:sql_index]))
        self.assertTrue(any(event[0] == "workspace" for event in events[:sql_index]))
        self.assertTrue(any(event[0] == "inspection" for event in events[:sql_index]))

    def test_private_mutation_rejects_an_unprefixed_table_before_sql(self):
        statement = (
            "CREATE OR REPLACE TABLE "
            f"{DATA_NAMESPACE}.stores "
            "(store_id INT) USING DELTA"
        )
        with patch.object(data_provisioner, "run_databricks") as sql_boundary:
            with self.assertRaisesRegex(ValueError, "private table"):
                data_provisioner.execute_sql(PROFILE, WAREHOUSE_ID, statement)
        sql_boundary.assert_not_called()

    def test_table_provisioning_never_mutates_catalog_or_schema(self):
        statements: list[str] = []
        argv = [
            "provision_databricks_assets.py",
            "--profile",
            PROFILE,
            "--warehouse-id",
            WAREHOUSE_ID,
            "--catalog",
            CATALOG,
            "--schema",
            DATA_SCHEMA,
            "--table-prefix",
            TABLE_PREFIX,
        ]
        with patch("sys.argv", argv), patch.object(
            data_provisioner,
            "execute_sql",
            side_effect=lambda profile, warehouse, statement: statements.append(statement) or {},
        ), patch.object(data_provisioner, "insert_rows"):
            data_provisioner.main()

        self.assertFalse(any("CREATE CATALOG" in statement for statement in statements))
        self.assertFalse(any("CREATE SCHEMA" in statement for statement in statements))


if __name__ == "__main__":
    unittest.main()
