from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from deploy.config import load_target
from deploy.genie import clone_target, export_source, main, rewrite_serialized_space, run_acceptance


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
SOURCE_CATALOG = "worldtour_ai_" "catalog"
SOURCE_SCHEMA = "bobabricks_store_" "ops"
SOURCE_NAMESPACE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}"
SOURCE_WAREHOUSE_ID = "88fd32ee6d94" "38ac"
TARGET_WAREHOUSE_ID = "02e73edf357eb637"
TARGET_SPACE_ID = "01f00000000000000000000000000001"
TARGET_TITLE = "gurary_bobabricks_store_" "operations"
TARGET_NAMESPACE = "gurary_" "catalog.gurary_bobabricks_store_" "ops"


class GenieRewriteTest(unittest.TestCase):
    def test_rewrites_all_exact_table_identifiers_without_touching_prose(self):
        serialized = json.dumps(
            {
                "version": 2,
                "data_sources": {
                    "tables": [
                        {"identifier": f"{SOURCE_NAMESPACE}.{table}"}
                        for table in SOURCE_TABLE_NAMES
                    ]
                },
                "nested": {
                    "prose": "Do not replace worldtour_ai_"
                    "catalogue.bobabricks_store_operation.stores",
                },
            }
        )

        rewritten = rewrite_serialized_space(serialized, load_target("field_eng"))

        self.assertNotIn(SOURCE_NAMESPACE, rewritten)
        self.assertNotIn(SOURCE_WAREHOUSE_ID, rewritten)
        for table in SOURCE_TABLE_NAMES:
            self.assertIn(
                f"gurary_"
                f"catalog.gurary_bobabricks_store_"
                f"ops.gurary_{table}",
                rewritten,
            )
        parsed = json.loads(rewritten)
        self.assertEqual(
            parsed["nested"]["prose"],
            "Do not replace worldtour_ai_"
            "catalogue.bobabricks_store_operation.stores",
        )

    def test_rejects_duplicate_private_tables_that_hide_a_missing_mapping(self):
        identifiers = [f"{SOURCE_NAMESPACE}.{table}" for table in SOURCE_TABLE_NAMES]
        identifiers[-1] = identifiers[0]
        serialized = json.dumps(
            {"version": 2, "data_sources": {"tables": [{"identifier": value} for value in identifiers]}}
        )

        with self.assertRaisesRegex(ValueError, "exact private tables"):
            rewrite_serialized_space(serialized, load_target("field_eng"))

    def test_rejects_a_standalone_source_catalog_value(self):
        serialized = json.loads(source_serialized_space())
        serialized["standalone_reference"] = SOURCE_CATALOG

        with self.assertRaisesRegex(ValueError, "protected source reference"):
            rewrite_serialized_space(json.dumps(serialized), load_target("field_eng"))

    def test_rejects_a_standalone_source_schema_value(self):
        serialized = json.loads(source_serialized_space())
        serialized["standalone_reference"] = SOURCE_SCHEMA

        with self.assertRaisesRegex(ValueError, "protected source reference"):
            rewrite_serialized_space(json.dumps(serialized), load_target("field_eng"))


class GenieSourceExportTest(unittest.TestCase):
    def test_export_uses_cli_compatible_rest_shape_and_allowlists_saved_fields(self):
        serialized = json.dumps(
            {
                "version": 2,
                "data_sources": {
                    "tables": [
                        {"identifier": f"{SOURCE_NAMESPACE}.{table}"}
                        for table in SOURCE_TABLE_NAMES
                    ]
                },
            }
        )
        calls = []

        def run_cli(profile, args, payload=None):
            calls.append((profile, args, payload))
            return {
                "space_id": "source-space-id",
                "title": "Bobabricks Store Operations",
                "serialized_space": serialized,
                "warehouse_id": SOURCE_WAREHOUSE_ID,
                "etag": "source-etag",
                "access_token": "must-not-be-saved",
                "authorization": "must-not-be-saved",
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "source.json"
            export_source(
                profile="fevm-worldtour-ai",
                space_id="source-space-id",
                output=output,
                run_cli=run_cli,
            )
            saved = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(
            calls,
            [
                (
                    "fevm-worldtour-ai",
                    ["api", "get", "/api/2.0/genie/spaces/source-space-id?include_serialized_space=true"],
                    None,
                )
            ],
        )
        self.assertEqual(set(saved), {"serialized_space", "title"})
        self.assertEqual(saved["serialized_space"], serialized)
        self.assertNotIn("token", json.dumps(saved).lower())
        self.assertNotIn("authorization", json.dumps(saved).lower())

    def test_export_source_command_runs_the_installed_cli_compatible_rest_read(self):
        calls = []

        def run_cli(profile, args, payload=None):
            calls.append((profile, args, payload))
            return {
                "title": "Bobabricks Store Operations",
                "serialized_space": source_serialized_space(),
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "source.json"
            main(
                [
                    "export-source",
                    "--profile",
                    "fevm-worldtour-ai",
                    "--space-id",
                    "source-space-id",
                    "--output",
                    str(output),
                ],
                run_cli=run_cli,
            )
            saved = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(saved["title"], "Bobabricks Store Operations")
        self.assertEqual(calls[0][1][0:2], ["api", "get"])


def source_serialized_space() -> str:
    return json.dumps(
        {
            "version": 2,
            "data_sources": {
                "tables": [
                    {"identifier": f"{SOURCE_NAMESPACE}.{table}"}
                    for table in SOURCE_TABLE_NAMES
                ]
            },
        }
    )


class TargetBoundary:
    def __init__(
        self,
        existing=False,
        invisible_reads_after_create=0,
        split_permission_evidence=False,
    ):
        self.events = []
        self.invisible_reads_after_create = invisible_reads_after_create
        self.split_permission_evidence = split_permission_evidence
        self.space = (
            {
                "space_id": TARGET_SPACE_ID,
                "title": TARGET_TITLE,
                "warehouse_id": TARGET_WAREHOUSE_ID,
                "etag": "live-etag",
                "serialized_space": rewrite_serialized_space(
                    source_serialized_space(), load_target("field_eng")
                ),
            }
            if existing
            else None
        )

    def verify_profile(self, profile, host):
        self.events.append(("binding", profile, host))
        return {
            "host": host,
            "current_user": {"userName": "adam.gurary@databricks.com"},
        }

    def workspace_id(self, profile):
        self.events.append(("workspace", profile))
        return "715783009495722"

    def update_space(self, profile, space_id, payload, etag):
        self.events.append(("mutation", "update", profile, space_id, payload, etag))
        self.space = {**self.space, **payload, "etag": "updated-etag"}
        return dict(self.space)

    def __call__(self, profile, args, payload=None):
        self.assert_profile(profile)
        key = tuple(args)
        if key == ("warehouses", "get", TARGET_WAREHOUSE_ID):
            self.events.append(("inspection", "warehouse"))
            return {"id": TARGET_WAREHOUSE_ID, "name": "gurary_bobabricks_" "warehouse"}
        if key == ("genie", "list-spaces"):
            self.events.append(("inspection", "spaces"))
            if self.space and self.invisible_reads_after_create:
                self.invisible_reads_after_create -= 1
                return {"spaces": []}
            return {"spaces": [dict(self.space)] if self.space else []}
        if key == (
            "api",
            "get",
            f"/api/2.0/genie/spaces/{TARGET_SPACE_ID}?include_serialized_space=true",
        ):
            self.events.append(("inspection", "space"))
            return dict(self.space)
        if key == ("api", "post", "/api/2.0/genie/spaces"):
            self.events.append(("mutation", "create", payload))
            self.space = {
                **payload,
                "space_id": TARGET_SPACE_ID,
                "etag": "created-etag",
            }
            return dict(self.space)
        if key == ("permissions", "get", "genie", TARGET_SPACE_ID):
            self.events.append(("inspection", "permissions"))
            return {"access_control_list": []}
        if key == ("permissions", "update", "genie", TARGET_SPACE_ID):
            self.events.append(("mutation", "permissions", payload))
            if self.split_permission_evidence:
                return {
                    "object_id": TARGET_SPACE_ID,
                    "object_type": "genie",
                    "access_control_list": [
                        {
                            "user_name": "adam.gurary@databricks.com",
                            "all_permissions": [{"permission_level": "CAN_USE"}],
                        },
                        {
                            "service_principal_name": "unrelated-principal",
                            "all_permissions": [{"permission_level": "CAN_MANAGE"}],
                        },
                    ],
                }
            return {
                "object_id": TARGET_SPACE_ID,
                "object_type": "genie",
                "access_control_list": [
                    {
                        "user_name": "adam.gurary@databricks.com",
                        "all_permissions": [{"permission_level": "CAN_MANAGE"}],
                    }
                ],
            }
        if len(args) == 4 and args[:3] == ["genie", "start-conversation", TARGET_SPACE_ID]:
            question = args[3]
            self.events.append(("mutation", "conversation", question))
            if "Pacific" in question:
                sql = (
                    "SELECT SUM(s.scheduled_training_hours) FROM "
                    f"{TARGET_NAMESPACE}.gurary_schedules s JOIN "
                    f"{TARGET_NAMESPACE}.gurary_stores st ON s.store_id = st.store_id "
                    "WHERE st.region = 'Pacific'"
                )
                rows = 1
            else:
                sql = (
                    "SELECT training_completion_pct FROM "
                    f"{TARGET_NAMESPACE}.gurary_store_weekly_metrics "
                    "WHERE store_id = 104 ORDER BY week_start_date DESC LIMIT 1"
                )
                rows = 1
            return {
                "space_id": TARGET_SPACE_ID,
                "conversation_id": "conversation-id",
                "message_id": "message-id",
                "status": "COMPLETED",
                "attachments": [
                    {
                        "query": {
                            "query": sql,
                            "query_result_metadata": {"row_count": rows, "is_truncated": False},
                        }
                    }
                ],
            }
        raise AssertionError(f"unexpected target call: {args}, {payload}")

    def assert_profile(self, profile):
        if profile != "dogfood-vs":
            raise AssertionError(f"unexpected profile: {profile}")


class GenieTargetCloneTest(unittest.TestCase):
    def write_source(self, root: Path) -> Path:
        source = root / "source.json"
        source.write_text(
            json.dumps(
                {
                    "serialized_space": source_serialized_space(),
                    "title": "Bobabricks Store Operations",
                }
            ),
            encoding="utf-8",
        )
        return source

    def test_create_binds_exact_private_resources_grants_owner_and_preserves_state(self):
        boundary = TargetBoundary()
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = root / "field_eng.json"
            state.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": TARGET_WAREHOUSE_ID}),
                encoding="utf-8",
            )
            result = clone_target(
                source_path=self.write_source(root),
                state_path=state,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                update_space=boundary.update_space,
            )
            saved_state = json.loads(state.read_text(encoding="utf-8"))

        self.assertEqual(
            saved_state,
            {
                "target": "field_eng",
                "warehouse_id": TARGET_WAREHOUSE_ID,
                "genie_space_id": TARGET_SPACE_ID,
            },
        )
        self.assertEqual(result["action"], "created")
        create = next(event for event in boundary.events if event[:2] == ("mutation", "create"))
        self.assertEqual(create[2]["title"], TARGET_TITLE)
        self.assertEqual(create[2]["warehouse_id"], TARGET_WAREHOUSE_ID)
        self.assertEqual(create[2]["parent_path"], "/Users/adam.gurary@databricks.com")
        self.assertNotIn(SOURCE_NAMESPACE, create[2]["serialized_space"])
        permission = next(
            event for event in boundary.events if event[:2] == ("mutation", "permissions")
        )
        self.assertEqual(
            permission[2],
            {
                "access_control_list": [
                    {
                        "user_name": "adam.gurary@databricks.com",
                        "permission_level": "CAN_MANAGE",
                    }
                ]
            },
        )
        self.assertGreaterEqual(
            sum(event[0] == "binding" for event in boundary.events),
            2,
        )

    def test_update_uses_live_etag_and_rewrites_only_the_owned_space(self):
        boundary = TargetBoundary(existing=True)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = root / "field_eng.json"
            state.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": TARGET_WAREHOUSE_ID}),
                encoding="utf-8",
            )
            result = clone_target(
                source_path=self.write_source(root),
                state_path=state,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                update_space=boundary.update_space,
            )

        update = next(event for event in boundary.events if event[:2] == ("mutation", "update"))
        self.assertEqual(update[3], TARGET_SPACE_ID)
        self.assertEqual(update[5], "live-etag")
        self.assertEqual(update[4]["title"], TARGET_TITLE)
        self.assertEqual(result["action"], "updated")

    def test_update_is_idempotent_with_the_task_8_space_id_already_in_state(self):
        boundary = TargetBoundary(existing=True)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = root / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            result = clone_target(
                source_path=self.write_source(root),
                state_path=state,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                update_space=boundary.update_space,
            )
            saved = json.loads(state.read_text(encoding="utf-8"))

        self.assertEqual(result["action"], "updated")
        self.assertEqual(saved["genie_space_id"], TARGET_SPACE_ID)

    def test_create_retries_eventually_consistent_readback_without_second_create(self):
        boundary = TargetBoundary(invisible_reads_after_create=1)
        waits = []
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = root / "field_eng.json"
            state.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": TARGET_WAREHOUSE_ID}),
                encoding="utf-8",
            )
            result = clone_target(
                source_path=self.write_source(root),
                state_path=state,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                update_space=boundary.update_space,
                waiter=waits.append,
            )

        self.assertEqual(result["action"], "created")
        self.assertEqual(waits, [1.0])
        self.assertEqual(
            sum(event[:2] == ("mutation", "create") for event in boundary.events),
            1,
        )

    def test_rejects_permission_evidence_split_across_different_principals(self):
        boundary = TargetBoundary(existing=True, split_permission_evidence=True)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = root / "field_eng.json"
            state.write_text(
                json.dumps({"target": "field_eng", "warehouse_id": TARGET_WAREHOUSE_ID}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "owner CAN_MANAGE"):
                clone_target(
                    source_path=self.write_source(root),
                    state_path=state,
                    run_cli=boundary,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                    update_space=boundary.update_space,
                )


class GenieAcceptanceTest(unittest.TestCase):
    def test_both_questions_require_completed_private_namespace_queries(self):
        boundary = TargetBoundary(existing=True)
        with tempfile.TemporaryDirectory() as temporary_directory:
            state = Path(temporary_directory) / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            evidence = run_acceptance(
                state_path=state,
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
            )

        self.assertEqual(len(evidence), 2)
        self.assertTrue(all(item["status"] == "COMPLETED" for item in evidence))
        self.assertTrue(
            all(
                reference.startswith(f"{TARGET_NAMESPACE}.gurary_")
                for item in evidence
                for reference in item["table_references"]
            )
        )
        self.assertEqual(
            sum(event[:2] == ("mutation", "conversation") for event in boundary.events),
            2,
        )
        self.assertGreaterEqual(sum(event[0] == "binding" for event in boundary.events), 2)

    def test_rejects_acceptance_sql_that_reads_a_shared_source_table(self):
        boundary = TargetBoundary(existing=True)
        original = boundary.__call__

        def unsafe(profile, args, payload=None):
            response = original(profile, args, payload)
            if len(args) == 4 and args[:3] == ["genie", "start-conversation", TARGET_SPACE_ID]:
                response["attachments"][0]["query"]["query"] = (
                    f"SELECT * FROM {SOURCE_NAMESPACE}.stores"
                )
            return response

        with tempfile.TemporaryDirectory() as temporary_directory:
            state = Path(temporary_directory) / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "non-private table"):
                run_acceptance(
                    state_path=state,
                    run_cli=unsafe,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                )

    def test_rejects_a_shared_table_hidden_after_a_private_comma_join(self):
        boundary = TargetBoundary(existing=True)
        original = boundary.__call__

        def unsafe(profile, args, payload=None):
            response = original(profile, args, payload)
            if len(args) == 4 and args[:3] == ["genie", "start-conversation", TARGET_SPACE_ID]:
                response["attachments"][0]["query"]["query"] = (
                    f"SELECT * FROM {TARGET_NAMESPACE}.gurary_stores private_store, "
                    f"{SOURCE_NAMESPACE}.stores shared_store"
                )
            return response

        with tempfile.TemporaryDirectory() as temporary_directory:
            state = Path(temporary_directory) / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "non-private table"):
                run_acceptance(
                    state_path=state,
                    run_cli=unsafe,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                )

    def test_rejects_a_two_part_relation_hidden_after_a_private_comma_join(self):
        boundary = TargetBoundary(existing=True)
        original = boundary.__call__

        def unsafe(profile, args, payload=None):
            response = original(profile, args, payload)
            if len(args) == 4 and args[:3] == ["genie", "start-conversation", TARGET_SPACE_ID]:
                response["attachments"][0]["query"]["query"] = (
                    f"SELECT * FROM {TARGET_NAMESPACE}.gurary_stores private_store, "
                    f"{SOURCE_SCHEMA}.stores shared_store"
                )
            return response

        with tempfile.TemporaryDirectory() as temporary_directory:
            state = Path(temporary_directory) / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "non-private table"):
                run_acceptance(
                    state_path=state,
                    run_cli=unsafe,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                )

    def test_rejects_an_unqualified_relation_hidden_after_a_private_comma_join(self):
        boundary = TargetBoundary(existing=True)
        original = boundary.__call__

        def unsafe(profile, args, payload=None):
            response = original(profile, args, payload)
            if len(args) == 4 and args[:3] == ["genie", "start-conversation", TARGET_SPACE_ID]:
                response["attachments"][0]["query"]["query"] = (
                    f"SELECT * FROM {TARGET_NAMESPACE}.gurary_stores private_store, stores shared_store"
                )
            return response

        with tempfile.TemporaryDirectory() as temporary_directory:
            state = Path(temporary_directory) / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "non-private table"):
                run_acceptance(
                    state_path=state,
                    run_cli=unsafe,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                )

    def test_rejects_a_two_part_comma_relation_hidden_by_a_block_comment(self):
        boundary = TargetBoundary(existing=True)
        original = boundary.__call__

        def unsafe(profile, args, payload=None):
            response = original(profile, args, payload)
            if len(args) == 4 and args[:3] == ["genie", "start-conversation", TARGET_SPACE_ID]:
                response["attachments"][0]["query"]["query"] = (
                    f"SELECT * FROM {TARGET_NAMESPACE}.gurary_stores p, "
                    f"/* comment */ {SOURCE_SCHEMA}.stores s"
                )
            return response

        with tempfile.TemporaryDirectory() as temporary_directory:
            state = Path(temporary_directory) / "field_eng.json"
            state.write_text(
                json.dumps(
                    {
                        "target": "field_eng",
                        "warehouse_id": TARGET_WAREHOUSE_ID,
                        "genie_space_id": TARGET_SPACE_ID,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "non-private table"):
                run_acceptance(
                    state_path=state,
                    run_cli=unsafe,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                )


if __name__ == "__main__":
    unittest.main()
