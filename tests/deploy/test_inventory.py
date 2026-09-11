import json
import unittest
from unittest.mock import patch

from deploy.databricks_cli import assert_profile, run_json
from deploy.inventory import inventory_target


SHARED_APP = "bobabricks-store-ops-" "demo"
SHARED_CATALOG = "worldtour_ai_" "catalog"
SHARED_SCHEMA = "bobabricks_store_" "ops"
SHARED_WAREHOUSE = "88fd32ee6d94" "38ac"
SHARED_GENIE = "01f1968067e81bd" "09eacb0d88bcc57de"


class JsonSafeDatabricksCliTest(unittest.TestCase):
    def test_assert_profile_rejects_a_normalized_host_mismatch(self):
        responses = [
            {"host": "https://wrong-workspace.cloud.databricks.com/"},
            {"user_name": "adam.gurary@databricks.com", "id": "123"},
        ]
        with patch("deploy.databricks_cli.run_json", side_effect=responses):
            with self.assertRaisesRegex(RuntimeError, "host mismatch"):
                assert_profile(
                    "fevm-worldtour-ai", "https://fevm-worldtour-ai.cloud.databricks.com"
                )

    def test_run_json_sends_payload_on_standard_input(self):
        completed = type(
            "Completed",
            (),
            {"stdout": '{"application_id": "app-123", "status": {"state": "RUNNING"}}'},
        )()
        payload = {"changes": [{"principal": "sp-123", "add": ["USE_CATALOG"]}]}
        with patch("deploy.databricks_cli.subprocess.run", return_value=completed) as run:
            result = run_json("fevm-worldtour-ai", ["grants", "update", "catalog", "demo"], payload)

        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(
            run.call_args.kwargs["input"], json.dumps(payload),
        )
        self.assertEqual(
            run.call_args.args[0],
            [
                "databricks",
                "grants",
                "update",
                "catalog",
                "demo",
                "--json",
                "@/dev/stdin",
                "--profile",
                "fevm-worldtour-ai",
                "--output",
                "json",
            ],
        )
        self.assertNotIn("sp-123", " ".join(run.call_args.args[0]))


class InventoryTargetTest(unittest.TestCase):
    def _inventory_with_apps(self, apps):
        responses = iter(
            [
                {"apps": apps},
                {"warehouses": []},
                {"spaces": []},
                {"experiments": []},
                {"tables": []},
                {"service_principals": []},
                {"privilege_assignments": []},
                {"projects": []},
                {"connections": []},
            ]
        )
        with patch("deploy.inventory.assert_profile", return_value={
            "host": "https://fevm-worldtour-ai.cloud.databricks.com",
            "current_user": {"user_name": "adam.gurary@databricks.com", "id": "123"},
        }), patch("deploy.inventory.run_json", side_effect=lambda *_args: next(responses)):
            return inventory_target("fevm")

    def test_inventory_rejects_substring_ownership_in_urls_and_display_names(self):
        # Catches reintroducing substring-based ownership detection in _shared_for_fevm.
        inventory = self._inventory_with_apps(
            [
                {
                    "name": "legacy-presenter-url",
                    "url": "https://gurary-incidental.example",
                    "owner": "gurary-incidental-owner",
                },
                {
                    "name": "legacy-presenter-display",
                    "display_name": "gurary-incidental-display",
                    "description": "gurary_appears_only_in_metadata",
                },
            ]
        )

        self.assertEqual(
            [app["mutation_allowed"] for app in inventory["apps"]], [False, False]
        )

    def test_inventory_fails_closed_for_missing_app_identity(self):
        # Catches allowing an unknown record to become owned from its URL or display name.
        inventory = self._inventory_with_apps(
            [
                {
                    "app_id": "unknown-app",
                    "name": None,
                    "url": "https://gurary-incidental.example",
                    "display_name": "gurary-incidental-display",
                }
            ]
        )

        self.assertFalse(inventory["apps"][0]["mutation_allowed"])

    def test_fevm_inventory_marks_shared_objects_read_only_and_scrubs_secrets(self):
        responses = {
            ("apps", "list"): {
                "apps": [
                    {
                        "name": SHARED_APP,
                        "app_id": "shared-app-id",
                        "url": "https://shared.example",
                        "owner": "adam.gurary@databricks.com",
                        "app_status": {"state": "RUNNING"},
                        "oauth_access_token": "never-persist",
                    }
                ]
            },
            ("warehouses", "list"): {
                "warehouses": [
                    {"id": SHARED_WAREHOUSE, "name": "Shared SQL", "state": "RUNNING"}
                ]
            },
            ("genie", "list-spaces"): {
                "spaces": [{"space_id": SHARED_GENIE, "title": "Shared Genie"}]
            },
            ("experiments", "list"): {
                "experiments": [
                    {
                        "experiment_id": "9",
                        "name": "/Shared/bobabricks-store-ops",
                        "tags": [{"key": "team", "value": "ops"}],
                    }
                ]
            },
            ("tables", "list", SHARED_CATALOG, SHARED_SCHEMA): {
                "tables": [
                    {
                        "full_name": f"{SHARED_CATALOG}.{SHARED_SCHEMA}.ops_tasks",
                        "owner": "data-team",
                    }
                ]
            },
            ("service-principals", "list"): {
                "service_principals": [{"id": "sp-shared", "display_name": "shared-service"}]
            },
            ("grants", "get", "schema", f"{SHARED_CATALOG}.{SHARED_SCHEMA}"): {
                "privilege_assignments": [
                    {"principal": "shared-service", "privileges": ["SELECT"]}
                ]
            },
            ("database", "projects", "list"): {
                "projects": [{"name": "bobabricks", "status": "ACTIVE"}]
            },
            ("connections", "list"): {
                "connections": [
                    {
                        "name": "system_ai_agent_atlassian_mcp",
                        "connection_type": "HTTP",
                        "options": {"authorization": "Bearer secret", "token": "[REDACTED]"},
                        "client_secret": "never-persist",
                    }
                ]
            },
        }

        def fake_run_json(_profile, args, payload=None):
            self.assertIsNone(payload)
            return responses[tuple(args)]

        with patch("deploy.inventory.assert_profile", return_value={
            "host": "https://fevm-worldtour-ai.cloud.databricks.com",
            "current_user": {"user_name": "adam.gurary@databricks.com", "id": "123"},
        }), patch("deploy.inventory.run_json", side_effect=fake_run_json):
            inventory = inventory_target("fevm")

        self.assertEqual(
            set(inventory),
            {
                "workspace",
                "current_user",
                "apps",
                "warehouses",
                "genie_spaces",
                "experiments",
                "uc_objects",
                "service_principals",
                "effective_grants",
                "lakebase",
                "connections",
            },
        )
        self.assertTrue(all(
            item["mutation_allowed"] is False
            for category in (
                "apps", "warehouses", "genie_spaces", "experiments", "uc_objects",
                "service_principals", "effective_grants", "lakebase", "connections",
            )
            for item in inventory[category]
        ))
        serialized = json.dumps(inventory)
        for secret in ("never-persist", "Bearer secret", "[REDACTED]"):
            self.assertNotIn(secret, serialized)


if __name__ == "__main__":
    unittest.main()
