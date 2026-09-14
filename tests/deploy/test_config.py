import unittest

from deploy.config import load_state, load_target
from deploy.safety import (
    UnsafeMutation,
    assert_safe_fevm_grant,
    assert_safe_mutation,
    assert_safe_name,
)


class ConfigSafetyTest(unittest.TestCase):
    def test_known_targets_have_expected_hosts(self):
        self.assertEqual(load_target("fevm").host, "https://fevm-worldtour-ai.cloud.databricks.com")
        self.assertEqual(load_target("field_eng").workspace_id, "715783009495722")

    def test_created_names_must_be_gurary_namespaced(self):
        for name in ["gurary_table", "gurary-bobabricks-store-ops"]:
            assert_safe_name(name)
        with self.assertRaises(UnsafeMutation):
            assert_safe_name("bobabricks-store-ops-demo")

    def test_fevm_shared_resources_are_forbidden_mutation_targets(self):
        target = load_target("fevm")
        for name in [
            "bobabricks-store-ops-demo",
            "bobabricks-storetime-mcp",
            "bobabricks-opstask-mcp",
            "worldtour_ai_catalog.bobabricks_store_ops.ops_tasks",
        ]:
            with self.assertRaises(UnsafeMutation):
                assert_safe_mutation(target, "shared", name)

    def test_fevm_allows_only_additive_trace_grants_for_owned_app(self):
        assert_safe_fevm_grant(
            "gurary-bobabricks-store-ops",
            "worldtour_ai_catalog.ai_gateway_demo",
            {"USE_SCHEMA", "CREATE_TABLE"},
        )
        with self.assertRaises(UnsafeMutation):
            assert_safe_fevm_grant(
                "bobabricks-store-ops-demo",
                "worldtour_ai_catalog.ai_gateway_demo",
                {"USE_SCHEMA"},
            )
        with self.assertRaises(UnsafeMutation):
            assert_safe_fevm_grant(
                "gurary-bobabricks-store-ops",
                "worldtour_ai_catalog.bobabricks_store_ops",
                {"SELECT"},
            )

    def test_state_contracts_differ_only_by_confluence(self):
        baseline = load_state("baseline")
        upgraded = load_state("upgraded")
        self.assertFalse(baseline.confluence_enabled)
        self.assertTrue(upgraded.confluence_enabled)
        self.assertEqual(upgraded.extra_tools, ("search_confluence",))


if __name__ == "__main__":
    unittest.main()
