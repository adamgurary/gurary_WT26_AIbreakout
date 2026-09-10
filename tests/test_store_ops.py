import unittest
from unittest.mock import patch

from app.services.store_ops import LocalDataStore, StoreOpsEngine


class StoreOpsEngineTest(unittest.TestCase):
    def setUp(self):
        self.data_store = LocalDataStore()
        self.engine = StoreOpsEngine(self.data_store)

    def test_pacific_metrics_include_store_104(self):
        metrics = self.data_store.store_metrics("Pacific")
        store_ids = {row["store_id"] for row in metrics}
        self.assertIn("104", store_ids)

    def test_store_104_is_behind_on_training(self):
        playbooks = self.data_store.playbooks()
        target = float(playbooks["FY26 Store Operations Goals"]["training_completion_target_pct"])
        metrics = {row["store_id"]: row for row in self.data_store.store_metrics("Pacific")}
        completion = float(metrics["104"]["training_completion_pct"])
        self.assertLess(completion, target)

    def test_tool_status_reflects_short_demo_tools(self):
        with patch.dict("os.environ", {"CONFLUENCE_MCP_ENABLED": "false"}, clear=False):
            tool_status = {row["tool"]: row["status"] for row in self.engine.tool_status()}

        # Short demo baseline surfaces exactly these tools; Inventory MCP,
        # Confluence, and Lakebase memory are not listed as tools.
        self.assertEqual({"Genie Agent", "StoreTime", "OpsTask"}, set(tool_status))
        self.assertEqual("configured", tool_status["Genie Agent"])
        self.assertNotIn("Confluence", tool_status)
        self.assertNotIn("Inventory MCP", tool_status)
        self.assertNotIn("Lakebase memory", tool_status)

    def test_tool_status_adds_confluence_after_upgrade(self):
        with patch.dict("os.environ", {"CONFLUENCE_MCP_ENABLED": "true"}, clear=False):
            tool_status = {row["tool"]: row["status"] for row in self.engine.tool_status()}

        self.assertEqual("configured", tool_status["Confluence"])
        self.assertIn("Genie Agent", tool_status)
        self.assertIn("StoreTime", tool_status)
        self.assertIn("OpsTask", tool_status)

    def test_field_eng_opstask_status_keeps_approval_gate_visible(self):
        status = {row["tool"]: row for row in self.engine.tool_status()}

        self.assertEqual(
            status["OpsTask"]["purpose"],
            "list follow-up tickets; create only after approval",
        )


if __name__ == "__main__":
    unittest.main()
