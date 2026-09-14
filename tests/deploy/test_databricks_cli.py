"""Unit tests for deploy.databricks_cli helpers."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch


class LiveWorkspaceIdTest(unittest.TestCase):
    def test_returns_workspace_id_from_config(self):
        mock_config = MagicMock()
        mock_config.workspace_id = "715783009495722"

        with patch("databricks.sdk.core.Config", return_value=mock_config) as MockConfig:
            from deploy.databricks_cli import live_workspace_id

            result = live_workspace_id("dogfood-vs")

        MockConfig.assert_called_once_with(profile="dogfood-vs")
        self.assertEqual(result, "715783009495722")

    def test_raises_when_no_workspace_id_configured(self):
        mock_config = MagicMock()
        mock_config.workspace_id = None

        with patch("databricks.sdk.core.Config", return_value=mock_config):
            from deploy.databricks_cli import live_workspace_id

            with self.assertRaises(RuntimeError) as ctx:
                live_workspace_id("bad-profile")

        self.assertIn("no workspace_id configured", str(ctx.exception))

    def test_raises_when_workspace_id_empty_string(self):
        mock_config = MagicMock()
        mock_config.workspace_id = ""

        with patch("databricks.sdk.core.Config", return_value=mock_config):
            from deploy.databricks_cli import live_workspace_id

            with self.assertRaises(RuntimeError):
                live_workspace_id("empty-profile")


if __name__ == "__main__":
    unittest.main()
