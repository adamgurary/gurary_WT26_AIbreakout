#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "script.md",
    "CODEX_UPGRADE_PROMPT.txt",
    "agent-store-ops/agent.yaml",
    "agent-store-ops/system_prompt.md",
    "agent-store-ops/mcp_servers.yaml",
    "agent-store-ops/tools.yaml",
    "agent-store-ops/policies.yaml",
    "app/app.yaml",
    "app.yaml",
    "app.yml",
    "app/app.py",
    "data/store_metrics.csv",
    "data/schedules.csv",
    "data/confluence_playbooks.json",
    "data/ops_tasks.json",
    "app/services/store_ops.py",
    "tests/test_store_ops.py",
    "archive/long-version/script.md",
    "archive/long-version/BOBABRICKS_DEMO_RUNBOOK.md",
    "archive/long-version/CODEX_UPGRADE_PROMPT.txt",
]

# The short demo's spine: baseline (Genie, StoreTime, OpsTask) has no Confluence,
# the Codex upgrade adds exactly one tool (managed Atlassian/Confluence MCP), and
# Inventory MCP stays disabled. Guard against the long-version features creeping back
# into the active demo surfaces.
REQUIRED_TEXT = {
    "CODEX_UPGRADE_PROMPT.txt": [
        "Atlassian Confluence MCP",
        "search_confluence",
        "CONFLUENCE_MCP_ENABLED=true",
    ],
    "agent-store-ops/mcp_servers.yaml": [
        "INVENTORY_APP_MCP_ENABLED:-false",
        # Upgrade: managed Atlassian/Confluence MCP block must be present
        "managed_external_mcp",
        "system_ai_agent_atlassian_mcp",
    ],
    "agent-store-ops/tools.yaml": [
        # Upgrade: exactly one search_confluence tool must be present
        "search_confluence",
    ],
    "app/services/store_ops.py": [
        "class StoreOpsEngine",
        "class OpsTaskClient",
    ],
}

# These strings belong to the long version only; they must NOT appear in the
# active short-demo app surfaces.
FORBIDDEN_TEXT = {
    "scripts/start_app.py": [
        "Generate Weekly Regional Briefing",
    ],
    "app/app.py": [
        "Approve and Create OpsTask Tickets",
        "Inventory MCP",
    ],
    "agent-store-ops/mcp_servers.yaml": [
        # managed_atlassian_mcp was the old long-version key; the upgrade uses
        # managed_external_mcp with connection system_ai_agent_atlassian_mcp instead.
        "managed_atlassian_mcp",
    ],
    "app/app.yaml": [
        "Generate Weekly Regional Briefing",
    ],
}


def main():
    failures = []
    for relative_path in REQUIRED_FILES:
        if not (ROOT / relative_path).exists():
            failures.append(f"Missing {relative_path}")

    for relative_path, snippets in REQUIRED_TEXT.items():
        text = (ROOT / relative_path).read_text() if (ROOT / relative_path).exists() else ""
        for snippet in snippets:
            if snippet not in text:
                failures.append(f"Missing text in {relative_path}: {snippet}")

    for relative_path, snippets in FORBIDDEN_TEXT.items():
        text = (ROOT / relative_path).read_text() if (ROOT / relative_path).exists() else ""
        for snippet in snippets:
            if snippet in text:
                failures.append(f"Long-version text should not appear in {relative_path}: {snippet}")

    if failures:
        print("Demo validation failed:")
        for failure in failures:
            print(f"- {failure}")
        sys.exit(1)

    print("Demo validation passed.")


if __name__ == "__main__":
    main()
