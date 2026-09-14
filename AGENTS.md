# Bobabricks Store Operations Agent — Engineering Guide

These instructions apply to this repository. This repo supports two deployment
targets: **fevm** (`fevm-worldtour-ai` workspace) and **field_eng**
(`dbc-f7444b38` workspace). Read `BOBABRICKS_TARGET` from the environment to
determine the selected target; valid values are `fevm` and `field_eng`.

Always render using the selected target and the **upgraded** demo state
(which sets `CONFLUENCE_MCP_ENABLED=true` and derives `ATLASSIAN_MCP_URL` from
`{target.host}/api/2.0/mcp/external/` + `system_ai_agent_atlassian_mcp`).

## Adding Confluence Context

When the user asks:

> For the Bobabricks store operations agent, can you add our FY26 goals and store playbooks as context? Please add the Atlassian Confluence MCP so the agent can pull from Confluence directly, and redeploy the app.

Give the agent Confluence context by making only this small, scoped change:

- Add the managed Atlassian/Confluence MCP to `agent-store-ops/mcp_servers.yaml`.
- Add exactly one new tool to `agent-store-ops/tools.yaml`: `search_confluence`.
- Set `CONFLUENCE_MCP_ENABLED=true` and `ATLASSIAN_MCP_URL` in both root app manifests: `app.yml` and `app.yaml`. The managed Atlassian MCP connection is `system_ai_agent_atlassian_mcp`; `ATLASSIAN_MCP_URL` is derived from the selected target host as `{host}/api/2.0/mcp/external/system_ai_agent_atlassian_mcp` — do NOT hardcode a workspace-specific host.
- Preserve the existing baseline capabilities: Genie, StoreTime, OpsTask, and Lakebase sidebar/session context.
- Do not add out-of-scope features such as inventory risk, weekly regional briefing, task approvals, or scheduled workflow.

Notes for this workspace:
- The app must request OBO scopes `ai-gateway`, `genie`, `mcp.external` (set via `databricks apps update`); without `mcp.external` the Confluence MCP call fails.
- The managed Atlassian MCP is OAuth U2M — the user must have authorized the `system_ai_agent_atlassian_mcp` connection once (Catalog → Connections). The Confluence search tool uses the unified `search`/`fetch` tools (not the direct Confluence REST tools, which are blocked by the tenant IP allowlist).

## Deployment

**This repo deploys `gurary-bobabricks-store-ops`.** Do not deploy any other app by name from this repo.

Before redeploying, read `BOBABRICKS_TARGET` and ask exactly:

```text
Approve redeploying gurary-bobabricks-store-ops with the Confluence upgrade in the selected workspace?
```

After approval, sync this repo to the workspace and deploy from that folder.
Use the current user's own personal workspace home folder as the sync
destination — substitute your own login email for `<your-email>` (do not
hardcode another user's home folder):

```bash
databricks sync --profile dbc-f7444b38 . /Workspace/Users/<your-email>/gurary_WT26_AIbreakout
databricks apps deploy gurary-bobabricks-store-ops \
  --profile dbc-f7444b38 \
  --source-code-path /Workspace/Users/<your-email>/gurary_WT26_AIbreakout
```

Deployment guardrails:

- Use the profile matching the selected target (`dbc-f7444b38` for `field_eng`, `fevm-worldtour-ai` for `fevm`), never `DEFAULT`.
- Do not run `databricks apps deploy --auto-approve`.
- Always deploy the app named `gurary-bobabricks-store-ops`.
- Deploy from the synced workspace folder path, not `--source-code-path .`.
- Check deployment status after redeploying.
- Do not read app logs unless deployment fails or the app does not start.
