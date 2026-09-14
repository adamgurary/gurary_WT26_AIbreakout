# Bobabricks Demo Runbook (Adam Gurary)

This document covers everything needed to prepare, rehearse, and deliver the
Bobabricks store-operations demo. Two lanes are defined. The field-eng lane runs
in the Vector Search dogfood-staging workspace and is the primary demo surface.
The FEVM lane is Adam-owned, stable, and is the reliable fallback when the staging
workspace GCs resources.

---

## Lane Reference

### Field-Eng Lane (primary)

| Item | Value |
|---|---|
| Profile | `dbc-f7444b38` |
| Host | `https://dbc-f7444b38-7453.staging.cloud.databricks.com` |
| Workspace ID | `715783009495722` |
| Presenter app | `https://gurary-bobabricks-store-ops-715783009495722.staging.aws.databricksapps.com` |
| Warehouse | `affaf01a87dede80` (`gurary_bobabricks_warehouse`, serverless, auto-stop 10 min) |
| Genie space | `01f1afeff3071a40a14857372a9cf6a1` |
| UC data schema | `gurary_catalog.gurary_bobabricks_store_ops` (12 tables) |
| UC trace schema | `gurary_catalog.gurary_ai_gateway_demo` |
| Experiment path | `/Users/b2ac2774-a7c8-432a-b5b3-7b82bce69de8/gurary_bobabricks_store_ops_uc` |
| Lakebase project | `gurary-bobabricks` |
| StoreTime MCP | `https://gurary-bobabricks-storetime-715783009495722.staging.aws.databricksapps.com/mcp` |
| OpsTask MCP | `https://gurary-bobabricks-opstask-mcp-715783009495722.staging.aws.databricksapps.com/mcp` |

### FEVM Fallback Lane

| Item | Value |
|---|---|
| Host | `https://fevm-worldtour-ai.cloud.databricks.com` |
| Workspace ID | `7474657163903557` |
| Presenter app | `https://gurary-bobabricks-store-ops-7474657163903557.aws.databricksapps.com` |
| Warehouse | `88fd32ee6d9438ac` (shared, read-only dependency, see warnings) |

---

## One-Time Setup

### GitHub authentication

The fork lives at `adamgurary/gurary_WT26_AIbreakout`. Log in with the personal
account before any `gh` or push operations:

```bash
gh auth login
# When prompted, select GitHub.com and log in as adamgurary.
```

Verify:

```bash
gh auth status
# Should report: Logged in to github.com as adamgurary
```

### Databricks authentication (field-eng lane)

```bash
databricks auth login --profile dbc-f7444b38
# Browser opens. Complete the OAuth flow for the staging workspace.
```

Verify:

```bash
databricks auth token --profile dbc-f7444b38
# Should print a valid token.
```

### Databricks authentication (FEVM lane)

```bash
databricks auth login --profile fevm-worldtour-ai
```

---

## Building the Field-Eng Foundation

Run these commands from the fork root. Each step is idempotent. Running again
does no harm if resources already exist.

```bash
# 1. Provision all warehouse, Genie space, MCP apps, data, and traces.
python scripts/provision_field_eng.py --apply

# 2. Clone the Genie space into the field-eng workspace.
#    (Genie must exist before the Lakebase reconcile step.)
python -m deploy.genie clone-target

# 3. Reconcile the app, Lakebase branch, and any config that depends on
#    the Genie space ID produced in step 2.
python scripts/provision_field_eng.py --apply

# 4. Wire the MCP endpoints into the presenter app configuration.
python scripts/provision_field_eng.py --apply-mcp

# 5. Deploy (or redeploy) the presenter app.
python scripts/provision_field_eng.py --apply-presenter
```

After these steps, the presenter URL listed in Lane Reference above should
return a running app.

---

## Pre-Demo Checklist (run within 30 minutes of presenting)

The dogfood-staging workspace is shared infrastructure. Idle serverless
warehouses can be auto-deleted. The steps below prevent cold-start failures.

**a) Rebuild the foundation if needed**

Check whether the warehouse still exists:

```bash
databricks warehouses get affaf01a87dede80 --profile dbc-f7444b38 2>&1 | head -5
```

If the command returns a 404 or a "not found" error, re-run the full foundation
build sequence above before continuing. This already happened once during prep.

**b) Pre-warm the warehouse**

A cold serverless warehouse can 500 the first tool call. Pre-warm it by running
a trivial SQL statement at least two minutes before presenting:

```bash
databricks sql execute-statement \
  --warehouse-id affaf01a87dede80 \
  --profile dbc-f7444b38 \
  --statement "SELECT 1"
```

Wait for the response before opening the presenter.

**c) Confirm the presenter app is running**

Open the presenter URL in a browser tab. If it shows a loading spinner or
error, redeploy:

```bash
python scripts/provision_field_eng.py --apply-presenter
```

---

## The Four Baseline Prompts

Run these in order in the presenter app. The expected behavior at each step is
described below.

1. `What tools do you have?`

   Expected: the agent lists Genie, StoreTime, and OpsTask. Confluence is NOT
   listed. This confirms the baseline state.

2. `Show average versus actual training hours for my Pacific stores and flag any stores falling behind.`

   Expected: the agent queries Genie for training hours data and returns a
   table comparing average to actual, with underperforming stores highlighted.

3. `Why is Store 104 behind on training?`

   Expected: the agent calls OpsTask to pull open tasks and StoreTime to
   check schedule data for Store 104, then gives a specific explanation.

4. `What are our FY26 training goals, and how does Store 104 stack up?`

   Expected: the agent retrieves the FY26 goals and gives an honest gap
   assessment for Store 104. It does NOT cite Confluence because Confluence
   is not connected in the baseline state.

---

## Validation

Run the static and live acceptance checks:

```bash
# Field-eng lane
python scripts/validate_stack.py \
  --target field_eng \
  --expected-state baseline \
  --output /tmp/validate-field-eng-$(date +%Y%m%dT%H%M%SZ).json

# FEVM lane
python scripts/validate_stack.py \
  --target fevm \
  --expected-state baseline \
  --output /tmp/validate-fevm-$(date +%Y%m%dT%H%M%SZ).json
```

Both commands must exit 0 before presenting.

---

## Upgrade Flow (Confluence-Connected State)

The `demo-upgraded-reference` branch adds one tool (`search_confluence`) and
wires the managed `system_ai_agent_atlassian_mcp` MCP into the app.

### Pre-requisite: authorize the Atlassian MCP connection

The workspace owner (or an admin) must authorize the connection once per
workspace via U2M OAuth before the upgrade can work:

Open this URL in a browser while authenticated to the staging workspace:

```
https://dbc-f7444b38-7453.staging.cloud.databricks.com/explore/connections/system_ai_agent_atlassian_mcp
```

Complete the OAuth flow. The connection status should change to "Connected".

### Deploy the upgrade

```bash
python scripts/provision_field_eng.py --apply-mcp   # adds Confluence MCP
python scripts/provision_field_eng.py --apply-presenter
```

After the upgrade, prompt 1 (`What tools do you have?`) will list Confluence
alongside Genie, StoreTime, and OpsTask.

### Return to baseline after the demo

```bash
# Revert MCP config and redeploy.
python scripts/provision_field_eng.py --apply-mcp --state baseline
python scripts/provision_field_eng.py --apply-presenter
```

Confirm prompt 1 no longer lists Confluence before leaving the session.

---

## Creating a Disposable Rehearsal Worktree

To practice the demo on an isolated copy of the baseline code without
touching the live fork:

```bash
python scripts/new_demo_worktree.py
```

This creates a timestamped directory under
`/Users/adam.gurary/Desktop/bobabricks-rehearsals/` checked out at the
`demo-baseline-v1` tag. The output line labelled `Omnigent working directory`
gives the path to open in Omnigent. The rehearsal worktree is disposable.
Delete it when done:

```bash
git worktree remove /Users/adam.gurary/Desktop/bobabricks-rehearsals/rehearsal-<TIMESTAMP>
```

---

## Failover to FEVM

The field-eng lane lives in a shared staging workspace that recycles resources.
If the field-eng presenter fails during the demo and cannot be recovered in
under one minute, open the FEVM presenter:

```
https://gurary-bobabricks-store-ops-7474657163903557.aws.databricksapps.com
```

Run the same four prompts. The demo continues without interruption. The FEVM
app is Adam-owned and always RUNNING.

---

## Known Gaps and Follow-Ups

**1. Lakebase persistent memory is degraded on the field-eng presenter.**

The app service principal cannot currently OAuth-authenticate to Lakebase.
The grant mechanism needed is a Databricks Lakebase principal-to-role assignment.
A plain `CREATE ROLE` statement is not sufficient. Until this is resolved,
the presenter agent does not persist memory across conversation turns.

The demo works without persistent memory. The agent still answers all four
prompts correctly. It does not remember the context from an earlier
session. Do not hide this limitation if asked.

**2. Warehouse cold-start.**

A serverless warehouse that was idle for more than 10 minutes will be in a
stopped state. The first tool call after a cold start can return a 500 error.
The pre-warm step in the Pre-Demo Checklist above prevents this. If it happens
during the demo, wait 10 seconds and re-run the failing prompt.

---

## Strict Boundaries: What You Must Never Touch

These resources belong to Amber. Deploying, stopping, resetting, or changing
permissions on any of them will break the shared environment for others.

- App: `bobabricks-store-ops-demo`
- MCP apps: `bobabricks-storetime-mcp`, `bobabricks-opstask-mcp`
- Genie space: `01f1968067e81bd09eacb0d88bcc57de`
- Warehouse: `88fd32ee6d9438ac`
- Catalog and schema: `worldtour_ai_catalog.bobabricks_store_ops`
- Managed MCP connection: `system_ai_agent_atlassian_mcp` (read-only; do not delete or modify)

The FEVM warehouse `88fd32ee6d9438ac` must be started before the FEVM presenter
can answer queries, but it must not be stopped, resized, or reconfigured.

---

## Quick Reference Links

| Resource | URL |
|---|---|
| Field-eng presenter | `https://gurary-bobabricks-store-ops-715783009495722.staging.aws.databricksapps.com` |
| FEVM presenter | `https://gurary-bobabricks-store-ops-7474657163903557.aws.databricksapps.com` |
| Field-eng warehouse | `https://dbc-f7444b38-7453.staging.cloud.databricks.com/sql/warehouses/affaf01a87dede80` |
| Field-eng Genie space | `https://dbc-f7444b38-7453.staging.cloud.databricks.com/genie/rooms/01f1afeff3071a40a14857372a9cf6a1` |
| Field-eng UC schema | `https://dbc-f7444b38-7453.staging.cloud.databricks.com/explore/data/gurary_catalog/gurary_bobabricks_store_ops` |
| Field-eng experiment | `https://dbc-f7444b38-7453.staging.cloud.databricks.com/ml/experiments` (search `gurary_bobabricks_store_ops_uc`) |
| Lakebase project | `https://dbc-f7444b38-7453.staging.cloud.databricks.com/lakebase/projects/gurary-bobabricks` |
| Atlassian MCP connection | `https://dbc-f7444b38-7453.staging.cloud.databricks.com/explore/connections/system_ai_agent_atlassian_mcp` |
