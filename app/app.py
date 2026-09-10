import json
import os
import re
import time
import uuid

import requests
import streamlit as st
from databricks.sdk import WorkspaceClient

from bobabricks_agent import AGENT_MODEL, run_bobabricks_agent
from services.lakebase_memory import LakebaseMemoryClient
from services.store_ops import LocalDataStore, OpsTaskClient, StoreOpsEngine

MAS_ENDPOINT_NAME = os.environ.get("MAS_ENDPOINT_NAME", "").strip()
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f1968067e81bd09eacb0d88bcc57de").strip()
STORETIME_MCP_URL = os.environ.get(
    "STORETIME_MCP_URL",
    "https://bobabricks-storetime-mcp-7474657163903557.aws.databricksapps.com/mcp",
).strip()
OPSTASK_MCP_URL = os.environ.get(
    "OPSTASK_MCP_URL",
    "https://bobabricks-opstask-mcp-7474657163903557.aws.databricksapps.com/mcp",
).strip()
DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "88fd32ee6d9438ac").strip()
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "bobabricks_demo").strip()
DATABRICKS_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "store_ops").strip()
DATABRICKS_TABLE_PREFIX = os.environ.get("DATABRICKS_TABLE_PREFIX", "").strip()
CONFLUENCE_MCP_ENABLED = os.environ.get("CONFLUENCE_MCP_ENABLED", "false").strip().lower() in {"1", "true", "on", "yes"}
ATLASSIAN_MCP_URL = os.environ.get("ATLASSIAN_MCP_URL", "").strip()


def configure_mlflow_autologging() -> None:
    try:
        import mlflow
    except Exception:
        return

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "").strip()
    registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", "").strip()
    experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID", "").strip()
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "").strip()

    try:
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        if registry_uri:
            mlflow.set_registry_uri(registry_uri)
        if experiment_name:
            os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
            mlflow.set_experiment(experiment_name=experiment_name)
        elif experiment_id:
            mlflow.set_experiment(experiment_id=experiment_id)
        mlflow.openai.autolog()
    except Exception:
        return


st.set_page_config(
    page_title="Bobabricks Powered by Databricks",
    page_icon="🧋",
    layout="centered",
)

configure_mlflow_autologging()

st.markdown(
    """
    <style>
    @keyframes blink {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.25; }
    }
    .block-container { max-width: 880px; padding-top: 2rem; }
    div[data-testid="stSidebar"] { background: #faf7f2; border-right: 1px solid #eadfd3; }
    .bb-intro { color: #59463a; font-size: 1.02rem; line-height: 1.55; margin-bottom: 1.2rem; }
    .lakebase-panel {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2d3a4a;
        border-radius: 8px;
        padding: 14px;
        margin-bottom: 12px;
        color: #e2e8f0;
        font-size: 13px;
    }
    .lakebase-panel .label { color: #94a3b8; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px; }
    .lakebase-panel .value { color: #f1f5f9; font-weight: 500; font-family: monospace; font-size: 12px; overflow-wrap: anywhere; }
    .db-dot {
        animation: blink 1s ease-in-out infinite;
        background-color: #22c55e;
        border-radius: 50%;
        display: inline-block;
        height: 10px;
        margin-right: 6px;
        vertical-align: middle;
        width: 10px;
    }
    .db-dot-off { animation: none; background-color: #9ca3af; }
    .tool-pill { border: 1px solid #d8c8b8; border-radius: 999px; display: inline-block; font-size: 0.82rem; margin: 0.15rem 0.15rem 0.15rem 0; padding: 0.22rem 0.55rem; }
    .tool-pill-ok { background: #f0fbf4; border-color: #b7dfc5; color: #067647; }
    .tool-pill-warn { background: #fff7f5; border-color: #f1c7c1; color: #b42318; }
    </style>
    """,
    unsafe_allow_html=True,
)


def response_for_prompt(prompt: str, selected_region: str, engine: StoreOpsEngine, ops_tasks: OpsTaskClient, status_rows: list[dict]) -> str:
    lowered = prompt.lower()
    st.session_state["tool_status"] = None

    if "tool" in lowered:
        return render_live_tool_registry(selected_region, engine, status_rows)

    if is_training_hours_comparison(prompt):
        return render_training_hours_markdown(selected_region, engine)

    if "fy26" in lowered or "goal" in lowered or "standard" in lowered:
        return render_fy26_goals_markdown(selected_region, engine)

    if "104" in lowered and "training" in lowered:
        return (
            "Store 104 is behind because scheduled training time was converted to rush-window service coverage. "
            "That is a completion/utilization issue, not a scheduling issue.\n\n"
            "StoreTime evidence shows converted training hours and a rush-window coverage reason. "
            "The right follow-up is to protect non-rush training blocks for the Store 104 team."
        )

    return (
        "Ask for available tools, average versus actual training hours, Store 104 training diagnostics, "
        "or FY26 operating standards."
    )


@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()


def clean_agent_response(full_text: str) -> str:
    text = re.sub(
        r"<name>([^<]+)</name>",
        lambda match: f"*Calling tool... {match.group(1).replace('_', ' ').replace('-', ' ')}*",
        full_text,
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text or "No response from the agent."


def record_mlflow_chat_trace(prompt: str, response: str, selected_region: str, route: str) -> None:
    try:
        import mlflow
    except Exception:
        return

    start_span = getattr(mlflow, "start_span", None)
    if not start_span:
        return

    try:
        with start_span(name="bobabricks_chat_turn") as span:
            if hasattr(span, "set_inputs"):
                span.set_inputs({"prompt": prompt, "selected_region": selected_region})
            if hasattr(span, "set_outputs"):
                span.set_outputs({"response": response[:8000]})
            if hasattr(span, "set_attribute"):
                span.set_attribute("bobabricks.route", route)
                span.set_attribute("bobabricks.model", AGENT_MODEL)
            elif hasattr(span, "set_attributes"):
                span.set_attributes({"bobabricks.route": route, "bobabricks.model": AGENT_MODEL})
    except Exception:
        return


class DatabricksSqlResourceClient:
    def __init__(self):
        self.workspace = get_workspace_client()

    @staticmethod
    def table(name: str) -> str:
        return f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{DATABRICKS_TABLE_PREFIX}{name}"

    def query(self, statement: str, parameters: dict | None = None) -> list[dict]:
        response = self.workspace.api_client.do(
            "POST",
            "/api/2.0/sql/statements",
            body={
                "warehouse_id": DATABRICKS_WAREHOUSE_ID,
                "statement": statement,
                "parameters": [
                    {"name": key, "value": str(value)}
                    for key, value in (parameters or {}).items()
                    if value is not None
                ],
                "wait_timeout": "30s",
                "on_wait_timeout": "CONTINUE",
            },
        )
        statement_id = response.get("statement_id")
        while response.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            time.sleep(1)
            response = self.workspace.api_client.do("GET", f"/api/2.0/sql/statements/{statement_id}")
        if response.get("status", {}).get("state") != "SUCCEEDED":
            raise RuntimeError(json.dumps(response.get("status", response), default=str))
        columns = [col["name"] for col in response.get("manifest", {}).get("schema", {}).get("columns", [])]
        return [dict(zip(columns, row, strict=False)) for row in response.get("result", {}).get("data_array", [])]


class JsonRpcMcpResourceClient:
    def __init__(self, url: str):
        self.url = url
        self.workspace = get_workspace_client()

    def call_tool(self, name: str, arguments: dict | None = None) -> list[dict] | dict:
        headers = self.workspace.config.authenticate()
        headers["Content-Type"] = "application/json"
        response = requests.post(
            self.url,
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": str(uuid.uuid4()),
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments or {}},
            },
            timeout=45,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"].get("message", payload["error"]))
        content = payload.get("result", {}).get("content", [])
        if not content:
            return {}
        text = content[0].get("text", "{}")
        return json.loads(text)

    def list_tools(self) -> list[dict]:
        headers = self.workspace.config.authenticate()
        headers["Content-Type"] = "application/json"
        response = requests.post(
            self.url,
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": str(uuid.uuid4()),
                "method": "tools/list",
                "params": {},
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"].get("message", payload["error"]))
        return payload.get("result", {}).get("tools", [])


class DirectStoreOpsDataStore(LocalDataStore):
    def __init__(self):
        super().__init__()
        self.sql = DatabricksSqlResourceClient()

    def store_metrics(self, region: str) -> list[dict[str, str]]:
        rows = self.sql.query(
            f"""
            SELECT region, store_id, store_name, sales_vs_plan_pct, training_completion_pct,
                   avg_wait_minutes, customer_satisfaction
            FROM {self.sql.table("store_metrics")}
            WHERE lower(region) = lower(:region)
            ORDER BY store_id
            """,
            {"region": region},
        )
        return [{key: str(value) for key, value in row.items()} for row in rows]

    def schedules(self) -> dict[str, dict[str, str]]:
        rows = self.sql.query(
            f"""
            SELECT store_id, week, scheduled_training_hours, completed_training_hours,
                   converted_training_hours, conversion_reason
            FROM {self.sql.table("schedules")}
            ORDER BY store_id
            """
        )
        return {str(row["store_id"]): {key: str(value or "") for key, value in row.items()} for row in rows}

    def inventory(self) -> list[dict[str, str]]:
        rows = self.sql.query(
            f"""
            SELECT store_id, ingredient, on_hand_units, forecast_7d_units, reorder_eta_days, stockout_risk
            FROM {self.sql.table("inventory")}
            ORDER BY store_id, ingredient
            """
        )
        return [{key: str(value) for key, value in row.items()} for row in rows]

    def regions(self) -> list[str]:
        rows = self.sql.query(
            f"""
            SELECT DISTINCT region
            FROM {self.sql.table("store_metrics")}
            ORDER BY region
            """
        )
        return [str(row["region"]) for row in rows]

    def existing_tasks(self) -> list[dict]:
        rows = self.sql.query(
            f"""
            SELECT task_id, store_id, title, description, category, severity, status, created_date, owner
            FROM {self.sql.table("ops_tasks")}
            ORDER BY created_date DESC, task_id
            LIMIT 100
            """
        )
        return rows


class DirectOpsTaskClient(OpsTaskClient):
    def __init__(self, data_store: LocalDataStore):
        super().__init__(data_store)
        self.mcp = JsonRpcMcpResourceClient(OPSTASK_MCP_URL)

    def list_tasks(self) -> list[dict]:
        return self.mcp.call_tool("list_existing_ops_tasks", {})

    def create_tasks(self, drafts) -> list[dict]:
        created = []
        for draft in drafts:
            created.append(
                self.mcp.call_tool(
                    "create_ops_task",
                    {
                        "store_id": int(draft.store_id) if str(draft.store_id).isdigit() else draft.store_id,
                        "title": draft.title,
                        "description": draft.description,
                        "category": draft.category,
                        "severity": draft.severity,
                    },
                )
            )
        return created


def build_resource_clients() -> tuple[LocalDataStore, OpsTaskClient, bool]:
    try:
        data = DirectStoreOpsDataStore()
        data.regions()
        return data, DirectOpsTaskClient(data), True
    except Exception:
        data = LocalDataStore()
        return data, OpsTaskClient(data), False


def render_training_hours_markdown(region: str, engine: StoreOpsEngine) -> str:
    metrics = engine.data_store.store_metrics(region)
    try:
        storetime = JsonRpcMcpResourceClient(STORETIME_MCP_URL)
        storetime_rows = []
        for metric in metrics:
            result = storetime.call_tool("inspect_schedule", {"store_id": int(metric["store_id"])})
            storetime_rows.extend(result if isinstance(result, list) else [result])
        schedules = {str(row["store_id"]): {key: str(value or "") for key, value in row.items()} for row in storetime_rows}
        storetime_source = "StoreTime MCP `inspect_schedule`"
    except Exception:
        schedules = engine.data_store.schedules()
        storetime_source = "StoreTime fallback schedule table"
    targets = engine.data_store.playbooks()["FY26 Store Operations Goals"]
    target_training = float(targets["training_completion_target_pct"])

    rows = []
    total_scheduled = 0.0
    total_completed = 0.0
    for metric in metrics:
        schedule = schedules.get(metric["store_id"], {})
        scheduled = float(schedule.get("scheduled_training_hours") or 0)
        completed = float(schedule.get("completed_training_hours") or 0)
        converted = float(schedule.get("converted_training_hours") or 0)
        completion = float(metric["training_completion_pct"])
        total_scheduled += scheduled
        total_completed += completed
        rows.append(
            {
                "store_id": metric["store_id"],
                "store_name": metric["store_name"],
                "scheduled": scheduled,
                "completed": completed,
                "converted": converted,
                "completion": completion,
                "reason": schedule.get("conversion_reason") or "none",
                "behind": completion < target_training,
            }
        )

    avg_scheduled = total_scheduled / len(rows) if rows else 0
    avg_completed = total_completed / len(rows) if rows else 0
    avg_actual_pct = (total_completed / total_scheduled * 100) if total_scheduled else 0

    lines = [
        f"### {region} training hours",
        "",
        "**Evidence used**",
        f"- **Genie/data resource:** store-level training completion metrics from `{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.store_metrics`.",
        f"- **StoreTime:** scheduled, completed, and converted training hours from {storetime_source}.",
        "",
        (
            f"Across {len(rows)} {region} stores, average scheduled training is "
            f"{avg_scheduled:.1f} hours and average actual completed training is {avg_completed:.1f} hours "
            f"({avg_actual_pct:.0f}% of scheduled hours). The FY26 completion target is {target_training:.0f}%."
        ),
        "",
        "| Store | Scheduled | Actual completed | Completion | Converted | Behind? | Evidence |",
        "|---|---:|---:|---:|---:|---|---|",
    ]

    for row in rows:
        behind = "Yes" if row["behind"] else "No"
        evidence = (
            f"{row['converted']:.0f} hours converted for {row['reason']}"
            if row["converted"]
            else "no converted training hours"
        )
        lines.append(
            f"| {row['store_id']} - {row['store_name']} | {row['scheduled']:.0f} | "
            f"{row['completed']:.0f} | {row['completion']:.0f}% | {row['converted']:.0f} | "
            f"{behind} | {evidence} |"
        )

    behind_rows = [row for row in rows if row["behind"]]
    if behind_rows:
        stores = ", ".join(f"Store {row['store_id']}" for row in behind_rows)
        lines.extend(
            [
                "",
                f"**Flagged stores:** {stores}.",
                "Store 104 is the clearest issue: 25 of 42 scheduled hours were completed, and "
                "14 training hours were converted to rush-window service coverage.",
            ]
        )
    else:
        lines.extend(["", "**Flagged stores:** none."])

    return "\n".join(lines)


def render_fy26_goals_markdown(region: str, engine: StoreOpsEngine) -> str:
    if not CONFLUENCE_MCP_ENABLED:
        return (
            "I can compare Store 104's training performance from Genie and StoreTime, but I do not have a company-context "
            "tool connected yet for the FY26 training goals or operating standards. Store 104 is the clearest Pacific-region "
            "training issue: completed training is behind schedule because protected training blocks were converted to "
            "rush-window service coverage.\n\n"
            "Sources: Genie, StoreTime."
        )

    playbooks = engine.data_store.playbooks()
    goals = playbooks["FY26 Store Operations Goals"]
    training_standard = playbooks["Training Completion Operating Standard"]["guidance"]
    target_training = float(goals["training_completion_target_pct"])
    target_wait = float(goals["customer_wait_target_minutes"])
    metrics = engine.data_store.store_metrics(region)
    schedules = engine.data_store.schedules()
    store_104 = next((row for row in metrics if str(row["store_id"]) == "104"), None)
    schedule_104 = schedules.get("104", {})

    if not store_104:
        return (
            "I found the FY26 goals in Confluence, but Store 104 is not in the selected region. "
            "Switch the region to Pacific to compare Store 104."
        )

    completion = float(store_104["training_completion_pct"])
    gap = target_training - completion
    completed_hours = float(schedule_104.get("completed_training_hours") or 0)
    scheduled_hours = float(schedule_104.get("scheduled_training_hours") or 0)
    converted_hours = float(schedule_104.get("converted_training_hours") or 0)
    conversion_reason = schedule_104.get("conversion_reason") or "none"

    return "\n".join(
        [
            "### FY26 training goals and Store 104",
            "",
            "**Confluence operating standard**",
            f"- Training completion target: **{target_training:.0f}%**.",
            f"- Customer wait target: **{target_wait:.0f} minutes**.",
            f"- Training standard: {training_standard}",
            "",
            "**Store 104 comparison**",
            f"- Store 104 training completion is **{completion:.0f}%**, which is **{gap:.0f} points below** the FY26 target.",
            f"- StoreTime shows **{completed_hours:.0f} of {scheduled_hours:.0f}** scheduled training hours completed.",
            f"- **{converted_hours:.0f} hours** were converted to service coverage for **{conversion_reason}**.",
            "",
            "**Answer**",
            "Store 104 is not missing scheduled training capacity; it is losing protected training time to service coverage. "
            "That makes this a completion/utilization problem. The right next step is to protect the training blocks and, "
            "if you approve, create an OpsTask follow-up for the Store 104 manager.",
        ]
    )


def render_live_tool_registry(
    region: str,
    engine: StoreOpsEngine,
    status_rows: list[dict],
    persist_to_session: bool = True,
) -> str:
    checks = []

    try:
        metrics = engine.data_store.store_metrics(region)
        checks.append(
            {
                "tool": "Genie/data resource",
                "status": "reachable",
                "purpose": "governed store metrics and first-pass inventory",
                "evidence": f"Queried {len(metrics)} {region} store metric rows.",
            }
        )
    except Exception as exc:
        checks.append(
            {
                "tool": "Genie/data resource",
                "status": "unreachable",
                "purpose": "governed store metrics and first-pass inventory",
                "evidence": exc.__class__.__name__,
            }
        )

    mcp_apps = (
        (
            "StoreTime",
            STORETIME_MCP_URL,
            "schedules, training conversion, and coverage reasons",
            "App resource `bobabricks-storetime-mcp` is attached with CAN_USE.",
        ),
        (
            "OpsTask",
            OPSTASK_MCP_URL,
            "approval-gated follow-up task listing and creation",
            "App resource `bobabricks-opstask-mcp` is attached with CAN_USE.",
        ),
    )
    for label, url, purpose, evidence in mcp_apps:
        checks.append(
            {
                "tool": label,
                "status": "configured" if url else "not configured",
                "purpose": purpose,
                "evidence": evidence if url else "No MCP URL in app env.",
            }
        )

    if CONFLUENCE_MCP_ENABLED:
        checks.append(
            {
                "tool": "Confluence",
                "status": "configured" if ATLASSIAN_MCP_URL else "not configured",
                "purpose": "FY26 goals and operating playbooks",
                "evidence": "Managed Atlassian MCP enabled for this deployment."
                if ATLASSIAN_MCP_URL
                else "No ATLASSIAN_MCP_URL in app env.",
            }
        )

    # Confluence/Atlassian is intentionally absent from the baseline tool inventory
    # and only appears after the live upgrade enables it.

    if persist_to_session:
        st.session_state["tool_status"] = checks
    lines = [
        "Here are the Bobabricks tools attached to this demo:",
        "",
        "| Tool | Live status | What it does | Check result |",
        "|---|---|---|---|",
    ]
    for check in checks:
        lines.append(
            f"| {check['tool']} | {check['status']} | {check['purpose']} | {check['evidence']} |"
        )
    return "\n".join(lines)


def render_store_ops_context(region: str, engine: StoreOpsEngine, ops_tasks: OpsTaskClient) -> str:
    metrics = engine.data_store.store_metrics(region)
    store_ids = {str(row["store_id"]) for row in metrics}
    schedules = {
        store_id: schedule
        for store_id, schedule in engine.data_store.schedules().items()
        if str(store_id) in store_ids
    }
    inventory = [
        row
        for row in engine.data_store.inventory()
        if str(row.get("store_id")) in store_ids
    ]
    playbooks = engine.data_store.playbooks()
    try:
        tasks = [
            task
            for task in ops_tasks.list_tasks()
            if str(task.get("store_id")) in store_ids
        ]
    except Exception:
        tasks = []

    context = {
        "selected_region": region,
        "store_metrics": metrics,
        "training_schedules": schedules,
        "inventory": inventory,
        "playbooks": playbooks,
        "existing_ops_tasks": tasks,
    }
    return json.dumps(context, indent=2)


def stream_mas(messages: list[dict]):
    workspace = get_workspace_client()
    url = f"{workspace.config.host.rstrip('/')}/serving-endpoints/{MAS_ENDPOINT_NAME}/invocations"
    headers = workspace.config.authenticate()
    headers["Content-Type"] = "application/json"

    response = requests.post(
        url,
        headers=headers,
        json={"input": messages, "stream": True},
        stream=True,
        timeout=300,
    )
    response.raise_for_status()

    emitted_tools = set()
    for line in response.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]":
            break
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue

        event_type = event.get("type", "")
        if event_type in ("response.output_item.added", "response.output_item.done"):
            item = event.get("item", {})
            tool_name = item.get("name") if item.get("type") == "function_call" else None
            for block in item.get("content", []):
                text = block.get("text", "").strip()
                match = re.match(r"^<name>([^<]+)</name>$", text)
                if match:
                    tool_name = match.group(1)
            if tool_name and tool_name not in emitted_tools:
                emitted_tools.add(tool_name)
                yield f"\n\n*Calling tool... {tool_name.replace('_', ' ').replace('-', ' ')}*\n\n"

        if event_type == "response.output_text.delta":
            yield event.get("delta", "")
        elif "delta" in event:
            delta = event["delta"]
            yield delta if isinstance(delta, str) else delta.get("content", "")


def query_mas(messages: list[dict]) -> str:
    workspace = get_workspace_client()
    url = f"{workspace.config.host.rstrip('/')}/serving-endpoints/{MAS_ENDPOINT_NAME}/invocations"
    headers = workspace.config.authenticate()
    headers["Content-Type"] = "application/json"

    response = requests.post(url, headers=headers, json={"input": messages}, timeout=300)
    response.raise_for_status()
    data = response.json()

    parts = []
    for item in data.get("output", []):
        if item.get("type") == "message":
            for block in item.get("content", []):
                if block.get("type") == "output_text":
                    parts.append(block["text"])
    return clean_agent_response("\n\n".join(parts))


def build_agent_messages(prompt: str, prior_messages: list[dict], selected_region: str, status_rows: list[dict]) -> list[dict]:
    tool_context = "\n".join(
        f"- {row['tool']} ({row['status']}): {row['purpose']}" for row in status_rows
    )
    confluence_guidance = (
        "Confluence is connected for FY26 standards and operating playbooks. "
        if CONFLUENCE_MCP_ENABLED
        else "Confluence is not connected in the baseline demo; if FY26 standards are needed, say that no company-context tool is available yet. "
    )
    system_context = (
        "You are the Bobabricks store operations demo assistant. "
        f"The selected region is {selected_region}. "
        "Use the attached Databricks resources and MCP tools when answering: Genie for metrics, "
        "StoreTime for scheduling/training evidence, and OpsTask for follow-up tasks. "
        f"{confluence_guidance}"
        "When you use a tool, briefly name what evidence came from it.\n\n"
        f"Available tools:\n{tool_context}"
    )
    conversation = [{"role": "system", "content": system_context}]
    conversation.extend(prior_messages[-8:])
    conversation.append({"role": "user", "content": prompt})
    return conversation


def tool_call_order(prompt: str) -> list[tuple[str, str]]:
    lowered = prompt.lower()
    calls: list[tuple[str, str]] = []

    if "tool" in lowered:
        calls.append(("MCP registry", "list available tools, status, and purpose"))
        return calls

    if any(term in lowered for term in ("average", "actual", "training", "store")):
        calls.append(("Genie Agent", "query governed store metrics and training performance"))

    if any(term in lowered for term in ("training", "schedule", "scheduling", "104", "storetime", "completion")):
        calls.append(("StoreTime", "inspect training blocks, converted hours, and coverage reasons"))

    if any(term in lowered for term in ("task", "tasks")):
        calls.append(("OpsTask", "list existing follow-ups"))

    return calls


def render_tool_call_order(prompt: str) -> str:
    lines = ["**Tool call order**"]
    for index, (tool, action) in enumerate(tool_call_order(prompt), start=1):
        lines.append(f"{index}. **{tool}** - {action}")
    return "\n".join(lines)


def local_tool_response(
    prompt: str,
    selected_region: str,
    engine: StoreOpsEngine,
    ops_tasks: OpsTaskClient,
    status_rows: list[dict],
) -> str:
    return f"{render_tool_call_order(prompt)}\n\n{response_for_prompt(prompt, selected_region, engine, ops_tasks, status_rows)}"


def is_training_hours_comparison(prompt: str) -> bool:
    lowered = prompt.lower()
    return "training" in lowered and any(term in lowered for term in ("average", "actual", "hours"))


def should_preserve_structured_response(prompt: str) -> bool:
    return is_training_hours_comparison(prompt)


data_store, ops_tasks, _direct_resources_enabled = build_resource_clients()
engine = StoreOpsEngine(data_store)
memory = LakebaseMemoryClient()

regions = data_store.regions()
region_options = sorted(set(regions) | {"Pacific", "Mountain", "Atlantic"})
if "selected_region" not in st.session_state:
    st.session_state.selected_region = "Pacific" if "Pacific" in region_options else region_options[0]
if "messages" not in st.session_state:
    st.session_state.messages = []
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = str(uuid.uuid4())

selected_region = st.session_state.selected_region
status_rows = engine.tool_status()
memory_status = memory.status()
memory.remember_preference("demo_user", "selected_region", selected_region)

st.title("🧋 Bobabricks")
st.caption("Powered by Databricks")
st.markdown(
    """
    <div class="bb-intro">
    Bobabricks is a premium boba tea company running a store operations demo on Databricks.
    This assistant helps regional leaders inspect FY26 training goals, store execution risk,
    and scheduling evidence across Genie, StoreTime, and OpsTask.
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    dot_class = "db-dot" if memory_status.enabled else "db-dot db-dot-off"
    status_text = "Connected" if memory_status.enabled else "Disconnected"
    st.markdown(
        f"""
        <div class="lakebase-panel">
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <span class="{dot_class}"></span>
                <strong style="font-size: 14px;">LAKEBASE</strong>
                <span style="margin-left: auto; font-size: 11px; color: #22c55e;">{status_text}</span>
            </div>
            <div class="label">Instance</div>
            <div class="value">{memory.instance_name}</div>
            <div class="label" style="margin-top: 8px;">Database</div>
            <div class="value">{memory.database}</div>
            <div class="label" style="margin-top: 8px;">Schema</div>
            <div class="value">{memory.schema}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(memory_status.message)

    st.divider()
    st.markdown("**Region**")
    selected_region = st.selectbox(
        "Region",
        region_options,
        index=region_options.index(st.session_state.selected_region),
        label_visibility="collapsed",
    )
    st.session_state.selected_region = selected_region
    st.caption("Region preference is written to Lakebase when available.")

    st.divider()
    st.markdown("**Connected tools**")
    for row in status_rows:
        status_class = "tool-pill-warn" if row["status"] in {"disabled", "fallback to Genie"} else "tool-pill-ok"
        st.markdown(
            f'<span class="tool-pill {status_class}">{row["tool"]}: {row["status"]}</span>',
            unsafe_allow_html=True,
        )
        st.caption(row["purpose"])

    st.divider()
    if st.button("New Chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.conversation_id = str(uuid.uuid4())
        st.rerun()

examples = [
    "What tools do you have?",
    f"Show average versus actual training hours for my {selected_region} stores and flag any stores falling behind.",
    "Why is Store 104 behind on training?",
    "What are our FY26 training goals, and how does Store 104 stack up?",
]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

last_role = st.session_state.messages[-1]["role"] if st.session_state.messages else None
if last_role in (None, "assistant"):
    st.markdown("**Try asking:**")
    for example in examples:
        if st.button(f'"{example}"', key=f"example_{example}", use_container_width=True):
            st.session_state.example_prompt = example
            st.rerun()

prompt = st.session_state.pop("example_prompt", None)
typed_prompt = st.chat_input("Ask about training, store risk, standards, or follow-up tasks...")
if typed_prompt:
    prompt = typed_prompt

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        placeholder = st.empty()
        raw_response = f"*Calling OpenAI Agents SDK... Bobabricks Store Operations Agent*\n\n{render_tool_call_order(prompt)}\n\n"
        placeholder.markdown(raw_response)

        sdk_response = None
        if should_preserve_structured_response(prompt):
            sdk_response = run_bobabricks_agent(prompt, selected_region)
            if sdk_response and "| Store |" in sdk_response:
                response = f"{render_tool_call_order(prompt)}\n\n{clean_agent_response(sdk_response)}"
                response_route = "openai_agents_sdk"
            else:
                response = local_tool_response(prompt, selected_region, engine, ops_tasks, status_rows)
                response_route = "structured_tool_response"
        else:
            sdk_response = run_bobabricks_agent(prompt, selected_region)
            if sdk_response:
                response = f"{render_tool_call_order(prompt)}\n\n{clean_agent_response(sdk_response)}"
                response_route = "openai_agents_sdk"
            else:
                response = None
                response_route = "pending_fallback"

        if not response and MAS_ENDPOINT_NAME:
            agent_messages = build_agent_messages(prompt, st.session_state.messages[:-1], selected_region, status_rows)
            try:
                mas_response = ""
                for chunk in stream_mas(agent_messages):
                    mas_response += chunk
                    placeholder.markdown(f"{render_tool_call_order(prompt)}\n\n{clean_agent_response(mas_response)}")
                response = f"{render_tool_call_order(prompt)}\n\n{clean_agent_response(mas_response)}"
                response_route = "model_serving_endpoint"
            except Exception:
                response = local_tool_response(prompt, selected_region, engine, ops_tasks, status_rows)
                response_route = "local_fallback"

        if not response:
            response = local_tool_response(prompt, selected_region, engine, ops_tasks, status_rows)
            response_route = "local_fallback"

        if response_route not in {"openai_agents_sdk", "model_serving_endpoint"}:
            record_mlflow_chat_trace(prompt, response, selected_region, response_route)
        placeholder.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})

if st.session_state.get("tool_status"):
    with st.expander("Available tool details", expanded=True):
        st.dataframe(st.session_state["tool_status"], hide_index=True, use_container_width=True)
