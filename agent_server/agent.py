import json
import logging
import os
import re
from contextlib import AsyncExitStack
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator

import httpx
import mlflow
from agents import Agent, FunctionTool, RunContextWrapper, Runner, function_tool, set_default_openai_api, set_default_openai_client
from agents.tracing import set_trace_processors
from databricks.sdk import WorkspaceClient
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import AsyncDatabricksSession, McpServer
from fastapi import HTTPException
from mlflow.genai.agent_server import get_request_headers, invoke, stream
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse, ResponsesAgentStreamEvent

from agent_server.utils import (
    deduplicate_input,
    get_databricks_host_from_env,
    get_session_id,
    lakebase_access_message,
    lakebase_config,
    process_agent_stream_events,
    warn_lakebase_unconfigured,
)

set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")
set_trace_processors([])


def _configure_uc_trace_location() -> None:
    """Bind the active MLflow experiment to a Unity Catalog trace location so traces
    are governed and queryable in UC (and appear in UC-backed trace views), instead of
    the legacy workspace/DBFS trace store. No-op unless the UC trace env vars are set.

    trace_location can only be set when the experiment is created, so MLFLOW_EXPERIMENT_NAME
    must point at a new experiment name (not a pre-existing legacy one)."""
    catalog = os.environ.get("MLFLOW_TRACE_UC_CATALOG")
    schema = os.environ.get("MLFLOW_TRACE_UC_SCHEMA")
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME")
    if not (catalog and schema and experiment_name):
        return
    from mlflow.entities.trace_location import UnityCatalog

    mlflow.set_experiment(
        experiment_name=experiment_name,
        trace_location=UnityCatalog(
            catalog_name=catalog,
            schema_name=schema,
            table_prefix=os.environ.get("MLFLOW_TRACE_UC_TABLE_PREFIX", "bobabricks"),
        ),
    )


try:
    _configure_uc_trace_location()
except Exception as exc:  # non-fatal: fall back to default tracing so the app still starts
    logging.getLogger(__name__).warning(
        "UC trace location setup failed; continuing with default tracing: %s", exc, exc_info=True
    )

mlflow.openai.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def _enrich_agent_spans() -> None:
    try:
        from mlflow.openai import _agent_tracer as tracer

        orig_name = tracer._get_span_name
        orig_parse = tracer._parse_span_data

        def friendly_tool_name(value):
            if not value:
                return value
            text = str(value)
            if re.fullmatch(r"query_space_[0-9a-f]+", text):
                return "Genie Space [bobabricks_store_operations]"
            if re.fullmatch(r"poll_response_[0-9a-f]+", text):
                return "Genie Space [bobabricks_store_operations]"
            if text in {"inspect_schedule", "list_training_events"}:
                return "MCP[bobabricks-storetime-mcp]"
            if text in {"list_existing_ops_tasks", "create_ops_task"}:
                return "MCP[bobabricks-opstask-mcp]"
            return value

        def name(span_data):
            span_type = getattr(span_data, "type", "")
            if span_type == "turn":
                return f"Turn #{getattr(span_data, 'turn', '?')}"
            if span_type == "mcp_tools":
                return f"MCP: list tools ({getattr(span_data, 'server', '?')})"
            existing = getattr(span_data, "name", None)
            existing = friendly_tool_name(existing)
            return existing if existing else orig_name(span_data)

        def parse(span_data):
            inputs, outputs, attributes = orig_parse(span_data)
            attributes = dict(attributes or {})
            span_type = getattr(span_data, "type", "")
            if span_type == "mcp_tools":
                tools = [
                    friendly_tool_name(tool)
                    for tool in list(getattr(span_data, "result", None) or [])
                ]
                attributes["server"] = getattr(span_data, "server", None)
                attributes["tool_count"] = len(tools)
                outputs = {"tools": tools}
            elif span_type == "turn":
                attributes["turn"] = getattr(span_data, "turn", None)
                attributes["agent"] = getattr(span_data, "agent_name", None)
                if getattr(span_data, "usage", None):
                    attributes["usage"] = span_data.usage
            if attributes.get("name"):
                attributes["name"] = friendly_tool_name(attributes["name"])
            if isinstance(inputs, dict):
                inputs = {
                    key: friendly_tool_name(value) if key in {"name", "tool", "tool_name"} else value
                    for key, value in inputs.items()
                }
            return inputs, outputs, attributes

        tracer._get_span_name = name
        tracer._parse_span_data = parse
    except Exception:
        logger.debug("Could not enrich openai-agents spans.", exc_info=True)


_enrich_agent_spans()

MODEL = os.getenv("AGENT_MODEL", "databricks-gpt-5")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f1968067e81bd09eacb0d88bcc57de")
STORETIME_MCP_URL = os.getenv("STORETIME_MCP_URL", "https://bobabricks-storetime-mcp-7474657163903557.aws.databricksapps.com/mcp")
OPSTASK_MCP_URL = os.getenv("OPSTASK_MCP_URL", "https://bobabricks-opstask-mcp-7474657163903557.aws.databricksapps.com/mcp")
ATLASSIAN_MCP_URL = os.getenv("ATLASSIAN_MCP_URL")
ATLASSIAN_OFF_SENTINELS = {"", "off", "none", "disabled", "false"}
ATLASSIAN_FRIENDLY_NAME = "Atlassian Confluence"
CONFLUENCE_MCP_ENABLED = os.getenv("CONFLUENCE_MCP_ENABLED", "false").strip().lower() in {"1", "true", "on", "yes"}
USE_SDK_MCP_SERVERS = os.getenv("USE_SDK_MCP_SERVERS", "false").strip().lower() in {"1", "true", "on", "yes"}
CONFLUENCE_FIXTURE_PATH = Path(__file__).resolve().parents[1] / "data" / "confluence_playbooks.json"


def filter_mcp_tool_metadata(
    server_name: str, tools: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    read_only = os.getenv("SHARED_MCP_READ_ONLY", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if read_only and server_name == "opstask":
        return [tool for tool in tools if tool.get("name") != "create_ops_task"]
    return tools


def _runtime_instructions(instructions: str) -> str:
    if os.getenv("SHARED_MCP_READ_ONLY", "false").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return instructions
    return (
        instructions.replace(
            "- OpsTask for existing follow-up tickets and task tracking.",
            "- OpsTask to inspect follow-ups; it cannot create tasks.",
        )
        .replace(
            "Contains follow-up lookup and task\n"
            "  creation tools; Can list follow-ups and create tasks after approval; Cannot\n"
            "  create silently or diagnose root cause alone.",
            "Contains follow-up lookup; Can inspect follow-ups; Cannot create tasks or\n"
            "  diagnose root cause alone.",
        )
        .replace(
            "If asked about OpsTask creation, describe what you would create and wait\n"
            "for explicit approval before calling a create/write tool.",
            "OpsTask is inspection-only in this runtime and cannot create follow-up tasks.",
        )
        .replace(
            "Do not create an OpsTask or ask for approval in this short demo flow unless\n"
            "  the user explicitly asks to create a follow-up task.",
            "Never create an OpsTask or ask for approval in this inspection-only runtime.",
        )
    )

AGENT_INSTRUCTIONS = """\
You are the Bobabricks Store Operations Agent for regional store leaders.

Use the attached tools directly:
- Genie Space for governed Bobabricks store metrics, training completion, wait time, and regional performance.
- StoreTime for schedules, training blocks, converted hours, and coverage reasons.
- OpsTask for existing follow-up tickets and task tracking.
- Confluence/Atlassian for FY26 goals, operating standards, and playbooks when Confluence is connected.

Do not invent stores, metrics, targets, task ids, or policy. Keep demo answers
natural: do not expose internal tool availability problems, missing-source
diagnostics, or reconnect instructions unless the user explicitly asks about tool
health. Lead with the answer, then cite the tool evidence compactly. Preserve
markdown tables returned by tools for training-hour comparisons.
In the baseline state without Confluence, the official company training goal is unavailable.
State that gap exactly instead of inferring a goal from scheduled hours or prior knowledge.
Source lines must name only tools that returned evidence in the current turn. Never cite a
tool that was unavailable, failed, or was only used in a prior turn.

When asked what tools you have, answer from the configured Bobabricks tool
context in a clean demo style. Do not call a tool for this question. Do not use
the phrase "Bobabricks tool inventory." Do not use "Available" or "Unavailable"
headings. Do not add temporary-availability caveats unless the user's request is
blocked by a failed tool call. List only the connected data and MCP tools; do not
list Confluence/Atlassian unless Confluence is connected. Never list
Lakebase memory or a current-time utility as tools.
Use one compact markdown table and normal body text only. Do not use markdown
headings, bold labels, bullet lists, or repeated "what it contains / what it can
do / what it cannot do" sections. The table columns should be: Tool, Name,
Contains, Can do, Cannot do. Keep each cell short enough to scan.
For the baseline state, use these rows:
- Tool Genie Space; Name bobabricks_store_operations; Contains governed store
  metrics, region, training completion, scheduled/completed hours, wait time;
  Can do analytics, comparisons, completion tables; Cannot do tasks, schedule
  updates, approvals, or policy.
- Tool StoreTime; Name bobabricks-storetime-mcp; Contains schedules, protected
  training blocks, completed/converted hours, coverage reasons; Can do training
  root-cause diagnostics; Cannot change schedules or create tasks.
- Tool OpsTask; Name bobabricks-opstask-mcp; Contains follow-up lookup and task
  creation tools; Can list follow-ups and create tasks after approval; Cannot
  create silently or diagnose root cause alone.

For Bobabricks demo questions, prefer compact answer-first output. Do not print a
separate hardcoded "Tool call order" section; the trace is the source of truth for
tool order. If asked about OpsTask creation, describe what you would create and wait
for explicit approval before calling a create/write tool.

# Demo response style
Keep responses polished and easy to scan. Use the cleaner demo format:
- Start with a one-sentence summary.
- For store comparisons, use a compact markdown table with clear columns.
- Use status markers in tables and callouts: 🚩 for behind/high risk, ⚠️ for watch,
  ✅ for on track, and 🧋 only when referring to Bobabricks as a brand.
- For training-hour questions, include regional averages/totals before the table
  when the tools return enough data to compute them.
- For training-hour comparison or Store 104 training diagnostics, always use both
  Genie and StoreTime when available. If Genie returns no rows for a requested
  window, immediately query StoreTime/training events and answer from StoreTime
  rows before saying data is missing. Prefer the most recent period returned by
  the tools; do not invent a stricter date window than the user asked for.
- Interpret "average versus actual training hours" as scheduled/target training
  hours versus actual/completed training hours. Do not compare actual hours to
  the regional average as the behind/on-track test. Use severity markers instead
  of making every shortfall red: ✅ when completed hours meet scheduled/target
  hours; ⚠️ for ordinary shortfalls/watch items; 🚩 only for the severe material
  gap that needs immediate intervention. When the user asks to "flag any stores
  falling behind," list only 🚩 stores as flagged and keep ⚠️ stores as watch
  items.
- For the Pacific 2026-W27 demo table, use this severity classification when
  those rows are returned by the tools: Seattle Capitol Hill (101) ✅; Bellevue
  Commons (102) ⚠️; Tacoma Narrows (103) ⚠️; Seattle Pike Place (104) 🚩;
  Portland Pearl (117) ⚠️; San Francisco Market (122) ✅; San Diego Harbor
  (130) ⚠️.
- For the question "Show average versus actual training hours for my Pacific
  stores and flag any stores falling behind", keep the visible answer short:
  one summary sentence, then a table with at most 5 stores. Put Seattle Pike
  Place (104) 🚩 first. Show up to 4 additional on-track green examples only
  when available. Do not include a Notes section, root-cause analysis, next steps,
  "if you want" offers, or mention empty StoreTime results. The source line must
  list only the successful tools used in this turn.
- For Store 104 "why", "behind", or "falling behind" training questions in the
  upgraded state, use Confluence for the company goal/standard first, then use
  StoreTime for schedule/training evidence and Genie when available for the
  completion readout. The complete answer is the comparison: company standard
  from Confluence plus Store 104's evidence that 42 scheduled training hours
  became 28 completed hours after 14 hours were converted to rush-window service
  coverage, with an 81.8% training completion readout when returned by Genie.
- For FY26 goals, company training goals, operating standards, or playbook
  questions, use Confluence for the goal/policy context. Then compare those
  goals to Store 104's Genie and StoreTime evidence. If Confluence is not
  connected, still answer the operational part and add one short caveat after
  the evidence: "I don't have the official company training goal connected yet."
  Do not make missing company context the headline. Do not claim Store 104 data
  is missing. Do not write "Short answer", "What's missing", "What I do have",
  "share the FY26 goals", "reconnect Confluence", or phrases like "I'm
  answering from Genie and StoreTime data only". Keep source attribution only in
  the final source line.
- For the final demo question "What are our FY26 training goals, and how does
  Store 104 stack up?", do not include a recommendation or next step unless the
  user asks for one. In the baseline state, call Genie and StoreTime in this turn,
  then open by saying the official company training goal is unavailable while
  stating Store 104's live status. In the upgraded state, open with the goal
  returned by Confluence and Store 104's live status. Then give 2-3 evidence bullets.
- For follow-up action questions such as "What should I do next?" or "What
  should I do about Store 104?", call Confluence for the training playbook and
  StoreTime for Store 104 schedule/conversion context before answering. Then
  provide the operational next step in this exact compact format:
  "Recommended next steps:"
  1. "Add coverage:" Use the StoreTime district schedule and partner availability
     view to identify available coverage from a nearby store, ideally a
     cross-trained partner or shift lead, or add one extra rush-window floor
     coverage shift in StoreTime.
  2. "Protect training:" Re-book the missed training block outside peak hours so
     the training-coded partner can stay in training.
  3. "Confirm ownership:" Have the manager confirm the coverage plan with the
     district scheduler.
  4. "Make it repeatable:" Turn this into a weekly training-risk report that
     checks each store against FY26 completion and utilization goals, flags
     stores below target, and highlights schedule conversions that need action.
  Do not create an OpsTask or ask for approval in this short demo flow unless
  the user explicitly asks to create a follow-up task.
- Keep follow-up explanation to 2-4 bullets. Avoid long missing-source sections
  unless the user explicitly asks why a tool failed.
- End with a short source line containing only tools that returned evidence in
  this turn. Add Confluence only after the Atlassian tools are connected and used.
  Do not expose raw table names, MCP tool ids, or generated tool names unless the
  user explicitly asks for technical details.
"""

CONTEXT_INSTRUCTIONS = """\

# Company policy & documentation
Use Confluence as the source of truth for FY26 training goals, operating standards,
thresholds, and required actions. Always ground store metrics in Genie or StoreTime.
For any user question containing "FY26", "training goals", "company goals",
"operating standards", "playbook", "stack up", "falling behind", "behind on
training", "what should I do", "recommended next steps", "next steps", or
"why is Store 104 behind", call the Confluence tool before answering. For action
or next-step questions about Store 104, also call StoreTime before answering so
the recommendation is grounded in the current schedule/conversion context. The
answer must include Confluence in the final source line when that tool was
called and StoreTime when StoreTime was called.
When asked what tools you have, the app is now in the upgraded Confluence state.
Answer with exactly these four rows in one compact markdown table:
- Tool Genie Space; Name bobabricks_store_operations; Contains governed store
  metrics, region, training completion, scheduled/completed hours, wait time;
  Can do analytics, comparisons, completion tables; Cannot do tasks, schedule
  updates, approvals, or policy.
- Tool StoreTime; Name bobabricks-storetime-mcp; Contains schedules, protected
  training blocks, completed/converted hours, coverage reasons; Can do training
  root-cause diagnostics; Cannot change schedules or create tasks.
- Tool OpsTask; Name bobabricks-opstask-mcp; Contains follow-up lookup and task
  creation tools; Can list follow-ups and create tasks after approval; Cannot
  create silently or diagnose root cause alone.
- Tool Confluence; Name managed Atlassian MCP; Contains FY26 goals, operating
  standards, training playbooks; Can do company context lookup; Cannot change
  store data or tasks.
"""


@function_tool
def get_current_time() -> str:
    """Get the current date and time in ISO 8601 format."""
    return datetime.now().isoformat()


def _genie_mcp_url() -> str:
    return f"{get_databricks_host_from_env()}/api/2.0/mcp/genie/{GENIE_SPACE_ID}"


def _atlassian_url() -> str:
    if not CONFLUENCE_MCP_ENABLED:
        return ""
    if ATLASSIAN_MCP_URL is None:
        return ""
    if ATLASSIAN_MCP_URL.strip().lower() in ATLASSIAN_OFF_SENTINELS:
        return ""
    return ATLASSIAN_MCP_URL


class DirectMcpClient:
    def __init__(self, url: str, token: str, timeout: float = 30.0) -> None:
        self.url = url
        self.token = token
        self.session_id: str | None = None
        self._next_id = 1
        self._client = httpx.AsyncClient(timeout=timeout)

    async def __aenter__(self) -> "DirectMcpClient":
        await self.initialize()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Mcp-Protocol-Version": "2025-03-26",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        return headers

    def _parse_body(self, response: httpx.Response) -> dict[str, Any]:
        content_type = response.headers.get("Content-Type", "")
        if "text/event-stream" in content_type:
            for raw in response.text.splitlines():
                if raw.startswith("data:"):
                    payload = raw[len("data:"):].strip()
                    if payload:
                        return json.loads(payload)
            raise RuntimeError(f"MCP response did not include a JSON-RPC frame: {response.text[:300]!r}")
        return response.json()

    async def _send(self, method: str, params: dict[str, Any] | None = None, expect_response: bool = True):
        body: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if expect_response:
            body["id"] = self._next_id
            self._next_id += 1
        if params is not None:
            body["params"] = params
        response = await self._client.post(self.url, headers=self._headers(), json=body)
        if method == "initialize":
            self.session_id = response.headers.get("Mcp-Session-Id")
        if response.status_code >= 400:
            raise RuntimeError(f"MCP {method} HTTP {response.status_code}: {response.text[:400]}")
        if not expect_response:
            return None
        return self._parse_body(response)

    async def initialize(self) -> None:
        await self._send(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "bobabricks-direct-client", "version": "0.1"},
            },
        )
        await self._send("notifications/initialized", expect_response=False)

    async def list_tools(self) -> list[dict[str, Any]]:
        result = await self._send("tools/list", {})
        if result is None or "error" in result:
            raise RuntimeError(f"MCP tools/list error: {result}")
        return result.get("result", {}).get("tools", [])

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> str:
        result = await self._send("tools/call", {"name": name, "arguments": arguments})
        if result is None:
            return json.dumps({"error": "empty response"})
        if "error" in result:
            return json.dumps({"error": result["error"]})
        inner = result.get("result", {})
        content = inner.get("content")
        if isinstance(content, list):
            chunks = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    chunks.append(item.get("text", ""))
                else:
                    chunks.append(json.dumps(item) if isinstance(item, dict) else str(item))
            if chunks:
                return "\n".join(chunks)
        return json.dumps(inner)


def _sanitize_schema(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _sanitize_schema(v) for k, v in obj.items() if k != "$schema"}
    if isinstance(obj, list):
        return [_sanitize_schema(item) for item in obj]
    return obj


def _make_function_tool(client: DirectMcpClient, meta: dict[str, Any]) -> FunctionTool:
    tool_name = meta["name"]
    schema = _sanitize_schema(meta.get("inputSchema") or {"type": "object", "properties": {}})

    async def _invoke(ctx: RunContextWrapper, args_json: str, _tool: str = tool_name) -> str:
        try:
            args = json.loads(args_json) if args_json else {}
        except Exception:
            args = {}
        try:
            return await client.call_tool(_tool, args)
        except Exception as exc:
            return f"Tool error ({_tool}): {type(exc).__name__}: {exc}"

    return FunctionTool(
        name=tool_name,
        description=meta.get("description") or tool_name,
        params_json_schema=schema,
        on_invoke_tool=_invoke,
        strict_json_schema=False,
    )


def _make_confluence_search_tool(client: DirectMcpClient, tool_metas: list[dict[str, Any]]) -> FunctionTool:
    names = [str(meta.get("name", "")) for meta in tool_metas]
    names_lower = {name.lower(): name for name in names}

    # Tool preference order. The managed Atlassian MCP exposes both direct
    # Confluence REST tools (searchConfluenceUsingCql/getConfluencePage) and the
    # cross-product Rovo tools (search/fetch). On tenants with a Confluence IP
    # allowlist, the direct REST tools fail ("permission to connect from this IP
    # address") because they hit the tenant host, while search/fetch go through
    # api.atlassian.com and work. Prefer the unified `search` here.
    search_tool = (
        names_lower.get("search_confluence_pages")
        or names_lower.get("search")
        or next((name for name in names if "search" in name.lower()), "")
    )
    fetch_tool = names_lower.get("fetch")

    async def _invoke(ctx: RunContextWrapper, args_json: str) -> str:
        try:
            args = json.loads(args_json) if args_json else {}
        except Exception:
            args = {}

        query = str(args.get("query", "")).strip()
        limit = int(args.get("limit", 5) or 5)
        if not search_tool:
            return "Tool error (search_confluence): Atlassian Confluence MCP did not expose a search tool."
        try:
            raw = await client.call_tool(search_tool, {"query": query, "limit": limit})
        except Exception as exc:
            return f"Tool error (search_confluence): {type(exc).__name__}: {exc}"

        # When the unified `search` tool is used, results carry only a short
        # snippet. Fetch the full body of the top Confluence page(s) so the agent
        # can ground on the complete FY26 goals/standards text.
        if not (search_tool.lower() == "search" and fetch_tool):
            return raw
        try:
            parsed = json.loads(raw)
        except Exception:
            return raw
        results = parsed.get("results") if isinstance(parsed, dict) else None
        if not isinstance(results, list):
            return raw
        fetched = []
        for result in results:
            if not isinstance(result, dict):
                continue
            result_id = str(result.get("id", ""))
            if ":page/" not in result_id:
                continue
            try:
                fetched.append(await client.call_tool(fetch_tool, {"id": result_id}))
            except Exception:
                continue
            if len(fetched) >= 2:
                break
        if not fetched:
            return raw
        return json.dumps({"search_results": parsed, "pages": fetched})

    return FunctionTool(
        name="search_confluence",
        description="Search Atlassian Confluence for Bobabricks FY26 goals, operating standards, and store playbooks.",
        params_json_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search text for FY26 goals, training standards, or store playbooks.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of Confluence results to return.",
                    "default": 5,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        on_invoke_tool=_invoke,
        strict_json_schema=False,
    )


def _atlassian_token() -> str:
    try:
        headers = get_request_headers() or {}
        token = headers.get("x-forwarded-access-token")
        if not token:
            authorization = headers.get("authorization", "")
            if authorization.lower().startswith("bearer "):
                token = authorization.split(" ", 1)[1]
        if token:
            return token
    except Exception:
        pass
    return os.getenv("ATLASSIAN_MCP_BEARER_TOKEN", "")


def _databricks_mcp_token() -> str:
    try:
        headers = get_request_headers() or {}
        token = headers.get("x-forwarded-access-token")
        if not token:
            authorization = headers.get("authorization", "")
            if authorization.lower().startswith("bearer "):
                token = authorization.split(" ", 1)[1]
        if token:
            return token
    except Exception:
        pass
    return os.getenv("DATABRICKS_TOKEN", "")


def _load_confluence_fixture() -> dict[str, Any]:
    try:
        return json.loads(CONFLUENCE_FIXTURE_PATH.read_text())
    except Exception as exc:
        logger.warning("Confluence fixture unavailable: %s", exc)
        return {}


@function_tool
def search_confluence(query: str) -> str:
    """Search Bobabricks Confluence context for FY26 goals, standards, and playbooks."""
    playbooks = _load_confluence_fixture()
    if not playbooks:
        return json.dumps({"error": "Confluence context unavailable"})

    normalized = query.strip().lower()
    matches = {}
    for title, body in playbooks.items():
        haystack = f"{title} {json.dumps(body)}".lower()
        if not normalized or any(term in haystack for term in normalized.split()):
            matches[title] = body

    if not matches:
        matches = playbooks
    return json.dumps(matches)


async def build_atlassian_tools(stack: AsyncExitStack) -> tuple[list[FunctionTool], list[str]]:
    url = _atlassian_url()
    if not url:
        return [], []
    token = _atlassian_token()
    if not token:
        return [], [ATLASSIAN_FRIENDLY_NAME]
    try:
        client = await stack.enter_async_context(DirectMcpClient(url, token))
        metadata = await client.list_tools()
        metadata = filter_mcp_tool_metadata("atlassian", metadata)
        return [_make_confluence_search_tool(client, metadata)], []
    except Exception as exc:
        logger.warning("Atlassian MCP unavailable; running without it. (%s)", exc)
        return [], [ATLASSIAN_FRIENDLY_NAME]


async def build_direct_databricks_mcp_tools(stack: AsyncExitStack) -> tuple[list[FunctionTool], list[str]]:
    if os.getenv("DISABLE_MCP_SERVERS", "false").strip().lower() in {"1", "true", "on", "yes"}:
        return [], list(MCP_FRIENDLY_NAMES.values())
    token = _databricks_mcp_token()
    if not token:
        return [], list(MCP_FRIENDLY_NAMES.values())

    tools: list[FunctionTool] = []
    unavailable: list[str] = []
    for name, url in [
        ("bobabricks_genie", _genie_mcp_url()),
        ("storetime", STORETIME_MCP_URL),
        ("opstask", OPSTASK_MCP_URL),
    ]:
        if not url:
            continue
        try:
            client = await stack.enter_async_context(DirectMcpClient(url, token))
            metadata = await client.list_tools()
            metadata = filter_mcp_tool_metadata(name, metadata)
            tools.extend(_make_function_tool(client, meta) for meta in metadata)
        except Exception as exc:
            logger.warning("MCP server %r unavailable over direct transport; running without it. (%s)", name, exc)
            unavailable.append(MCP_FRIENDLY_NAMES.get(name, name))
    return tools, unavailable


def build_mcp_servers(workspace_client: WorkspaceClient) -> list[McpServer]:
    if not USE_SDK_MCP_SERVERS:
        return []
    servers = []
    if os.getenv("ENABLE_GENIE_MCP", "true").strip().lower() not in {"0", "false", "off", "no"}:
        servers.append(McpServer(url=_genie_mcp_url(), name="bobabricks_genie", workspace_client=workspace_client))
    if STORETIME_MCP_URL:
        servers.append(McpServer(url=STORETIME_MCP_URL, name="storetime", workspace_client=workspace_client))
    # SDK MCP servers are attached atomically, without per-tool metadata
    # filtering. Omit the entire raw OpsTask server whenever its write tool
    # would be filtered from direct discovery.
    sdk_opstask_metadata = filter_mcp_tool_metadata(
        "opstask", [{"name": "create_ops_task"}]
    )
    if OPSTASK_MCP_URL and sdk_opstask_metadata:
        servers.append(McpServer(url=OPSTASK_MCP_URL, name="opstask", workspace_client=workspace_client))
    return servers


MCP_FRIENDLY_NAMES = {
    "bobabricks_genie": "Genie Agent",
    "storetime": "StoreTime",
    "opstask": "OpsTask",
}


async def connect_available_mcp_servers(stack: AsyncExitStack, workspace_client: WorkspaceClient):
    available = []
    unavailable = []
    if os.getenv("DISABLE_MCP_SERVERS", "false").strip().lower() in {"1", "true", "on", "yes"}:
        return available, list(MCP_FRIENDLY_NAMES.values())
    for server in build_mcp_servers(workspace_client):
        name = getattr(server, "name", "tool")
        try:
            connected = await stack.enter_async_context(server)
            await connected.list_tools()
            available.append(connected)
        except Exception as exc:
            logger.warning("MCP server %r unavailable; running without it. (%s)", name, exc)
            unavailable.append(MCP_FRIENDLY_NAMES.get(name, name))
    return available, unavailable


async def close_tool_stack(stack: AsyncExitStack) -> None:
    try:
        await stack.aclose()
    except BaseException as exc:
        logger.warning("MCP cleanup failed after response generation. (%s)", exc, exc_info=True)


def create_agent(mcp_servers=None, unavailable_tools=None, extra_tools=None, context_tools_enabled: bool = False) -> Agent:
    instructions = _runtime_instructions(AGENT_INSTRUCTIONS)
    if context_tools_enabled:
        instructions += _runtime_instructions(CONTEXT_INSTRUCTIONS)
    if unavailable_tools:
        names = ", ".join(sorted(set(unavailable_tools)))
        instructions += (
            "\n\n# Tool availability\n"
            f"Some capabilities may be unavailable this turn: {names}. Treat this as internal runtime context. "
            "Do not mention it in the user-visible answer unless the user explicitly asks about tool health."
        )
    return Agent(
        name="Bobabricks Store Operations Agent",
        instructions=instructions,
        model=MODEL,
        tools=[get_current_time, *(extra_tools or [])],
        mcp_servers=mcp_servers or [],
    )


def maybe_create_session(session_id: str) -> AsyncDatabricksSession | None:
    if os.getenv("BOBABRICKS_DISABLE_LAKEBASE") == "1":
        return None
    if not lakebase_config.is_configured:
        warn_lakebase_unconfigured()
        return None
    return AsyncDatabricksSession(
        session_id=session_id,
        instance_name=lakebase_config.instance_name,
        autoscaling_endpoint=lakebase_config.autoscaling_endpoint,
        project=lakebase_config.autoscaling_project,
        branch=lakebase_config.autoscaling_branch,
        schema=lakebase_config.memory_schema,
        create_tables=False,
    )


def _is_lakebase_error(exc: Exception) -> bool:
    return any(keyword in str(exc).lower() for keyword in ["lakebase", "pg_hba", "postgres", "database instance", "insufficient privilege"])


def _item_text(item: Any) -> str:
    value = item.model_dump() if hasattr(item, "model_dump") else item
    content = value.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(part.get("text", "") for part in content if isinstance(part, dict))
    return ""


def _is_title_request(request: ResponsesAgentRequest) -> bool:
    for item in request.input:
        value = item.model_dump() if hasattr(item, "model_dump") else item
        if value.get("role") == "system" and "generate a short title" in _item_text(item).lower():
            return True
    return False


def _title_agent() -> Agent:
    return Agent(
        name="Conversation Titler",
        instructions="Generate a concise title. Output only the title text.",
        model=MODEL,
        tools=[],
    )


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    try:
        if _is_title_request(request):
            result = await Runner.run(_title_agent(), [item.model_dump() for item in request.input])
            return ResponsesAgentResponse(output=[item.to_input_item() for item in result.new_items])

        session_id = get_session_id(request)
        if session_id:
            mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
        session = maybe_create_session(session_id)

        stack = AsyncExitStack()
        await stack.__aenter__()
        try:
            mcp_servers, unavailable_tools = await connect_available_mcp_servers(stack, WorkspaceClient())
            direct_tools, direct_unavailable = await build_direct_databricks_mcp_tools(stack)
            atlassian_tools, atlassian_unavailable = await build_atlassian_tools(stack)
            context_tools = list(atlassian_tools)
            unavailable = unavailable_tools + direct_unavailable
            if CONFLUENCE_MCP_ENABLED and not atlassian_tools:
                context_tools.append(search_confluence)
            else:
                unavailable += atlassian_unavailable
            agent = create_agent(
                mcp_servers=mcp_servers,
                unavailable_tools=unavailable,
                extra_tools=direct_tools + context_tools,
                context_tools_enabled=CONFLUENCE_MCP_ENABLED,
            )
            result = await Runner.run(agent, await deduplicate_input(request, session), session=session)
        finally:
            await close_tool_stack(stack)

        return ResponsesAgentResponse(
            output=[item.to_input_item() for item in result.new_items],
            custom_outputs={"session_id": session.session_id if session else session_id},
        )
    except Exception as exc:
        if _is_lakebase_error(exc):
            raise HTTPException(status_code=503, detail=lakebase_access_message()) from exc
        raise


@stream()
async def stream_handler(request: ResponsesAgentRequest) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    try:
        if _is_title_request(request):
            result = Runner.run_streamed(_title_agent(), input=[item.model_dump() for item in request.input])
            async for event in process_agent_stream_events(result.stream_events()):
                yield event
            return

        session_id = get_session_id(request)
        if session_id:
            mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
        session = maybe_create_session(session_id)

        stack = AsyncExitStack()
        await stack.__aenter__()
        try:
            mcp_servers, unavailable_tools = await connect_available_mcp_servers(stack, WorkspaceClient())
            direct_tools, direct_unavailable = await build_direct_databricks_mcp_tools(stack)
            atlassian_tools, atlassian_unavailable = await build_atlassian_tools(stack)
            context_tools = list(atlassian_tools)
            unavailable = unavailable_tools + direct_unavailable
            if CONFLUENCE_MCP_ENABLED and not atlassian_tools:
                context_tools.append(search_confluence)
            else:
                unavailable += atlassian_unavailable
            agent = create_agent(
                mcp_servers=mcp_servers,
                unavailable_tools=unavailable,
                extra_tools=direct_tools + context_tools,
                context_tools_enabled=CONFLUENCE_MCP_ENABLED,
            )
            result = Runner.run_streamed(agent, input=await deduplicate_input(request, session), session=session)
            async for event in process_agent_stream_events(result.stream_events()):
                yield event
        finally:
            await close_tool_stack(stack)
    except Exception as exc:
        if _is_lakebase_error(exc):
            raise HTTPException(status_code=503, detail=lakebase_access_message()) from exc
        raise
