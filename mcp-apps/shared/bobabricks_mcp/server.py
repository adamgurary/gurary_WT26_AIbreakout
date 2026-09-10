from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, request


ToolHandler = Callable[[dict[str, Any]], dict[str, Any] | list[dict[str, Any]]]


@dataclass(frozen=True)
class McpTool:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: ToolHandler


class DatabricksTableClient:
    def __init__(self, warehouse_id: str | None = None):
        self.warehouse_id = warehouse_id or os.environ["DATABRICKS_WAREHOUSE_ID"]
        self.workspace = WorkspaceClient()

    @staticmethod
    def table_name(name: str) -> str:
        catalog = os.getenv("DATABRICKS_CATALOG", "bobabricks_demo")
        schema = os.getenv("DATABRICKS_SCHEMA", "store_ops")
        prefix = os.getenv("DATABRICKS_TABLE_PREFIX", "")
        return f"{catalog}.{schema}.{prefix}{name}"

    def query(self, statement: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        response = self.workspace.api_client.do(
            "POST",
            "/api/2.0/sql/statements",
            body={
                "warehouse_id": self.warehouse_id,
                "statement": statement,
                "parameters": self._parameters(parameters or {}),
                "wait_timeout": "30s",
                "on_wait_timeout": "CONTINUE",
            },
        )
        statement_id = response.get("statement_id")
        while response.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            time.sleep(1)
            response = self.workspace.api_client.do("GET", f"/api/2.0/sql/statements/{statement_id}")

        state = response.get("status", {}).get("state")
        if state != "SUCCEEDED":
            raise RuntimeError(json.dumps(response.get("status", response), default=str))

        columns = [column["name"] for column in response.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = response.get("result", {}).get("data_array", [])
        return [dict(zip(columns, row, strict=False)) for row in rows]

    @staticmethod
    def _parameters(parameters: dict[str, Any]) -> list[dict[str, str]]:
        return [{"name": key, "value": str(value)} for key, value in parameters.items() if value is not None]


class JsonRpcMcpApp:
    def __init__(self, name: str, tools: list[McpTool]):
        self.name = name
        self.tools = {tool.name: tool for tool in tools}
        self.app = Flask(name)
        self.app.add_url_rule("/", "health", self.health, methods=["GET"])
        self.app.add_url_rule("/mcp", "mcp", self.handle_mcp, methods=["POST"])

    def health(self):
        return jsonify({"name": self.name, "status": "ok", "tools": sorted(self.tools)})

    def handle_mcp(self):
        payload = request.get_json(force=True, silent=False)
        if isinstance(payload, list):
            return jsonify([self._dispatch(message) for message in payload])
        return jsonify(self._dispatch(payload))

    def _dispatch(self, message: dict[str, Any]) -> dict[str, Any]:
        request_id = message.get("id")
        method = message.get("method")
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": self.name, "version": "0.1.0"},
                    "capabilities": {"tools": {}},
                }
            elif method == "tools/list":
                result = {"tools": [self._tool_spec(tool) for tool in self.tools.values()]}
            elif method == "tools/call":
                result = self._call_tool(message.get("params", {}))
            else:
                return self._error(request_id, -32601, f"Unsupported method: {method}")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except Exception as exc:
            return self._error(request_id, -32000, str(exc))

    @staticmethod
    def _tool_spec(tool: McpTool) -> dict[str, Any]:
        return {"name": tool.name, "description": tool.description, "inputSchema": tool.input_schema}

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        if name not in self.tools:
            raise ValueError(f"Unknown tool: {name}")
        arguments = params.get("arguments") or {}
        result = self.tools[name].handler(arguments)
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]}

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}

    def run(self) -> None:
        self.app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
