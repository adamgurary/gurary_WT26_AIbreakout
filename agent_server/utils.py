import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncGenerator, AsyncIterator, Optional
from uuid import uuid4

try:
    from agents.models.fake_id import FAKE_RESPONSES_ID
except ImportError:
    FAKE_RESPONSES_ID = "__fake_id__"

from agents.result import StreamEvent
from databricks.sdk import WorkspaceClient
from databricks_openai.agents import AsyncDatabricksSession
from mlflow.genai.agent_server import get_request_headers
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentStreamEvent

try:
    from uuid_utils import uuid7
except Exception:
    from uuid import uuid4 as uuid7

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LakebaseConfig:
    instance_name: Optional[str]
    autoscaling_endpoint: Optional[str]
    autoscaling_project: Optional[str]
    autoscaling_branch: Optional[str]
    memory_schema: Optional[str] = None

    @property
    def description(self) -> str:
        return self.autoscaling_endpoint or self.instance_name or f"{self.autoscaling_project}/{self.autoscaling_branch}"

    @property
    def is_configured(self) -> bool:
        return bool(
            self.autoscaling_endpoint
            or self.instance_name
            or (self.autoscaling_project and self.autoscaling_branch)
        )


def init_lakebase_config() -> LakebaseConfig:
    endpoint = os.getenv("LAKEBASE_AUTOSCALING_ENDPOINT") or os.getenv("LAKEBASE_ENDPOINT") or None
    instance_name = os.getenv("LAKEBASE_INSTANCE_NAME") or None
    project = os.getenv("LAKEBASE_AUTOSCALING_PROJECT") or None
    branch = os.getenv("LAKEBASE_AUTOSCALING_BRANCH") or None
    memory_schema = os.getenv("LAKEBASE_AGENT_MEMORY_SCHEMA") or os.getenv("LAKEBASE_MEMORY_SCHEMA") or None

    if endpoint:
        return LakebaseConfig(None, endpoint, None, None, memory_schema)
    if project and branch:
        return LakebaseConfig(None, None, project, branch, memory_schema)
    if instance_name:
        return LakebaseConfig(instance_name, None, None, None, memory_schema)

    return LakebaseConfig(None, None, None, None, memory_schema)


lakebase_config = init_lakebase_config()
_lakebase_warning_emitted = False


def warn_lakebase_unconfigured() -> None:
    global _lakebase_warning_emitted
    if lakebase_config.is_configured or _lakebase_warning_emitted:
        return
    _lakebase_warning_emitted = True
    logger.warning("Lakebase is not configured; running without persistent session memory.")


def get_databricks_host_from_env() -> str:
    return WorkspaceClient().config.host.rstrip("/")


def get_session_id(request: ResponsesAgentRequest) -> str:
    custom_inputs = dict(request.custom_inputs or {})
    if custom_inputs.get("session_id"):
        return str(custom_inputs["session_id"])
    if request.context and getattr(request.context, "conversation_id", None):
        return str(request.context.conversation_id)
    return str(uuid7())


def get_user_workspace_client() -> WorkspaceClient:
    try:
        token = (get_request_headers() or {}).get("x-forwarded-access-token")
        if token:
            return WorkspaceClient(token=token, auth_type="pat")
    except Exception:
        logger.warning("OBO token unavailable; using app service principal.", exc_info=True)
    return WorkspaceClient()


def replace_fake_id(obj: Any, real_id: str) -> Any:
    if isinstance(obj, dict):
        return {k: replace_fake_id(v, real_id) for k, v in obj.items()}
    if isinstance(obj, list):
        return [replace_fake_id(item, real_id) for item in obj]
    if isinstance(obj, str) and obj == FAKE_RESPONSES_ID:
        return real_id
    return obj


async def deduplicate_input(request: ResponsesAgentRequest, session: Optional[AsyncDatabricksSession]) -> list[dict]:
    messages = [item.model_dump() for item in request.input]
    messages = [
        msg
        for msg in messages
        if not (
            isinstance(msg, dict)
            and msg.get("type") in {"function_call", "function_call_output"}
        )
    ]
    for msg in messages:
        if (
            isinstance(msg, dict)
            and msg.get("type") == "message"
            and msg.get("role") == "assistant"
        ):
            if isinstance(msg.get("content"), str):
                msg["content"] = [{"type": "output_text", "text": msg["content"], "annotations": []}]
            # The Agents SDK chatcmpl converter (maybe_response_output_message) requires
            # assistant message items to carry an "id"; replayed history from the chat UI
            # can omit it, which otherwise raises "Unhandled item type or structure".
            if not msg.get("id"):
                msg["id"] = f"msg_{uuid4().hex}"
            if msg.get("status") is None:
                msg["status"] = "completed"
    if session is None:
        return messages
    session_items = await session.get_items()
    if len(session_items) >= len(messages) - 1:
        return [messages[-1]]
    return messages


async def process_agent_stream_events(
    async_stream: AsyncIterator[StreamEvent],
    response_id: str | None = None,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    curr_item_id = str(uuid4())
    async for event in async_stream:
        if event.type == "raw_response_event":
            event_data = event.data.model_dump()
            if response_id is not None:
                event_data = replace_fake_id(event_data, response_id)
            if event_data["type"] == "response.output_item.added":
                curr_item_id = str(uuid4())
                event_data["item"]["id"] = curr_item_id
            elif event_data.get("item") is not None and event_data["item"].get("id") is not None:
                event_data["item"]["id"] = curr_item_id
            elif event_data.get("item_id") is not None:
                event_data["item_id"] = curr_item_id
            yield event_data
        elif event.type == "run_item_stream_event" and event.item.type == "tool_call_output_item":
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done",
                item=event.item.to_input_item(),
            )


def lakebase_access_message() -> str:
    return (
        f"Failed to connect to Lakebase instance '{lakebase_config.description}'. "
        "Verify the Bobabricks app has the Lakebase resource attached and the app service "
        "principal has permission to create/use the agent memory schema."
    )
