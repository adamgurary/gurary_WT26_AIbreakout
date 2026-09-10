import logging
import os
from contextlib import asynccontextmanager

from databricks_ai_bridge.long_running import LongRunningAgentServer
from databricks_openai.agents import AsyncDatabricksSession
from mlflow.genai.agent_server import setup_mlflow_git_based_version_tracking

from agent_server.utils import (
    lakebase_access_message,
    lakebase_config,
    replace_fake_id,
    warn_lakebase_unconfigured,
)

import agent_server.agent  # noqa: F401

logger = logging.getLogger(__name__)


async def run_lakebase_session_setup() -> None:
    if not lakebase_config.is_configured:
        warn_lakebase_unconfigured()
        return
    session = AsyncDatabricksSession(
        session_id="__startup__",
        instance_name=lakebase_config.instance_name,
        autoscaling_endpoint=lakebase_config.autoscaling_endpoint,
        project=lakebase_config.autoscaling_project,
        branch=lakebase_config.autoscaling_branch,
        schema=lakebase_config.memory_schema,
    )
    await session._ensure_tables()


class AgentServer(LongRunningAgentServer):
    def transform_stream_event(self, event, response_id):
        return replace_fake_id(event, response_id)


agent_server = AgentServer(
    "ResponsesAgent",
    enable_chat_proxy=True,
    db_instance_name=lakebase_config.instance_name,
    db_autoscaling_endpoint=lakebase_config.autoscaling_endpoint,
    db_project=lakebase_config.autoscaling_project,
    db_branch=lakebase_config.autoscaling_branch,
    task_timeout_seconds=float(os.getenv("TASK_TIMEOUT_SECONDS", "3600")),
    poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "1.0")),
)

logging.getLogger("agent_server").setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
_original_lifespan = agent_server.app.router.lifespan_context


@asynccontextmanager
async def _lifespan(app):
    try:
        await run_lakebase_session_setup()
    except Exception as exc:
        os.environ["BOBABRICKS_DISABLE_LAKEBASE"] = "1"
        logger.warning(
            "Lakebase session setup failed; continuing without persistent session memory: %s\n\n%s",
            exc,
            lakebase_access_message(),
            exc_info=True,
        )
    try:
        async with _original_lifespan(app):
            yield
    except Exception as exc:
        logger.warning("Long-running DB initialization failed: %s. Background mode disabled.", exc)
        yield


agent_server.app.router.lifespan_context = _lifespan
app = agent_server.app
try:
    setup_mlflow_git_based_version_tracking()
except Exception as exc:
    logger.warning("MLflow version tracking setup failed; continuing app startup: %s", exc, exc_info=True)


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")


if __name__ == "__main__":
    main()
