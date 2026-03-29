"""
Utility functions for the BlackIce agent server.

- Session management (conversation ID extraction)
- Workspace client (per-request auth via forwarded tokens)
- Stream event normalization (LangGraph → ResponsesAgentStreamEvent)
- Structured logging
"""

import os
import json
import uuid
import logging
from typing import AsyncIterator

from databricks.sdk import WorkspaceClient
from mlflow.genai.agent_server import get_request_headers
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentStreamEvent,
    create_text_delta,
)
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("digital_artha")


def get_session_id(request: ResponsesAgentRequest) -> str:
    """
    Extract session/conversation ID for memory persistence.

    Priority:
    1. Responses API conversation_id (from Databricks Apps)
    2. custom_inputs.session_id or thread_id (from API clients)
    3. Generate a unique ID (prevents cross-session memory leakage)
    """
    # Try Responses API conversation_id
    if request.context and hasattr(request.context, "conversation_id") and request.context.conversation_id:
        return request.context.conversation_id

    # Try custom_inputs
    if request.custom_inputs:
        sid = request.custom_inputs.get("session_id") or request.custom_inputs.get("thread_id")
        if sid:
            return str(sid)

    # Generate unique session (NOT "default" — prevents cross-user memory leakage)
    return f"session-{uuid.uuid4().hex[:12]}"


def get_user_workspace_client() -> WorkspaceClient:
    """
    Create a WorkspaceClient authenticated as the current user.

    In Databricks Apps, uses the forwarded access token for user-level auth.
    Falls back to default auth (machine credentials) for local development.
    """
    try:
        headers = get_request_headers()
        access_token = headers.get("X-Forwarded-Access-Token")
        host = os.environ.get("DATABRICKS_HOST", "")

        if access_token and host:
            logger.debug("Using forwarded access token for user-level auth")
            return WorkspaceClient(host=host, token=access_token)
    except Exception as e:
        logger.debug(f"Forwarded token unavailable (expected in local dev): {e}")

    # Fallback to default auth
    return WorkspaceClient()


async def process_agent_astream_events(
    event_stream: AsyncIterator,
) -> AsyncIterator[ResponsesAgentStreamEvent]:
    """
    Normalize LangGraph create_react_agent stream events into
    MLflow ResponsesAgentStreamEvent format.

    Event structure from create_react_agent with stream_mode="updates":
    {
        "agent": {"messages": [AIMessage(...)]},       # Agent reasoning/response
        "tools": {"messages": [ToolMessage(...)]},     # Tool results
    }

    We yield:
    - AIMessage/AIMessageChunk content → text deltas (visible to user)
    - ToolMessage content → logged but not streamed (internal)
    """
    async for event in event_stream:
        if not isinstance(event, dict):
            continue

        for node_name, node_output in event.items():
            if not isinstance(node_output, dict):
                continue

            messages = node_output.get("messages", [])
            for msg in messages:
                # AI message content → stream to user
                if isinstance(msg, (AIMessage, AIMessageChunk)):
                    if msg.content and isinstance(msg.content, str):
                        yield create_text_delta(msg.content)

                # Tool message → log only (don't stream raw tool output)
                elif isinstance(msg, ToolMessage):
                    content = msg.content
                    if not isinstance(content, str):
                        try:
                            content = json.dumps(content, ensure_ascii=False, default=str)
                        except (TypeError, ValueError):
                            content = str(content)
                    logger.debug(f"Tool '{msg.name}' result ({len(content)} chars): {content[:200]}")
