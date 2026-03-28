"""
Utility functions for the Digital-Artha agent server.
Adapted from the bharatbricksiitb starter kit pattern.
"""

import os
from databricks.sdk import WorkspaceClient
from mlflow.genai.agent_server import get_request_headers
from mlflow.types.responses import ResponsesAgentRequest


def get_session_id(request: ResponsesAgentRequest) -> str:
    """
    Extract session ID from the request for conversation tracking.
    Falls back to a default if not provided.
    """
    try:
        if hasattr(request, "context") and request.context:
            if hasattr(request.context, "conversation_id"):
                return request.context.conversation_id
        if hasattr(request, "custom_inputs") and request.custom_inputs:
            return request.custom_inputs.get("session_id", "default")
    except Exception:
        pass
    return "default"


def get_user_workspace_client() -> WorkspaceClient:
    """
    Create a WorkspaceClient authenticated as the current user.
    Uses the forwarded access token from Databricks Apps.
    """
    try:
        headers = get_request_headers()
        access_token = headers.get("X-Forwarded-Access-Token")
        host = os.environ.get("DATABRICKS_HOST", "")

        if access_token and host:
            return WorkspaceClient(host=host, token=access_token)
    except Exception:
        pass

    # Fallback to default auth
    return WorkspaceClient()


def process_agent_astream_events(events):
    """
    Convert LangGraph stream events to MLflow ResponsesAgentStreamEvent format.
    Handles both ToolMessage and AIMessageChunk events.
    """
    from mlflow.types.responses import ResponsesAgentStreamEvent, create_text_delta

    for event in events:
        if isinstance(event, dict):
            if "output" in event:
                yield create_text_delta(event["output"])
        elif hasattr(event, "content"):
            if event.content:
                yield create_text_delta(str(event.content))
