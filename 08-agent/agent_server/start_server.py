"""
Digital-Artha Agent Server

Starts the MLflow AgentServer with:
- ResponsesAgent handlers (invoke + stream)
- Built-in chat proxy UI at port 8000
- MLflow experiment tracking
"""

import os
import sys

from dotenv import load_dotenv
load_dotenv()

# Import agent module to register @invoke and @stream handlers
import agent_server.agent  # noqa: F401

from mlflow.genai.agent_server import AgentServer


def main():
    """Start the agent server."""
    port = int(os.environ.get("PORT", "8000"))

    # Create server
    agent_server = AgentServer(
        "ResponsesAgent",
        enable_chat_proxy=True  # Gives us a free chat UI at /
    )

    print(f"Digital-Artha Agent Server starting on port {port}")
    print(f"Chat UI: http://localhost:{port}")
    print(f"API: http://localhost:{port}/invocations")

    # Run
    agent_server.run(port=port)


# For ASGI deployment (uvicorn)
load_dotenv()
import agent_server.agent  # noqa: F401
from mlflow.genai.agent_server import AgentServer as _AS
_server = _AS("ResponsesAgent", enable_chat_proxy=True)
app = _server.app


if __name__ == "__main__":
    main()
