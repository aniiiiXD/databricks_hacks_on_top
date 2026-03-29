"""
BlackIce Agent Server

Starts the MLflow AgentServer with:
- Startup validation (env vars, Databricks connectivity)
- ResponsesAgent handlers (invoke + stream)
- Built-in chat proxy UI at port 8000
- /health endpoint for monitoring
- MLflow experiment tracking
"""

import os
import sys
import logging

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("digital_artha")


def validate_environment():
    """Check required config before starting. Warns but does not exit (allows partial functionality for demo)."""
    errors = []
    warnings = []

    if not os.environ.get("WAREHOUSE_ID"):
        errors.append("WAREHOUSE_ID not set — fraud lookup and loan eligibility tools will fail. Set it in .env or app.yaml.")

    if not os.environ.get("GENIE_SPACE_ID"):
        warnings.append("GENIE_SPACE_ID not set — financial analytics (Genie) tool will be unavailable.")

    catalog = os.environ.get("CATALOG", "digital_artha")
    schema = os.environ.get("SCHEMA", "main")
    logger.info(f"Config: catalog={catalog}, schema={schema}")

    # Test workspace connectivity
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        current_user = w.current_user.me()
        logger.info(f"Authenticated as: {current_user.user_name}")
    except Exception as e:
        errors.append(f"Cannot connect to Databricks workspace: {e}")

    for w_msg in warnings:
        logger.warning(f"[STARTUP] {w_msg}")
    for e_msg in errors:
        logger.error(f"[STARTUP] {e_msg}")

    if errors:
        logger.error("Startup validation found errors. Some tools may not work. Fix the above and restart.")


# --- Import agent module to register @invoke and @stream handlers ---
import agent_server.agent  # noqa: F401

from mlflow.genai.agent_server import AgentServer as _AS

_server = _AS("ResponsesAgent", enable_chat_proxy=True)
app = _server.app


# --- Health Endpoint ---
from fastapi import Request
from fastapi.responses import JSONResponse


@app.get("/health")
async def health_check():
    """Health check endpoint for Databricks Apps and load balancers."""
    from agent_server.agent import get_agent, GENIE_SPACE_ID, WAREHOUSE_ID

    status = {
        "status": "healthy",
        "agent_loaded": False,
        "tools": [],
        "warehouse_configured": bool(WAREHOUSE_ID),
        "genie_configured": bool(GENIE_SPACE_ID),
    }

    try:
        agent = get_agent()
        status["agent_loaded"] = True
        # Extract tool names from the agent graph
        if hasattr(agent, "tools"):
            status["tools"] = [t.name for t in agent.tools]
        elif hasattr(agent, "get_graph"):
            # LangGraph agents may expose tools differently
            status["tools"] = ["agent loaded (tool introspection unavailable)"]
    except Exception as e:
        status["status"] = "degraded"
        status["error"] = str(e)

    code = 200 if status["status"] == "healthy" else 503
    return JSONResponse(status, status_code=code)


def main():
    """Start the agent server with validation."""
    validate_environment()

    port = int(os.environ.get("PORT", "8000"))
    logger.info(f"BlackIce Agent Server starting on port {port}")
    logger.info(f"Chat UI:  http://localhost:{port}")
    logger.info(f"API:      http://localhost:{port}/invocations")
    logger.info(f"Health:   http://localhost:{port}/health")

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
