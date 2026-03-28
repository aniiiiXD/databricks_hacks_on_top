"""
Digital-Artha: Financial Intelligence Agent

MCP-based LangGraph agent with three tool surfaces:
1. Vector Search MCP — RBI circular retrieval (semantic search)
2. Genie MCP — Financial analytics queries (natural language SQL)
3. Custom tools — Loan eligibility, fraud lookup, time

Architecture follows the bharatbricksiitb starter kit pattern:
LangGraph agent + DatabricksMCPServer + MLflow Responses API
"""

import os
import json
import asyncio
from datetime import datetime

import mlflow
from mlflow.genai.agent_server import invoke, stream, get_request_headers
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
    create_text_delta,
)

from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from databricks_langchain import ChatDatabricks
from databricks_langchain.mcp import DatabricksMCPServer, DatabricksMultiServerMCPClient
from databricks.sdk import WorkspaceClient

from agent_server.utils import get_session_id, get_user_workspace_client, process_agent_astream_events

# --- Configuration ---
CATALOG = os.environ.get("CATALOG", "digital_artha")
SCHEMA = os.environ.get("SCHEMA", "main")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct")

# --- MLflow ---
mlflow.langchain.autolog()

# --- LLM ---
llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=1000)

# --- System Prompt ---
SYSTEM_PROMPT = """You are Digital-Artha, an AI-powered financial intelligence assistant for Indian citizens.

You help with three things:
1. **UPI Fraud Detection** — Explain fraud patterns, look up suspicious transactions, show risk analytics
2. **RBI Regulations** — Answer questions about RBI circulars and financial regulations, with source citations
3. **Loan & Scheme Eligibility** — Help users find government financial inclusion schemes they qualify for

BEHAVIOR:
- Always cite sources when using information from RBI circulars (use [Source] notation)
- Respond in the user's language (if they write in Hindi, respond in Hindi)
- Explain complex financial/regulatory concepts in simple terms
- Be warm and encouraging, especially for financial inclusion queries
- For fraud analytics, use the financial-analytics (Genie) tool
- For RBI regulation questions, use the circular-search (Vector Search) tool
- For loan eligibility, use the check_loan_eligibility tool
- Amounts are in Indian Rupees (₹). Format numbers with commas.

TOOLS AVAILABLE:
- circular-search: Search RBI circulars by semantic similarity. Use for regulation questions.
- financial-analytics: Query UPI transaction data and fraud metrics via natural language. Use for statistics.
- check_loan_eligibility: Check which government schemes a user qualifies for based on their profile.
- get_current_time: Get the current date and time.
"""

# --- Custom Tools ---

@tool
def get_current_time() -> str:
    """Returns the current date and time in ISO format."""
    return datetime.now().isoformat()


@tool
def check_loan_eligibility(
    age: int,
    income: float,
    occupation: str,
    state: str,
    gender: str = "all",
    language: str = "english"
) -> str:
    """
    Check which government financial inclusion schemes a user is eligible for.

    Args:
        age: User's age in years
        income: Annual income in INR
        occupation: User's occupation (e.g., farmer, street_vendor, salaried, student)
        state: Indian state of residence (e.g., Maharashtra, Uttar Pradesh)
        gender: male, female, or all
        language: Response language (english, hindi, marathi, tamil)

    Returns:
        JSON string with matching schemes and explanation
    """
    try:
        w = WorkspaceClient()

        # Query scheme matching via SQL (runs on the warehouse)
        query = f"""
        SELECT scheme_name, ministry, benefits, plain_summary, url,
               income_limit, age_min, age_max, occupation, state, gender
        FROM {CATALOG}.{SCHEMA}.gold_schemes
        WHERE (age_min <= {age} AND age_max >= {age})
          AND (income_limit = 0 OR income_limit >= {income})
          AND (gender = 'all' OR gender = '{gender.lower()}')
          AND (occupation = 'all' OR LOWER(occupation) LIKE '%{occupation.lower()}%')
          AND (state = 'All India' OR LOWER(state) LIKE '%{state.lower()}%')
        """

        # Use the serving endpoint for explanation
        schemes_result = []
        try:
            # Try to execute via Spark SQL endpoint
            response = w.statement_execution.execute_statement(
                warehouse_id=os.environ.get("WAREHOUSE_ID", ""),
                statement=query,
                catalog=CATALOG,
                schema=SCHEMA
            )
            if response.result and response.result.data_array:
                for row in response.result.data_array:
                    schemes_result.append({
                        "scheme_name": row[0],
                        "ministry": row[1],
                        "benefits": row[2],
                        "summary": row[3],
                        "url": row[4],
                    })
        except Exception:
            # Fallback: return a helpful message
            return json.dumps({
                "message": f"Searching schemes for: age={age}, income=₹{income:,.0f}, occupation={occupation}, state={state}",
                "note": "Please run the 05-loan-eligibility.py notebook for full matching functionality.",
                "total_matches": 0
            })

        # Generate explanation via LLM
        if schemes_result:
            scheme_text = "\n".join([f"- {s['scheme_name']} ({s['ministry']}): {s.get('summary', s.get('benefits', ''))}" for s in schemes_result])
            explanation_prompt = f"A {age}-year-old {occupation} from {state} earning ₹{income:,.0f}/year qualifies for:\n{scheme_text}\n\nExplain each scheme briefly in {language}."

            llm_response = w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[{"role": "user", "content": explanation_prompt}],
                max_tokens=600,
                temperature=0.2
            )
            explanation = llm_response.choices[0].message.content
        else:
            explanation = "No matching schemes found for this profile. Try broadening your criteria."

        return json.dumps({
            "user_profile": {"age": age, "income": income, "occupation": occupation, "state": state, "gender": gender},
            "matched_schemes": schemes_result,
            "total_matches": len(schemes_result),
            "explanation": explanation,
        }, ensure_ascii=False)

    except Exception as e:
        return json.dumps({"error": str(e), "total_matches": 0})


# --- MCP Tools (Vector Search + Genie) ---

def _build_mcp_tools():
    """Build MCP tool connections to Vector Search and Genie Space."""
    w = WorkspaceClient()
    host = w.config.host
    servers = []

    # Vector Search MCP — for RBI circular retrieval
    vs_index = f"{CATALOG}.{SCHEMA}.circular_chunks_vs_index"
    try:
        vs_server = DatabricksMCPServer(
            name="circular-search",
            url=f"{host}/api/2.0/mcp/vector-search/{CATALOG}/{SCHEMA}/circular_chunks_vs_index",
            workspace_client=w
        )
        servers.append(vs_server)
    except Exception as e:
        print(f"Vector Search MCP not available: {e}")

    # Genie MCP — for financial analytics
    if GENIE_SPACE_ID:
        try:
            genie_server = DatabricksMCPServer(
                name="financial-analytics",
                url=f"{host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}",
                workspace_client=w
            )
            servers.append(genie_server)
        except Exception as e:
            print(f"Genie MCP not available: {e}")

    return servers


# --- Build Agent ---

def _create_agent():
    """Create the LangGraph agent with all tools."""
    custom_tools = [get_current_time, check_loan_eligibility]

    # Try to connect MCP tools
    mcp_tools = []
    try:
        servers = _build_mcp_tools()
        if servers:
            mcp_client = DatabricksMultiServerMCPClient(servers)
            mcp_tools = mcp_client.get_tools()
            print(f"MCP tools loaded: {[t.name for t in mcp_tools]}")
    except Exception as e:
        print(f"MCP tools unavailable, using custom tools only: {e}")

    all_tools = custom_tools + mcp_tools

    # Build prompt
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        MessagesPlaceholder("chat_history", optional=True),
        ("human", "{input}"),
        MessagesPlaceholder("agent_scratchpad"),
    ])

    # Create agent
    agent = create_tool_calling_agent(llm, all_tools, prompt)
    executor = AgentExecutor(
        agent=agent,
        tools=all_tools,
        verbose=True,
        max_iterations=10,
        handle_parsing_errors=True,
        return_intermediate_steps=True,
    )

    return executor


# Lazy initialization
_agent = None

def get_agent():
    global _agent
    if _agent is None:
        _agent = _create_agent()
    return _agent


# --- MLflow Responses API Handlers ---

@invoke()
async def handle_invoke(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    """Handle synchronous (non-streaming) requests."""
    agent = get_agent()
    session_id = get_session_id(request)

    # Convert Responses API format to chat format
    chat_input = to_chat_completions_input(request)
    user_message = chat_input["messages"][-1]["content"] if chat_input["messages"] else ""

    # Run agent
    result = await agent.ainvoke({
        "input": user_message,
        "chat_history": chat_input["messages"][:-1],
    })

    output_text = result.get("output", "I couldn't generate a response. Please try again.")

    # Track session in MLflow
    mlflow.update_current_trace(metadata={"session_id": session_id})

    return ResponsesAgentResponse.from_text(output_text)


@stream()
async def handle_stream(request: ResponsesAgentRequest):
    """Handle streaming requests — yields events as they're generated."""
    agent = get_agent()
    session_id = get_session_id(request)

    chat_input = to_chat_completions_input(request)
    user_message = chat_input["messages"][-1]["content"] if chat_input["messages"] else ""

    # Stream agent execution
    async for event in agent.astream(
        {"input": user_message, "chat_history": chat_input["messages"][:-1]},
    ):
        # Yield intermediate results
        if "output" in event:
            yield create_text_delta(event["output"])

    mlflow.update_current_trace(metadata={"session_id": session_id})
