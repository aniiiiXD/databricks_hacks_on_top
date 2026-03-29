"""
Digital-Artha: Financial Intelligence Agent

LangGraph ReAct agent with MCP tools + conversation memory + MLflow Responses API.

Architecture:
  create_react_agent (LangGraph)
    ├─ MemorySaver (conversation persistence per session)
    ├─ MCP: Vector Search (RBI circular retrieval)
    ├─ MCP: Genie Space (financial analytics)
    ├─ Tool: lookup_fraud_alerts (parameterized SQL)
    ├─ Tool: check_loan_eligibility (parameterized SQL + LLM)
    └─ Tool: get_current_time
"""

import os
import json
import logging
from datetime import datetime

import mlflow
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
    create_text_delta,
)
import uuid


def make_response(text: str) -> ResponsesAgentResponse:
    """Create a ResponsesAgentResponse with proper structure."""
    return ResponsesAgentResponse(output=[{
        "type": "message",
        "id": str(uuid.uuid4()),
        "role": "assistant",
        "content": [{"type": "output_text", "text": text}]
    }])

from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AIMessage, ToolMessage, AIMessageChunk
from langchain_core.tools import tool
from databricks_langchain import ChatDatabricks, DatabricksMCPServer, DatabricksMultiServerMCPClient
from databricks.sdk.service.sql import StatementParameterListItem

from agent_server.utils import get_session_id, get_user_workspace_client

# --- Logging ---
logger = logging.getLogger("digital_artha")

# --- Configuration ---
CATALOG = os.environ.get("CATALOG", "digital_artha")
SCHEMA = os.environ.get("SCHEMA", "main")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct")

# --- MLflow ---
mlflow.langchain.autolog()

# --- LLM ---
llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=1000)

# --- System Prompt ---
SYSTEM_PROMPT = """You are Digital-Artha, an AI-powered financial intelligence assistant for Indian citizens.

CAPABILITIES:
1. **UPI Fraud Detection** — Look up flagged fraud alerts, explain why transactions were flagged, show risk scores and patterns from the ML ensemble pipeline.
2. **RBI Regulations** — Search RBI circulars semantically. Always cite sources with the circular title and date.
3. **Loan & Scheme Eligibility** — Help users find government financial inclusion schemes they qualify for based on their age, income, occupation, state, and gender.
4. **Financial Analytics** — Query UPI transaction data, fraud metrics, and trends using natural language.

5. **Fraud Recovery Help** — If someone has been scammed, explain exactly what to do: who to report to, RBI liability rules, time limits, step-by-step recovery process.

BEHAVIOR:
- Respond in the user's language. If they write in Hindi, respond in Hindi. Same for Marathi, Tamil, etc.
- Explain complex financial and regulatory concepts in simple terms that a first-generation bank user can understand.
- Be warm and encouraging, especially for financial inclusion queries. These people may be scared and confused.
- Amounts are in Indian Rupees (₹). Format with commas: ₹1,50,000.
- When showing fraud alerts, format them clearly: Transaction ID, Amount, Category, Risk Score, and WHY it was flagged.
- When citing RBI circulars, include the circular title.
- Use available tools to provide data-backed answers. Do not guess statistics — query them.
- If someone says they were scammed, ALWAYS use the fraud_recovery_guide tool first. Show empathy, then give actionable steps.
- If a tool fails, don't just say "error" — explain what you were trying to do and suggest an alternative.
"""

# --- Conversation Memory ---
checkpointer = MemorySaver()


# ============================================================
# TOOLS
# ============================================================

@tool
def get_current_time() -> str:
    """Returns the current date and time in ISO format."""
    return datetime.now().isoformat()


@tool
def fraud_recovery_guide(fraud_type: str = "") -> str:
    """
    Get step-by-step recovery guidance for fraud victims.

    Args:
        fraud_type: Type of fraud (e.g., 'QR code scam', 'phishing', 'SIM swap',
                    'remote access', 'fake UPI', 'merchant fraud'). Leave empty to get all types.

    Returns:
        JSON with recovery steps, RBI liability rules, who to report to, and time limits.
    """
    if not WAREHOUSE_ID:
        return json.dumps({"error": "Warehouse not configured", "guides": []})

    try:
        w = get_user_workspace_client()

        if fraud_type and fraud_type.strip():
            query = f"""
            SELECT fraud_type, description, rbi_rule, recovery_steps, report_to,
                   time_limit_days, max_liability_inr, common_in_states
            FROM {CATALOG}.{SCHEMA}.fraud_recovery_guide
            WHERE LOWER(fraud_type) LIKE CONCAT('%', :fraud_type, '%')
               OR LOWER(description) LIKE CONCAT('%', :fraud_type, '%')
            """
            from databricks.sdk.service.sql import StatementParameterListItem
            params = [StatementParameterListItem(name="fraud_type", value=fraud_type.lower().strip(), type="STRING")]
        else:
            query = f"""
            SELECT fraud_type, description, rbi_rule, recovery_steps, report_to,
                   time_limit_days, max_liability_inr, common_in_states
            FROM {CATALOG}.{SCHEMA}.fraud_recovery_guide
            """
            params = []

        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=params if params else None,
        )

        guides = []
        if response.result and response.result.data_array:
            columns = [col.name for col in response.manifest.schema.columns]
            for row in response.result.data_array:
                guides.append(dict(zip(columns, row)))

        return json.dumps({
            "guides": guides,
            "total": len(guides),
            "important": "Report fraud within 3 days for zero liability under RBI rules. Call your bank helpline FIRST.",
        }, ensure_ascii=False, default=str)

    except Exception as e:
        logger.error(f"Recovery guide lookup failed: {e}")
        return json.dumps({
            "error": str(e),
            "fallback": "If you were scammed: 1) Call your bank immediately 2) File complaint at cybercrime.gov.in 3) File FIR at police station. You have 3 days for zero liability under RBI rules.",
            "guides": []
        })


@tool
def lookup_fraud_alerts(
    limit: int = 10,
    min_risk_score: float = 0.5,
    category: str = "",
    sender_id: str = "",
) -> str:
    """
    Look up recent fraud alerts from the ML-scored UPI transaction database.

    Args:
        limit: Maximum number of alerts to return (1-50, default 10)
        min_risk_score: Minimum ensemble fraud score threshold (0.0-1.0, default 0.5)
        category: Filter by merchant category (e.g., 'electronics', 'grocery'). Empty string means all categories.
        sender_id: Filter by specific sender UPI ID. Empty string means all senders.

    Returns:
        JSON with matching fraud alerts sorted by risk score descending, including
        transaction details, risk scores, and risk tier classification.
    """
    if not WAREHOUSE_ID:
        return json.dumps({"error": "SQL warehouse not configured. Cannot query fraud alerts.", "fraud_alerts": []})

    try:
        limit = min(max(1, limit), 50)
        min_risk_score = max(0.0, min(1.0, min_risk_score))

        w = get_user_workspace_client()

        conditions = ["ensemble_flag = true", "ensemble_score >= :min_score"]
        params = [
            StatementParameterListItem(name="min_score", value=str(min_risk_score), type="DOUBLE"),
        ]

        if category and category.strip():
            conditions.append("LOWER(category) LIKE CONCAT('%', :category, '%')")
            params.append(StatementParameterListItem(name="category", value=category.strip().lower(), type="STRING"))

        if sender_id and sender_id.strip():
            conditions.append("sender_id = :sender_id")
            params.append(StatementParameterListItem(name="sender_id", value=sender_id.strip(), type="STRING"))

        where_clause = " AND ".join(conditions)
        query = f"""
        SELECT transaction_id, amount, sender_id, receiver_id, category,
               merchant_name, CAST(ensemble_score AS DOUBLE) AS ensemble_score,
               final_risk_tier, time_slot,
               transaction_date, hour_of_day, is_weekend
        FROM {CATALOG}.{SCHEMA}.gold_fraud_alerts_ml
        WHERE {where_clause}
        ORDER BY ensemble_score DESC
        LIMIT {limit}
        """

        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=params,
        )

        alerts = []
        if response.result and response.result.data_array:
            columns = [col.name for col in response.manifest.schema.columns]
            for row in response.result.data_array:
                alert = dict(zip(columns, row))
                # Add human-readable risk explanation
                alert["risk_explanation"] = _explain_risk_factors(alert)
                alerts.append(alert)

        return json.dumps({
            "fraud_alerts": alerts,
            "total_returned": len(alerts),
            "filters": {
                "min_risk_score": min_risk_score,
                "category": category or "all",
                "sender_id": sender_id or "all",
            },
        }, ensure_ascii=False, default=str)

    except Exception as e:
        logger.error(f"Fraud alert lookup failed: {e}")
        return json.dumps({"error": str(e), "fraud_alerts": [], "total_returned": 0})


def _explain_risk_factors(alert: dict) -> str:
    """Generate a concise risk explanation from available alert fields."""
    factors = []
    try:
        score = float(alert.get("ensemble_score") or 0)
        if score > 0.8:
            factors.append(f"Very high risk score ({score:.2f})")
        elif score > 0.6:
            factors.append(f"High risk score ({score:.2f})")

        amount = float(alert.get("amount") or 0)
        if amount > 50000:
            factors.append(f"Large transaction (₹{amount:,.0f})")
        elif amount > 10000:
            factors.append(f"Significant amount (₹{amount:,.0f})")

        hour = int(float(alert.get("hour_of_day") or 12))
        if hour < 6:
            factors.append("Late-night transaction (0:00-6:00)")
        elif hour >= 22:
            factors.append("Late-night transaction (22:00+)")

        if str(alert.get("is_weekend", "")).lower() in ("true", "1"):
            factors.append("Weekend transaction")

        tier = alert.get("final_risk_tier", "")
        if tier == "critical":
            factors.append("Classified as CRITICAL by ML ensemble")

        category = alert.get("category", "")
        if category:
            factors.append(f"Category: {category}")
    except (ValueError, TypeError):
        pass

    return "; ".join(factors) if factors else "Multiple risk signals detected"


@tool
def check_loan_eligibility(
    age: int,
    income: float,
    occupation: str,
    state: str,
    gender: str = "all",
    language: str = "english",
) -> str:
    """
    Check which government financial inclusion schemes a user is eligible for.

    Args:
        age: User's age in years (must be 0-150)
        income: Annual income in INR (must be non-negative)
        occupation: User's occupation (e.g., farmer, street_vendor, salaried, student, self_employed)
        state: Indian state of residence (e.g., Maharashtra, Uttar Pradesh, Tamil Nadu)
        gender: male, female, or all (default: all)
        language: Preferred response language: english, hindi, marathi, tamil (default: english)

    Returns:
        JSON with matching government schemes, their benefits, and a plain-language
        explanation of why the user qualifies for each scheme.
    """
    if not WAREHOUSE_ID:
        return json.dumps({"error": "SQL warehouse not configured. Cannot query schemes.", "matched_schemes": []})

    # Input validation
    age = max(0, min(150, age))
    income = max(0, income)
    occupation = (occupation or "").strip()
    state = (state or "").strip()
    gender = (gender or "all").strip().lower()
    if gender not in ("male", "female", "all"):
        gender = "all"

    try:
        w = get_user_workspace_client()

        # Parameterized query — no SQL injection
        query = f"""
        SELECT scheme_name, ministry, benefits, plain_summary, url,
               income_limit, age_min, age_max, occupation, state, gender
        FROM {CATALOG}.{SCHEMA}.gold_schemes
        WHERE (age_min <= :age AND age_max >= :age)
          AND (income_limit = 0 OR income_limit >= :income)
          AND (gender = 'all' OR gender = :gender)
          AND (occupation = 'all' OR LOWER(occupation) LIKE CONCAT('%', :occupation, '%'))
          AND (state = 'All India' OR LOWER(state) LIKE CONCAT('%', :state, '%'))
        """

        params = [
            StatementParameterListItem(name="age", value=str(age), type="INT"),
            StatementParameterListItem(name="income", value=str(income), type="DOUBLE"),
            StatementParameterListItem(name="gender", value=gender, type="STRING"),
            StatementParameterListItem(name="occupation", value=occupation.lower(), type="STRING"),
            StatementParameterListItem(name="state", value=state.lower(), type="STRING"),
        ]

        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=params,
        )

        schemes = []
        if response.result and response.result.data_array:
            columns = [col.name for col in response.manifest.schema.columns]
            for row in response.result.data_array:
                schemes.append(dict(zip(columns, row)))

        # Generate explanation via LLM
        explanation = ""
        if schemes:
            scheme_text = "\n".join([
                f"- {s['scheme_name']} ({s['ministry']}): {s.get('plain_summary') or s.get('benefits', 'N/A')}"
                for s in schemes[:10]
            ])
            lang_instruction = {
                "hindi": "Respond in Hindi (Devanagari script).",
                "marathi": "Respond in Marathi.",
                "tamil": "Respond in Tamil.",
            }.get(language.lower(), "Respond in English.")

            explanation_prompt = (
                f"{lang_instruction}\n"
                f"A {age}-year-old {occupation} from {state} earning ₹{income:,.0f}/year qualifies for these schemes:\n"
                f"{scheme_text}\n\n"
                f"For each scheme, explain in 1-2 simple sentences what it offers and why this person qualifies."
            )

            try:
                llm_response = w.serving_endpoints.query(
                    name=LLM_ENDPOINT,
                    messages=[{"role": "user", "content": explanation_prompt}],
                    max_tokens=600,
                    temperature=0.2,
                )
                explanation = llm_response.choices[0].message.content
            except Exception as e:
                logger.warning(f"LLM explanation failed, returning raw data: {e}")
                explanation = "Explanation unavailable. See matched schemes below."
        else:
            explanation = "No matching schemes found for this profile. Try broadening your criteria (e.g., set gender to 'all' or try a different occupation)."

        return json.dumps({
            "user_profile": {"age": age, "income": income, "occupation": occupation, "state": state, "gender": gender},
            "matched_schemes": [
                {"name": s.get("scheme_name"), "ministry": s.get("ministry"), "benefits": s.get("benefits", ""), "url": s.get("url", "")}
                for s in schemes
            ],
            "total_matches": len(schemes),
            "explanation": explanation,
        }, ensure_ascii=False, default=str)

    except Exception as e:
        logger.error(f"Loan eligibility check failed: {e}")
        return json.dumps({"error": str(e), "matched_schemes": [], "total_matches": 0})


# ============================================================
# MCP TOOL LOADING
# ============================================================

async def _load_mcp_tools_async() -> list:
    """Load MCP tools from Databricks Vector Search and Genie Space (async). Graceful degradation."""
    tools = []
    try:
        w = get_user_workspace_client()
        host = w.config.host
    except Exception as e:
        logger.warning(f"Cannot connect to Databricks workspace for MCP tools: {e}")
        return tools

    # Vector Search MCP
    vs_url = f"{host}/api/2.0/mcp/vector-search/{CATALOG}/{SCHEMA}/circular_chunks_vs_index"
    try:
        vs_server = DatabricksMCPServer(name="circular-search", url=vs_url, workspace_client=w)
        vs_client = DatabricksMultiServerMCPClient([vs_server])
        vs_tools = await vs_client.get_tools()
        tools.extend(vs_tools)
        logger.info(f"Vector Search MCP loaded: {[t.name for t in vs_tools]}")
    except Exception as e:
        logger.warning(f"Vector Search MCP unavailable: {e}")

    # Genie MCP
    if GENIE_SPACE_ID:
        genie_url = f"{host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}"
        try:
            genie_server = DatabricksMCPServer(name="financial-analytics", url=genie_url, workspace_client=w)
            genie_client = DatabricksMultiServerMCPClient([genie_server])
            genie_tools = await genie_client.get_tools()
            tools.extend(genie_tools)
            logger.info(f"Genie MCP loaded: {[t.name for t in genie_tools]}")
        except Exception as e:
            logger.warning(f"Genie MCP unavailable: {e}")
    else:
        logger.warning("GENIE_SPACE_ID not set — financial analytics tool unavailable")

    return tools


def _load_mcp_tools() -> list:
    """Sync wrapper for async MCP tool loading."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Already in async context — can't use asyncio.run
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(_load_mcp_tools_async())
        else:
            return asyncio.run(_load_mcp_tools_async())
    except Exception as e:
        logger.warning(f"MCP tool loading failed: {e}")
        return []


# ============================================================
# AGENT CREATION
# ============================================================

_agent = None


def get_agent():
    """Lazy-initialize the LangGraph ReAct agent with all tools and memory."""
    global _agent
    if _agent is not None:
        return _agent

    custom_tools = [get_current_time, lookup_fraud_alerts, check_loan_eligibility, fraud_recovery_guide]
    mcp_tools = _load_mcp_tools()
    all_tools = custom_tools + mcp_tools

    logger.info(f"Creating agent with {len(all_tools)} tools: {[t.name for t in all_tools]}")

    _agent = create_react_agent(
        model=llm,
        tools=all_tools,
        prompt=SYSTEM_PROMPT,
        checkpointer=checkpointer,
    )

    return _agent


def _extract_user_message(request: ResponsesAgentRequest) -> str:
    """Extract the last user message from any request format."""
    # Method 1: MLflow's built-in converter
    try:
        chat_input = to_chat_completions_input(request)
        if chat_input and chat_input.get("messages"):
            last = chat_input["messages"][-1]
            content = last.get("content", "")
            if isinstance(content, str):
                return content
            elif isinstance(content, list):
                return " ".join(c.get("text", "") for c in content if isinstance(c, dict))
    except Exception:
        pass

    # Method 2: Parse request.input directly
    try:
        if hasattr(request, "input") and request.input:
            for item in reversed(request.input):
                # Pydantic model with .role and .content
                role = getattr(item, "role", None) or (item.get("role") if isinstance(item, dict) else None)
                if role == "user":
                    content = getattr(item, "content", None) or (item.get("content") if isinstance(item, dict) else None)
                    if isinstance(content, str):
                        return content
                    elif isinstance(content, list):
                        for c in content:
                            text = getattr(c, "text", None) or (c.get("text") if isinstance(c, dict) else None)
                            if text:
                                return text
    except Exception:
        pass

    return ""


# ============================================================
# MLflow Responses API Handlers
# ============================================================

@invoke()
async def handle_invoke(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    """Handle synchronous (non-streaming) requests with conversation memory."""
    agent = get_agent()
    session_id = get_session_id(request)
    config = {"configurable": {"thread_id": session_id}}

    # Extract user message — handle multiple input formats
    user_message = _extract_user_message(request)

    if not user_message:
        return make_response("I didn't receive a message. Please try again.")

    try:
        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": user_message}]},
            config=config,
        )

        # Extract last AI message from result
        output_text = "I couldn't generate a response. Please try again."
        messages = result.get("messages", [])
        for msg in reversed(messages):
            if isinstance(msg, AIMessage) and msg.content and not msg.tool_calls:
                output_text = msg.content
                break

    except Exception as e:
        logger.error(f"Agent invoke failed: {e}")
        output_text = f"I encountered an error processing your request. Please try again. (Error: {type(e).__name__})"

    mlflow.update_current_trace(metadata={"session_id": session_id})
    return make_response(output_text)


@stream()
async def handle_stream(request: ResponsesAgentRequest):
    """Handle streaming requests — yields text deltas as the agent reasons and responds."""
    agent = get_agent()
    session_id = get_session_id(request)
    config = {"configurable": {"thread_id": session_id}}

    user_message = _extract_user_message(request)

    try:
        async for event in agent.astream(
            {"messages": [{"role": "user", "content": user_message}]},
            config=config,
            stream_mode="updates",
        ):
            if not isinstance(event, dict):
                continue
            for node_name, node_output in event.items():
                messages = node_output.get("messages", [])
                for msg in messages:
                    # Stream AI message content (the actual response text)
                    if isinstance(msg, (AIMessage, AIMessageChunk)):
                        if msg.content and isinstance(msg.content, str):
                            yield create_text_delta(msg.content)
                    # Log tool results (don't stream raw tool output to user)
                    elif isinstance(msg, ToolMessage):
                        content = msg.content
                        if not isinstance(content, str):
                            try:
                                content = json.dumps(content, ensure_ascii=False, default=str)
                            except (TypeError, ValueError):
                                content = str(content)
                        logger.debug(f"Tool '{msg.name}' returned: {content[:300]}")

    except Exception as e:
        logger.error(f"Agent stream failed: {e}")
        yield create_text_delta(f"\n\nI encountered an error: {type(e).__name__}. Please try again.")

    mlflow.update_current_trace(metadata={"session_id": session_id})
