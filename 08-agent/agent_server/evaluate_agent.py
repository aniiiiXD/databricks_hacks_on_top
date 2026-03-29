"""
BlackIce Agent Evaluation

Tests the agent with realistic scenarios covering:
- Fraud alert lookup (new tool)
- RBI circular search (MCP tool)
- Loan eligibility (parameterized SQL + LLM)
- Multi-turn conversation (memory)
- Hindi language support
- Error handling (invalid inputs)
- Analytics queries (Genie MCP)

Uses MLflow ConversationSimulator + scorers.
"""

import asyncio
import mlflow
from mlflow.genai.evaluate import evaluate
from mlflow.genai.scorers import (
    Completeness,
    Fluency,
    RelevanceToQuery,
    Safety,
    ToolCallCorrectness,
)


EVAL_DATA = [
    # --- Fraud Detection ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "Show me the top 5 highest-risk fraud alerts"}
        ]},
        "expected": "The agent should use the lookup_fraud_alerts tool with limit=5 and return transaction details with risk scores and risk explanations."
    },
    {
        "inputs": {"messages": [
            {"role": "user", "content": "Are there any suspicious transactions in the electronics category?"}
        ]},
        "expected": "The agent should use lookup_fraud_alerts with category='electronics' and return matching alerts."
    },

    # --- RBI Regulations ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "What are RBI guidelines for preventing UPI fraud?"}
        ]},
        "expected": "The agent should use the circular-search (Vector Search MCP) tool to retrieve relevant RBI circulars and cite them with [Source] notation."
    },

    # --- Loan Eligibility ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "I am a 30 year old farmer from Uttar Pradesh earning 2 lakh per year. What government schemes can I apply for?"}
        ]},
        "expected": "The agent should use check_loan_eligibility with age=30, income=200000, occupation=farmer, state=Uttar Pradesh."
    },

    # --- Hindi Language ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "यूपीआई में सबसे ज्यादा फ्रॉड किस कैटेगरी में होता है?"}
        ]},
        "expected": "The agent should respond in Hindi and use the financial-analytics (Genie MCP) or lookup_fraud_alerts tool to query fraud by category."
    },

    # --- Financial Analytics ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "What is the current fraud rate across all UPI transactions?"}
        ]},
        "expected": "The agent should use the financial-analytics (Genie MCP) tool to query overall fraud statistics."
    },

    # --- Multi-turn Conversation (Memory Test) ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "I am a 28 year old street vendor from Maharashtra earning 1.5 lakh per year."},
            {"role": "assistant", "content": "I'll check which government financial inclusion schemes you're eligible for."},
            {"role": "user", "content": "What about schemes specifically for women?"},
        ]},
        "expected": "The agent should use check_loan_eligibility with the same profile (age=28, income=150000, occupation=street_vendor, state=Maharashtra) but with gender=female, demonstrating it remembered context from the first message."
    },

    # --- Error Handling ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "Check loan eligibility for a -5 year old with income of -1000 rupees"}
        ]},
        "expected": "The agent should handle invalid inputs gracefully — either validate and ask for corrections, or clamp values to valid ranges and explain."
    },

    # --- Edge Case: Tool Chaining ---
    {
        "inputs": {"messages": [
            {"role": "user", "content": "Show me recent fraud alerts and explain what RBI says about preventing this type of fraud"}
        ]},
        "expected": "The agent should chain two tools: first lookup_fraud_alerts to get recent alerts, then circular-search to find relevant RBI guidelines."
    },
]


def run_evaluation():
    """Run agent evaluation with MLflow scorers."""
    from mlflow.genai.agent_server import get_invoke_function
    import nest_asyncio
    nest_asyncio.apply()

    invoke_fn = get_invoke_function()

    def predict(inputs):
        from mlflow.types.responses import ResponsesAgentRequest
        request = ResponsesAgentRequest(**inputs)
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(invoke_fn(request))
        return response

    results = evaluate(
        data=EVAL_DATA,
        predict=predict,
        scorers=[
            Completeness(),
            Fluency(),
            RelevanceToQuery(),
            Safety(),
            ToolCallCorrectness(),
        ],
    )

    print("\n=== BlackIce Agent Evaluation Results ===")
    print(results.tables["eval_results"])
    return results


if __name__ == "__main__":
    run_evaluation()
