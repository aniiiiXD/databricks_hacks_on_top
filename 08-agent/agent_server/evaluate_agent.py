"""
Digital-Artha Agent Evaluation

Uses MLflow ConversationSimulator to test the agent with realistic scenarios.
Evaluates: Completeness, Safety, Fluency, Tool Call Correctness.
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

# Test cases for the agent
EVAL_DATA = [
    {
        "inputs": {"messages": [{"role": "user", "content": "What is the current fraud rate across all UPI transactions?"}]},
        "expected": "The agent should use the financial-analytics (Genie) tool to query fraud statistics."
    },
    {
        "inputs": {"messages": [{"role": "user", "content": "What are RBI guidelines for preventing UPI fraud?"}]},
        "expected": "The agent should use the circular-search tool to retrieve relevant RBI circulars and cite them."
    },
    {
        "inputs": {"messages": [{"role": "user", "content": "I am a 30 year old farmer from Uttar Pradesh earning 2 lakh per year. What government schemes can I apply for?"}]},
        "expected": "The agent should use the check_loan_eligibility tool with age=30, income=200000, occupation=farmer, state=Uttar Pradesh."
    },
    {
        "inputs": {"messages": [{"role": "user", "content": "यूपीआई में सबसे ज्यादा फ्रॉड किस कैटेगरी में होता है?"}]},
        "expected": "The agent should respond in Hindi and use the financial-analytics tool to query fraud by category."
    },
    {
        "inputs": {"messages": [{"role": "user", "content": "Show me the top 5 highest risk transactions today"}]},
        "expected": "The agent should use the financial-analytics tool to query high-risk transactions sorted by ensemble_score."
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

    print("Evaluation Results:")
    print(results.tables["eval_results"])
    return results


if __name__ == "__main__":
    run_evaluation()
