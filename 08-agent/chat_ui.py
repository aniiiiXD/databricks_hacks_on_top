"""
Digital-Artha: Financial Intelligence Platform (Gradio)
Run: python3 chat_ui.py
Opens at: http://localhost:7860

3-tab interface:
  Tab 1: Fraud Monitor — query fraud alerts with filters
  Tab 2: Ask Digital-Artha — AI chat with tool visibility
  Tab 3: Scheme Finder — form-based loan scheme matching
"""

import gradio as gr
import requests
import json
import os

AGENT_URL = os.environ.get("AGENT_URL", "http://localhost:8000/invocations")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-eafd901f-d4a9.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Load token from .env if not in environment
if not DATABRICKS_TOKEN:
    try:
        with open(os.path.join(os.path.dirname(__file__), ".env")) as f:
            for line in f:
                if line.startswith("DATABRICKS_TOKEN="):
                    DATABRICKS_TOKEN = line.strip().split("=", 1)[1]
                elif line.startswith("DATABRICKS_HOST="):
                    DATABRICKS_HOST = line.strip().split("=", 1)[1]
    except:
        pass

WAREHOUSE_ID = ""
try:
    with open(os.path.join(os.path.dirname(__file__), ".env")) as f:
        for line in f:
            if line.startswith("WAREHOUSE_ID="):
                WAREHOUSE_ID = line.strip().split("=", 1)[1]
except:
    pass


def query_databricks_sql(sql):
    """Execute SQL directly against Databricks warehouse."""
    try:
        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"},
            json={"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s"},
            timeout=60
        )
        data = resp.json()
        if data.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = data.get("result", {}).get("data_array", [])
            return cols, rows
        return [], []
    except Exception as e:
        return ["error"], [[str(e)]]


def call_agent(message, history):
    """Send message to agent backend."""
    try:
        input_messages = []
        for h in history:
            if isinstance(h, (list, tuple)) and len(h) == 2:
                input_messages.append({"type": "message", "role": "user", "content": [{"type": "input_text", "text": h[0]}]})
                if h[1]:
                    input_messages.append({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": h[1]}]})

        input_messages.append({"type": "message", "role": "user", "content": [{"type": "input_text", "text": message}]})

        response = requests.post(AGENT_URL, json={"input": input_messages},
                                 headers={"Content-Type": "application/json"}, timeout=120)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "output" in data:
                output = data["output"]
                if isinstance(output, list):
                    texts = []
                    for item in output:
                        if isinstance(item, dict):
                            content = item.get("content", [])
                            if isinstance(content, list):
                                for c in content:
                                    if isinstance(c, dict) and "text" in c:
                                        texts.append(c["text"])
                    return "\n".join(texts) if texts else json.dumps(output, indent=2, default=str)
            return json.dumps(data, indent=2, default=str)
        return f"Error {response.status_code}: {response.text[:300]}"
    except requests.exceptions.ConnectionError:
        return "Agent not running. Start with: `uv run start-server`"
    except Exception as e:
        return f"Error: {str(e)}"


# ============================================================
# TAB 1: FRAUD MONITOR
# ============================================================

def get_fraud_alerts(min_score, category, limit):
    """Fetch fraud alerts from Databricks."""
    where = "WHERE ensemble_flag = true"
    if min_score > 0:
        where += f" AND CAST(ensemble_score AS DOUBLE) >= {min_score}"
    if category and category != "All":
        where += f" AND category = '{category}'"

    sql = f"""
    SELECT transaction_id, ROUND(amount, 2) AS amount_inr, category,
           ROUND(CAST(ensemble_score AS DOUBLE), 3) AS risk_score,
           final_risk_tier, time_slot, location, hour_of_day
    FROM digital_artha.main.gold_fraud_alerts_ml
    {where}
    ORDER BY CAST(ensemble_score AS DOUBLE) DESC
    LIMIT {int(limit)}
    """
    cols, rows = query_databricks_sql(sql)
    if not rows:
        return "No fraud alerts found matching your criteria."

    # Format as markdown table
    md = f"**{len(rows)} Fraud Alerts Found** (min score: {min_score})\n\n"
    md += "| " + " | ".join(cols) + " |\n"
    md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
    for row in rows:
        md += "| " + " | ".join(str(v) for v in row) + " |\n"
    return md


def get_fraud_summary():
    """Get overall fraud statistics."""
    sql = """
    SELECT total_transactions, flagged_transactions, fraud_rate_pct,
           anomaly_patterns_found, high_risk_senders, schemes_available
    FROM digital_artha.main.viz_kpis
    """
    cols, rows = query_databricks_sql(sql)
    if rows:
        r = rows[0]
        return f"**Total Txns:** {r[0]:,} | **Flagged:** {r[1]:,} | **Fraud Rate:** {r[2]}% | **Patterns:** {r[3]} | **High Risk Senders:** {r[4]} | **Schemes:** {r[5]}"
    return "Unable to fetch summary."


def get_pattern_breakdown():
    """Get anomaly pattern breakdown."""
    sql = """
    SELECT anomaly_pattern, occurrence_count, ROUND(avg_amount, 2) AS avg_amount,
           ROUND(total_amount_at_risk, 2) AS amount_at_risk
    FROM digital_artha.main.viz_anomaly_patterns
    ORDER BY occurrence_count DESC
    """
    cols, rows = query_databricks_sql(sql)
    if not rows:
        return "No patterns found."

    md = "**Anomaly Patterns:**\n\n"
    md += "| Pattern | Count | Avg Amount | ₹ at Risk |\n"
    md += "| --- | --- | --- | --- |\n"
    for row in rows:
        md += f"| {row[0]} | {row[1]} | ₹{row[2]} | ₹{row[3]} |\n"
    return md


# ============================================================
# TAB 3: SCHEME FINDER
# ============================================================

def find_schemes(age, income, occupation, state, gender):
    """Find matching government schemes."""
    msg = f"I am a {age} year old {occupation} from {state} earning {income} rupees per year. Gender: {gender}. What government schemes can I apply for?"
    return call_agent(msg, [])


# ============================================================
# BUILD UI
# ============================================================

with gr.Blocks(title="Digital-Artha") as demo:

    gr.Markdown("""
    # Digital-Artha: Financial Intelligence for India
    **UPI Fraud Detection** | **RBI Regulations** | **Government Scheme Finder** | **Fraud Recovery**
    """)

    with gr.Tabs():
        # ---- TAB 1: FRAUD MONITOR ----
        with gr.Tab("Fraud Monitor"):
            gr.Markdown("### Live Fraud Alert Feed")
            summary_output = gr.Markdown("Loading...")
            summary_btn = gr.Button("Refresh Summary", variant="secondary", size="sm")
            summary_btn.click(fn=get_fraud_summary, outputs=summary_output)

            with gr.Row():
                score_slider = gr.Slider(0, 1, value=0.3, step=0.05, label="Min Risk Score")
                cat_dropdown = gr.Dropdown(
                    ["All", "Education", "Shopping", "Utilities", "Food", "Grocery",
                     "Entertainment", "Fuel", "Healthcare", "Transport", "Other"],
                    value="All", label="Category"
                )
                limit_slider = gr.Slider(5, 50, value=20, step=5, label="Max Results")

            alerts_btn = gr.Button("Search Fraud Alerts", variant="primary")
            alerts_output = gr.Markdown("Click 'Search Fraud Alerts' to begin.")
            alerts_btn.click(fn=get_fraud_alerts, inputs=[score_slider, cat_dropdown, limit_slider], outputs=alerts_output)

            gr.Markdown("---")
            pattern_btn = gr.Button("Show Anomaly Patterns", variant="secondary")
            pattern_output = gr.Markdown()
            pattern_btn.click(fn=get_pattern_breakdown, outputs=pattern_output)

        # ---- TAB 2: CHAT ----
        with gr.Tab("Ask Digital-Artha"):
            gr.Markdown("""
            ### AI-Powered Financial Assistant
            Ask about fraud alerts, RBI regulations, government schemes, or what to do if you were scammed.
            """)
            gr.ChatInterface(
                fn=call_agent,
                examples=[
                    "Show me the top 5 highest risk fraud alerts",
                    "I was scammed via QR code. What should I do?",
                    "What are the RBI guidelines for UPI fraud prevention?",
                    "Which merchant categories have the highest fraud rate?",
                    "I am a 25 year old farmer from UP earning 2 lakh. What schemes can I apply for?",
                    "यूपीआई फ्रॉड के बारे में बताओ",
                ],
            )

        # ---- TAB 3: SCHEME FINDER ----
        with gr.Tab("Scheme Finder"):
            gr.Markdown("""
            ### Government Scheme Eligibility Checker
            Enter your details to find matching financial inclusion schemes from 170+ programs.
            """)
            with gr.Row():
                age_input = gr.Number(value=25, label="Age", minimum=18, maximum=100)
                income_input = gr.Number(value=150000, label="Annual Income (₹)")
            with gr.Row():
                occupation_input = gr.Dropdown(
                    ["farmer", "street_vendor", "artisan", "student", "salaried",
                     "self_employed", "entrepreneur", "daily_wage", "fisherman"],
                    value="farmer", label="Occupation"
                )
                state_input = gr.Dropdown(
                    ["Maharashtra", "Uttar Pradesh", "Tamil Nadu", "Karnataka",
                     "Delhi", "Gujarat", "Rajasthan", "West Bengal", "Bihar",
                     "Andhra Pradesh", "Telangana", "Kerala", "Madhya Pradesh"],
                    value="Maharashtra", label="State"
                )
                gender_input = gr.Radio(["male", "female", "all"], value="all", label="Gender")

            scheme_btn = gr.Button("Find Matching Schemes", variant="primary")
            scheme_output = gr.Markdown("Enter your details and click 'Find Matching Schemes'")
            scheme_btn.click(fn=find_schemes,
                           inputs=[age_input, income_input, occupation_input, state_input, gender_input],
                           outputs=scheme_output)

        # ---- TAB 4: ABOUT ----
        with gr.Tab("About"):
            gr.Markdown("""
            ### About Digital-Artha

            **Digital-Artha** detects UPI fraud using ML, answers RBI regulation questions in Hindi/English via RAG,
            and matches rural Indians to government loan schemes they qualify for.

            **Architecture:** Bronze → Silver → Gold → Platinum (4-tier medallion on Databricks)

            **ML Pipeline:** IsolationForest + KMeans ensemble with stratified sampling

            **Agent:** LangGraph ReAct agent with 7 tools (4 custom + 3 MCP)

            **Data:** 9 real Indian datasets (RBI circulars, PMJDY, bank fraud stats, UPI transactions)

            **Databricks Features:** 17 platform features used

            ---
            *Bharat Bricks Hackathon 2026 | IIT Bombay | Track: Digital-Artha*
            """)

    # Auto-load summary on page load
    demo.load(fn=get_fraud_summary, outputs=summary_output)


if __name__ == "__main__":
    demo.launch(server_port=7860, share=False)
