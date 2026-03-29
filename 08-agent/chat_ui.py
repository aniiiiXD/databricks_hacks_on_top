"""
Digital-Artha: Financial Intelligence Platform
Run: python3 chat_ui.py
Public URL for judges: auto-generated via share=True
"""

import gradio as gr
import requests
import json
import os

AGENT_URL = os.environ.get("AGENT_URL", "http://localhost:8000/invocations")
DATABRICKS_HOST = ""
DATABRICKS_TOKEN = ""
WAREHOUSE_ID = ""

# Load from .env
try:
    with open(os.path.join(os.path.dirname(__file__), ".env")) as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                if k == "DATABRICKS_TOKEN": DATABRICKS_TOKEN = v
                elif k == "DATABRICKS_HOST": DATABRICKS_HOST = v
                elif k == "WAREHOUSE_ID": WAREHOUSE_ID = v
except:
    pass


def query_sql(sql):
    """Execute SQL against Databricks warehouse."""
    try:
        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s"},
            timeout=60
        )
        data = resp.json()
        if data.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = data.get("result", {}).get("data_array", [])
            return cols, rows
        return [], []
    except:
        return [], []


def call_agent(message, history):
    """Send message to agent."""
    try:
        input_messages = []
        for h in history:
            if isinstance(h, (list, tuple)) and len(h) == 2:
                input_messages.append({"type": "message", "role": "user", "content": [{"type": "input_text", "text": str(h[0])}]})
                if h[1]:
                    input_messages.append({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": str(h[1])}]})
        input_messages.append({"type": "message", "role": "user", "content": [{"type": "input_text", "text": message}]})

        response = requests.post(AGENT_URL, json={"input": input_messages},
                                 headers={"Content-Type": "application/json"}, timeout=120)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "output" in data:
                texts = []
                for item in data["output"]:
                    if isinstance(item, dict):
                        content = item.get("content", [])
                        if isinstance(content, list):
                            for c in content:
                                if isinstance(c, dict) and "text" in c:
                                    texts.append(c["text"])
                return "\n".join(texts) if texts else json.dumps(data, indent=2, default=str)
            return json.dumps(data, indent=2, default=str)
        return f"Error {response.status_code}: {response.text[:300]}"
    except requests.exceptions.ConnectionError:
        return "**Agent not running.** Start with: `uv run start-server` in the 08-agent directory."
    except Exception as e:
        return f"Error: {str(e)}"


def make_table(cols, rows, max_rows=20):
    """Convert SQL results to markdown table."""
    if not rows:
        return "No data found."
    rows = rows[:max_rows]
    md = "| " + " | ".join(cols) + " |\n"
    md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
    for row in rows:
        md += "| " + " | ".join(str(v) if v is not None else "-" for v in row) + " |\n"
    return md


# ============================================================
# TAB 1: FRAUD COMMAND CENTER
# ============================================================

def get_kpis():
    cols, rows = query_sql("SELECT * FROM digital_artha.main.viz_kpis")
    if not rows:
        return "Unable to connect to Databricks. Check your .env configuration."
    r = rows[0]
    return f"""
| Metric | Value |
|--------|-------|
| Total Transactions Analyzed | **{r[0]}** |
| Flagged as Suspicious | **{r[1]}** |
| Fraud Rate | **{r[2]}%** |
| Total Amount at Risk | **₹{r[3]}** |
| Anomaly Patterns Discovered | **{r[4]}** |
| High Risk Senders | **{r[5]}** |
| Government Schemes Indexed | **{r[6]}** |
| RBI Circulars Searchable | **{r[7]}** |
| Fraud Recovery Types | **{r[8]}** |
"""


def get_alerts(min_score, category, limit):
    where = "WHERE ensemble_flag = true"
    if min_score > 0:
        where += f" AND CAST(ensemble_score AS DOUBLE) >= {min_score}"
    if category and category != "All":
        where += f" AND category = '{category}'"
    sql = f"""
    SELECT transaction_id, ROUND(amount, 2) AS amount, category,
           ROUND(CAST(ensemble_score AS DOUBLE), 3) AS risk_score,
           final_risk_tier AS tier, time_slot, location
    FROM digital_artha.main.gold_fraud_alerts_ml
    {where} ORDER BY CAST(ensemble_score AS DOUBLE) DESC LIMIT {int(limit)}
    """
    cols, rows = query_sql(sql)
    if not rows:
        return "No alerts found matching criteria."
    header = f"**{len(rows)} Fraud Alerts** (min score: {min_score})\n\n"
    return header + make_table(cols, rows)


def get_patterns():
    cols, rows = query_sql("""
    SELECT anomaly_pattern AS Pattern, occurrence_count AS Count,
           ROUND(avg_amount, 0) AS 'Avg ₹', ROUND(total_amount_at_risk, 0) AS 'Total ₹ at Risk',
           unique_senders AS Senders
    FROM digital_artha.main.viz_anomaly_patterns ORDER BY occurrence_count DESC
    """)
    return "**Anomaly Patterns Discovered by ML Ensemble:**\n\n" + make_table(cols, rows)


def get_risky_senders():
    cols, rows = query_sql("""
    SELECT sender_id, total_transactions AS txns, fraud_count AS frauds,
           ROUND(composite_risk, 3) AS risk, ROUND(late_night_pct, 1) AS 'night%',
           ROUND(weekend_pct, 1) AS 'weekend%'
    FROM digital_artha.main.viz_risky_senders LIMIT 15
    """)
    return "**Top 15 Riskiest Sender Accounts:**\n\n" + make_table(cols, rows)


# ============================================================
# TAB 2: INDIA STORY
# ============================================================

def get_india_story():
    # UPI growth
    _, upi = query_sql("SELECT date, volume_millions FROM digital_artha.main.viz_upi_growth ORDER BY date DESC LIMIT 1")
    upi_latest = f"{upi[0][1]}M" if upi else "N/A"

    # Bank fraud
    _, fraud = query_sql("SELECT SUM(loss_crore) FROM digital_artha.main.viz_bank_fraud WHERE fiscal_year = '2023-24'")
    fraud_total = f"₹{fraud[0][0]} Cr" if fraud and fraud[0][0] else "N/A"

    # Vulnerability
    cols, vuln = query_sql("""
    SELECT state, ROUND(vulnerability_index, 2) AS vuln_index, vulnerability_level,
           ROUND(fraud_rate_pct, 2) AS fraud_rate, ROUND(internet_per_100, 0) AS internet
    FROM digital_artha.main.viz_state_vulnerability
    WHERE vulnerability_index > 0 ORDER BY vulnerability_index DESC LIMIT 10
    """)

    md = f"""
**India's UPI ecosystem** processes **{upi_latest} transactions/month** — the world's largest real-time payment network.

**But fraud is rising.** Indian banks reported **{fraud_total}** in digital payment fraud losses in FY2023-24.

**The most vulnerable states** have high fraud rates AND low internet penetration AND low banking access:

"""
    md += make_table(cols, vuln)
    md += "\n*Vulnerability Index = fraud_risk × digital_divide × inclusion_gap*"
    return md


def get_bank_fraud():
    cols, rows = query_sql("""
    SELECT bank_name AS Bank, fraud_count AS Cases, ROUND(loss_crore, 1) AS 'Loss ₹Cr',
           ROUND(recovered_crore, 1) AS 'Recovered ₹Cr',
           ROUND(recovered_crore * 100 / NULLIF(loss_crore, 0), 0) AS 'Recovery%'
    FROM digital_artha.main.viz_bank_fraud WHERE fiscal_year = '2023-24'
    ORDER BY loss_crore DESC LIMIT 10
    """)
    return "**Bank-wise Fraud Losses FY2023-24:**\n\n" + make_table(cols, rows)


# ============================================================
# TAB 3: SCHEME FINDER
# ============================================================

def find_schemes(age, income, occupation, state, gender, language):
    lang_note = f" Respond in {language}." if language != "English" else ""
    msg = f"I am a {int(age)} year old {occupation} from {state} earning {int(income)} rupees per year. Gender: {gender}. What government schemes can I apply for?{lang_note}"
    return call_agent(msg, [])


# ============================================================
# TAB 4: FRAUD RECOVERY
# ============================================================

def get_recovery_guide(fraud_type):
    if fraud_type == "All Types":
        where = ""
    else:
        where = f"WHERE fraud_type = '{fraud_type}'"
    cols, rows = query_sql(f"""
    SELECT fraud_type AS Type, rbi_rule AS 'RBI Rule',
           recovery_steps AS 'What To Do', report_to AS 'Report To',
           time_limit_days AS 'Days Limit', max_liability_inr AS 'Max Liability ₹'
    FROM digital_artha.main.viz_recovery_guide {where}
    """)
    if not rows:
        return "No recovery guide found."
    md = "**RBI-Mandated Fraud Recovery Steps:**\n\n"
    for row in rows:
        md += f"### {row[0]}\n"
        md += f"**RBI Rule:** {row[1]}\n\n"
        md += f"**What to do:**\n{row[2]}\n\n"
        md += f"**Report to:** {row[3]}\n\n"
        md += f"**Time limit:** {row[4]} days | **Max liability:** ₹{row[5]}\n\n---\n\n"
    return md


# ============================================================
# BUILD THE APP
# ============================================================

custom_css = """
.gradio-container { max-width: 1200px !important; }
footer { display: none !important; }
"""

with gr.Blocks(title="Digital-Artha", css=custom_css) as demo:

    gr.Markdown("""
    <div style="text-align: center; padding: 20px 0;">
    <h1>Digital-Artha</h1>
    <h3>AI-Powered Financial Intelligence for India</h3>
    <p><b>Fraud Detection</b> &nbsp;|&nbsp; <b>RBI Regulations</b> &nbsp;|&nbsp; <b>Government Schemes</b> &nbsp;|&nbsp; <b>Fraud Recovery</b></p>
    <p style="color: #888; font-size: 0.9em;">Powered by Databricks Lakehouse &nbsp;|&nbsp; IsolationForest + KMeans Ensemble &nbsp;|&nbsp; RAG over 80 RBI Circulars &nbsp;|&nbsp; 170 Government Schemes</p>
    </div>
    """)

    with gr.Tabs():

        # ---- TAB 1: COMMAND CENTER ----
        with gr.Tab("Command Center"):
            kpi_output = gr.Markdown("Loading...")
            with gr.Row():
                refresh_btn = gr.Button("Refresh KPIs", variant="secondary", size="sm")
            refresh_btn.click(fn=get_kpis, outputs=kpi_output)

            gr.Markdown("---")
            gr.Markdown("### Fraud Alert Search")
            with gr.Row():
                score_slider = gr.Slider(0, 1, value=0.3, step=0.05, label="Min Risk Score")
                cat_dd = gr.Dropdown(["All", "Education", "Shopping", "Utilities", "Food", "Grocery",
                                      "Entertainment", "Fuel", "Healthcare", "Transport", "Other"],
                                     value="All", label="Category")
                limit_sl = gr.Slider(5, 50, value=15, step=5, label="Results")
            search_btn = gr.Button("Search Alerts", variant="primary")
            alerts_out = gr.Markdown()
            search_btn.click(fn=get_alerts, inputs=[score_slider, cat_dd, limit_sl], outputs=alerts_out)

            gr.Markdown("---")
            with gr.Row():
                with gr.Column():
                    pat_btn = gr.Button("Show Anomaly Patterns", size="sm")
                    pat_out = gr.Markdown()
                    pat_btn.click(fn=get_patterns, outputs=pat_out)
                with gr.Column():
                    risk_btn = gr.Button("Show Risky Senders", size="sm")
                    risk_out = gr.Markdown()
                    risk_btn.click(fn=get_risky_senders, outputs=risk_out)

        # ---- TAB 2: ASK AGENT ----
        with gr.Tab("Ask Digital-Artha"):
            gr.Markdown("### AI Financial Assistant — 7 Tools | Hindi Support | MCP Protocol")
            gr.ChatInterface(
                fn=call_agent,
                examples=[
                    "Show me the top 5 highest risk fraud alerts",
                    "I was scammed via QR code. What should I do?",
                    "What are the RBI guidelines for UPI fraud prevention?",
                    "Which merchant categories have the highest fraud rate?",
                    "I am a 25 year old farmer from UP earning 2 lakh. What schemes can I apply for?",
                    "यूपीआई फ्रॉड से कैसे बचें?",
                    "मैं एक 30 साल की महिला हूं, महाराष्ट्र से, आय 1.5 लाख। कौन सी योजनाएं हैं?",
                ],
            )

        # ---- TAB 3: INDIA STORY ----
        with gr.Tab("India Story"):
            story_out = gr.Markdown("Loading...")
            with gr.Row():
                story_btn = gr.Button("Load India's Digital Payment Story", variant="primary")
                bank_btn = gr.Button("Bank Fraud Losses", variant="secondary")
            story_btn.click(fn=get_india_story, outputs=story_out)
            bank_out = gr.Markdown()
            bank_btn.click(fn=get_bank_fraud, outputs=bank_out)

        # ---- TAB 4: SCHEME FINDER ----
        with gr.Tab("Scheme Finder"):
            gr.Markdown("### Government Scheme Eligibility — 170+ Programs Indexed")
            with gr.Row():
                age_in = gr.Number(value=25, label="Age", minimum=18, maximum=100)
                income_in = gr.Number(value=150000, label="Annual Income (₹)")
            with gr.Row():
                occ_in = gr.Dropdown(["farmer", "street_vendor", "artisan", "student", "salaried",
                                      "self_employed", "entrepreneur", "daily_wage", "fisherman"],
                                     value="farmer", label="Occupation")
                state_in = gr.Dropdown(["Maharashtra", "Uttar Pradesh", "Tamil Nadu", "Karnataka",
                                        "Delhi", "Gujarat", "Rajasthan", "West Bengal", "Bihar",
                                        "Andhra Pradesh", "Telangana", "Kerala", "Madhya Pradesh",
                                        "Punjab", "Haryana", "Odisha", "Jharkhand", "Assam"],
                                       value="Maharashtra", label="State")
            with gr.Row():
                gender_in = gr.Radio(["male", "female", "all"], value="all", label="Gender")
                lang_in = gr.Dropdown(["English", "Hindi", "Marathi", "Tamil", "Telugu"],
                                      value="English", label="Response Language")
            scheme_btn = gr.Button("Find Matching Schemes", variant="primary")
            scheme_out = gr.Markdown("Enter your details and click 'Find Matching Schemes'")
            scheme_btn.click(fn=find_schemes, inputs=[age_in, income_in, occ_in, state_in, gender_in, lang_in], outputs=scheme_out)

        # ---- TAB 5: FRAUD RECOVERY ----
        with gr.Tab("Fraud Recovery"):
            gr.Markdown("### If You Were Scammed — RBI-Mandated Recovery Steps")
            fraud_dd = gr.Dropdown(
                ["All Types", "QR Code Scam", "Phishing / Fake Bank Call", "SIM Swap Fraud",
                 "Fake UPI Collect Request", "Remote Access / Screen Sharing", "Fake Merchant / Refund Fraud"],
                value="All Types", label="Select Fraud Type"
            )
            recov_btn = gr.Button("Show Recovery Steps", variant="primary")
            recov_out = gr.Markdown()
            recov_btn.click(fn=get_recovery_guide, inputs=fraud_dd, outputs=recov_out)

        # ---- TAB 6: ABOUT ----
        with gr.Tab("Architecture"):
            gr.Markdown("""
### System Architecture

```
Data Sources (9 real Indian datasets)
  → Auto Loader (CSV/JSON ingestion)
    → Bronze Layer (750K+ rows, PK constraints, CDF)
      → Silver Layer (DLT EXPECT constraints, validated)
        → Gold Layer (business-ready, liquid clustering)  [COLD]
          → Platinum Layer (ML ensemble + patterns)       [WARM]
            → Serving Layer                               [HOT]
              ├── Dashboard (4 pages, 20+ widgets)
              ├── Genie Space (NL queries)
              ├── Agent (7 tools, MCP, Hindi)
              └── This App (Gradio, public URL)
```

### ML Pipeline
- **IsolationForest** (300 trees, 0.02 contamination) + **KMeans** (k=8)
- **Ensemble**: 0.45×IF + 0.30×KM + 0.25×Rules → Risk tiers
- **Stratified sampling**: All fraud preserved, normal sampled
- **8 named anomaly patterns** discovered

### Agent Tools (7)
| Tool | Function |
|------|----------|
| Fraud Alert Lookup | Parameterized SQL via Databricks SDK |
| Loan Eligibility | PySpark rules + LLM explanation |
| Fraud Recovery Guide | RBI-mandated recovery steps |
| RBI Circular Search | MCP Vector Search (FAISS) |
| Genie Analytics | MCP Genie Space (NL → SQL) |
| Genie Poll | Get async Genie results |
| Current Time | Utility |

### Databricks Features (17+)
Delta Lake, Unity Catalog, Auto Loader, ETL Pipeline (DLT), Spark SQL,
Foundation Model API, Vector Search (MCP), Genie Space (MCP),
Lakeview Dashboard, Metric Views, Databricks SDK, Change Data Feed,
Liquid Clustering, UC Tags, FAISS on Volumes, MCP Protocol, Apps Architecture

---
*Bharat Bricks Hackathon 2026 | IIT Bombay*
""")

    demo.load(fn=get_kpis, outputs=kpi_output)
    demo.load(fn=get_india_story, outputs=story_out)


if __name__ == "__main__":
    print("\n" + "="*50)
    print("Digital-Artha: Financial Intelligence Platform")
    print("="*50)
    print("\nStarting Gradio app with public URL...")
    print("Share the public URL with judges!\n")
    demo.launch(server_port=7860, share=True)
