"""
BlackIce: Financial Intelligence Platform
Run: python3 chat_ui.py
Public URL for judges: auto-generated via share=True
"""

import gradio as gr
import plotly.graph_objects as go
import requests
import json
import os
import time

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

# -- Plotly dark theme defaults --
GOLD = "#c5a059"
GOLD_LIGHT = "rgba(197,160,89,0.6)"
GOLD_DIM = "rgba(197,160,89,0.15)"
BG = "#0a0a0a"
TEXT = "#ededed"
TEXT_DIM = "#888888"
GRID = "rgba(255,255,255,0.04)"
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color=TEXT, size=12),
    margin=dict(l=50, r=20, t=40, b=50),
    xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    yaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
)


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
            if isinstance(h, dict) and "role" in h:
                role = h["role"]
                text = str(h.get("content", ""))
                if role == "user":
                    input_messages.append({"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]})
                elif role == "assistant" and text:
                    input_messages.append({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]})
            elif isinstance(h, (list, tuple)) and len(h) == 2:
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


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _safe_int(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


# ============================================================
# KPIs — HTML cards instead of markdown table
# ============================================================

def get_kpis():
    cols, rows = query_sql("SELECT * FROM digital_artha.main.viz_kpis")
    if not rows:
        return "<div style='color:#888; padding:20px;'>Unable to connect to Databricks. Check .env configuration.</div>"
    r = rows[0]
    kpis = [
        ("Transactions", f"{_safe_int(r[0]):,}", "Analyzed by ML ensemble"),
        ("Flagged", str(r[1]), "Suspicious transactions"),
        ("Fraud Rate", f"{r[2]}%", "Ensemble detection rate"),
        ("Amount at Risk", f"\u20b9{r[3]}", "Total flagged volume"),
        ("Patterns", str(r[4]), "Anomaly types found"),
        ("Risky Senders", str(r[5]), "High composite risk"),
        ("Schemes", str(r[6]), "Gov programs indexed"),
        ("RBI Circulars", str(r[7]), "Searchable via RAG"),
        ("Recovery Types", str(r[8]), "Fraud recovery guides"),
    ]
    cards = ""
    for label, value, sub in kpis:
        cards += f"""<div style="flex:1; min-width:140px; padding:20px 16px; border:1px solid rgba(197,160,89,0.12); border-radius:8px; background:rgba(255,255,255,0.015); text-align:center;">
            <div style="font-family:'Montserrat',sans-serif; font-size:0.65em; text-transform:uppercase; letter-spacing:2px; color:#888; margin-bottom:8px;">{label}</div>
            <div style="font-family:'Playfair Display',serif; font-size:1.8em; color:#c5a059; font-weight:400;">{value}</div>
            <div style="font-size:0.7em; color:#555; margin-top:4px;">{sub}</div>
        </div>"""
    return f'<div style="display:flex; gap:12px; flex-wrap:wrap; margin:10px 0;">{cards}</div>'


# ============================================================
# CHARTS — Plotly visualizations from unused viz_ views
# ============================================================

def get_fraud_heatmap():
    """Hour x Day fraud heatmap."""
    cols, rows = query_sql("""
    SELECT hour_of_day, day_of_week, fraud_count, total_txns
    FROM digital_artha.main.viz_fraud_heatmap
    ORDER BY day_of_week, hour_of_day
    """)
    if not rows:
        return go.Figure().update_layout(title="No heatmap data", **PLOTLY_LAYOUT)

    day_map = {"Sunday": 0, "Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6}
    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    matrix = [[0]*24 for _ in range(7)]
    for row in rows:
        h = _safe_int(row[0])
        d_str = str(row[1]).strip()
        d = day_map.get(d_str, _safe_int(row[1]))
        count = _safe_int(row[2])
        if 0 <= h < 24 and 0 <= d < 7:
            matrix[d][h] = count

    fig = go.Figure(data=go.Heatmap(
        z=matrix, x=list(range(24)), y=day_names,
        colorscale=[[0, "#0a0a0a"], [0.3, "#2a1a08"], [0.6, "#c5a059"], [1, "#ff4444"]],
        colorbar=dict(title="Frauds", tickfont=dict(color=TEXT_DIM)),
    ))
    fig.update_layout(title="Fraud Density: Hour × Day", xaxis_title="Hour", yaxis_title="", **PLOTLY_LAYOUT)
    return fig


def get_risk_distribution():
    """Risk score distribution bar chart."""
    cols, rows = query_sql("""
    SELECT risk_bucket, txn_count, fraud_in_bucket
    FROM digital_artha.main.viz_risk_distribution
    ORDER BY risk_bucket
    """)
    if not rows:
        return go.Figure().update_layout(title="No distribution data", **PLOTLY_LAYOUT)

    buckets = [r[0] for r in rows]
    counts = [_safe_int(r[1]) for r in rows]
    frauds = [_safe_int(r[2]) for r in rows]
    fraud_pcts = [round(f*100/max(c, 1), 1) for c, f in zip(counts, frauds)]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=buckets, y=counts, name="Total Txns",
        marker_color=GOLD_DIM,
        hovertemplate="%{x}<br>Transactions: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=buckets, y=frauds, name="Fraud Txns",
        marker_color="#ff4444",
        hovertemplate="%{x}<br>Fraud: %{y:,}<extra></extra>",
    ))
    fig.update_layout(
        title="Risk Score Distribution",
        xaxis_title="Risk Score Bucket", yaxis_title="Transaction Count",
        barmode="overlay", bargap=0.15,
        legend=dict(font=dict(color=TEXT_DIM)),
        **PLOTLY_LAYOUT,
    )
    return fig


def get_monthly_trend():
    """Monthly fraud trend line chart."""
    cols, rows = query_sql("""
    SELECT month, total_txns, fraud_txns, fraud_rate_pct, fraud_volume
    FROM digital_artha.main.viz_monthly_trend
    ORDER BY month
    """)
    if not rows:
        return go.Figure().update_layout(title="No trend data", **PLOTLY_LAYOUT)

    months = [str(r[0])[:7] for r in rows]
    rates = [_safe_float(r[3]) for r in rows]
    fraud_txns = [_safe_int(r[2]) for r in rows]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=months, y=rates, name="Fraud Rate %",
        line=dict(color=GOLD, width=2), mode="lines+markers",
        marker=dict(size=7),
    ))
    fig.add_trace(go.Bar(
        x=months, y=fraud_txns, name="Fraud Txns",
        marker_color="rgba(255,68,68,0.3)", yaxis="y2",
    ))
    trend_layout = {**PLOTLY_LAYOUT}
    trend_layout["yaxis"] = dict(title="Fraud Rate %", gridcolor=GRID, side="left")
    fig.update_layout(
        title="Monthly Fraud Trend",
        xaxis_title="Month",
        yaxis2=dict(title="Fraud Txn Count", overlaying="y", side="right", gridcolor="rgba(0,0,0,0)"),
        legend=dict(font=dict(color=TEXT_DIM)),
        **trend_layout,
    )
    return fig


def get_amount_comparison():
    """Fraud vs normal amount distribution."""
    cols, rows = query_sql("""
    SELECT amount_range, fraud_count, normal_count
    FROM digital_artha.main.viz_amount_comparison
    """)
    if not rows:
        return go.Figure().update_layout(title="No amount data", **PLOTLY_LAYOUT)

    ranges = [r[0] for r in rows]
    fraud = [_safe_int(r[1]) for r in rows]
    normal = [_safe_int(r[2]) for r in rows]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=ranges, y=normal, name="Normal", marker_color=GOLD_DIM))
    fig.add_trace(go.Bar(x=ranges, y=fraud, name="Fraud", marker_color="#ff4444"))
    fig.update_layout(
        title="Transaction Amounts: Fraud vs Normal",
        xaxis_title="Amount Range", yaxis_title="Count",
        barmode="group", bargap=0.2,
        legend=dict(font=dict(color=TEXT_DIM)),
        **PLOTLY_LAYOUT,
    )
    return fig


def get_category_risk_matrix():
    """Category x time slot heatmap."""
    cols, rows = query_sql("""
    SELECT category, time_slot, fraud_rate_pct, total_txns
    FROM digital_artha.main.viz_category_time_matrix
    """)
    if not rows:
        return go.Figure().update_layout(title="No category data", **PLOTLY_LAYOUT)

    categories = sorted(set(r[0] for r in rows if r[0]))
    slots = sorted(set(r[1] for r in rows if r[1]))
    cat_idx = {c: i for i, c in enumerate(categories)}
    slot_idx = {s: i for i, s in enumerate(slots)}

    matrix = [[0.0]*len(slots) for _ in range(len(categories))]
    for row in rows:
        c, s = row[0], row[1]
        if c in cat_idx and s in slot_idx:
            matrix[cat_idx[c]][slot_idx[s]] = _safe_float(row[2])

    fig = go.Figure(data=go.Heatmap(
        z=matrix, x=slots, y=categories,
        colorscale=[[0, "#0a0a0a"], [0.3, "#2a1a08"], [0.6, "#c5a059"], [1, "#ff4444"]],
        colorbar=dict(title="Fraud %", tickfont=dict(color=TEXT_DIM)),
    ))
    fig.update_layout(
        title="Category x Time Slot Risk Matrix",
        **PLOTLY_LAYOUT,
    )
    return fig


# ============================================================
# FRAUD ALERTS with drill-down
# ============================================================

def get_alerts(min_score, category, limit):
    where = "WHERE ensemble_flag = true"
    if min_score > 0:
        where += f" AND CAST(ensemble_score AS DOUBLE) >= {min_score}"
    if category and category != "All":
        where += f" AND category = '{category}'"
    sql = f"""
    SELECT transaction_id, ROUND(amount, 2) AS amount, category,
           ROUND(CAST(ensemble_score AS DOUBLE), 3) AS risk_score,
           final_risk_tier AS tier, time_slot, location, sender_id
    FROM digital_artha.main.gold_fraud_alerts_ml
    {where} ORDER BY CAST(ensemble_score AS DOUBLE) DESC LIMIT {int(limit)}
    """
    cols, rows = query_sql(sql)
    if not rows:
        return "No alerts found matching criteria."
    header = f"**{len(rows)} Fraud Alerts** (min score: {min_score})\n\n"
    return header + make_table(cols, rows)


def load_categories():
    """Fetch actual categories from the data."""
    cols, rows = query_sql("""
    SELECT DISTINCT category FROM digital_artha.main.gold_fraud_alerts_ml
    WHERE category IS NOT NULL ORDER BY category
    """)
    cats = ["All"] + [r[0] for r in rows if r[0]]
    return cats if len(cats) > 1 else ["All", "Education", "Shopping", "Utilities", "Food", "Grocery",
                                        "Entertainment", "Fuel", "Healthcare", "Transport", "Other"]


# ============================================================
# ANOMALY PATTERNS with drill-down
# ============================================================

def get_patterns():
    cols, rows = query_sql("""
    SELECT anomaly_pattern AS Pattern, occurrence_count AS Count,
           ROUND(avg_amount, 0) AS 'Avg Amount', ROUND(total_amount_at_risk, 0) AS 'Total at Risk',
           unique_senders AS Senders, ROUND(pct_of_all_fraud, 1) AS 'Share %'
    FROM digital_artha.main.viz_anomaly_patterns ORDER BY occurrence_count DESC
    """)
    if not rows:
        return "No pattern data found.", gr.update(choices=[], value=None)
    pattern_names = [r[0] for r in rows if r[0]]
    md = "**Anomaly Patterns Discovered by ML Ensemble:**\n\n" + make_table(cols, rows)
    md += "\n\n*Select a pattern below to see the senders driving it.*"
    return md, gr.update(choices=pattern_names, value=pattern_names[0] if pattern_names else None)


def drill_pattern_senders(pattern_name):
    """Show top senders for a specific anomaly pattern."""
    if not pattern_name:
        return "Select a pattern above first."
    cols, rows = query_sql(f"""
    SELECT sender_id, COUNT(*) AS flagged_txns, ROUND(SUM(amount), 0) AS total_amount,
           ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 3) AS avg_score
    FROM digital_artha.main.gold_fraud_alerts_ml
    WHERE anomaly_pattern = '{pattern_name}' AND ensemble_flag = true
    GROUP BY sender_id ORDER BY flagged_txns DESC LIMIT 15
    """)
    if not rows:
        return f"No senders found for pattern: {pattern_name}"
    return f"**Senders driving '{pattern_name}':**\n\n" + make_table(cols, rows)


# ============================================================
# RISKY SENDERS with drill-down
# ============================================================

def get_risky_senders():
    cols, rows = query_sql("""
    SELECT sender_id, total_transactions AS txns, fraud_count AS frauds,
           ROUND(composite_risk, 3) AS risk, ROUND(late_night_pct, 1) AS 'night%',
           ROUND(weekend_pct, 1) AS 'weekend%', unique_receivers AS receivers
    FROM digital_artha.main.viz_risky_senders LIMIT 20
    """)
    if not rows:
        return "No sender data found.", gr.update(choices=[], value=None)
    sender_ids = [r[0] for r in rows if r[0]]
    md = "**Top 20 Riskiest Sender Accounts:**\n\n" + make_table(cols, rows)
    md += "\n\n*Select a sender below to see their flagged transactions.*"
    return md, gr.update(choices=sender_ids, value=sender_ids[0] if sender_ids else None)


def drill_sender_txns(sender_id):
    """Show flagged transactions for a specific sender."""
    if not sender_id:
        return "Select a sender above first."
    cols, rows = query_sql(f"""
    SELECT transaction_id, ROUND(amount, 2) AS amount, category,
           ROUND(CAST(ensemble_score AS DOUBLE), 3) AS score, final_risk_tier AS tier,
           time_slot, location, receiver_id
    FROM digital_artha.main.gold_fraud_alerts_ml
    WHERE sender_id = '{sender_id}' AND ensemble_flag = true
    ORDER BY CAST(ensemble_score AS DOUBLE) DESC LIMIT 20
    """)
    if not rows:
        return f"No flagged transactions for sender: {sender_id}"
    return f"**Flagged transactions for `{sender_id}`:**\n\n" + make_table(cols, rows)


# ============================================================
# FRAUD RINGS
# ============================================================

def get_fraud_rings():
    cols, rows = query_sql("""
    SELECT ring_id, accounts AS size, total_transactions AS txns,
           ROUND(fraud_rate, 2) AS fraud_rate,
           ROUND(density, 3) AS density, has_cycles,
           ROUND(total_amount, 0) AS total_amount
    FROM digital_artha.main.detected_fraud_rings
    WHERE accounts >= 3
    ORDER BY total_amount DESC LIMIT 20
    """)
    if not rows:
        return "No fraud rings detected."
    md = "**Detected Fraud Rings** (NetworkX: connected components + cycle detection)\n\n"
    md += make_table(cols, rows)
    return md


def get_ring_network_graph():
    """Build a Plotly network graph from fraud ring edge data."""
    cols, rows = query_sql("""
    SELECT source, target, weight, volume
    FROM digital_artha.main.viz_fraud_ring_edges
    ORDER BY weight DESC
    LIMIT 200
    """)
    if not rows:
        return go.Figure().update_layout(title="No ring edge data", **PLOTLY_LAYOUT)

    import math
    # Build adjacency
    nodes = set()
    edges = []
    for row in rows:
        src, tgt = str(row[0]), str(row[1])
        w = _safe_int(row[2], 1)
        vol = _safe_float(row[3])
        nodes.add(src)
        nodes.add(tgt)
        edges.append((src, tgt, w, vol))

    # Layout: circular with some randomization
    node_list = sorted(nodes)
    n = len(node_list)
    pos = {}
    for i, node in enumerate(node_list):
        angle = 2 * math.pi * i / n
        r = 1.0 + (hash(node) % 100) / 200.0  # slight randomization
        pos[node] = (r * math.cos(angle), r * math.sin(angle))

    # Edge traces
    edge_x, edge_y = [], []
    for src, tgt, w, vol in edges:
        x0, y0 = pos[src]
        x1, y1 = pos[tgt]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y, mode='lines',
        line=dict(width=0.8, color='rgba(100,180,255,0.25)'),
        hoverinfo='none'
    ))

    # Node trace — bright colors against dark background
    node_x = [pos[n][0] for n in node_list]
    node_y = [pos[n][1] for n in node_list]
    degree = {}
    for src, tgt, w, vol in edges:
        degree[src] = degree.get(src, 0) + w
        degree[tgt] = degree.get(tgt, 0) + w
    node_colors = [degree.get(n, 0) for n in node_list]
    node_sizes = [max(6, min(25, degree.get(n, 0) * 3)) for n in node_list]
    node_text = [f"<b>{n}</b><br>Connections: {degree.get(n, 0)}" for n in node_list]

    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode='markers',
        marker=dict(
            size=node_sizes,
            color=node_colors,
            colorscale=[[0, "#00cc88"], [0.3, "#00aaff"], [0.6, "#ff8800"], [1, "#ff2222"]],
            colorbar=dict(title="Connections", tickfont=dict(color=TEXT_DIM)),
            line=dict(width=1, color='rgba(255,255,255,0.3)')
        ),
        text=node_text, hoverinfo='text'
    ))

    layout = {**PLOTLY_LAYOUT}
    layout["xaxis"] = dict(showgrid=False, zeroline=False, showticklabels=False)
    layout["yaxis"] = dict(showgrid=False, zeroline=False, showticklabels=False)
    fig.update_layout(
        title=f"Fraud Ring Network ({n} accounts, {len(edges)} connections)",
        showlegend=False,
        **layout
    )
    return fig


def get_ring_members(ring_id):
    if not ring_id:
        return "Enter a ring ID above."
    cols, rows = query_sql(f"""
    SELECT ring_id, members, accounts AS size, has_cycles, ROUND(fraud_rate, 2) AS fraud_rate
    FROM digital_artha.main.detected_fraud_rings
    WHERE ring_id = '{ring_id}'
    """)
    if not rows:
        return f"Ring not found: {ring_id}"
    r = rows[0]
    members_str = str(r[1]) if r[1] else "N/A"
    members_list = [m.strip() for m in members_str.split(",") if m.strip()]
    md = f"### Ring `{r[0]}` — {r[2]} accounts | Cycles: **{r[3]}** | Fraud Rate: **{r[4]}**\n\n"
    md += "**Members:**\n"
    for m in members_list[:30]:
        md += f"- `{m}`\n"
    if len(members_list) > 30:
        md += f"\n*...and {len(members_list)-30} more*\n"
    return md


# ============================================================
# INDIA STORY
# ============================================================

def get_india_story():
    _, upi = query_sql("SELECT date, volume_millions FROM digital_artha.main.viz_upi_growth ORDER BY date DESC LIMIT 1")
    upi_latest = f"{upi[0][1]}M" if upi else "N/A"

    _, fraud = query_sql("SELECT SUM(loss_crore) FROM digital_artha.main.viz_bank_fraud WHERE fiscal_year = '2023-24'")
    fraud_total = f"\u20b9{fraud[0][0]} Cr" if fraud and fraud[0][0] else "N/A"

    cols, vuln = query_sql("""
    SELECT state, ROUND(vulnerability_index, 2) AS vuln_index, vulnerability_level,
           ROUND(fraud_rate_pct, 2) AS fraud_rate, ROUND(internet_per_100, 0) AS internet
    FROM digital_artha.main.viz_state_vulnerability
    WHERE vulnerability_index > 0 ORDER BY vulnerability_index DESC LIMIT 10
    """)

    md = f"""**India's UPI ecosystem** processes **{upi_latest} transactions/month** — the world's largest real-time payment network.

**But fraud is rising.** Indian banks reported **{fraud_total}** in digital payment fraud losses in FY2023-24.

**The most vulnerable states** have high fraud rates AND low internet penetration AND low banking access:

"""
    md += make_table(cols, vuln)
    md += "\n*Vulnerability Index = fraud_risk x digital_divide x inclusion_gap*"
    return md


def get_bank_fraud():
    cols, rows = query_sql("""
    SELECT bank_name AS Bank, fraud_count AS Cases, ROUND(loss_crore, 1) AS 'Loss Cr',
           ROUND(recovered_crore, 1) AS 'Recovered Cr',
           ROUND(recovered_crore * 100 / NULLIF(loss_crore, 0), 0) AS 'Recovery%'
    FROM digital_artha.main.viz_bank_fraud WHERE fiscal_year = '2023-24'
    ORDER BY loss_crore DESC LIMIT 10
    """)
    return "**Bank-wise Fraud Losses FY2023-24:**\n\n" + make_table(cols, rows)


def get_upi_growth_chart():
    """UPI volume growth over time."""
    cols, rows = query_sql("""
    SELECT date, volume_millions, value_crore
    FROM digital_artha.main.viz_upi_growth ORDER BY date
    """)
    if not rows:
        return go.Figure().update_layout(title="No UPI data", **PLOTLY_LAYOUT)

    dates = [str(r[0]) for r in rows]
    volumes = [_safe_float(r[1]) for r in rows]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=volumes, fill="tozeroy",
        line=dict(color=GOLD, width=2),
        fillcolor=GOLD_DIM,
    ))
    fig.update_layout(
        title="UPI Transaction Volume Growth (Millions/Month)",
        xaxis_title="Date", yaxis_title="Volume (Millions)",
        **PLOTLY_LAYOUT,
    )
    return fig


def get_complaint_surge():
    """RBI complaint surge data."""
    cols, rows = query_sql("""
    SELECT complaint_type, fiscal_year, complaints_received, pct_of_year
    FROM digital_artha.main.viz_complaint_surge
    ORDER BY fiscal_year, complaints_received DESC
    """)
    if not rows:
        return "No complaint data available."

    # Group by fiscal year
    years = sorted(set(r[1] for r in rows if r[1]))
    md = "**RBI Complaint Trends by Type:**\n\n"
    for year in years[-3:]:  # Last 3 years
        year_rows = [r for r in rows if r[1] == year][:8]
        md += f"**{year}:**\n"
        for r in year_rows:
            pct = _safe_float(r[3])
            bar_len = int(pct / 3)
            md += f"- {r[0]}: {_safe_int(r[2]):,} ({pct}%) {'|' * bar_len}\n"
        md += "\n"
    return md


# ============================================================
# SCHEME FINDER
# ============================================================

def find_schemes(age, income, occupation, state, gender, language):
    lang_note = f" Respond in {language}." if language != "English" else ""
    msg = f"I am a {int(age)} year old {occupation} from {state} earning {int(income)} rupees per year. Gender: {gender}. What government schemes can I apply for?{lang_note}"
    return call_agent(msg, [])


# ============================================================
# FRAUD RECOVERY
# ============================================================

def get_recovery_guide(fraud_type):
    if fraud_type == "All Types":
        where = ""
    else:
        search = fraud_type.replace("'", "''").split("/")[0].strip().split("(")[0].strip()
        where = f"WHERE LOWER(fraud_type) LIKE LOWER('%{search}%')"

    # Try both table names (view and source)
    for table in ["fraud_recovery_guide", "viz_recovery_guide"]:
        cols, rows = query_sql(f"""
        SELECT * FROM digital_artha.main.{table} {where}
        """)
        if rows and len(rows) > 0:
            break

    if not rows:
        return f"No recovery guide found for '{fraud_type}'. Try 'All Types'."

    md = ""
    for row in rows:
        row_dict = dict(zip(cols, row)) if cols else {}
        fraud_name = row_dict.get("fraud_type", row_dict.get("Type", str(row[0])))
        rbi_rule = row_dict.get("rbi_rule", row_dict.get("rbi_liability_rule", row_dict.get("RBI Rule", "")))
        steps = row_dict.get("recovery_steps", row_dict.get("What To Do", ""))
        report = row_dict.get("report_to", row_dict.get("Report To", ""))
        days = row_dict.get("time_limit_days", row_dict.get("Days Limit", "3"))
        liability = row_dict.get("max_liability_inr", row_dict.get("Max Liability", "0"))

        md += f"### {fraud_name}\n"
        md += f"**RBI Rule:** {rbi_rule}\n\n"
        md += f"**Recovery Steps:**\n{steps}\n\n"
        md += f"**Report to:** {report}\n\n"
        md += f"**Time limit:** {days} days | **Max liability:** ₹{liability}\n\n---\n\n"
    return md


def emergency_recovery():
    """Immediate steps for any fraud victim."""
    return """## IMMEDIATE STEPS — Do This NOW

**1. Call your bank helpline immediately** — Block your UPI ID and linked bank account.

**2. File online complaint** at [cybercrime.gov.in](https://cybercrime.gov.in) or call **1930** (National Cybercrime Helpline).

**3. File FIR** at your nearest police station. Get a copy.

**4. Report to RBI** via the [CMS portal](https://cms.rbi.org.in).

---

**RBI Zero Liability Rule:** If you report within **3 days**, you have **ZERO liability** for unauthorized transactions. After 3 days, liability increases.

**Keep these ready:**
- Transaction ID / UTR number
- Date and time of fraud
- Amount lost
- Screenshots of messages/calls

---
*Select your specific fraud type below for detailed recovery steps, or talk to the AI Agent for personalized help.*
"""


# ============================================================
# BUILD THE APP
# ============================================================

custom_css = """
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,600;1,400&family=Montserrat:wght@300;400;500;600&family=Inter:wght@300;400;500&display=swap');

body { font-family: 'Inter', sans-serif !important; font-weight: 300 !important; background: #060606 !important; }
.gradio-container { max-width: 100% !important; padding: 56px 4% 0 !important; background: #060606 !important; min-height: 100vh; }

h1, h2, h3, h4, h5, h6 { font-family: 'Playfair Display', serif !important; font-weight: 400 !important; }

/* Compact Sticky Navbar */
.tabs > .tab-nav { position: fixed; top: 0; left: 0; width: 100%; display: flex; justify-content: center; align-items: center; background: rgba(6,6,6,0.85) !important; backdrop-filter: blur(12px) !important; -webkit-backdrop-filter: blur(12px) !important; z-index: 9999; border-bottom: 1px solid rgba(197,160,89,0.12) !important; margin: 0 !important; padding: 0 !important; box-shadow: 0 2px 20px rgba(0,0,0,0.4); }
.tabs > .tab-nav > button { border: none !important; background: transparent !important; color: rgba(255,255,255,0.4) !important; font-family: 'Montserrat', sans-serif !important; font-weight: 400 !important; font-size: 0.7em !important; letter-spacing: 2px !important; text-transform: uppercase !important; padding: 14px 12px !important; margin: 0 8px !important; transition: all 0.3s ease !important; border-bottom: 2px solid transparent !important; }
.tabs > .tab-nav > button.selected { border-bottom: 2px solid #c5a059 !important; color: #c5a059 !important; }
.tabs > .tab-nav > button:hover:not(.selected) { color: #fff !important; }

/* Tables */
table { border-collapse: collapse; width: 100%; margin-top: 12px; font-family: 'Inter', sans-serif; font-size: 0.82em; font-weight: 300; overflow: hidden; border: 1px solid rgba(255,255,255,0.05); border-radius: 8px; }
th { background: rgba(255,255,255,0.02) !important; color: #888 !important; font-family: 'Montserrat', sans-serif; font-weight: 400; padding: 10px 14px; text-align: left; text-transform: uppercase; font-size: 0.72em; letter-spacing: 1px; border-bottom: 1px solid rgba(255,255,255,0.06); }
td { border-bottom: 1px solid rgba(255,255,255,0.025) !important; padding: 10px 14px; }
tr:last-child td { border-bottom: none !important; }
tr:hover td { background: rgba(197,160,89,0.03) !important; }

a { color: #c5a059 !important; text-decoration: none; }
a:hover { opacity: 0.7; }
footer { display: none !important; }

/* Rounded Buttons */
.gr-button { min-height: 40px !important; padding: 0 28px !important; font-size: 0.72em !important; font-family: 'Montserrat', sans-serif !important; font-weight: 400 !important; letter-spacing: 1.5px !important; text-transform: uppercase !important; border-radius: 999px !important; transition: all 0.25s ease !important; }
.gr-button-primary { background: rgba(197,160,89,0.12) !important; color: #c5a059 !important; border: 1px solid rgba(197,160,89,0.25) !important; }
.gr-button-primary:hover { background: rgba(197,160,89,0.22) !important; border-color: rgba(197,160,89,0.5) !important; color: #fff !important; }
.gr-button-secondary { background: rgba(255,255,255,0.04) !important; color: #ccc !important; border: 1px solid rgba(255,255,255,0.08) !important; }
.gr-button-secondary:hover { background: rgba(255,255,255,0.08) !important; color: #fff !important; border-color: rgba(255,255,255,0.2) !important; }

/* Inputs */
.gr-box, .gr-input, .gr-dropdown { min-height: 36px !important; font-size: 0.85em !important; border-radius: 8px !important; background-color: rgba(255,255,255,0.02) !important; border: 1px solid rgba(255,255,255,0.08) !important; color: #fff !important; }
.gr-box:focus-within, .gr-input:focus, .gr-dropdown:focus { border-color: #c5a059 !important; }

/* Sliders */
input[type="range"] { height: 2px !important; background: rgba(255,255,255,0.08) !important; -webkit-appearance: none !important; border-radius: 1px !important; margin: 12px 0 !important; outline: none !important; }
input[type="range"]::-webkit-slider-thumb { -webkit-appearance: none !important; width: 14px !important; height: 14px !important; background: #c5a059 !important; border-radius: 50% !important; cursor: pointer !important; margin-top: -6px !important; }

/* Chat */
div[class*='message'] { border-radius: 12px !important; font-family: 'Inter', sans-serif !important; font-weight: 300 !important; font-size: 0.92em !important; }
div[class*='message'][class*='bot'] { background: rgba(255,255,255,0.02) !important; border: 1px solid rgba(255,255,255,0.05) !important; border-left: 3px solid #c5a059 !important; }
div[class*='message'][class*='user'] { background: rgba(197,160,89,0.04) !important; border: 1px solid rgba(255,255,255,0.05) !important; }

textarea { font-family: 'Inter', sans-serif !important; font-weight: 300 !important; min-height: 38px !important; padding: 10px 14px !important; border-radius: 8px !important; }

/* Plotly */
.js-plotly-plot .plotly .main-svg { background: transparent !important; }

/* Emergency button */
.emergency-btn { background: rgba(255,68,68,0.1) !important; color: #ff4444 !important; border: 1px solid rgba(255,68,68,0.3) !important; border-radius: 999px !important; }
.emergency-btn:hover { background: rgba(255,68,68,0.2) !important; border-color: #ff4444 !important; }
"""

theme = gr.themes.Monochrome(
    font=[gr.themes.GoogleFont("Inter"), "sans-serif"],
).set(
    body_background_fill="#060606",
    block_background_fill="#0f0f0f",
    block_border_color="rgba(255,255,255,0.05)",
    border_color_primary="rgba(255,255,255,0.05)",
    border_color_accent="#c5a059",
    body_text_color="#ededed",
    block_label_text_color="#888888",
    block_title_text_color="#ededed",
    input_background_fill="#0a0a0a",
    input_border_color="rgba(255,255,255,0.05)",
    input_border_color_focus="#c5a059",
    input_placeholder_color="#555555",
    slider_color="#c5a059",
    checkbox_background_color_selected="#c5a059",
    checkbox_border_color_selected="#c5a059",
    radio_circle="rgba(255, 255, 255, 0.05)",
    button_primary_background_fill="rgba(197, 160, 89, 0.15)",
    button_primary_text_color="#c5a059",
    button_primary_border_color="rgba(197, 160, 89, 0.3)",
    button_secondary_background_fill="rgba(255, 255, 255, 0.05)",
    button_secondary_text_color="#ededed",
    button_secondary_border_color="rgba(255, 255, 255, 0.1)",
    block_radius="4px",
    input_radius="4px",
    button_large_radius="4px",
    button_small_radius="4px",
    color_accent_soft="rgba(197, 160, 89, 0.05)",
    panel_background_fill="#060606",
)

# Load dynamic categories at startup
try:
    CATEGORIES = load_categories()
except:
    CATEGORIES = ["All", "Education", "Shopping", "Utilities", "Food", "Grocery",
                   "Entertainment", "Fuel", "Healthcare", "Transport", "Other"]

with gr.Blocks(title="BlackIce Platform") as demo:

    # ---- COMPACT HEADER ----
    gr.Markdown("""
    <div style="display:flex; align-items:center; gap:20px; padding:12px 0 8px 0; flex-wrap:wrap;">
      <h1 style="font-size:2em; margin:0; color:#fff; font-family:'Playfair Display',serif; font-weight:400; letter-spacing:-1px;">
        BlackIce<span style="font-style:italic; color:#c5a059;">.</span>
      </h1>
      <div style="display:flex; gap:16px; font-family:'Montserrat',sans-serif; font-size:0.65em; color:#666; text-transform:uppercase; letter-spacing:1.5px; flex-wrap:wrap;">
        <span><span style="color:#c5a059;">&loz;</span> Threats</span>
        <span><span style="color:#c5a059;">&loz;</span> RBI</span>
        <span><span style="color:#c5a059;">&loz;</span> Schemes</span>
        <span><span style="color:#c5a059;">&loz;</span> Recovery</span>
        <span><span style="color:#c5a059;">&loz;</span> Rings</span>
      </div>
    </div>
    <div style="width:100%; height:1px; background:rgba(255,255,255,0.04); margin-bottom:16px;"></div>
    """)

    with gr.Tabs():

        # ================================================================
        # TAB 0: HOME — Emergency Recovery + Overview
        # ================================================================
        with gr.Tab("Home"):
            gr.HTML("""
            <div style="display: flex; gap: 24px; flex-wrap: wrap; margin: 20px 0 40px 0;">
                <div style="flex: 1; min-width: 250px; padding: 30px; border: 1px solid rgba(197,160,89,0.15); border-radius: 12px; background: rgba(255,255,255,0.02); backdrop-filter: blur(10px); box-shadow: 0 10px 30px rgba(0,0,0,0.2);">
                    <h3 style="color: #c5a059; margin-top: 0; font-family: 'Montserrat', sans-serif; letter-spacing: 1px; font-size: 1em; text-transform: uppercase;">Real-Time ML Ensemble</h3>
                    <p style="color: #aaa; font-size: 0.9em; line-height: 1.6; font-weight: 300;">Streaming transaction analysis via Databricks Auto Loader. Isolation Forest + K-Means ensemble scoring with graph-based fraud ring detection.</p>
                </div>
                <div style="flex: 1; min-width: 250px; padding: 30px; border: 1px solid rgba(197,160,89,0.15); border-radius: 12px; background: rgba(255,255,255,0.02); backdrop-filter: blur(10px); box-shadow: 0 10px 30px rgba(0,0,0,0.2);">
                    <h3 style="color: #c5a059; margin-top: 0; font-family: 'Montserrat', sans-serif; letter-spacing: 1px; font-size: 1em; text-transform: uppercase;">Agentic MCP Network</h3>
                    <p style="color: #aaa; font-size: 0.9em; line-height: 1.6; font-weight: 300;">AI agent with 9 tools: Vector Search for RBI circulars, fraud ring queries, sender profiling, scheme matching, and live SQL analytics.</p>
                </div>
                <div style="flex: 1; min-width: 250px; padding: 30px; border: 1px solid rgba(197,160,89,0.15); border-radius: 12px; background: rgba(255,255,255,0.02); backdrop-filter: blur(10px); box-shadow: 0 10px 30px rgba(0,0,0,0.2);">
                    <h3 style="color: #c5a059; margin-top: 0; font-family: 'Montserrat', sans-serif; letter-spacing: 1px; font-size: 1em; text-transform: uppercase;">RBI Legal Workflows</h3>
                    <p style="color: #aaa; font-size: 0.9em; line-height: 1.6; font-weight: 300;">Recovery workflows modeled against Indian banking guidelines. Zero-liability tracking, complaint routing, and step-by-step victim assistance.</p>
                </div>
            </div>
            """)

            # Emergency section
            emergency_btn = gr.Button("I WAS JUST SCAMMED \u2014 GET IMMEDIATE HELP", variant="primary", elem_classes=["emergency-btn"])
            emergency_out = gr.Markdown()
            emergency_btn.click(fn=emergency_recovery, outputs=emergency_out)

            gr.Markdown("---")
            gr.Markdown("### Fraud Recovery Guide")
            gr.Markdown("*Select your fraud type for RBI-mandated recovery steps:*")
            fraud_dd = gr.Dropdown(
                ["All Types", "QR Code", "Phishing", "SIM Swap",
                 "Fake UPI", "Collect Request", "Remote Access", "Merchant Fraud", "Refund"],
                value="All Types", label="Fraud Type"
            )
            with gr.Row():
                recov_btn = gr.Button("Show Recovery Steps", variant="primary")
                chat_help_btn = gr.Button("Ask AI for Personalized Help", variant="secondary")
            recov_out = gr.Markdown("*Select a fraud type above and click a button*")
            chat_help_out = gr.Markdown()

            def safe_recovery_guide(fraud_type):
                result = get_recovery_guide(fraud_type)
                if not result or result.strip() == "":
                    return "No recovery steps found. Try selecting 'All Types'."
                return result

            def ask_agent_recovery(fraud_type):
                if not fraud_type or fraud_type == "All Types":
                    msg = "I was scammed. What should I do? Give me detailed step by step recovery process with RBI rules."
                else:
                    msg = f"I was a victim of {fraud_type} fraud. What are the RBI liability rules? What should I do step by step? Who should I report to and what is the time limit?"
                result = call_agent(msg, [])
                if not result or result.strip() == "":
                    return "Agent did not respond. Please try the 'Show Recovery Steps' button instead."
                return f"### AI Agent Response\n\n{result}"

            recov_btn.click(fn=safe_recovery_guide, inputs=fraud_dd, outputs=recov_out)
            chat_help_btn.click(fn=ask_agent_recovery, inputs=fraud_dd, outputs=chat_help_out)

        # ================================================================
        # TAB 1: COMMAND CENTER — KPIs + Alerts + Charts + Drill-downs
        # ================================================================
        with gr.Tab("Command Center"):
            # KPI Cards
            kpi_output = gr.HTML("Loading...")
            refresh_btn = gr.Button("Refresh", variant="secondary", size="sm")
            refresh_btn.click(fn=get_kpis, outputs=kpi_output)

            gr.Markdown("---")

            # ----- CHARTS ROW -----
            gr.Markdown("### Analytics")
            with gr.Row():
                heatmap_plot = gr.Plot(label="Fraud Heatmap")
                risk_dist_plot = gr.Plot(label="Risk Distribution")
            with gr.Row():
                trend_plot = gr.Plot(label="Monthly Trend")
                amount_plot = gr.Plot(label="Amount Distribution")

            with gr.Row():
                load_charts_btn = gr.Button("Load All Charts", variant="primary")

            def load_all_charts():
                return get_fraud_heatmap(), get_risk_distribution(), get_monthly_trend(), get_amount_comparison()

            load_charts_btn.click(fn=load_all_charts, outputs=[heatmap_plot, risk_dist_plot, trend_plot, amount_plot])

            gr.Markdown("---")

            # ----- ALERT SEARCH -----
            gr.Markdown("### Fraud Alert Search")
            with gr.Row():
                score_slider = gr.Slider(0, 1, value=0.3, step=0.05, label="Min Risk Score")
                cat_dd = gr.Dropdown(CATEGORIES, value="All", label="Category")
                limit_sl = gr.Slider(5, 50, value=15, step=5, label="Results")
            search_btn = gr.Button("Search Alerts", variant="primary")
            alerts_out = gr.Markdown()
            search_btn.click(fn=get_alerts, inputs=[score_slider, cat_dd, limit_sl], outputs=alerts_out)

            gr.Markdown("---")

            # ----- PATTERNS + DRILL-DOWN -----
            with gr.Row():
                with gr.Column():
                    gr.Markdown("### Anomaly Patterns")
                    pat_btn = gr.Button("Load Patterns", size="sm", variant="secondary")
                    pat_out = gr.Markdown()
                    pattern_dd = gr.Dropdown([], label="Drill into Pattern", interactive=True)
                    pat_drill_out = gr.Markdown()

                    pat_btn.click(fn=get_patterns, outputs=[pat_out, pattern_dd])
                    pattern_dd.change(fn=drill_pattern_senders, inputs=pattern_dd, outputs=pat_drill_out)

                with gr.Column():
                    gr.Markdown("### Risky Senders")
                    risk_btn = gr.Button("Load Senders", size="sm", variant="secondary")
                    risk_out = gr.Markdown()
                    sender_dd = gr.Dropdown([], label="Drill into Sender", interactive=True)
                    sender_drill_out = gr.Markdown()

                    risk_btn.click(fn=get_risky_senders, outputs=[risk_out, sender_dd])
                    sender_dd.change(fn=drill_sender_txns, inputs=sender_dd, outputs=sender_drill_out)

            gr.Markdown("---")

            # ----- FRAUD RINGS -----
            gr.Markdown("### Fraud Ring Network")
            gr.Markdown("*Graph analysis: Connected components + PageRank on transaction network*")
            ring_graph_plot = gr.Plot(label="Ring Network Graph")
            ring_graph_btn = gr.Button("Load Network Graph", variant="primary")
            ring_graph_btn.click(fn=get_ring_network_graph, outputs=ring_graph_plot)

            with gr.Row():
                rings_btn = gr.Button("Show Ring Table", variant="secondary", size="sm")
                ring_id_input = gr.Textbox(label="Ring ID", placeholder="e.g. RING_0001", scale=2)
                ring_members_btn = gr.Button("Show Members", variant="secondary", size="sm")
            rings_out = gr.Markdown()
            ring_members_out = gr.Markdown()
            rings_btn.click(fn=get_fraud_rings, outputs=rings_out)
            ring_members_btn.click(fn=get_ring_members, inputs=ring_id_input, outputs=ring_members_out)

            # ----- STREAMING PIPELINE STATUS -----
            gr.Markdown("---")
            gr.Markdown("### Streaming Pipeline (Live Fraud Scoring)")
            streaming_out = gr.Markdown("Loading streaming status...")

            def get_streaming_status():
                _, bronze = query_sql("SELECT COUNT(*) FROM digital_artha.main.streaming_bronze")
                _, silver = query_sql("SELECT COUNT(*) FROM digital_artha.main.streaming_silver")
                _, plat = query_sql("SELECT COUNT(*) FROM digital_artha.main.streaming_platinum")
                _, alerts = query_sql("SELECT COUNT(*) FROM digital_artha.main.streaming_platinum WHERE final_risk_tier IN ('high', 'critical')")
                _, recent = query_sql("""
                    SELECT transaction_id, ROUND(amount, 0) AS amount, category,
                           ROUND(ensemble_score, 3) AS risk_score, final_risk_tier, anomaly_pattern
                    FROM digital_artha.main.streaming_platinum
                    WHERE final_risk_tier IN ('high', 'medium', 'critical')
                    ORDER BY ensemble_score DESC LIMIT 5
                """)

                b = bronze[0][0] if bronze else 0
                s = silver[0][0] if silver else 0
                p = plat[0][0] if plat else 0
                a = alerts[0][0] if alerts else 0

                md = f"""
**Pipeline:** `Files → Auto Loader → Bronze ({b:,}) → Silver ({s:,}) → Platinum ({p:,})`

**Processing:** Incremental, checkpoint-based, exactly-once semantics

**Fraud Alerts from Stream:** {a} high/critical transactions detected

| Feature | Status |
|---------|--------|
| Auto Loader (cloudFiles) | ✅ Ingests new files automatically |
| Bronze → Silver | ✅ Quality checks (null IDs, invalid amounts) |
| Silver → Platinum | ✅ ML scoring + anomaly pattern classification |
| Checkpointing | ✅ Exactly-once, no duplicates |
| Judge test | ✅ Drop a custom transaction, watch it flow through |

"""
                if recent:
                    cols_r = ["TXN", "Amount", "Category", "Risk Score", "Tier", "Pattern"]
                    md += "**Recent High-Risk Streamed Transactions:**\n\n"
                    md += make_table(cols_r, recent)

                md += "\n\n*To see live streaming: run `11-streaming-simulation.py` on Databricks. Each batch flows through all 3 tiers in real-time.*"
                return md

        # ================================================================
        # TAB 2: ASK AGENT
        # ================================================================
        with gr.Tab("Ask BlackIce"):
            gr.Markdown("### Intelligent Agent \u2014 9 Tools | Hindi Support | MCP Protocol")
            gr.Markdown("*Ask about fraud alerts, fraud rings, sender profiles, RBI circulars, government schemes, or recovery steps.*")
            gr.ChatInterface(
                fn=call_agent,
                examples=[
                    "Show me the top 5 highest risk fraud alerts",
                    "What fraud rings have been detected? Show the most severe ones",
                    "Look up the risk profile for sender_1234@upi",
                    "I was scammed via QR code. What should I do?",
                    "What are the RBI guidelines for UPI fraud prevention?",
                    "Which merchant categories have the highest fraud rate?",
                    "I am a 25 year old farmer from UP earning 2 lakh. What schemes can I apply for?",
                    "\u092f\u0942\u092a\u0940\u0906\u0908 \u092b\u094d\u0930\u0949\u0921 \u0938\u0947 \u0915\u0948\u0938\u0947 \u092c\u091a\u0947\u0902?",
                    "\u092e\u0948\u0902 \u090f\u0915 30 \u0938\u093e\u0932 \u0915\u0940 \u092e\u0939\u093f\u0932\u093e \u0939\u0942\u0902, \u092e\u0939\u093e\u0930\u093e\u0937\u094d\u091f\u094d\u0930 \u0938\u0947, \u0906\u092f 1.5 \u0932\u093e\u0916\u0964 \u0915\u094c\u0928 \u0938\u0940 \u092f\u094b\u091c\u0928\u093e\u090f\u0902 \u0939\u0948\u0902?",
                ],
            )

        # ================================================================
        # TAB 3: INDIA STORY
        # ================================================================
        with gr.Tab("India Story"):
            def load_full_india_story():
                """Load everything at once — no clicking needed."""
                story = get_india_story()
                bank = get_bank_fraud()
                try:
                    complaint = get_complaint_surge()
                except:
                    complaint = ""

                # Add streaming + fraud ring context
                _, stream = query_sql("""
                SELECT COUNT(*) FROM digital_artha.main.streaming_platinum
                """)
                stream_count = stream[0][0] if stream else "N/A"

                _, rings = query_sql("""
                SELECT COUNT(*) AS rings, SUM(accounts) AS accounts,
                       ROUND(SUM(total_amount), 0) AS volume
                FROM digital_artha.main.detected_fraud_rings
                """)

                ring_stats = ""
                if rings and rings[0]:
                    ring_stats = f"""
---

### Fraud Ring Intelligence (Graph Analysis)

| Metric | Value |
|--------|-------|
| Fraud rings detected | **{rings[0][0]}** |
| Accounts in rings | **{rings[0][1]}** |
| Money flowing through rings | **₹{rings[0][2]}** |
| Detection method | NetworkX: connected components + PageRank |
"""

                streaming_stats = f"""
---

### Real-Time Streaming Pipeline

| Metric | Value |
|--------|-------|
| Transactions streamed | **{stream_count}** |
| Pipeline | Bronze → Silver → Platinum (incremental, exactly-once) |
| Trigger | Auto Loader with checkpoints |
| Scoring | Rule-based ensemble (amount + time + risk signals) |
"""

                full = story + "\n\n" + bank + "\n\n" + complaint + ring_stats + streaming_stats
                return full

            story_out = gr.Markdown("Loading India's financial landscape...")
            try:
                upi_chart = gr.Plot()
            except:
                pass

            def load_india_page():
                try:
                    chart = get_upi_growth_chart()
                except:
                    chart = go.Figure().update_layout(title="UPI data not available", **PLOTLY_LAYOUT)
                return load_full_india_story(), chart

            story_out = gr.Markdown("Loading...")
            upi_chart = gr.Plot(label="UPI Growth")

        # ================================================================
        # TAB 4: SCHEME FINDER
        # ================================================================
        with gr.Tab("Scheme Finder"):
            gr.Markdown("### Government Scheme Eligibility \u2014 170+ Programs Indexed")
            gr.Markdown("*Enter your details to find schemes you qualify for. The AI agent will explain each match.*")
            with gr.Row():
                age_in = gr.Number(value=25, label="Age", minimum=18, maximum=100)
                income_in = gr.Number(value=150000, label="Annual Income (\u20b9)")
            with gr.Row():
                occ_in = gr.Dropdown(["farmer", "street_vendor", "artisan", "student", "salaried",
                                      "self_employed", "entrepreneur", "daily_wage", "fisherman",
                                      "homemaker", "teacher", "merchant"],
                                     value="farmer", label="Occupation")
                state_in = gr.Dropdown(["Maharashtra", "Uttar Pradesh", "Tamil Nadu", "Karnataka",
                                        "Delhi", "Gujarat", "Rajasthan", "West Bengal", "Bihar",
                                        "Andhra Pradesh", "Telangana", "Kerala", "Madhya Pradesh",
                                        "Punjab", "Haryana", "Odisha", "Jharkhand", "Assam",
                                        "Chhattisgarh", "Uttarakhand", "Goa", "Tripura",
                                        "Meghalaya", "Manipur", "Nagaland", "Mizoram",
                                        "Arunachal Pradesh", "Sikkim", "Himachal Pradesh",
                                        "Jammu and Kashmir", "Ladakh", "Puducherry",
                                        "Chandigarh", "Andaman and Nicobar"],
                                       value="Maharashtra", label="State")
            with gr.Row():
                gender_in = gr.Radio(["male", "female", "all"], value="all", label="Gender")
                lang_in = gr.Dropdown(["English", "Hindi", "Marathi", "Tamil", "Telugu", "Bengali", "Kannada"],
                                      value="English", label="Response Language")
            scheme_btn = gr.Button("Find Matching Schemes", variant="primary")
            scheme_out = gr.Markdown("Enter your details and click 'Find Matching Schemes'")
            scheme_btn.click(fn=find_schemes, inputs=[age_in, income_in, occ_in, state_in, gender_in, lang_in], outputs=scheme_out)

    # Auto-load KPIs on page open
    # Auto-load everything on page open — no manual clicking needed
    demo.load(fn=get_kpis, outputs=kpi_output)
    demo.load(fn=load_all_charts, outputs=[heatmap_plot, risk_dist_plot, trend_plot, amount_plot])
    demo.load(fn=get_ring_network_graph, outputs=ring_graph_plot)
    demo.load(fn=load_india_page, outputs=[story_out, upi_chart])
    demo.load(fn=get_streaming_status, outputs=streaming_out)
    # cat_matrix removed


if __name__ == "__main__":
    print("\n" + "="*55)
    print("BlackIce // 0xArtha : APEX NETWORK ONLINE")
    print("SYSTEMS GREEN. HUNTING ANOMALIES.")
    print("="*55)
    print("\nInitializing Uplink with public URL...")
    print("Share the encrypted channel with judges!\n")
    demo.launch(server_port=7860, share=True, theme=theme, css=custom_css)
