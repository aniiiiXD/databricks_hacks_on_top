# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Genie Space Deployment
# MAGIC
# MAGIC Deploys a Genie Space via REST API for natural language analytics
# MAGIC over financial data. Judges can ask questions like:
# MAGIC - "Show fraud rate by merchant category"
# MAGIC - "What's the total flagged amount this month?"
# MAGIC - "Which senders have the most suspicious transactions?"

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
host = w.config.host

assert warehouse_id, "Please provide a SQL Warehouse ID (find it in SQL Warehouses page)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Genie Space Configuration

# COMMAND ----------

import json

space_config = {
    "title": "Digital-Artha: Financial Intelligence",
    "description": "Ask questions about UPI transactions, fraud patterns, risk analysis, and financial schemes. Powered by AI.",
    "warehouse_id": warehouse_id,
    "version": 2,
    "data_sources": {
        "tables": [
            {
                "catalog": catalog,
                "schema": schema,
                "table": "gold_transactions_enriched",
                "columns": [
                    {"name": "transaction_id"},
                    {"name": "amount", "enable_format_assistance": True},
                    {"name": "transaction_time"},
                    {"name": "transaction_date"},
                    {"name": "sender_id", "enable_entity_matching": True},
                    {"name": "receiver_id", "enable_entity_matching": True},
                    {"name": "category", "enable_entity_matching": True},
                    {"name": "merchant_name", "enable_entity_matching": True},
                    {"name": "payment_mode", "enable_entity_matching": True},
                    {"name": "location", "enable_entity_matching": True},
                    {"name": "final_risk_tier", "enable_entity_matching": True},
                    {"name": "ensemble_score", "enable_format_assistance": True},
                    {"name": "ensemble_flag"},
                    {"name": "isolation_forest_score", "enable_format_assistance": True},
                    {"name": "km_score", "enable_format_assistance": True},
                    {"name": "ai_risk_score", "enable_format_assistance": True},
                    {"name": "txn_velocity_1h", "enable_format_assistance": True},
                    {"name": "amount_deviation", "enable_format_assistance": True},
                    {"name": "amount_bucket", "enable_entity_matching": True},
                    {"name": "time_slot", "enable_entity_matching": True},
                    {"name": "is_weekend"},
                ]
            },
            {
                "catalog": catalog,
                "schema": schema,
                "table": "gold_fraud_alerts_ml",
                "columns": [
                    {"name": "transaction_id"},
                    {"name": "amount", "enable_format_assistance": True},
                    {"name": "sender_id", "enable_entity_matching": True},
                    {"name": "category", "enable_entity_matching": True},
                    {"name": "final_risk_tier", "enable_entity_matching": True},
                    {"name": "ensemble_score", "enable_format_assistance": True},
                ]
            },
            {
                "catalog": catalog,
                "schema": schema,
                "table": "gold_schemes",
                "columns": [
                    {"name": "scheme_name", "enable_entity_matching": True},
                    {"name": "ministry", "enable_entity_matching": True},
                    {"name": "target_group", "enable_entity_matching": True},
                    {"name": "income_limit", "enable_format_assistance": True},
                    {"name": "benefits"},
                ]
            }
        ],
        "metric_views": [
            {
                "catalog": catalog,
                "schema": schema,
                "metric_view": "financial_metrics",
                "columns": [
                    {"name": "total_transactions", "enable_format_assistance": True},
                    {"name": "total_amount", "enable_format_assistance": True},
                    {"name": "fraud_rate", "enable_format_assistance": True},
                    {"name": "flagged_transactions", "enable_format_assistance": True},
                    {"name": "flagged_amount", "enable_format_assistance": True},
                    {"name": "avg_amount", "enable_format_assistance": True},
                    {"name": "avg_ensemble_score", "enable_format_assistance": True},
                    {"name": "unique_senders", "enable_format_assistance": True},
                    {"name": "high_risk_count", "enable_format_assistance": True},
                ]
            }
        ]
    },
    "instructions": {
        "text_instructions": f"""You are an analytics assistant for Digital-Artha, a UPI fraud detection and financial inclusion platform.

DATA CONTEXT:
- gold_transactions_enriched: All UPI transactions with ML-based fraud scores and risk tiers
- gold_fraud_alerts_ml: Only transactions flagged as fraudulent by the ML ensemble
- gold_schemes: Government financial inclusion schemes
- financial_metrics: Pre-defined metric view with dimensions and measures

KEY COLUMNS:
- ensemble_score: Combined fraud risk score (0-1, higher = riskier). Blends Isolation Forest, KMeans, and AI classification.
- final_risk_tier: low, medium, high, critical
- ensemble_flag: true if transaction is flagged as potentially fraudulent
- txn_velocity_1h: Number of transactions by the same sender in the last 1 hour
- amount_deviation: Z-score of transaction amount vs sender's historical average

FORMATTING:
- Amounts are in Indian Rupees (INR). Format as ₹X,XXX.XX
- Fraud rate is a percentage. Format as X.XX%
- When asked about "fraud" or "suspicious", query ensemble_flag = true or final_risk_tier IN ('high', 'critical')

COMMON QUERY PATTERNS:
- "fraud rate by X" → GROUP BY X, compute flagged/total * 100
- "top fraudulent X" → filter ensemble_flag = true, ORDER BY ensemble_score DESC
- "trend" → GROUP BY transaction_date or transaction_month, ORDER BY date""",
        "example_question_sqls": [
            {
                "question": "What is the overall fraud rate?",
                "sql": f"SELECT COUNT(*) as total, SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) as flagged, ROUND(SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct FROM {catalog}.{schema}.gold_transactions_enriched"
            },
            {
                "question": "Show fraud rate by merchant category",
                "sql": f"SELECT category, COUNT(*) as total_txns, SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) as flagged, ROUND(SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate FROM {catalog}.{schema}.gold_transactions_enriched GROUP BY category ORDER BY fraud_rate DESC"
            },
            {
                "question": "Which time of day has the most fraud?",
                "sql": f"SELECT time_slot, COUNT(*) as flagged_count, ROUND(AVG(ensemble_score), 3) as avg_risk FROM {catalog}.{schema}.gold_fraud_alerts_ml GROUP BY time_slot ORDER BY flagged_count DESC"
            },
            {
                "question": "Show available government schemes",
                "sql": f"SELECT scheme_name, ministry, target_group, income_limit, benefits FROM {catalog}.{schema}.gold_schemes ORDER BY ministry"
            }
        ]
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Genie Space via REST API

# COMMAND ----------

response = w.api_client.do(
    method="POST",
    path="/api/2.0/genie/spaces",
    body=space_config
)

space_id = response.get("space_id", response.get("id", "unknown"))
genie_url = f"{host}/genie/rooms/{space_id}"

print(f"Genie Space created!")
print(f"Space ID: {space_id}")
print(f"URL: {genie_url}")

# Save space ID for agent
dbutils.fs.put(
    "dbfs:/FileStore/digital_artha/genie_space_id.txt",
    space_id,
    overwrite=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Deployed
# MAGIC
# MAGIC **URL:** Open the link above to try natural language queries.
# MAGIC
# MAGIC **Sample queries to try:**
# MAGIC - "Show me the overall fraud rate"
# MAGIC - "Which merchant categories have the highest fraud rate?"
# MAGIC - "What's the trend of flagged transactions over time?"
# MAGIC - "Show the top 10 highest risk transactions"
# MAGIC - "What government schemes are available for farmers?"
# MAGIC
# MAGIC **Next step:** Run the agent (`08-agent/`) for the unified chat interface.

# COMMAND ----------

displayHTML(f'<h2>Genie Space Ready</h2><p><a href="{genie_url}" target="_blank">Open Genie Space →</a></p>')
