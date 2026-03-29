# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Genie Space Deployment
# MAGIC
# MAGIC Deploys a Genie Space for natural language analytics.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")

assert warehouse_id, "Set warehouse_id widget. Find it: SQL Warehouses → click warehouse → ID in URL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Genie Space

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()
host = w.config.host

space_config = {
    "title": "BlackIce: Financial Intelligence",
    "description": "Ask questions about UPI transactions, fraud patterns, and financial schemes",
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
                    {"name": "category", "enable_entity_matching": True},
                    {"name": "final_risk_tier", "enable_entity_matching": True},
                    {"name": "ensemble_score", "enable_format_assistance": True},
                    {"name": "ensemble_flag"},
                    {"name": "location", "enable_entity_matching": True},
                    {"name": "time_slot", "enable_entity_matching": True},
                ]
            },
            {
                "catalog": catalog,
                "schema": schema,
                "table": "gold_schemes",
                "columns": [
                    {"name": "scheme_name", "enable_entity_matching": True},
                    {"name": "ministry", "enable_entity_matching": True},
                    {"name": "benefits"},
                ]
            }
        ]
    },
    "instructions": {
        "text_instructions": f"""You are a financial analytics assistant for BlackIce.
- ensemble_flag = true means the transaction is flagged as potentially fraudulent
- final_risk_tier values: low, medium, high, critical
- ensemble_score: 0-1, higher = riskier
- Amounts are in Indian Rupees (INR)
- For fraud queries, filter on ensemble_flag = true or final_risk_tier IN ('high', 'critical')""",
        "example_question_sqls": [
            {
                "question": "What is the overall fraud rate?",
                "sql": f"SELECT COUNT(*) as total, SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) as flagged, ROUND(SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate FROM {catalog}.{schema}.gold_transactions_enriched"
            },
            {
                "question": "Show fraud rate by category",
                "sql": f"SELECT category, COUNT(*) as total, SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) as flagged, ROUND(SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate FROM {catalog}.{schema}.gold_transactions_enriched GROUP BY category ORDER BY fraud_rate DESC"
            }
        ]
    }
}

try:
    response = w.api_client.do(method="POST", path="/api/2.0/genie/spaces", body=space_config)
    space_id = response.get("space_id", response.get("id", "unknown"))
    genie_url = f"{host}/genie/rooms/{space_id}"
    print(f"Genie Space created!")
    print(f"Space ID: {space_id}")
    print(f"URL: {genie_url}")

    # Save space ID for agent
    spark.createDataFrame([{"genie_space_id": space_id, "url": genie_url}]).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.genie_config")
    print(f"Space ID saved to {catalog}.{schema}.genie_config")
except Exception as e:
    print(f"Genie Space creation failed: {e}")
    print("This may not be available on Free Edition.")
    print("You can create a Genie Space manually: left sidebar → Genie → Create")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC **Next:** Create a Lakeview Dashboard, then deploy the agent.
