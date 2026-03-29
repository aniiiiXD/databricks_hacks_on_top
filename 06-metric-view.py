# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Metric View + Analytics Views
# MAGIC
# MAGIC Creates analytics views over fraud-enriched transactions.
# MAGIC Tries YAML metric view first, falls back to regular SQL views.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Try YAML Metric View

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.financial_metrics
    WITH METRICS LANGUAGE YAML
    $$
    source:
      table: {catalog}.{schema}.gold_transactions_enriched

    dimensions:
      - name: transaction_date
        display_name: "Transaction Date"
        expr: "DATE(transaction_time)"

      - name: category
        display_name: "Merchant Category"
        expr: "COALESCE(category, 'Unknown')"
        synonyms: ["merchant type", "txn type"]

      - name: risk_tier
        display_name: "Risk Tier"
        expr: "final_risk_tier"
        synonyms: ["risk level", "fraud risk"]

      - name: time_slot
        display_name: "Time of Day"
        expr: "time_slot"

      - name: location
        display_name: "Location"
        expr: "COALESCE(location, 'Unknown')"
        synonyms: ["city", "state"]

    measures:
      - name: total_transactions
        display_name: "Total Transactions"
        expr: "COUNT(1)"
        format: "#,##0"
        synonyms: ["transaction count", "txn count"]

      - name: flagged_transactions
        display_name: "Flagged Transactions"
        expr: "SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END)"
        format: "#,##0"
        synonyms: ["fraud count", "suspicious"]

      - name: fraud_rate
        display_name: "Fraud Rate"
        expr: "SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(1), 0)"
        format: "#0.00'%'"
        synonyms: ["fraud percentage"]

      - name: avg_amount
        display_name: "Average Amount"
        expr: "AVG(amount)"
        format: "#,##0.00"

      - name: total_amount
        display_name: "Total Amount"
        expr: "SUM(amount)"
        format: "#,##0.00"
        synonyms: ["total value", "GMV"]

      - name: avg_risk_score
        display_name: "Average Risk Score"
        expr: "AVG(CAST(ensemble_score AS DOUBLE))"
        format: "#0.000"
    $$
    """)
    print("YAML Metric View created successfully!")
except Exception as e:
    print(f"YAML Metric View not supported: {e}")
    print("Creating regular SQL view instead...")

    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.financial_metrics AS
    SELECT
        DATE(transaction_time) AS transaction_date,
        COALESCE(category, 'Unknown') AS category,
        final_risk_tier AS risk_tier,
        time_slot,
        COALESCE(location, 'Unknown') AS location,
        COUNT(1) AS total_transactions,
        SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) AS flagged_transactions,
        ROUND(SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(1), 0), 2) AS fraud_rate,
        ROUND(AVG(amount), 2) AS avg_amount,
        ROUND(SUM(amount), 2) AS total_amount,
        ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 3) AS avg_risk_score
    FROM {catalog}.{schema}.gold_transactions_enriched
    GROUP BY ALL
    """)
    print("Regular SQL view created as fallback.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.financial_metrics LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Analytics Queries

# COMMAND ----------

display(spark.sql(f"""
SELECT category,
       COUNT(*) as total,
       SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) as flagged,
       ROUND(SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY category
ORDER BY fraud_rate_pct DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT final_risk_tier,
       COUNT(*) as count,
       ROUND(AVG(amount), 2) as avg_amount,
       ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 3) as avg_score
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY final_risk_tier
ORDER BY avg_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC **Next:** Run `07-deploy-genie.py`
