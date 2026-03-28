# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Metric View (YAML Semantic Layer)
# MAGIC
# MAGIC Creates a unified metric view over fraud-enriched transactions.
# MAGIC This powers Genie Space, Lakeview dashboards, and AI/BI analytics.
# MAGIC
# MAGIC Uses `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML` pattern
# MAGIC with `synonyms` and `comment` fields for Genie understanding.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Financial Metrics View (Transactions + Fraud)

# COMMAND ----------

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
    comment: "Date of the UPI transaction"
    synonyms: ["date", "txn date", "payment date"]

  - name: transaction_month
    display_name: "Transaction Month"
    expr: "DATE_TRUNC('MONTH', transaction_time)"
    comment: "Month of the transaction for trend analysis"
    synonyms: ["month", "monthly"]

  - name: category
    display_name: "Merchant Category"
    expr: "COALESCE(category, 'Unknown')"
    comment: "Merchant category of the UPI transaction"
    synonyms: ["merchant type", "txn type", "payment category", "merchant category"]

  - name: risk_tier
    display_name: "Risk Tier"
    expr: "final_risk_tier"
    comment: "ML ensemble risk classification: low, medium, high, critical"
    synonyms: ["risk level", "fraud risk", "threat level", "risk category"]

  - name: amount_bucket
    display_name: "Amount Range"
    expr: "amount_bucket"
    comment: "Transaction amount range: micro (<100), small (<1K), medium (<10K), large (<1L), very_large (>1L)"
    synonyms: ["amount range", "transaction size", "value bucket"]

  - name: time_slot
    display_name: "Time of Day"
    expr: "time_slot"
    comment: "Time slot: late_night (0-5), morning (6-11), afternoon (12-17), evening (18-21), night (22-23)"
    synonyms: ["time period", "time of day", "hour range"]

  - name: is_weekend
    display_name: "Weekend"
    expr: "CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END"
    comment: "Whether the transaction occurred on a weekend"
    synonyms: ["weekend or weekday", "day type"]

  - name: payment_mode
    display_name: "Payment Mode"
    expr: "COALESCE(payment_mode, 'Unknown')"
    comment: "UPI payment mode (QR, intent, collect, etc.)"
    synonyms: ["payment method", "payment type", "UPI mode"]

  - name: location
    display_name: "Location"
    expr: "COALESCE(location, 'Unknown')"
    comment: "City or region of the transaction"
    synonyms: ["city", "region", "place", "transaction location"]

  - name: sender_id
    display_name: "Sender"
    expr: "sender_id"
    comment: "UPI ID of the transaction sender"
    synonyms: ["payer", "from", "sender UPI", "customer"]

measures:
  - name: total_transactions
    display_name: "Total Transactions"
    expr: "COUNT(1)"
    comment: "Total number of UPI transactions"
    format: "#,##0"
    synonyms: ["transaction count", "txn count", "number of transactions", "total txns"]

  - name: total_amount
    display_name: "Total Transaction Value"
    expr: "SUM(amount)"
    comment: "Sum of all transaction amounts in Indian Rupees"
    format: "₹#,##0.00"
    synonyms: ["total value", "transaction volume", "total amount", "GMV"]

  - name: avg_amount
    display_name: "Average Transaction Amount"
    expr: "AVG(amount)"
    comment: "Average transaction amount in INR"
    format: "₹#,##0.00"
    synonyms: ["average amount", "mean amount", "avg txn value"]

  - name: median_amount
    display_name: "Median Transaction Amount"
    expr: "PERCENTILE_APPROX(amount, 0.5)"
    comment: "Median transaction amount in INR"
    format: "₹#,##0.00"
    synonyms: ["median amount", "middle value"]

  - name: flagged_transactions
    display_name: "Flagged Transactions"
    expr: "SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END)"
    comment: "Transactions flagged as potentially fraudulent by ML ensemble"
    format: "#,##0"
    synonyms: ["fraud count", "suspicious transactions", "anomalies", "flagged count", "alerts"]

  - name: fraud_rate
    display_name: "Fraud Rate"
    expr: "SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(1), 0)"
    comment: "Percentage of transactions flagged as fraudulent"
    format: "#0.00'%'"
    synonyms: ["fraud percentage", "anomaly rate", "flag rate", "fraud ratio"]

  - name: high_risk_count
    display_name: "High Risk Transactions"
    expr: "SUM(CASE WHEN final_risk_tier IN ('high', 'critical') THEN 1 ELSE 0 END)"
    comment: "Transactions classified as high or critical risk"
    format: "#,##0"
    synonyms: ["high risk count", "critical alerts", "severe flags"]

  - name: avg_ensemble_score
    display_name: "Average Risk Score"
    expr: "AVG(ensemble_score)"
    comment: "Average ensemble fraud risk score (0-1, higher = riskier)"
    format: "#0.000"
    synonyms: ["average risk", "mean risk score", "avg fraud score"]

  - name: flagged_amount
    display_name: "Flagged Transaction Value"
    expr: "SUM(CASE WHEN ensemble_flag = true THEN amount ELSE 0 END)"
    comment: "Total INR value of flagged transactions"
    format: "₹#,##0.00"
    synonyms: ["fraud value", "suspicious amount", "flagged value"]

  - name: unique_senders
    display_name: "Unique Senders"
    expr: "COUNT(DISTINCT sender_id)"
    comment: "Number of unique transaction senders"
    format: "#,##0"
    synonyms: ["unique customers", "unique payers", "distinct senders"]

  - name: unique_flagged_senders
    display_name: "Unique Flagged Senders"
    expr: "COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN sender_id END)"
    comment: "Number of unique senders with at least one flagged transaction"
    format: "#,##0"
    synonyms: ["suspicious senders", "flagged customers"]

  - name: avg_velocity_1h
    display_name: "Avg Transaction Velocity (1h)"
    expr: "AVG(txn_velocity_1h)"
    comment: "Average number of transactions per sender in a 1-hour window"
    format: "#0.0"
    synonyms: ["transaction frequency", "velocity", "txn speed"]
$$
""")

print(f"Created metric view: {catalog}.{schema}.financial_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify Metric View

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.financial_metrics LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Queries Against Metric View

# COMMAND ----------

# These are the types of queries Genie Space will handle
display(spark.sql(f"""
SELECT
    MEASURE(total_transactions),
    MEASURE(flagged_transactions),
    MEASURE(fraud_rate),
    MEASURE(total_amount),
    MEASURE(flagged_amount)
FROM {catalog}.{schema}.financial_metrics
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    category,
    MEASURE(total_transactions),
    MEASURE(fraud_rate),
    MEASURE(avg_amount)
FROM {catalog}.{schema}.financial_metrics
GROUP BY category
ORDER BY MEASURE(fraud_rate) DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    risk_tier,
    MEASURE(total_transactions),
    MEASURE(total_amount),
    MEASURE(avg_ensemble_score)
FROM {catalog}.{schema}.financial_metrics
GROUP BY risk_tier
ORDER BY MEASURE(avg_ensemble_score) DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View Complete
# MAGIC
# MAGIC **Created:** `financial_metrics` with 10 dimensions, 13 measures
# MAGIC
# MAGIC **Next step:** Run `07-deploy-genie.py` to create the Genie Space.
