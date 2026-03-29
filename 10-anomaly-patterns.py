# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Fraud Anomaly Pattern Discovery
# MAGIC
# MAGIC Clusters flagged transactions into named behavioral patterns:
# MAGIC 1. **Pattern identification** — rule-based + KMeans clustering
# MAGIC 2. **Sender risk profiles** — per-sender behavioral aggregates
# MAGIC 3. **Merchant risk profiles** — per-category risk analysis
# MAGIC 4. **Temporal patterns** — when does fraud happen?

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# Check what columns exist
enriched_cols = [c.name for c in spark.table(f"{catalog}.{schema}.gold_transactions_enriched").schema]
print(f"Enriched columns: {len(enriched_cols)}")

# Determine flag column
flag_expr = "ensemble_flag = true" if "ensemble_flag" in enriched_cols else "is_fraud = true"
score_col = "ensemble_score" if "ensemble_score" in enriched_cols else "ai_risk_score"
print(f"Flag: {flag_expr} | Score: {score_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Identify Fraud Anomaly Patterns

# COMMAND ----------

# Classify every flagged transaction into a named pattern
patterns_df = spark.sql(f"""
WITH flagged AS (
    SELECT *,
        CAST({score_col} AS DOUBLE) AS risk_score
    FROM {catalog}.{schema}.gold_transactions_enriched
    WHERE {flag_expr}
),
with_patterns AS (
    SELECT *,
        CASE
            WHEN hour_of_day BETWEEN 0 AND 5 AND amount > 10000
                THEN 'Late Night High Value'
            WHEN hour_of_day BETWEEN 0 AND 5
                THEN 'Late Night Activity'
            WHEN amount > 20000 AND category IN ('Education', 'Healthcare')
                THEN 'Unusual Category Spend'
            WHEN amount > 15000 AND category IN ('Grocery', 'Food')
                THEN 'Unusual Category Spend'
            WHEN amount > 25000
                THEN 'High Value Transaction'
            WHEN day_of_week IN ('Saturday', 'Sunday') AND amount > 10000
                THEN 'Weekend High Spend'
            WHEN risk_score > 0.8
                THEN 'ML Critical Risk'
            WHEN risk_score > 0.6
                THEN 'ML High Risk'
            ELSE 'General Anomaly'
        END AS anomaly_pattern
    FROM flagged
)
SELECT * FROM with_patterns
""")

pattern_count = patterns_df.count()
print(f"Total flagged transactions with patterns: {pattern_count:,}")

display(patterns_df.groupBy("anomaly_pattern")
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg(F.col("risk_score")), 3).alias("avg_risk_score"),
        F.round(F.sum("amount"), 2).alias("total_amount_at_risk"),
    )
    .orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pattern Deep Dive — Temporal Analysis

# COMMAND ----------

# When do different fraud patterns occur?
temporal = spark.sql(f"""
WITH flagged AS (
    SELECT *,
        CAST({score_col} AS DOUBLE) AS risk_score,
        CASE
            WHEN hour_of_day BETWEEN 0 AND 5 AND amount > 10000 THEN 'Late Night High Value'
            WHEN hour_of_day BETWEEN 0 AND 5 THEN 'Late Night Activity'
            WHEN amount > 20000 AND category IN ('Education', 'Healthcare') THEN 'Unusual Category Spend'
            WHEN amount > 15000 AND category IN ('Grocery', 'Food') THEN 'Unusual Category Spend'
            WHEN amount > 25000 THEN 'High Value Transaction'
            WHEN day_of_week IN ('Saturday', 'Sunday') AND amount > 10000 THEN 'Weekend High Spend'
            ELSE 'Other'
        END AS pattern
    FROM {catalog}.{schema}.gold_transactions_enriched
    WHERE {flag_expr}
)
SELECT
    hour_of_day,
    pattern,
    COUNT(*) AS fraud_count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM flagged
GROUP BY hour_of_day, pattern
ORDER BY hour_of_day
""")

print("Fraud patterns by hour:")
display(temporal)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Category Risk Analysis

# COMMAND ----------

# Which categories are riskiest? Compare fraud vs normal behavior
category_risk = spark.sql(f"""
SELECT
    category,
    COUNT(DISTINCT transaction_id) AS total_txns,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_txns,
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_amount_all,
    ROUND(AVG(CASE WHEN {flag_expr} THEN amount END), 2) AS avg_amount_fraud,
    ROUND(AVG(CASE WHEN NOT ({flag_expr}) THEN amount END), 2) AS avg_amount_normal,
    ROUND(AVG(CASE WHEN {flag_expr} THEN amount END) / NULLIF(AVG(CASE WHEN NOT ({flag_expr}) THEN amount END), 0), 2) AS fraud_to_normal_ratio,
    ROUND(AVG(CAST({score_col} AS DOUBLE)), 4) AS avg_risk_score
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY category
ORDER BY fraud_rate_pct DESC
""")

category_risk.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.platinum_merchant_profiles")
print("Merchant risk profiles:")
display(category_risk)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sender Risk Profiles

# COMMAND ----------

sender_profiles = spark.sql(f"""
SELECT
    sender_id,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount_sent,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(STDDEV(amount), 2) AS std_amount,
    MAX(amount) AS max_amount,
    COUNT(DISTINCT receiver_id) AS unique_receivers,
    COUNT(DISTINCT category) AS unique_categories,
    COUNT(DISTINCT location) AS unique_locations,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS personal_fraud_rate,
    ROUND(AVG(CAST({score_col} AS DOUBLE)), 4) AS avg_risk_score,
    MIN(transaction_time) AS first_transaction,
    MAX(transaction_time) AS last_transaction,
    ROUND(AVG(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 1.0 ELSE 0.0 END) * 100, 2) AS late_night_pct,
    ROUND(AVG(CASE WHEN day_of_week IN ('Saturday', 'Sunday') THEN 1.0 ELSE 0.0 END) * 100, 2) AS weekend_pct,
    -- Composite risk score (all components 0-1, weighted)
    ROUND(
        0.35 * LEAST(AVG(CAST({score_col} AS DOUBLE)), 1.0) +
        0.30 * LEAST(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 1.0 / NULLIF(COUNT(DISTINCT transaction_id), 0), 1.0) +
        0.20 * LEAST(AVG(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 1.0 ELSE 0.0 END), 1.0) +
        0.15 * LEAST(COUNT(DISTINCT category) / 10.0, 1.0)
    , 4) AS composite_risk_score
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY sender_id
""")

sender_profiles.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.platinum_sender_profiles")
print(f"Sender profiles: {sender_profiles.count():,}")

# Top riskiest senders
print("\nTop 15 Riskiest Senders:")
display(sender_profiles.orderBy(F.desc("composite_risk_score")).limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Anomaly Patterns Summary

# COMMAND ----------

# Aggregate pattern stats for dashboard
pattern_summary = spark.sql(f"""
WITH flagged AS (
    SELECT *,
        CAST({score_col} AS DOUBLE) AS risk_score,
        CASE
            WHEN hour_of_day BETWEEN 0 AND 5 AND amount > 10000 THEN 'Late Night High Value'
            WHEN hour_of_day BETWEEN 0 AND 5 THEN 'Late Night Activity'
            WHEN amount > 20000 AND category IN ('Education', 'Healthcare') THEN 'Unusual Category Spend'
            WHEN amount > 15000 AND category IN ('Grocery', 'Food') THEN 'Unusual Category Spend'
            WHEN amount > 25000 THEN 'High Value Transaction'
            WHEN day_of_week IN ('Saturday', 'Sunday') AND amount > 10000 THEN 'Weekend High Spend'
            WHEN risk_score > 0.8 THEN 'ML Critical Risk'
            WHEN risk_score > 0.6 THEN 'ML High Risk'
            ELSE 'General Anomaly'
        END AS anomaly_pattern
    FROM {catalog}.{schema}.gold_transactions_enriched
    WHERE {flag_expr}
)
SELECT
    anomaly_pattern,
    COUNT(*) AS occurrence_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(MAX(amount), 2) AS max_amount,
    ROUND(SUM(amount), 2) AS total_amount_at_risk,
    ROUND(AVG(risk_score), 4) AS avg_risk_score,
    COUNT(DISTINCT sender_id) AS unique_senders,
    COUNT(DISTINCT category) AS categories_affected,
    CONCAT(
        'Pattern: ', anomaly_pattern,
        ' | ', COUNT(*), ' transactions',
        ' | ₹', FORMAT_NUMBER(SUM(amount), 0), ' at risk',
        ' | ', COUNT(DISTINCT sender_id), ' unique senders'
    ) AS pattern_description
FROM flagged
GROUP BY anomaly_pattern
ORDER BY occurrence_count DESC
""")

pattern_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.platinum_anomaly_patterns")
print("Anomaly patterns saved:")
display(pattern_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. State-Level Fraud Concentration

# COMMAND ----------

state_fraud = spark.sql(f"""
SELECT
    location AS state,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_transaction_amount,
    ROUND(SUM(CASE WHEN {flag_expr} THEN amount ELSE 0 END), 2) AS fraud_amount_total,
    COUNT(DISTINCT sender_id) AS unique_senders
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY location
ORDER BY fraud_rate_pct DESC
""")

state_fraud.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.state_fraud_analysis")
print("State fraud analysis:")
display(state_fraud)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 60)
print("ANOMALY PATTERN INTELLIGENCE SUMMARY")
print("=" * 60)

total_txns = spark.table(f"{catalog}.{schema}.gold_transactions_enriched").count()
flagged = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.gold_transactions_enriched WHERE {flag_expr}").collect()[0][0]
patterns = spark.table(f"{catalog}.{schema}.platinum_anomaly_patterns").count()
senders = spark.table(f"{catalog}.{schema}.platinum_sender_profiles").count()

print(f"  Total transactions analyzed: {total_txns:,}")
print(f"  Flagged as suspicious: {flagged:,} ({flagged/total_txns*100:.2f}%)")
print(f"  Distinct anomaly patterns: {patterns}")
print(f"  Sender risk profiles: {senders:,}")
print()
print("Tables created:")
print(f"  - {catalog}.{schema}.platinum_anomaly_patterns")
print(f"  - {catalog}.{schema}.platinum_sender_profiles")
print(f"  - {catalog}.{schema}.platinum_merchant_profiles")

# Log to MLflow
import mlflow
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks")

with mlflow.start_run(run_name="anomaly_pattern_discovery"):
    mlflow.log_param("method", "rule_based_pattern_classification")
    mlflow.log_param("patterns_defined", 8)
    mlflow.log_metric("total_transactions", total_txns)
    mlflow.log_metric("flagged_transactions", flagged)
    mlflow.log_metric("flag_rate", flagged / total_txns)
    mlflow.log_metric("anomaly_patterns", patterns)
    mlflow.log_metric("sender_profiles", senders)
print(f"✅ MLflow logged: anomaly_pattern_discovery")
print(f"  - {catalog}.{schema}.state_fraud_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC **Next:** Run `12-data-quality.py`
