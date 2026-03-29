# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Dashboard Power Views
# MAGIC
# MAGIC Pre-computed views for visually stunning dashboard widgets.
# MAGIC Run this once — then add these views as data sources in your dashboard.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Check available columns
enriched_cols = [c.name for c in spark.table(f"{catalog}.{schema}.gold_transactions_enriched").schema]
flag_expr = "ensemble_flag = true" if "ensemble_flag" in enriched_cols else "is_fraud = true"
score_col = "ensemble_score" if "ensemble_score" in enriched_cols else "ai_risk_score"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fraud Heatmap: Hour × Day of Week

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_fraud_heatmap AS
SELECT
    hour_of_day,
    day_of_week,
    COUNT(DISTINCT transaction_id) AS total_txns,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(CASE WHEN {flag_expr} THEN CAST({score_col} AS DOUBLE) END), 3) AS avg_fraud_risk
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY hour_of_day, day_of_week
""")
print("✓ viz_fraud_heatmap")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Category × Time Slot Risk Matrix

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_category_time_matrix AS
SELECT
    category,
    time_slot,
    COUNT(DISTINCT transaction_id) AS total_txns,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(CASE WHEN {flag_expr} THEN amount ELSE 0 END), 2) AS fraud_amount
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY category, time_slot
""")
print("✓ viz_category_time_matrix")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Risk Score Distribution (Histogram Buckets)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_risk_distribution AS
SELECT
    risk_bucket,
    COUNT(*) AS txn_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(CASE WHEN {flag_expr} THEN 1 ELSE 0 END) AS fraud_in_bucket
FROM (
    SELECT *,
        CASE
            WHEN CAST({score_col} AS DOUBLE) < 0.1 THEN '0.0-0.1'
            WHEN CAST({score_col} AS DOUBLE) < 0.2 THEN '0.1-0.2'
            WHEN CAST({score_col} AS DOUBLE) < 0.3 THEN '0.2-0.3'
            WHEN CAST({score_col} AS DOUBLE) < 0.4 THEN '0.3-0.4'
            WHEN CAST({score_col} AS DOUBLE) < 0.5 THEN '0.4-0.5'
            WHEN CAST({score_col} AS DOUBLE) < 0.6 THEN '0.5-0.6'
            WHEN CAST({score_col} AS DOUBLE) < 0.7 THEN '0.6-0.7'
            WHEN CAST({score_col} AS DOUBLE) < 0.8 THEN '0.7-0.8'
            WHEN CAST({score_col} AS DOUBLE) < 0.9 THEN '0.8-0.9'
            ELSE '0.9-1.0'
        END AS risk_bucket
    FROM {catalog}.{schema}.gold_transactions_enriched
)
GROUP BY risk_bucket
ORDER BY risk_bucket
""")
print("✓ viz_risk_distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fraud vs Normal: Amount Distribution Comparison

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_amount_comparison AS
SELECT
    amount_range,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN NOT is_flagged THEN 1 ELSE 0 END) AS normal_count,
    ROUND(AVG(CASE WHEN is_flagged THEN amount END), 2) AS avg_fraud_amount,
    ROUND(AVG(CASE WHEN NOT is_flagged THEN amount END), 2) AS avg_normal_amount
FROM (
    SELECT
        amount,
        {flag_expr} AS is_flagged,
        CASE
            WHEN amount < 100 THEN '₹0-100'
            WHEN amount < 500 THEN '₹100-500'
            WHEN amount < 1000 THEN '₹500-1K'
            WHEN amount < 5000 THEN '₹1K-5K'
            WHEN amount < 10000 THEN '₹5K-10K'
            WHEN amount < 25000 THEN '₹10K-25K'
            WHEN amount < 50000 THEN '₹25K-50K'
            ELSE '₹50K+'
        END AS amount_range
    FROM {catalog}.{schema}.gold_transactions_enriched
)
GROUP BY amount_range
ORDER BY CASE amount_range
    WHEN '₹0-100' THEN 1 WHEN '₹100-500' THEN 2 WHEN '₹500-1K' THEN 3
    WHEN '₹1K-5K' THEN 4 WHEN '₹5K-10K' THEN 5 WHEN '₹10K-25K' THEN 6
    WHEN '₹25K-50K' THEN 7 ELSE 8 END
""")
print("✓ viz_amount_comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monthly Fraud Trend (Rolling)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_monthly_trend AS
SELECT
    DATE_TRUNC('month', transaction_time) AS month,
    COUNT(DISTINCT transaction_id) AS total_txns,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_txns,
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate_pct,
    ROUND(SUM(amount), 2) AS total_volume,
    ROUND(SUM(CASE WHEN {flag_expr} THEN amount ELSE 0 END), 2) AS fraud_volume,
    COUNT(DISTINCT sender_id) AS unique_senders
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY DATE_TRUNC('month', transaction_time)
ORDER BY month
""")
print("✓ viz_monthly_trend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. UPI Ecosystem Growth (Context Data)

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.viz_upi_growth AS
    SELECT
        date,
        payment_mode,
        volume_millions,
        value_crore,
        ROUND(value_crore / NULLIF(volume_millions, 0), 2) AS avg_txn_value_crore
    FROM {catalog}.{schema}.india_digital_payments
    WHERE payment_mode = 'UPI'
    ORDER BY date
    """)
    print("✓ viz_upi_growth")
except:
    print("✗ viz_upi_growth skipped (india_digital_payments not loaded)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Bank Fraud Losses (Context Data)

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.viz_bank_fraud AS
    SELECT
        bank_name,
        fiscal_year,
        fraud_count,
        loss_crore,
        recovered_crore,
        ROUND(recovered_crore * 100.0 / NULLIF(loss_crore, 0), 1) AS recovery_rate_pct,
        ROUND(loss_crore - recovered_crore, 2) AS net_loss_crore
    FROM {catalog}.{schema}.india_bank_fraud_stats
    ORDER BY fiscal_year, loss_crore DESC
    """)
    print("✓ viz_bank_fraud")
except:
    print("✗ viz_bank_fraud skipped (india_bank_fraud_stats not loaded)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. State Vulnerability Composite

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.viz_state_vulnerability AS
    SELECT
        s.state,
        s.fraud_rate_pct,
        s.total_transactions,
        s.fraud_count,
        COALESCE(i.total_subscribers_per_100, 0) AS internet_per_100,
        COALESCE(i.smartphone_penetration_pct, 0) AS smartphone_pct,
        COALESCE(j.total_beneficiaries_lakh, 0) AS jan_dhan_lakh,
        COALESCE(v.vulnerability_index, 0) AS vulnerability_index,
        CASE
            WHEN COALESCE(v.vulnerability_index, 0) > 0.6 THEN 'CRITICAL'
            WHEN COALESCE(v.vulnerability_index, 0) > 0.4 THEN 'HIGH'
            WHEN COALESCE(v.vulnerability_index, 0) > 0.2 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS vulnerability_level
    FROM {catalog}.{schema}.state_fraud_analysis s
    LEFT JOIN {catalog}.{schema}.india_internet_penetration i ON LOWER(s.state) = LOWER(i.state)
    LEFT JOIN {catalog}.{schema}.india_pmjdy_statewise j ON LOWER(s.state) = LOWER(j.state)
    LEFT JOIN {catalog}.{schema}.state_vulnerability_index v ON LOWER(s.state) = LOWER(v.state)
    ORDER BY vulnerability_index DESC
    """)
    print("✓ viz_state_vulnerability")
except Exception as e:
    print(f"✗ viz_state_vulnerability skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Anomaly Pattern Summary (for dashboard cards)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_anomaly_patterns AS
SELECT
    anomaly_pattern,
    occurrence_count,
    avg_amount,
    max_amount,
    total_amount_at_risk,
    avg_risk_score,
    unique_senders,
    categories_affected,
    ROUND(occurrence_count * 100.0 / SUM(occurrence_count) OVER (), 1) AS pct_of_all_fraud
FROM {catalog}.{schema}.platinum_anomaly_patterns
ORDER BY occurrence_count DESC
""")
print("✓ viz_anomaly_patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Fraud Recovery Guide (for display)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_recovery_guide AS
SELECT
    fraud_type,
    description,
    rbi_rule AS rbi_liability_rule,
    recovery_steps,
    report_to,
    time_limit_days,
    max_liability_inr,
    common_in_states
FROM {catalog}.{schema}.fraud_recovery_guide
""")
print("✓ viz_recovery_guide")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Top Risky Senders (Leaderboard)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_risky_senders AS
SELECT
    sender_id,
    total_transactions,
    ROUND(total_amount_sent, 2) AS total_amount,
    fraud_count,
    personal_fraud_rate,
    ROUND(avg_risk_score, 3) AS avg_risk_score,
    ROUND(composite_risk_score, 3) AS composite_risk,
    late_night_pct,
    weekend_pct,
    unique_receivers,
    unique_categories
FROM {catalog}.{schema}.platinum_sender_profiles
ORDER BY composite_risk_score DESC
LIMIT 50
""")
print("✓ viz_risky_senders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. KPI Counters (Single-Value Widgets)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.viz_kpis AS
SELECT
    (SELECT COUNT(DISTINCT transaction_id) FROM {catalog}.{schema}.gold_transactions_enriched) AS total_transactions,
    (SELECT COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) FROM {catalog}.{schema}.gold_transactions_enriched) AS flagged_transactions,
    (SELECT ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 / NULLIF(COUNT(DISTINCT transaction_id), 0), 2) FROM {catalog}.{schema}.gold_transactions_enriched) AS fraud_rate_pct,
    (SELECT ROUND(SUM(CASE WHEN {flag_expr} THEN amount ELSE 0 END), 2) FROM {catalog}.{schema}.gold_transactions_enriched) AS total_amount_at_risk,
    (SELECT COUNT(*) FROM {catalog}.{schema}.platinum_anomaly_patterns) AS anomaly_patterns_found,
    (SELECT COUNT(*) FROM {catalog}.{schema}.platinum_sender_profiles WHERE composite_risk_score > 0.3) AS high_risk_senders,
    (SELECT COUNT(*) FROM {catalog}.{schema}.gold_schemes) AS schemes_available,
    (SELECT COUNT(DISTINCT circular_id) FROM {catalog}.{schema}.gold_circular_chunks) AS rbi_circulars_indexed,
    (SELECT COUNT(*) FROM {catalog}.{schema}.fraud_recovery_guide) AS fraud_types_covered
""")
print("✓ viz_kpis")
display(spark.sql(f"SELECT * FROM {catalog}.{schema}.viz_kpis"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Complaint Surge (if available)

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.viz_complaint_surge AS
    SELECT
        fiscal_year,
        complaint_type,
        complaints_received,
        ROUND(complaints_received * 100.0 / SUM(complaints_received) OVER (PARTITION BY fiscal_year), 1) AS pct_of_year
    FROM {catalog}.{schema}.india_rbi_complaints
    ORDER BY fiscal_year, complaints_received DESC
    """)
    print("✓ viz_complaint_surge")
except:
    print("✗ viz_complaint_surge skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Views Created
# MAGIC
# MAGIC Add these as data sources in your dashboard:
# MAGIC
# MAGIC | View | Widget Type | Dashboard Page |
# MAGIC |------|------------|---------------|
# MAGIC | `viz_kpis` | Counters (top row) | Page 1: Command Center |
# MAGIC | `viz_fraud_heatmap` | Heatmap (hour × day) | Page 1 |
# MAGIC | `viz_monthly_trend` | Line chart (fraud over time) | Page 1 |
# MAGIC | `viz_amount_comparison` | Grouped bar (fraud vs normal) | Page 1 |
# MAGIC | `viz_risk_distribution` | Bar chart (score buckets) | Page 1 |
# MAGIC | `viz_category_time_matrix` | Heatmap (category × time) | Page 2: Intelligence |
# MAGIC | `viz_anomaly_patterns` | Bar + table | Page 2 |
# MAGIC | `viz_risky_senders` | Table | Page 2 |
# MAGIC | `viz_upi_growth` | Line chart | Page 3: India Story |
# MAGIC | `viz_bank_fraud` | Bar chart | Page 3 |
# MAGIC | `viz_state_vulnerability` | Bar chart (sorted) | Page 3 |
# MAGIC | `viz_complaint_surge` | Stacked bar | Page 3 |
# MAGIC | `viz_recovery_guide` | Table | Page 4: Help |
# MAGIC | `data_quality_metrics` | Table | Page 4: Pipeline |
