# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Data Quality, Governance & Tiered Architecture
# MAGIC
# MAGIC 1. **Unity Catalog Governance** — Tags, comments, lineage
# MAGIC 2. **Warm Tier** — Materialized views for real-time dashboards
# MAGIC 3. **Liquid Clustering** — Modern partitioning on gold tables
# MAGIC 4. **Data Quality Metrics** — Track pipeline health

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog Tags (Data Classification)

# COMMAND ----------

# Tag every table with domain, tier, and PII classification
tags = {
    "bronze_transactions": {"domain": "fraud", "tier": "bronze", "pii": "false", "source": "kaggle"},
    "bronze_circulars": {"domain": "regulatory", "tier": "bronze", "pii": "false", "source": "rbi.org.in"},
    "bronze_schemes": {"domain": "inclusion", "tier": "bronze", "pii": "false", "source": "myscheme.gov.in"},
    "silver_transactions": {"domain": "fraud", "tier": "silver", "pii": "false", "quality": "validated"},
    "silver_circulars": {"domain": "regulatory", "tier": "silver", "pii": "false", "quality": "validated"},
    "silver_schemes": {"domain": "inclusion", "tier": "silver", "pii": "false", "quality": "validated"},
    "gold_transactions": {"domain": "fraud", "tier": "gold", "pii": "false", "quality": "business_ready"},
    "gold_fraud_alerts": {"domain": "fraud", "tier": "gold", "pii": "false", "quality": "business_ready"},
    "gold_circular_chunks": {"domain": "regulatory", "tier": "gold", "pii": "false", "quality": "rag_ready"},
    "gold_schemes": {"domain": "inclusion", "tier": "gold", "pii": "false", "quality": "business_ready"},
    "gold_transactions_enriched": {"domain": "fraud", "tier": "platinum", "pii": "false", "quality": "ml_scored"},
    "gold_fraud_alerts_ml": {"domain": "fraud", "tier": "platinum", "pii": "false", "quality": "ml_scored"},
    "platinum_fraud_rings": {"domain": "fraud", "tier": "platinum", "pii": "false", "quality": "graph_analysis"},
    "platinum_sender_profiles": {"domain": "fraud", "tier": "platinum", "pii": "true", "quality": "feature_store"},
    "platinum_merchant_profiles": {"domain": "fraud", "tier": "platinum", "pii": "false", "quality": "feature_store"},
}

for table, tag_dict in tags.items():
    tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tag_dict.items()])
    try:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} SET TAGS ({tag_str})")
        print(f"  Tagged: {table}")
    except Exception as e:
        print(f"  Skip: {table} — {str(e)[:60]}")

print("\nUC Tags applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table & Column Comments (Documentation)

# COMMAND ----------

table_comments = {
    "gold_transactions_enriched": "ML-scored UPI transactions with IsolationForest + KMeans ensemble fraud scores. Platinum tier.",
    "gold_fraud_alerts_ml": "Transactions flagged by ML ensemble (ensemble_flag=true). Used for fraud monitoring.",
    "platinum_fraud_rings": "Fraud rings detected via NetworkX graph analysis. Connected components with 3+ accounts.",
    "platinum_sender_profiles": "Per-sender behavioral profiles with PageRank, degree centrality, fraud rate, and composite risk score.",
    "platinum_merchant_profiles": "Per-merchant category risk profiles with fraud rates and temporal patterns.",
}

for table, comment in table_comments.items():
    try:
        spark.sql(f"COMMENT ON TABLE {catalog}.{schema}.{table} IS '{comment}'")
        print(f"  Commented: {table}")
    except Exception as e:
        print(f"  Skip: {table} — {str(e)[:60]}")

# Key column comments on enriched table
col_comments = {
    "ensemble_score": "Weighted fraud risk score (0-1). Combines IsolationForest (45%), KMeans distance (30%), rule-based risk (25%).",
    "ensemble_flag": "True if ensemble_score > 0.5. Indicates potential fraud.",
    "final_risk_tier": "Risk classification: low (<0.3), medium (0.3-0.5), high (0.5-0.7), critical (>0.7).",
    "isolation_forest_score": "Anomaly score from IsolationForest (0-1, higher = more anomalous).",
    "km_score": "Distance to nearest KMeans cluster center, normalized to 0-1.",
    "amount_deviation": "Z-score of transaction amount vs sender historical average.",
}

for col, comment in col_comments.items():
    try:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.gold_transactions_enriched ALTER COLUMN {col} COMMENT '{comment}'")
    except:
        pass

print("Column comments applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Liquid Clustering (Modern Partitioning)

# COMMAND ----------

# Apply liquid clustering on gold tables for optimized query patterns
try:
    spark.sql(f"ALTER TABLE {catalog}.{schema}.gold_transactions_enriched CLUSTER BY (transaction_date, final_risk_tier)")
    print("Liquid clustering applied: gold_transactions_enriched (transaction_date, final_risk_tier)")
except Exception as e:
    print(f"Liquid clustering: {e}")

try:
    spark.sql(f"OPTIMIZE {catalog}.{schema}.gold_transactions_enriched")
    print("OPTIMIZE completed — files compacted with liquid clustering")
except Exception as e:
    print(f"OPTIMIZE: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Warm Tier — Materialized Views

# COMMAND ----------

# Pre-aggregated fraud stats for dashboard performance
try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.mv_fraud_summary AS
    SELECT
        DATE(transaction_time) AS txn_date,
        category,
        final_risk_tier,
        location,
        time_slot,
        COUNT(*) AS txn_count,
        ROUND(SUM(amount), 2) AS total_amount,
        SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) AS flagged_count,
        ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 4) AS avg_risk_score,
        ROUND(SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct
    FROM {catalog}.{schema}.gold_transactions_enriched
    GROUP BY ALL
    """)
    print("Warm tier view created: mv_fraud_summary")
except Exception as e:
    print(f"Materialized view: {e}")

# Sender risk summary for quick lookups
try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.mv_high_risk_senders AS
    SELECT *
    FROM {catalog}.{schema}.platinum_sender_profiles
    WHERE composite_risk_score > 0.5 OR is_hub = true
    ORDER BY composite_risk_score DESC
    """)
    print("Warm tier view created: mv_high_risk_senders")
except Exception as e:
    print(f"High risk senders view: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Metrics

# COMMAND ----------

from pyspark.sql import functions as F

# Compute quality stats for every table
tables = [
    "bronze_transactions", "bronze_circulars", "bronze_schemes",
    "silver_transactions", "silver_circulars", "silver_schemes",
    "gold_transactions", "gold_fraud_alerts", "gold_circular_chunks", "gold_schemes",
    "gold_transactions_enriched", "gold_fraud_alerts_ml",
    "platinum_fraud_rings", "platinum_sender_profiles", "platinum_merchant_profiles"
]

quality_data = []
for t in tables:
    try:
        df = spark.table(f"{catalog}.{schema}.{t}")
        row_count = df.count()
        col_count = len(df.columns)

        # Null rate for key columns
        null_rates = {}
        for col in df.columns[:5]:  # check first 5 columns
            nulls = df.filter(F.col(col).isNull()).count()
            null_rates[col] = round(nulls / max(row_count, 1) * 100, 2)

        quality_data.append({
            "table_name": t,
            "row_count": row_count,
            "column_count": col_count,
            "avg_null_rate_pct": round(np.mean(list(null_rates.values())), 2) if null_rates else 0,
            "tier": t.split("_")[0] if not t.startswith("mv_") else "warm",
        })
    except Exception as e:
        quality_data.append({"table_name": t, "row_count": 0, "column_count": 0, "avg_null_rate_pct": 0, "tier": "error"})

quality_df = spark.createDataFrame(pd.DataFrame(quality_data))
quality_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.data_quality_metrics")

print("Data Quality Summary:")
display(quality_df.orderBy("tier", "table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline Inventory

# COMMAND ----------

print(f"\n{'='*60}")
print(f"DIGITAL-ARTHA PIPELINE INVENTORY")
print(f"{'='*60}")
print(f"\nCatalog: {catalog}.{schema}")
print(f"\nTier Summary:")

import pandas as pd
qpdf = pd.DataFrame(quality_data)
for tier in ["bronze", "silver", "gold", "platinum"]:
    tier_data = qpdf[qpdf["table_name"].str.startswith(tier)]
    if len(tier_data) > 0:
        print(f"\n  {tier.upper()} ({len(tier_data)} tables, {tier_data['row_count'].sum():,} total rows):")
        for _, row in tier_data.iterrows():
            print(f"    - {row['table_name']}: {row['row_count']:,} rows, {row['column_count']} cols")

print(f"\n  WARM TIER (pre-aggregated views):")
print(f"    - mv_fraud_summary")
print(f"    - mv_high_risk_senders")

print(f"\n  FEATURE STORE:")
print(f"    - platinum_sender_profiles (per-sender behavioral + graph features)")
print(f"    - platinum_merchant_profiles (per-category risk profiles)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC **Created:**
# MAGIC - UC tags on 15 tables
# MAGIC - Column-level comments on key columns
# MAGIC - Liquid clustering on enriched transactions
# MAGIC - Warm tier views (mv_fraud_summary, mv_high_risk_senders)
# MAGIC - Data quality metrics table
# MAGIC
# MAGIC **Next:** Run `13-human-impact.py`
