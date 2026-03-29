# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Accuracy & Performance Metrics
# MAGIC
# MAGIC Metrics designed for **unsupervised anomaly detection** — not supervised classification.
# MAGIC Our ensemble finds behavioral outliers, not pre-labeled fraud.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Scale Metrics

# COMMAND ----------

print("=" * 60)
print("PIPELINE SCALE")
print("=" * 60)

tables_df = spark.table(f"{catalog}.{schema}.data_quality_metrics")
display(tables_df.orderBy("tier", "table_name"))

total_tables = tables_df.count()
total_rows = tables_df.select(F.sum("row_count")).collect()[0][0]
print(f"\n  Tables in pipeline: {total_tables}")
print(f"  Total rows: {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Anomaly Detection Metrics
# MAGIC
# MAGIC For unsupervised models, the right metrics are:
# MAGIC - **Detection rate** (what % flagged)
# MAGIC - **Anomaly characteristics** (are flagged txns genuinely different?)
# MAGIC - **Separation** (how different are flagged vs normal?)

# COMMAND ----------

print("=" * 60)
print("ANOMALY DETECTION PERFORMANCE")
print("=" * 60)

total = spark.sql(f"SELECT COUNT(DISTINCT transaction_id) FROM {catalog}.{schema}.gold_transactions_enriched").collect()[0][0]
flagged = spark.sql(f"SELECT COUNT(DISTINCT transaction_id) FROM {catalog}.{schema}.gold_transactions_enriched WHERE ensemble_flag = true").collect()[0][0]

print(f"\n  Transactions analyzed: {total:,}")
print(f"  Anomalies detected: {flagged:,}")
print(f"  Detection rate: {flagged/total*100:.2f}%")

# KEY METRIC: Are flagged transactions genuinely different from normal?
comparison = spark.sql(f"""
SELECT
    CASE WHEN ensemble_flag = true THEN 'Flagged' ELSE 'Normal' END AS group_type,
    COUNT(DISTINCT transaction_id) AS count,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(STDDEV(amount), 2) AS std_amount,
    ROUND(MAX(amount), 2) AS max_amount,
    ROUND(AVG(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_night_pct,
    ROUND(AVG(CASE WHEN day_of_week IN ('Saturday', 'Sunday') OR is_weekend = true THEN 1.0 ELSE 0.0 END) * 100, 1) AS weekend_pct,
    ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 4) AS avg_risk_score
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY CASE WHEN ensemble_flag = true THEN 'Flagged' ELSE 'Normal' END
""")

print("\n  Flagged vs Normal Transaction Comparison:")
display(comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Separation Score (Anomaly Quality)

# COMMAND ----------

# How well separated are flagged vs normal? Higher = better anomaly detection
sep = spark.sql(f"""
SELECT
    ROUND(
        (flagged_avg - normal_avg) / NULLIF(SQRT((flagged_std * flagged_std + normal_std * normal_std) / 2), 0)
    , 3) AS amount_separation,
    flagged_avg,
    normal_avg
FROM (
    SELECT
        AVG(CASE WHEN ensemble_flag = true THEN amount END) AS flagged_avg,
        AVG(CASE WHEN ensemble_flag = false OR ensemble_flag IS NULL THEN amount END) AS normal_avg,
        STDDEV(CASE WHEN ensemble_flag = true THEN amount END) AS flagged_std,
        STDDEV(CASE WHEN ensemble_flag = false OR ensemble_flag IS NULL THEN amount END) AS normal_std
    FROM {catalog}.{schema}.gold_transactions_enriched
)
""").collect()[0]

separation = float(sep["amount_separation"] or 0)
print(f"  Amount Separation Score (Cohen's d): {separation:.3f}")
print(f"  (>0.5 = medium effect, >0.8 = large effect)")
print(f"  Flagged avg amount: ₹{float(sep['flagged_avg'] or 0):,.2f}")
print(f"  Normal avg amount:  ₹{float(sep['normal_avg'] or 0):,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anomaly Pattern Metrics

# COMMAND ----------

print("=" * 60)
print("ANOMALY PATTERNS DISCOVERED")
print("=" * 60)

patterns = spark.table(f"{catalog}.{schema}.platinum_anomaly_patterns")
pattern_count = patterns.count()
total_at_risk = patterns.select(F.sum("total_amount_at_risk")).collect()[0][0] or 0

print(f"\n  Distinct patterns: {pattern_count}")
print(f"  Total ₹ at risk: ₹{total_at_risk:,.0f}")

display(patterns.select("anomaly_pattern", "occurrence_count",
    F.round("avg_amount", 0).alias("avg_amount"),
    F.round("total_amount_at_risk", 0).alias("amount_at_risk"),
    "unique_senders"
).orderBy(F.desc("occurrence_count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sender Risk Profile Metrics

# COMMAND ----------

print("=" * 60)
print("SENDER RISK PROFILES")
print("=" * 60)

senders = spark.table(f"{catalog}.{schema}.platinum_sender_profiles")
sender_count = senders.count()
high_risk = senders.filter("composite_risk_score > 0.2").count()
very_high = senders.filter("composite_risk_score > 0.3").count()

print(f"\n  Total sender profiles: {sender_count:,}")
print(f"  Elevated risk (>0.2): {high_risk:,}")
print(f"  High risk (>0.3): {very_high:,}")

display(spark.sql(f"""
SELECT
    CASE
        WHEN composite_risk_score > 0.3 THEN 'High (>0.3)'
        WHEN composite_risk_score > 0.2 THEN 'Elevated (0.2-0.3)'
        WHEN composite_risk_score > 0.1 THEN 'Moderate (0.1-0.2)'
        ELSE 'Low (<0.1)'
    END AS risk_bucket,
    COUNT(*) AS sender_count,
    ROUND(AVG(total_transactions), 0) AS avg_txns,
    ROUND(AVG(fraud_count), 1) AS avg_frauds
FROM {catalog}.{schema}.platinum_sender_profiles
GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RAG Pipeline Metrics

# COMMAND ----------

print("=" * 60)
print("RAG PIPELINE (RBI CIRCULAR SEARCH)")
print("=" * 60)

circulars = spark.sql(f"SELECT COUNT(DISTINCT circular_id) FROM {catalog}.{schema}.gold_circular_chunks").collect()[0][0]
chunks = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.gold_circular_chunks").collect()[0][0]
avg_chunk = spark.sql(f"SELECT ROUND(AVG(chunk_length), 0) FROM {catalog}.{schema}.gold_circular_chunks").collect()[0][0]

print(f"\n  RBI circulars indexed: {circulars}")
print(f"  Chunks created: {chunks}")
print(f"  Avg chunk size: {avg_chunk} chars")
print(f"  Embedding model: multilingual-e5-small (384 dims)")
print(f"  Vector index: FAISS (cosine similarity)")
print(f"  Languages: Hindi + English + 20 more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. BhashaBench-Finance Evaluation

# COMMAND ----------

print("=" * 60)
print("BHASHABENCH-FINANCE EVALUATION")
print("=" * 60)

try:
    bb = spark.table(f"{catalog}.{schema}.bhashabench_results")
    total_q = bb.count()
    perfect = bb.filter("score = 1.0").count()
    avg_score = bb.select(F.avg("score")).collect()[0][0] * 100

    print(f"\n  Overall: {avg_score:.0f}% ({perfect}/{total_q})")

    display(spark.sql(f"""
        SELECT category, COUNT(*) AS questions,
               ROUND(AVG(score) * 100, 0) AS accuracy,
               ROUND(AVG(answer_length), 0) AS avg_chars
        FROM {catalog}.{schema}.bhashabench_results
        GROUP BY category ORDER BY category
    """))

    display(spark.sql(f"""
        SELECT language, COUNT(*) AS questions,
               ROUND(AVG(score) * 100, 0) AS accuracy
        FROM {catalog}.{schema}.bhashabench_results
        GROUP BY language
    """))
except Exception as e:
    total_q, perfect, avg_score = 0, 0, 0
    print(f"  BhashaBench not run yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fraud Ring Intelligence

# COMMAND ----------

print("=" * 60)
print("FRAUD RING DETECTION (GRAPH ANALYSIS)")
print("=" * 60)

try:
    rings = spark.table(f"{catalog}.{schema}.detected_fraud_rings")
    ring_count = rings.count()
    ring_with_cycles = rings.filter("has_cycles = true").count()
    total_ring_accounts = rings.select(F.sum("accounts")).collect()[0][0] or 0
    total_ring_amount = rings.select(F.sum("total_amount")).collect()[0][0] or 0

    hubs = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.fraud_ring_hubs WHERE is_hub = true").collect()[0][0]

    print(f"\n  Rings detected: {ring_count}")
    print(f"  Rings with circular flows: {ring_with_cycles}")
    print(f"  Accounts in rings: {total_ring_accounts:,}")
    print(f"  Hub accounts (money mules): {hubs}")
    print(f"  Total ₹ in ring flows: ₹{total_ring_amount:,.0f}")
except Exception as e:
    ring_count, hubs, total_ring_amount = 0, 0, 0
    print(f"  Fraud rings not run yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Financial Inclusion Metrics

# COMMAND ----------

print("=" * 60)
print("FINANCIAL INCLUSION")
print("=" * 60)

schemes = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.gold_schemes").collect()[0][0]
ministries = spark.sql(f"SELECT COUNT(DISTINCT ministry) FROM {catalog}.{schema}.gold_schemes").collect()[0][0]

print(f"\n  Government schemes indexed: {schemes}")
print(f"  Ministries covered: {ministries}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SUMMARY CARD

# COMMAND ----------

print("=" * 60)
print("BLACKICE: ACCURACY SUMMARY CARD")
print("=" * 60)
print(f"""
  ANOMALY DETECTION:
    Transactions scored:       {total:,}
    Anomalies detected:        {flagged:,} ({flagged/total*100:.2f}%)
    Amount separation (d):     {separation:.3f}
    ₹ at risk:                 ₹{total_at_risk:,.0f}
    Anomaly patterns:          {pattern_count}

  SENDER INTELLIGENCE:
    Sender profiles:           {sender_count:,}
    Elevated risk senders:     {high_risk:,}
    High risk senders:         {very_high:,}

  GRAPH INTELLIGENCE:
    Fraud rings detected:      {ring_count}
    Hub accounts:              {hubs}
    Ring volume:               ₹{total_ring_amount:,.0f}

  NLP & MULTILINGUAL:
    BhashaBench score:         {avg_score:.0f}% ({perfect}/{total_q})
    RBI circulars indexed:     {circulars}
    RAG chunks:                {chunks}
    Languages supported:       22+

  FINANCIAL INCLUSION:
    Schemes indexed:           {schemes}
    Ministries covered:        {ministries}

  PLATFORM:
    Pipeline tables:           {total_tables}
    Total rows processed:      {total_rows:,}
    Databricks features:       17+
    MLflow runs logged:        5+
""")
