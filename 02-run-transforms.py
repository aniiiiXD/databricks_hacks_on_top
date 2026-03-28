# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Data Transforms (Bronze → Silver → Gold)
# MAGIC
# MAGIC Runs all transformations as regular SQL (no DLT required).
# MAGIC Creates Silver (cleaned) and Gold (business-ready) tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Working in: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Transactions (cleaned + time features)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_transactions AS
SELECT DISTINCT
    transaction_id,
    amount,
    transaction_time,
    sender_id,
    receiver_id,
    sender_name,
    receiver_name,
    category,
    merchant_id,
    merchant_name,
    payment_mode,
    device_type,
    location,
    is_fraud,
    sender_bank,
    receiver_bank,
    sender_age_group,
    receiver_age_group,
    network_type,
    transaction_status,
    hour_of_day,
    day_of_week,
    day_of_month,
    month,
    CASE WHEN day_of_week IN ('Saturday', 'Sunday') OR is_weekend = true THEN true ELSE false END AS is_weekend,
    CASE
      WHEN hour_of_day BETWEEN 0 AND 5 THEN 'late_night'
      WHEN hour_of_day BETWEEN 6 AND 11 THEN 'morning'
      WHEN hour_of_day BETWEEN 12 AND 17 THEN 'afternoon'
      WHEN hour_of_day BETWEEN 18 AND 21 THEN 'evening'
      ELSE 'night'
    END AS time_slot,
    CASE
      WHEN amount < 100 THEN 'micro'
      WHEN amount < 1000 THEN 'small'
      WHEN amount < 10000 THEN 'medium'
      WHEN amount < 100000 THEN 'large'
      ELSE 'very_large'
    END AS amount_bucket,
    ingested_at
FROM {catalog}.{schema}.bronze_transactions
WHERE transaction_id IS NOT NULL
  AND amount > 0
  AND transaction_time IS NOT NULL
""")

count = spark.table(f"{catalog}.{schema}.silver_transactions").count()
print(f"silver_transactions: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Circulars (cleaned)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_circulars AS
SELECT DISTINCT
    circular_id,
    TRIM(title) AS title,
    date,
    circular_date,
    COALESCE(department, 'Reserve Bank of India') AS department,
    COALESCE(circular_category, 'general') AS circular_category,
    TRIM(full_text) AS full_text,
    url,
    text_length,
    SIZE(SPLIT(TRIM(full_text), ' ')) AS word_count,
    ingested_at
FROM {catalog}.{schema}.bronze_circulars
WHERE circular_id IS NOT NULL
  AND full_text IS NOT NULL
  AND LENGTH(TRIM(full_text)) > 50
""")

count = spark.table(f"{catalog}.{schema}.silver_circulars").count()
print(f"silver_circulars: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Schemes (cleaned + structured eligibility)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_schemes AS
SELECT
    scheme_id,
    TRIM(scheme_name) AS scheme_name,
    COALESCE(TRIM(ministry), 'Government of India') AS ministry,
    COALESCE(TRIM(description), '') AS description,
    COALESCE(TRIM(eligibility_criteria), '') AS eligibility_criteria,
    COALESCE(TRIM(benefits), '') AS benefits,
    COALESCE(TRIM(target_group), 'All') AS target_group,
    COALESCE(income_limit, 0) AS income_limit,
    COALESCE(age_min, 0) AS age_min,
    COALESCE(age_max, 999) AS age_max,
    COALESCE(LOWER(TRIM(gender)), 'all') AS gender,
    COALESCE(LOWER(TRIM(occupation)), 'all') AS occupation,
    COALESCE(TRIM(state), 'All India') AS state,
    url,
    ingested_at
FROM {catalog}.{schema}.bronze_schemes
WHERE scheme_id IS NOT NULL
  AND scheme_name IS NOT NULL
""")

count = spark.table(f"{catalog}.{schema}.silver_schemes").count()
print(f"silver_schemes: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Transactions (with anomaly flag)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_transactions AS
SELECT
    *,
    CASE WHEN is_fraud = true THEN true ELSE false END AS anomaly_flag,
    CASE
      WHEN is_fraud = true THEN 0.9
      ELSE 0.1
    END AS ai_risk_score,
    CASE
      WHEN is_fraud = true THEN 'high'
      ELSE 'low'
    END AS ai_risk_label,
    DATE(transaction_time) AS transaction_date
FROM {catalog}.{schema}.silver_transactions
""")

count = spark.table(f"{catalog}.{schema}.gold_transactions").count()
fraud = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.gold_transactions WHERE anomaly_flag = true").collect()[0][0]
print(f"gold_transactions: {count:,} rows ({fraud:,} flagged)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Fraud Alerts

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_fraud_alerts AS
SELECT
    transaction_id,
    amount,
    transaction_time,
    DATE(transaction_time) AS transaction_date,
    sender_id,
    sender_name,
    receiver_id,
    receiver_name,
    category,
    merchant_name,
    payment_mode,
    device_type,
    location,
    hour_of_day,
    day_of_week,
    is_weekend,
    time_slot,
    amount_bucket,
    ai_risk_label,
    ai_risk_score,
    is_fraud,
    CONCAT(
      'Risk: ', UPPER(ai_risk_label),
      ' | Amount: ', CAST(ROUND(amount, 2) AS STRING),
      ' | Time: ', time_slot,
      ' | Category: ', COALESCE(category, 'unknown')
    ) AS alert_summary,
    CURRENT_TIMESTAMP() AS flagged_at
FROM {catalog}.{schema}.gold_transactions
WHERE anomaly_flag = true
ORDER BY ai_risk_score DESC, amount DESC
""")

count = spark.table(f"{catalog}.{schema}.gold_fraud_alerts").count()
print(f"gold_fraud_alerts: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold Circular Chunks (for Vector Search / RAG)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_circular_chunks AS
WITH chunked AS (
    SELECT
        circular_id,
        title,
        circular_date,
        department,
        circular_category AS topic_label,
        url,
        AGGREGATE(
            SPLIT(full_text, '\n\n'),
            named_struct(
                'chunks', ARRAY(CAST('' AS STRING)),
                'cur', CAST('' AS STRING)
            ),
            (acc, paragraph) ->
                CASE
                    WHEN LENGTH(acc.cur) + LENGTH(paragraph) + 2 > 4000
                    THEN named_struct(
                        'chunks', CONCAT(acc.chunks, ARRAY(acc.cur)),
                        'cur', paragraph
                    )
                    ELSE named_struct(
                        'chunks', acc.chunks,
                        'cur', CASE
                            WHEN LENGTH(acc.cur) = 0 THEN paragraph
                            ELSE CONCAT(acc.cur, '\n\n', paragraph)
                        END
                    )
                END,
            acc -> CONCAT(acc.chunks, ARRAY(acc.cur))
        ) AS chunks_array
    FROM {catalog}.{schema}.silver_circulars
    WHERE full_text IS NOT NULL AND LENGTH(TRIM(full_text)) > 0
)
SELECT
    CONCAT(circular_id, '_chunk_', CAST(chunk_index AS STRING)) AS chunk_id,
    circular_id,
    title,
    circular_date,
    department,
    topic_label,
    url,
    chunk AS chunk_text,
    chunk_index,
    LENGTH(chunk) AS chunk_length,
    SIZE(chunks_array) AS total_chunks_in_circular,
    CONCAT('RBI Circular: ', title, ' (', COALESCE(CAST(circular_date AS STRING), 'undated'), ') - ', department) AS citation
FROM chunked
LATERAL VIEW POSEXPLODE(chunks_array) AS chunk_index, chunk
WHERE LENGTH(TRIM(chunk)) > 0
""")

count = spark.table(f"{catalog}.{schema}.gold_circular_chunks").count()
print(f"gold_circular_chunks: {count:,} chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold Schemes

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_schemes AS
SELECT
    scheme_id,
    scheme_name,
    ministry,
    description,
    eligibility_criteria,
    benefits,
    target_group,
    income_limit,
    age_min,
    age_max,
    gender,
    occupation,
    state,
    url,
    CONCAT(scheme_name, ': ', LEFT(COALESCE(benefits, description, ''), 200)) AS plain_summary,
    ingested_at
FROM {catalog}.{schema}.silver_schemes
""")

count = spark.table(f"{catalog}.{schema}.gold_schemes").count()
print(f"gold_schemes: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

tables = [
    "silver_transactions", "silver_circulars", "silver_schemes",
    "gold_transactions", "gold_fraud_alerts", "gold_circular_chunks", "gold_schemes"
]
print("=== Transform Results ===")
for t in tables:
    try:
        count = spark.table(f"{catalog}.{schema}.{t}").count()
        print(f"  {t}: {count:,} rows")
    except Exception as e:
        print(f"  {t}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transforms Complete
# MAGIC
# MAGIC **Next step:** Run `03-fraud-detection.py` for ML pipeline.
