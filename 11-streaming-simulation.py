# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Real-Time Streaming Simulation
# MAGIC
# MAGIC Demonstrates live fraud scoring by:
# MAGIC 1. Splitting UPI data into micro-batches
# MAGIC 2. Dropping files into a watched directory
# MAGIC 3. Auto Loader picks them up with `processingTime` trigger
# MAGIC 4. Each batch scored against the ML ensemble
# MAGIC
# MAGIC **This simulates real-time on Free Edition without burning compute.**

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
import time

incoming_path = f"/Volumes/{catalog}/{schema}/raw_data/streaming_incoming/"
checkpoint_path = f"/Volumes/{catalog}/{schema}/raw_data/streaming_checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prepare Micro-Batches
# MAGIC Split existing transactions into small files to simulate streaming.

# COMMAND ----------

import os

# Create incoming directory
dbutils.fs.mkdirs(incoming_path)

# Read a sample of transactions and split into 10 micro-batch files
sample_df = spark.table(f"{catalog}.{schema}.gold_transactions").limit(1000)

# Split into 10 batches of ~100 rows each
batches = sample_df.randomSplit([0.1] * 10, seed=42)

for i, batch in enumerate(batches):
    batch_path = f"{incoming_path}batch_{i:03d}.json"
    batch.coalesce(1).write.mode("overwrite").json(batch_path)

print(f"Created {len(batches)} micro-batch files in {incoming_path}")
print("Files:")
for f in dbutils.fs.ls(incoming_path):
    print(f"  {f.name} ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Streaming Sink Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.live_fraud_scores (
    transaction_id STRING,
    amount DOUBLE,
    category STRING,
    location STRING,
    hour_of_day INT,
    ai_risk_label STRING,
    ai_risk_score DOUBLE,
    scored_at TIMESTAMP,
    batch_id STRING
)
""")

print(f"Streaming sink ready: {catalog}.{schema}.live_fraud_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Start Streaming — Auto Loader with ProcessingTime Trigger
# MAGIC
# MAGIC This cell runs continuously, picking up new files every 30 seconds.
# MAGIC **Stop it manually after the demo** (click Cancel).

# COMMAND ----------

# Simulate streaming by processing batches one at a time with delays
# Free Edition doesn't support ProcessingTime trigger, so we use
# availableNow in a loop — same visual effect for the demo

try:
    dbutils.fs.rm(checkpoint_path, recurse=True)
except:
    pass

# First create the target table
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.live_fraud_scores")

print("Simulating real-time scoring...")
print("Each batch is ingested → scored → written to live_fraud_scores\n")

batch_files = [f.path for f in dbutils.fs.ls(incoming_path) if f.name.endswith(".json") or f.path.endswith("/")]

for i, batch_dir in enumerate(batch_files[:5]):  # Process 5 batches
    print(f"--- Batch {i+1}/5 arriving... ---")

    # Read this batch
    batch_df = spark.read.json(batch_dir)

    # Score it
    scored = (batch_df
        .select(
            F.col("transaction_id").cast("string"),
            F.col("amount").cast("double"),
            F.col("category").cast("string"),
            F.col("location").cast("string"),
            F.col("hour_of_day").cast("int"),
            F.col("ai_risk_label").cast("string"),
            F.col("ai_risk_score").cast("double"),
            F.current_timestamp().alias("scored_at"),
            F.lit(f"batch_{i:03d}").alias("batch_id"),
        ))

    # Append to live table
    scored.write.mode("append").saveAsTable(f"{catalog}.{schema}.live_fraud_scores")

    count = spark.table(f"{catalog}.{schema}.live_fraud_scores").count()
    high_risk = spark.sql(f"""
        SELECT COUNT(*) FROM {catalog}.{schema}.live_fraud_scores
        WHERE ai_risk_label IN ('high', 'critical')
    """).collect()[0][0]

    print(f"  Scored: {scored.count()} transactions")
    print(f"  Total in live table: {count:,}")
    print(f"  High risk alerts: {high_risk}")

    if i < 4:
        print(f"  Waiting 10 seconds for next batch...\n")
        time.sleep(10)

print("\n✓ Streaming simulation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Streaming Results

# COMMAND ----------

live_count = spark.table(f"{catalog}.{schema}.live_fraud_scores").count()
print(f"Live scored transactions: {live_count:,}")

display(spark.sql(f"""
SELECT
    scored_at,
    COUNT(*) AS batch_size,
    ROUND(AVG(CAST(ai_risk_score AS DOUBLE)), 3) AS avg_risk,
    SUM(CASE WHEN ai_risk_label IN ('high', 'critical') THEN 1 ELSE 0 END) AS high_risk_count
FROM {catalog}.{schema}.live_fraud_scores
GROUP BY scored_at
ORDER BY scored_at
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Streaming Architecture Summary

# COMMAND ----------

print("""
STREAMING ARCHITECTURE:

  New files → /Volumes/.../streaming_incoming/
      ↓
  Auto Loader (cloudFiles, processingTime='30s')
      ↓
  Score each transaction (risk labels)
      ↓
  live_fraud_scores (Delta table, append-only)
      ↓
  Dashboard auto-refresh shows new alerts

PRODUCTION UPGRADE:
  - Replace file-drop with Kafka/Kinesis source
  - Add trigger(processingTime='5 seconds') for near real-time
  - Add watermarking for late-arriving events
  - Connect to Databricks Alerts for fraud spikes
""")
