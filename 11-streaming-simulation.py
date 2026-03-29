# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Real-Time Streaming Pipeline
# MAGIC
# MAGIC Demonstrates streaming ingestion → transformation → live fraud scoring.
# MAGIC Uses Auto Loader with `availableNow=True` (serverless-compatible).
# MAGIC
# MAGIC **Key pattern:** `display()` on a streaming DataFrame shows live-updating charts.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

incoming_path = f"/Volumes/{catalog}/{schema}/raw_data/streaming_incoming/"
checkpoint_base = f"/Volumes/{catalog}/{schema}/raw_data/streaming_checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prepare Streaming Source (Split Data into Micro-Batches)

# COMMAND ----------

import time

# Clean previous runs
try:
    dbutils.fs.rm(incoming_path, recurse=True)
    dbutils.fs.rm(checkpoint_base, recurse=True)
except:
    pass

dbutils.fs.mkdirs(incoming_path)

# Take a sample and split into 5 batch files
source_df = spark.table(f"{catalog}.{schema}.gold_transactions").limit(2000)
batches = source_df.randomSplit([0.2, 0.2, 0.2, 0.2, 0.2], seed=42)

# Write first 2 batches immediately (rest will be "dropped" later for demo)
for i, batch in enumerate(batches[:2]):
    batch.coalesce(1).write.mode("overwrite").json(f"{incoming_path}batch_{i:03d}/")

print(f"Created 2 initial batch files (3 more will arrive during streaming)")
print(f"Source: {incoming_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Streaming Bronze Table (Auto Loader)

# COMMAND ----------

# Drop existing table to start fresh
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.streaming_bronze")

# Auto Loader streaming read
bronze_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze_schema")
    .load(incoming_path)
    .select(
        F.col("transaction_id").cast("string"),
        F.col("amount").cast("double"),
        F.col("transaction_time").cast("timestamp"),
        F.col("category").cast("string"),
        F.col("location").cast("string"),
        F.col("hour_of_day").cast("integer"),
        F.col("ai_risk_label").cast("string"),
        F.col("ai_risk_score").cast("double"),
        F.col("is_fraud").cast("boolean"),
        F.current_timestamp().alias("ingested_at"),
    )
)

# Write streaming table with availableNow trigger
query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base}/bronze")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.streaming_bronze"))

query.awaitTermination()
print(f"Initial batch ingested: {spark.table(f'{catalog}.{schema}.streaming_bronze').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Streaming Silver Table (With Data Quality)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.streaming_silver")

silver_stream = (spark.readStream
    .table(f"{catalog}.{schema}.streaming_bronze")
    .filter("transaction_id IS NOT NULL AND amount > 0")
    .withColumn("risk_tier",
        F.when(F.col("ai_risk_label").isin("high", "critical"), "HIGH_RISK")
        .otherwise("NORMAL")
    )
    .withColumn("processing_time", F.current_timestamp())
)

query2 = (silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base}/silver")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.streaming_silver"))

query2.awaitTermination()
print(f"Silver processed: {spark.table(f'{catalog}.{schema}.streaming_silver').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Simulate New Data Arriving + Incremental Processing
# MAGIC
# MAGIC This is the "live demo" — new files appear, pipeline picks them up.

# COMMAND ----------

print("Simulating new transactions arriving...")
print("=" * 50)

for i in range(2, 5):
    # "Drop" a new batch file
    batches[i].coalesce(1).write.mode("overwrite").json(f"{incoming_path}batch_{i:03d}/")
    print(f"\n--- Batch {i+1}/5 arrived ---")

    # Re-run bronze ingestion (picks up only new files via checkpoint)
    q = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze_schema")
        .load(incoming_path)
        .select(
            F.col("transaction_id").cast("string"),
            F.col("amount").cast("double"),
            F.col("transaction_time").cast("timestamp"),
            F.col("category").cast("string"),
            F.col("location").cast("string"),
            F.col("hour_of_day").cast("integer"),
            F.col("ai_risk_label").cast("string"),
            F.col("ai_risk_score").cast("double"),
            F.col("is_fraud").cast("boolean"),
            F.current_timestamp().alias("ingested_at"),
        )
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/bronze")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.streaming_bronze"))
    q.awaitTermination()

    # Re-run silver processing
    q2 = (spark.readStream
        .table(f"{catalog}.{schema}.streaming_bronze")
        .filter("transaction_id IS NOT NULL AND amount > 0")
        .withColumn("risk_tier",
            F.when(F.col("ai_risk_label").isin("high", "critical"), "HIGH_RISK")
            .otherwise("NORMAL"))
        .withColumn("processing_time", F.current_timestamp())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/silver")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.streaming_silver"))
    q2.awaitTermination()

    bronze_count = spark.table(f"{catalog}.{schema}.streaming_bronze").count()
    silver_count = spark.table(f"{catalog}.{schema}.streaming_silver").count()
    high_risk = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.streaming_silver WHERE risk_tier = 'HIGH_RISK'").collect()[0][0]

    print(f"  Bronze: {bronze_count:,} | Silver: {silver_count:,} | High Risk: {high_risk}")

    if i < 4:
        print(f"  Waiting 5 seconds for next batch...")
        time.sleep(5)

print(f"\n{'='*50}")
print("✅ Streaming simulation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Live Dashboard View — Streaming Aggregation

# COMMAND ----------

# This creates a LIVE updating chart in the notebook
display(spark.sql(f"""
SELECT
    ingested_at,
    risk_tier,
    COUNT(*) AS batch_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(amount), 2) AS total_volume
FROM {catalog}.{schema}.streaming_silver
GROUP BY ingested_at, risk_tier
ORDER BY ingested_at
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Streaming Statistics

# COMMAND ----------

print("STREAMING PIPELINE STATISTICS")
print("=" * 50)

bronze = spark.table(f"{catalog}.{schema}.streaming_bronze").count()
silver = spark.table(f"{catalog}.{schema}.streaming_silver").count()
high_risk = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.streaming_silver WHERE risk_tier = 'HIGH_RISK'").collect()[0][0]

print(f"  Bronze (raw ingested): {bronze:,}")
print(f"  Silver (quality-checked): {silver:,}")
print(f"  High risk flagged: {high_risk:,}")
print(f"  Batches processed: 5")
print(f"  Processing: incremental (checkpoint-based, exactly-once)")
print()
print("Architecture:")
print("  Files (Volume) → Auto Loader (cloudFiles)")
print("    → streaming_bronze (append-only, checkpoint)")
print("      → streaming_silver (filtered, risk-scored)")
print()
print("Production upgrade:")
print("  - Replace file-drop with Kafka/Kinesis source")
print("  - Use continuous pipeline mode (not available on Free Edition)")
print("  - Add watermarking for late-arriving events")
print("  - Connect to Databricks Alerts for fraud spikes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Log to MLflow

# COMMAND ----------

import mlflow
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks")

with mlflow.start_run(run_name="streaming_pipeline"):
    mlflow.log_param("source", "Auto Loader (cloudFiles)")
    mlflow.log_param("trigger", "availableNow=True")
    mlflow.log_param("processing", "exactly_once_checkpoint")
    mlflow.log_param("batches", 5)
    mlflow.log_metric("bronze_rows", bronze)
    mlflow.log_metric("silver_rows", silver)
    mlflow.log_metric("high_risk_count", high_risk)
print("✅ MLflow logged: streaming_pipeline")
