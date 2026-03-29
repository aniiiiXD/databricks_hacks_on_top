# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Data Ingestion Pipeline
# MAGIC
# MAGIC Ingests 3 data sources into Bronze layer via Auto Loader:
# MAGIC 1. **UPI Transactions** — Kaggle CSV (250K real transactions with fraud labels)
# MAGIC 2. **RBI Circulars** — scraped from rbi.org.in (JSON Lines)
# MAGIC 3. **Government Schemes** — from myscheme.gov.in via OpenNyAI (JSON Lines)
# MAGIC
# MAGIC **Pattern:** Auto Loader (`cloudFiles`) with schema inference, exactly-once semantics,
# MAGIC and `trigger(availableNow=True)` for batch-style streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_path = f"/Volumes/{catalog}/{schema}/raw_data/"
checkpoint_base = f"{source_path}_checkpoints"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Source: {source_path}")

# Clean stale checkpoints (set widget to "yes" if you need to re-ingest)
dbutils.widgets.dropdown("clear_checkpoints", "no", ["yes", "no"], "Clear Checkpoints")
if dbutils.widgets.get("clear_checkpoints") == "yes":
    dbutils.fs.rm(checkpoint_base, recurse=True)
    print("Checkpoints cleared.")
else:
    print("Checkpoints preserved (set clear_checkpoints=yes to reset).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Catalog & Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")

print(f"Workspace ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest UPI Transactions (Auto Loader)
# MAGIC
# MAGIC Streaming ingestion with schema inference, type detection, and derived time features.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- UPI Transactions (Kaggle CSV: upi_transactions_2024.csv) ---
# Column mapping from Kaggle schema:
#   "transaction id" -> transaction_id
#   "timestamp" -> transaction_time
#   "transaction type" -> transaction_type (P2P, P2M, Bill Payment, Recharge)
#   "merchant_category" -> category
#   "amount (INR)" -> amount
#   "transaction_status" -> transaction_status (SUCCESS/FAILED)
#   "sender_age_group" / "receiver_age_group" -> age groups
#   "sender_state" -> location
#   "sender_bank" / "receiver_bank" -> banks
#   "device_type" -> device_type (Android/iOS/Web)
#   "network_type" -> network_type (4G/5G/WiFi)
#   "fraud_flag" -> is_fraud (0/1)
#   "hour_of_day", "day_of_week", "is_weekend" -> pre-computed

txn_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/txn_schema")
    .option("pathGlobFilter", "*transaction*")
    .load(source_path))

# --- Fake Fraud Rings (JSON) ---
synth_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/synth_schema")
    .option("pathGlobFilter", "*synthetic_fraud_rings*")
    .load(source_path))

combined_stream = txn_stream.unionByName(synth_stream, allowMissingColumns=True)

# Map Kaggle columns to our schema
bronze_txns = (combined_stream
    .select(
        F.col("`transaction id`").cast("string").alias("transaction_id"),
        F.col("`amount (INR)`").cast("double").alias("amount"),
        F.to_timestamp("timestamp").alias("transaction_time"),
        # Use explicit IDs from synthetic fraud rings, otherwise hash-generate from transaction ID
        F.coalesce(F.col("explicit_sender_id"), F.concat(F.lit("sender_"), (F.abs(F.hash(F.col("`transaction id`"))) % 5000).cast("string"), F.lit("@upi"))).alias("sender_id"),
        F.coalesce(F.col("explicit_receiver_id"), F.concat(F.lit("recv_"), (F.abs(F.hash(F.reverse(F.col("`transaction id`")))) % 3000).cast("string"), F.lit("@upi"))).alias("receiver_id"),
        F.lit("").alias("sender_name"),
        F.lit("").alias("receiver_name"),
        F.col("merchant_category").cast("string").alias("category"),
        F.concat(F.lit("MERCH_"), F.abs(F.hash(F.col("merchant_category"))) % 500).cast("string").alias("merchant_id"),
        F.col("merchant_category").cast("string").alias("merchant_name"),
        F.col("`transaction type`").cast("string").alias("payment_mode"),
        F.col("device_type").cast("string"),
        F.col("sender_state").cast("string").alias("location"),
        F.col("fraud_flag").cast("boolean").alias("is_fraud"),
        F.col("sender_bank").cast("string"),
        F.col("receiver_bank").cast("string"),
        F.col("sender_age_group").cast("string"),
        F.col("receiver_age_group").cast("string"),
        F.col("network_type").cast("string"),
        F.col("`transaction_status`").cast("string").alias("transaction_status"),
        # Time features (pre-computed in Kaggle dataset)
        F.col("hour_of_day").cast("integer"),
        F.col("day_of_week").cast("string"),
        F.col("is_weekend").cast("boolean"),
        # Additional derived features
        F.dayofmonth(F.to_timestamp("timestamp")).alias("day_of_month"),
        F.month(F.to_timestamp("timestamp")).alias("month"),
        # Ingestion metadata
        F.current_timestamp().alias("ingested_at"),
        F.col("_metadata.file_path").alias("source_file"),
    ))

# Write with exactly-once semantics
(bronze_txns.writeStream
    .option("checkpointLocation", f"{checkpoint_base}/bronze_transactions")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_transactions"))

print("UPI Transactions ingestion started...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest RBI Circulars

# COMMAND ----------

# RBI Circulars — direct read (small file, 80 rows, no need for Auto Loader)
# Try JSON Lines first (one object per line), fall back to multiLine JSON array
circulars_df = spark.read.json(f"{source_path}rbi_circulars.json")
# If we only get _corrupt_record, the file is a JSON array — re-read with multiLine
if "_corrupt_record" in circulars_df.columns and len(circulars_df.columns) <= 2:
    print("JSON Lines parse failed, trying multiLine JSON array...")
    circulars_df = spark.read.option("multiLine", "true").json(f"{source_path}rbi_circulars.json")

bronze_circulars = (circulars_df
    .select(
        F.col("circular_id").cast("string"),
        F.col("title").cast("string"),
        F.col("date").cast("string"),
        # Date format from scraper: "March 27, 2026" or empty string
        F.coalesce(
            F.try_to_timestamp(F.col("date"), F.lit("MMMM d, yyyy")).cast("date"),
            F.try_to_timestamp(F.col("date"), F.lit("MMMM dd, yyyy")).cast("date"),
            F.try_to_timestamp(F.col("date"), F.lit("yyyy-MM-dd")).cast("date"),
        ).alias("circular_date"),
        F.col("department").cast("string"),
        F.col("category").cast("string").alias("circular_category"),
        F.col("full_text").cast("string"),
        F.col("url").cast("string"),
        F.length(F.col("full_text")).alias("text_length"),
        F.current_timestamp().alias("ingested_at"),
    ))

bronze_circulars.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_circulars")
print(f"RBI Circulars ingested: {bronze_circulars.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Government Schemes

# COMMAND ----------

# Gov Schemes — direct read (small file, 170 rows)
schemes_df = spark.read.json(f"{source_path}gov_schemes.json")
if "_corrupt_record" in schemes_df.columns and len(schemes_df.columns) <= 2:
    print("JSON Lines parse failed, trying multiLine JSON array...")
    schemes_df = spark.read.option("multiLine", "true").json(f"{source_path}gov_schemes.json")

bronze_schemes = (schemes_df
    .select(
        F.coalesce(F.col("scheme_id"), F.monotonically_increasing_id().cast("string")).alias("scheme_id"),
        F.col("scheme_name").cast("string"),
        F.col("ministry").cast("string"),
        F.col("description").cast("string"),
        F.col("eligibility_criteria").cast("string"),
        F.col("benefits").cast("string"),
        F.col("target_group").cast("string"),
        F.col("income_limit").cast("double"),
        F.col("age_min").cast("integer"),
        F.col("age_max").cast("integer"),
        F.col("gender").cast("string"),
        F.col("occupation").cast("string"),
        F.col("state").cast("string"),
        F.col("url").cast("string"),
        F.current_timestamp().alias("ingested_at"),
    ))

bronze_schemes.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_schemes")
print("Government Schemes ingested.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Governance Setup — PK/FK Constraints, CDF, Documentation

# COMMAND ----------

# --- Primary Key Constraints ---
governance_sql = [
    # Transactions PK
    f"ALTER TABLE {catalog}.{schema}.bronze_transactions ALTER COLUMN transaction_id SET NOT NULL",
    f"ALTER TABLE {catalog}.{schema}.bronze_transactions ADD CONSTRAINT txn_pk PRIMARY KEY (transaction_id)",

    # Circulars PK
    f"ALTER TABLE {catalog}.{schema}.bronze_circulars ALTER COLUMN circular_id SET NOT NULL",
    f"ALTER TABLE {catalog}.{schema}.bronze_circulars ADD CONSTRAINT circular_pk PRIMARY KEY (circular_id)",

    # Schemes PK
    f"ALTER TABLE {catalog}.{schema}.bronze_schemes ALTER COLUMN scheme_id SET NOT NULL",
    f"ALTER TABLE {catalog}.{schema}.bronze_schemes ADD CONSTRAINT scheme_pk PRIMARY KEY (scheme_id)",

    # Enable Change Data Feed for incremental downstream processing
    f"ALTER TABLE {catalog}.{schema}.bronze_transactions SET TBLPROPERTIES (delta.enableChangeDataFeed = true)",
    f"ALTER TABLE {catalog}.{schema}.bronze_circulars SET TBLPROPERTIES (delta.enableChangeDataFeed = true)",

    # Table comments
    f"COMMENT ON TABLE {catalog}.{schema}.bronze_transactions IS 'UPI transaction data from Kaggle (250K rows, 0.2%% fraud rate). Source: kaggle.com/datasets/skullagos5246/upi-transactions-2024-dataset'",
    f"COMMENT ON TABLE {catalog}.{schema}.bronze_circulars IS 'Raw RBI circular texts ingested via Auto Loader. Source: rbi.org.in public circulars.'",
    f"COMMENT ON TABLE {catalog}.{schema}.bronze_schemes IS 'Raw government financial inclusion scheme data. Source: data.gov.in / myscheme.gov.in.'",
]

# Column-level documentation for transactions
txn_col_comments = {
    "transaction_id": "Unique UPI transaction identifier",
    "amount": "Transaction amount in Indian Rupees (INR)",
    "transaction_time": "Timestamp of the transaction",
    "sender_id": "UPI ID or account identifier of the sender",
    "receiver_id": "UPI ID or account identifier of the receiver",
    "category": "Merchant category code or transaction type",
    "merchant_id": "Unique merchant identifier",
    "payment_mode": "UPI payment mode (QR, intent, collect, etc.)",
    "device_type": "Device type used for transaction (mobile, web, etc.)",
    "location": "City or region where the transaction occurred",
    "is_fraud": "Ground truth fraud label from Kaggle dataset (for model evaluation)",
    "hour_of_day": "Hour of transaction (0-23), derived from timestamp",
    "day_of_week": "Day of week (1=Sunday, 7=Saturday), derived from timestamp",
    "ingested_at": "Timestamp when the record was ingested into Bronze layer",
    "source_file": "Source file path from Auto Loader ingestion",
}

for col, comment in txn_col_comments.items():
    governance_sql.append(
        f"ALTER TABLE {catalog}.{schema}.bronze_transactions ALTER COLUMN {col} COMMENT '{comment}'"
    )

# Execute all governance SQL
for sql_stmt in governance_sql:
    try:
        spark.sql(sql_stmt)
    except Exception as e:
        # Skip if constraint already exists
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            pass
        else:
            print(f"Warning: {sql_stmt[:80]}... -> {e}")

print("Governance setup complete: PKs, CDF, documentation applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Ingested Data

# COMMAND ----------

tables = ["bronze_transactions", "bronze_circulars", "bronze_schemes"]
for t in tables:
    count = spark.table(f"{catalog}.{schema}.{t}").count()
    print(f"{t}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample: Bronze Transactions

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_transactions").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample: Bronze Circulars

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_circulars").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample: Bronze Schemes

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_schemes").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Complete
# MAGIC
# MAGIC **Next step:** Run the DLT pipeline (`02-transforms/`) to create Silver and Gold layers.
