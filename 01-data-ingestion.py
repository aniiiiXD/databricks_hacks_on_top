# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Data Ingestion Pipeline
# MAGIC
# MAGIC Ingests 3 data sources into Bronze layer via Auto Loader:
# MAGIC 1. **Synthetic UPI Transactions** — streaming JSON ingestion
# MAGIC 2. **RBI Circulars** — regulatory text documents
# MAGIC 3. **Government Schemes** — financial inclusion programs
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

# --- UPI Transactions ---
txn_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/txn_schema")
    .option("pathGlobFilter", "*transaction*")
    .load(source_path))

# Flatten and derive features during ingestion
bronze_txns = (txn_stream
    .select(
        F.col("txn_id").cast("string").alias("transaction_id"),
        F.col("amount").cast("double"),
        F.coalesce(
            F.to_timestamp("timestamp"),
            F.from_unixtime(F.col("timestamp").cast("long"))
        ).alias("transaction_time"),
        F.col("sender_id").cast("string"),
        F.col("receiver_id").cast("string"),
        F.col("sender_name").cast("string"),
        F.col("receiver_name").cast("string"),
        F.col("category").cast("string"),
        F.col("merchant_id").cast("string"),
        F.col("merchant_name").cast("string"),
        F.col("payment_mode").cast("string"),
        F.col("device_type").cast("string"),
        F.col("location").cast("string"),
        F.col("is_fraud").cast("boolean"),
        # Derived time features
        F.hour(F.coalesce(
            F.to_timestamp("timestamp"),
            F.from_unixtime(F.col("timestamp").cast("long"))
        )).alias("hour_of_day"),
        F.dayofweek(F.coalesce(
            F.to_timestamp("timestamp"),
            F.from_unixtime(F.col("timestamp").cast("long"))
        )).alias("day_of_week"),
        F.dayofmonth(F.coalesce(
            F.to_timestamp("timestamp"),
            F.from_unixtime(F.col("timestamp").cast("long"))
        )).alias("day_of_month"),
        F.month(F.coalesce(
            F.to_timestamp("timestamp"),
            F.from_unixtime(F.col("timestamp").cast("long"))
        )).alias("month"),
        # Ingestion metadata
        F.current_timestamp().alias("ingested_at"),
        F.input_file_name().alias("source_file"),
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

# RBI Circulars — may be flat JSON or nested
circular_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/circular_schema")
    .option("pathGlobFilter", "*circular*")
    .load(source_path))

bronze_circulars = (circular_stream
    .select(
        F.col("circular_id").cast("string"),
        F.col("title").cast("string"),
        F.col("date").cast("string"),
        F.to_date(F.col("date"), "yyyy-MM-dd").alias("circular_date"),
        F.col("department").cast("string"),
        F.col("category").cast("string").alias("circular_category"),
        F.col("full_text").cast("string"),
        F.col("url").cast("string"),
        F.length(F.col("full_text")).alias("text_length"),
        F.current_timestamp().alias("ingested_at"),
        F.input_file_name().alias("source_file"),
    ))

(bronze_circulars.writeStream
    .option("checkpointLocation", f"{checkpoint_base}/bronze_circulars")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_circulars"))

print("RBI Circulars ingestion started...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Government Schemes

# COMMAND ----------

# Gov Schemes — likely CSV or JSON, use direct read (smaller dataset)
# Try JSON first, fall back to CSV
try:
    schemes_df = spark.read.json(f"{source_path}*scheme*")
    print("Loaded schemes from JSON")
except:
    try:
        schemes_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{source_path}*scheme*")
        print("Loaded schemes from CSV")
    except:
        print("WARNING: No scheme data found. Create an empty table for now.")
        schemes_schema = StructType([
            StructField("scheme_id", StringType()),
            StructField("scheme_name", StringType()),
            StructField("ministry", StringType()),
            StructField("description", StringType()),
            StructField("eligibility_criteria", StringType()),
            StructField("benefits", StringType()),
            StructField("target_group", StringType()),
            StructField("income_limit", DoubleType()),
            StructField("age_min", IntegerType()),
            StructField("age_max", IntegerType()),
            StructField("gender", StringType()),
            StructField("occupation", StringType()),
            StructField("state", StringType()),
            StructField("url", StringType()),
        ])
        schemes_df = spark.createDataFrame([], schemes_schema)

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
    f"COMMENT ON TABLE {catalog}.{schema}.bronze_transactions IS 'Raw synthetic UPI transaction data ingested via Auto Loader. Source: hackathon synthetic dataset.'",
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
    "is_fraud": "Ground truth fraud label from synthetic dataset (for model evaluation)",
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
