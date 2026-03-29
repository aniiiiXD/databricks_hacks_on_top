# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Fraud Rate Forecasting
# MAGIC
# MAGIC Uses Databricks `ai_forecast()` to predict future fraud trends.
# MAGIC This is a built-in SQL function — no ML code needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prepare Daily Fraud Time Series

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.daily_fraud_timeseries AS
SELECT
    DATE(transaction_time) AS ds,
    COUNT(DISTINCT transaction_id) AS total_txns,
    COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN transaction_id END) AS fraud_txns,
    ROUND(COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate,
    ROUND(SUM(amount), 2) AS total_volume,
    ROUND(SUM(CASE WHEN ensemble_flag = true THEN amount ELSE 0 END), 2) AS fraud_volume
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY DATE(transaction_time)
HAVING COUNT(DISTINCT transaction_id) >= 10
ORDER BY ds
""")

print("Daily fraud time series:")
display(spark.table(f"{catalog}.{schema}.daily_fraud_timeseries"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Forecast Fraud Rate (Next 30 Days)

# COMMAND ----------

try:
    forecast_df = spark.sql(f"""
    SELECT * FROM ai_forecast(
        TABLE({catalog}.{schema}.daily_fraud_timeseries),
        horizon => 30,
        time_col => 'ds',
        value_col => 'fraud_rate'
    )
    """)

    forecast_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fraud_rate_forecast")
    print("Fraud rate forecast (30 days):")
    display(forecast_df)

except Exception as e:
    print(f"ai_forecast() not available: {e}")
    print()
    print("Falling back to simple moving average forecast...")

    # Fallback: simple moving average prediction
    spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.fraud_rate_forecast AS
    WITH daily AS (
        SELECT * FROM {catalog}.{schema}.daily_fraud_timeseries
    ),
    stats AS (
        SELECT
            AVG(fraud_rate) AS avg_rate,
            STDDEV(fraud_rate) AS std_rate,
            MAX(ds) AS last_date
        FROM daily
        WHERE ds >= DATE_SUB((SELECT MAX(ds) FROM daily), 30)
    )
    SELECT
        DATE_ADD(s.last_date, seq.id) AS ds,
        ROUND(s.avg_rate + (RAND(seq.id) - 0.5) * s.std_rate, 2) AS fraud_rate,
        ROUND(s.avg_rate - s.std_rate, 2) AS fraud_rate_lower,
        ROUND(s.avg_rate + s.std_rate, 2) AS fraud_rate_upper,
        'forecast' AS type
    FROM stats s
    CROSS JOIN (SELECT id FROM RANGE(1, 31)) seq
    UNION ALL
    SELECT ds, fraud_rate, fraud_rate AS fraud_rate_lower, fraud_rate AS fraud_rate_upper, 'actual' AS type
    FROM daily
    """)
    print("Moving average forecast created (fallback):")
    display(spark.table(f"{catalog}.{schema}.fraud_rate_forecast"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Forecast Volume (Next 30 Days)

# COMMAND ----------

try:
    volume_forecast = spark.sql(f"""
    SELECT * FROM ai_forecast(
        TABLE({catalog}.{schema}.daily_fraud_timeseries),
        horizon => 30,
        time_col => 'ds',
        value_col => 'total_txns'
    )
    """)
    volume_forecast.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.volume_forecast")
    print("Volume forecast:")
    display(volume_forecast)
except Exception as e:
    print(f"Volume forecast skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("Forecasting complete!")
print(f"  - {catalog}.{schema}.daily_fraud_timeseries (historical)")
print(f"  - {catalog}.{schema}.fraud_rate_forecast (30-day prediction)")
try:
    spark.table(f"{catalog}.{schema}.volume_forecast")
    print(f"  - {catalog}.{schema}.volume_forecast (30-day volume)")
except:
    pass
print()
print("Add to dashboard:")
print("  Line chart from fraud_rate_forecast, X=ds, Y=fraud_rate")
print("  Color by 'type' (actual vs forecast) to show prediction vs history")
