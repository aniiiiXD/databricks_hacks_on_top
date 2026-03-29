# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: India's Financial Landscape
# MAGIC
# MAGIC Ingests macro-level Indian financial data to tell the story:
# MAGIC 1. **UPI Explosion** — Digital payment growth 2020-2025
# MAGIC 2. **Fraud Followed** — Bank-wise fraud losses
# MAGIC 3. **Victims Struggle** — RBI ombudsman complaint trends
# MAGIC 4. **Inclusion Gaps** — Jan Dhan penetration vs internet access by state
# MAGIC
# MAGIC This is the "why" behind BlackIce.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_path = f"/Volumes/{catalog}/{schema}/raw_data/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Digital Payment Growth (RBI Data)

# COMMAND ----------

payments_df = spark.read.json(f"{source_path}rbi_digital_payments.json")
payments_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.india_digital_payments")
print(f"Digital payments: {payments_df.count()} records")
display(payments_df.filter("payment_mode = 'UPI'").orderBy("date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bank-wise Fraud Statistics

# COMMAND ----------

fraud_stats = spark.read.json(f"{source_path}bank_fraud_stats.json")
fraud_stats.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.india_bank_fraud_stats")
print(f"Bank fraud stats: {fraud_stats.count()} records")
display(fraud_stats.orderBy("fiscal_year", "loss_crore"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RBI Ombudsman Complaints

# COMMAND ----------

complaints = spark.read.json(f"{source_path}rbi_complaints.json")
complaints.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.india_rbi_complaints")
print(f"Complaint records: {complaints.count()}")
display(complaints.filter("complaint_type = 'Mobile/Electronic Banking'").orderBy("fiscal_year"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Jan Dhan Financial Inclusion (PMJDY)

# COMMAND ----------

pmjdy = spark.read.json(f"{source_path}pmjdy_statewise.json")
pmjdy.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.india_pmjdy_statewise")
print(f"PMJDY state data: {pmjdy.count()} states/UTs")
display(pmjdy.orderBy("total_beneficiaries_lakh"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Internet Penetration by State

# COMMAND ----------

internet = spark.read.json(f"{source_path}internet_penetration.json")
internet.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.india_internet_penetration")
print(f"Internet data: {internet.count()} states/UTs")
display(internet.orderBy("total_subscribers_per_100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The Story: Cross-Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPI Growth vs Fraud Growth

# COMMAND ----------

from pyspark.sql import functions as F

display(spark.sql(f"""
SELECT
    p.fiscal_year,
    ROUND(SUM(p.volume_millions), 0) AS upi_volume_millions,
    ROUND(SUM(p.value_crore), 0) AS upi_value_crore,
    ROUND(SUM(f.fraud_count), 0) AS total_fraud_cases,
    ROUND(SUM(f.loss_crore), 2) AS total_loss_crore,
    ROUND(SUM(f.recovered_crore), 2) AS total_recovered_crore,
    ROUND(SUM(f.recovered_crore) * 100.0 / NULLIF(SUM(f.loss_crore), 0), 1) AS recovery_rate_pct
FROM (
    SELECT
        CASE
            WHEN date >= '2020-04-01' AND date < '2021-04-01' THEN '2020-21'
            WHEN date >= '2021-04-01' AND date < '2022-04-01' THEN '2021-22'
            WHEN date >= '2022-04-01' AND date < '2023-04-01' THEN '2022-23'
            WHEN date >= '2023-04-01' AND date < '2024-04-01' THEN '2023-24'
            WHEN date >= '2024-04-01' AND date < '2025-04-01' THEN '2024-25'
        END AS fiscal_year,
        volume_millions, value_crore
    FROM {catalog}.{schema}.india_digital_payments
    WHERE payment_mode = 'UPI'
) p
LEFT JOIN (
    SELECT fiscal_year, SUM(fraud_count) AS fraud_count, SUM(loss_crore) AS loss_crore, SUM(recovered_crore) AS recovered_crore
    FROM {catalog}.{schema}.india_bank_fraud_stats
    GROUP BY fiscal_year
) f ON p.fiscal_year = f.fiscal_year
WHERE p.fiscal_year IS NOT NULL
GROUP BY p.fiscal_year
ORDER BY p.fiscal_year
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### State Vulnerability Index: Fraud Risk × Digital Divide × Inclusion Gap

# COMMAND ----------

vulnerability = spark.sql(f"""
SELECT
    sf.state,
    sf.fraud_rate_pct,
    sf.total_transactions,
    sf.fraud_count,
    COALESCE(ip.total_subscribers_per_100, 0) AS internet_per_100,
    COALESCE(ip.smartphone_penetration_pct, 0) AS smartphone_pct,
    COALESCE(jd.total_beneficiaries_lakh, 0) AS jan_dhan_beneficiaries_lakh,
    -- Vulnerability score: high fraud + low internet + low jan dhan = most vulnerable
    ROUND(
        (sf.fraud_rate_pct / NULLIF((SELECT MAX(fraud_rate_pct) FROM {catalog}.{schema}.state_fraud_analysis), 0)) * 0.4 +
        (1 - COALESCE(ip.total_subscribers_per_100, 0) / NULLIF((SELECT MAX(total_subscribers_per_100) FROM {catalog}.{schema}.india_internet_penetration), 0)) * 0.3 +
        (1 - COALESCE(jd.total_beneficiaries_lakh, 0) / NULLIF((SELECT MAX(total_beneficiaries_lakh) FROM {catalog}.{schema}.india_pmjdy_statewise), 0)) * 0.3
    , 3) AS vulnerability_index
FROM {catalog}.{schema}.state_fraud_analysis sf
LEFT JOIN {catalog}.{schema}.india_internet_penetration ip ON LOWER(sf.state) = LOWER(ip.state)
LEFT JOIN {catalog}.{schema}.india_pmjdy_statewise jd ON LOWER(sf.state) = LOWER(jd.state)
ORDER BY vulnerability_index DESC
""")

vulnerability.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.state_vulnerability_index")
print("State Vulnerability Index (higher = more vulnerable):")
display(vulnerability)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complaint Surge: Mobile Banking Complaints Over Time

# COMMAND ----------

display(spark.sql(f"""
SELECT fiscal_year, complaint_type, complaints_received
FROM {catalog}.{schema}.india_rbi_complaints
WHERE complaint_type IN ('Mobile/Electronic Banking', 'ATM/Debit Cards', 'Credit Cards')
ORDER BY fiscal_year, complaints_received DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tag New Tables

# COMMAND ----------

new_tables = {
    "india_digital_payments": {"domain": "ecosystem", "tier": "context", "source": "rbi"},
    "india_bank_fraud_stats": {"domain": "fraud", "tier": "context", "source": "lok_sabha"},
    "india_rbi_complaints": {"domain": "consumer", "tier": "context", "source": "rbi_ombudsman"},
    "india_pmjdy_statewise": {"domain": "inclusion", "tier": "context", "source": "pmjdy.gov.in"},
    "india_internet_penetration": {"domain": "inclusion", "tier": "context", "source": "trai"},
    "state_vulnerability_index": {"domain": "analytics", "tier": "platinum", "source": "computed"},
}

for table, tag_dict in new_tables.items():
    tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tag_dict.items()])
    try:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} SET TAGS ({tag_str})")
    except:
        pass

print("Tags applied to new tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **New tables created:**
# MAGIC - `india_digital_payments` — UPI/IMPS/NEFT/RTGS growth data (432 records)
# MAGIC - `india_bank_fraud_stats` — Bank-wise fraud losses (75 records)
# MAGIC - `india_rbi_complaints` — Ombudsman complaint trends (48 records)
# MAGIC - `india_pmjdy_statewise` — Jan Dhan accounts by state (36 states)
# MAGIC - `india_internet_penetration` — Digital divide by state (36 states)
# MAGIC - `state_vulnerability_index` — Composite: fraud risk × digital divide × inclusion gap
# MAGIC
# MAGIC **The story:** UPI exploded → fraud followed → digitally naive states suffer most → BlackIce helps.
