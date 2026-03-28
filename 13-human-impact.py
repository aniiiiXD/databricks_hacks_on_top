# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Human Impact — Fraud Victim Support & Financial Inclusion
# MAGIC
# MAGIC This is the "why we built this" notebook:
# MAGIC 1. **Fraud Recovery Guide** — What should a fraud victim do? Step-by-step, mapped to RBI rules
# MAGIC 2. **State-Level Analysis** — Which states are most vulnerable? Where are schemes least known?
# MAGIC 3. **Inclusion Gap Analysis** — Who is underserved? What schemes are they missing?

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fraud Recovery Guide (RBI-Mandated Process)

# COMMAND ----------

# Load fraud recovery guide data
recovery_data = [
    {
        "fraud_type": "QR Code Scam",
        "description": "Victim scans a fraudulent QR code thinking they will receive money, but money is debited instead.",
        "rbi_rule": "RBI Circular on Limiting Liability of Customers: Zero liability if reported within 3 days for third-party breach.",
        "recovery_steps": "1. Do NOT scan any more QR codes\n2. Call your bank's helpline immediately\n3. File complaint on cybercrime.gov.in within 3 days\n4. Write to bank's Nodal Officer if no response in 7 days\n5. Escalate to RBI Ombudsman if unresolved in 30 days",
        "report_to": "Bank helpline → cybercrime.gov.in → RBI Ombudsman",
        "time_limit_days": 3,
        "max_liability_inr": 0,
        "common_in_states": "Maharashtra, Karnataka, Delhi, Tamil Nadu"
    },
    {
        "fraud_type": "Phishing / Fake Bank Call",
        "description": "Fraudster calls pretending to be bank staff, asks for OTP/PIN/CVV. Victim shares credentials.",
        "rbi_rule": "Customer liable if they shared credentials voluntarily. But bank must prove customer negligence.",
        "recovery_steps": "1. Change UPI PIN immediately\n2. Block UPI on your number via bank app\n3. Call bank helpline and report\n4. File FIR at local police station\n5. File online complaint at cybercrime.gov.in\n6. Preserve all call records and messages as evidence",
        "report_to": "Bank → Police FIR → cybercrime.gov.in",
        "time_limit_days": 3,
        "max_liability_inr": 25000,
        "common_in_states": "Uttar Pradesh, Bihar, Rajasthan, Madhya Pradesh"
    },
    {
        "fraud_type": "SIM Swap Fraud",
        "description": "Fraudster duplicates victim's SIM card, receives OTPs, and drains bank account.",
        "rbi_rule": "Zero liability for unauthorized transactions if reported within 3 days AND customer did not share credentials.",
        "recovery_steps": "1. Contact telecom provider immediately to block duplicate SIM\n2. Call bank to freeze account\n3. File FIR with cyber cell\n4. File complaint with telecom provider\n5. File complaint on cybercrime.gov.in\n6. Apply for new SIM with original documents",
        "report_to": "Telecom provider → Bank → Cyber Cell FIR → cybercrime.gov.in",
        "time_limit_days": 3,
        "max_liability_inr": 0,
        "common_in_states": "Delhi, Maharashtra, Telangana, Karnataka"
    },
    {
        "fraud_type": "Fake UPI Collect Request",
        "description": "Fraudster sends a collect request disguised as a refund or payment. Victim approves thinking money will come in.",
        "rbi_rule": "Collect requests require explicit approval. UPI apps must show clear 'PAY' vs 'RECEIVE' distinction.",
        "recovery_steps": "1. Never approve collect requests from unknown sources\n2. Report to bank immediately\n3. File complaint on UPI app (in-app dispute)\n4. Escalate to NPCI if unresolved: npci.org.in\n5. File on cybercrime.gov.in if amount > ₹10,000",
        "report_to": "UPI App → Bank → NPCI → cybercrime.gov.in",
        "time_limit_days": 3,
        "max_liability_inr": 0,
        "common_in_states": "All India"
    },
    {
        "fraud_type": "Remote Access / Screen Sharing",
        "description": "Victim installs AnyDesk/TeamViewer at fraudster's request, giving full access to their phone.",
        "rbi_rule": "Banks must warn customers about remote access risks. Customer may have limited liability.",
        "recovery_steps": "1. Uninstall remote access app immediately\n2. Change ALL passwords (UPI, banking, email)\n3. Call bank to block all transactions\n4. File FIR with details of remote session\n5. File on cybercrime.gov.in\n6. Format phone if possible (remove any installed malware)",
        "report_to": "Bank → Police FIR → cybercrime.gov.in",
        "time_limit_days": 3,
        "max_liability_inr": 25000,
        "common_in_states": "All India"
    },
    {
        "fraud_type": "Fake Merchant / Refund Fraud",
        "description": "Victim buys from fake online store or is told they'll get a refund but money is debited.",
        "rbi_rule": "Payment aggregators must verify merchant identity. RBI mandates escrow for customer payments.",
        "recovery_steps": "1. Take screenshot of the transaction and merchant details\n2. Report within UPI app (transaction dispute)\n3. File chargeback request with bank\n4. Report merchant to payment aggregator\n5. File consumer complaint on consumerhelpline.gov.in\n6. File on cybercrime.gov.in",
        "report_to": "UPI App → Bank → Payment Aggregator → consumerhelpline.gov.in",
        "time_limit_days": 7,
        "max_liability_inr": 0,
        "common_in_states": "All India"
    },
]

recovery_df = spark.createDataFrame(pd.DataFrame(recovery_data))
recovery_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fraud_recovery_guide")
print(f"Fraud recovery guide: {len(recovery_data)} fraud types")
display(recovery_df.select("fraud_type", "time_limit_days", "max_liability_inr", "report_to"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. State-Level Fraud Analysis

# COMMAND ----------

state_fraud = spark.sql(f"""
SELECT
    location AS state,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_transaction_amount,
    ROUND(SUM(CASE WHEN ensemble_flag = true THEN amount ELSE 0 END), 2) AS fraud_amount_total,
    COUNT(DISTINCT sender_id) AS unique_senders,
    ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 4) AS avg_risk_score
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY location
ORDER BY fraud_rate_pct DESC
""")

state_fraud.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.state_fraud_analysis")
print("State-level fraud analysis:")
display(state_fraud)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Scheme Coverage Analysis — Who Is Underserved?

# COMMAND ----------

# Which occupations/demographics have the most schemes vs fewest?
scheme_coverage = spark.sql(f"""
SELECT
    occupation,
    gender,
    COUNT(*) AS scheme_count,
    ROUND(AVG(income_limit), 0) AS avg_income_limit,
    ROUND(AVG(age_max - age_min), 0) AS avg_age_range,
    COUNT(DISTINCT ministry) AS ministries_involved
FROM {catalog}.{schema}.gold_schemes
GROUP BY occupation, gender
ORDER BY scheme_count DESC
""")

print("Scheme Coverage by Occupation & Gender:")
display(scheme_coverage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cross-Analysis: Fraud Vulnerability vs Scheme Availability

# COMMAND ----------

# Combine: states with high fraud but low scheme coverage
cross_analysis = spark.sql(f"""
WITH state_schemes AS (
    SELECT
        state,
        COUNT(*) AS schemes_available
    FROM {catalog}.{schema}.gold_schemes
    WHERE state != 'All India'
    GROUP BY state
),
state_fraud AS (
    SELECT
        location AS state,
        COUNT(*) AS total_txns,
        SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) AS fraud_count,
        ROUND(SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
    FROM {catalog}.{schema}.gold_transactions_enriched
    GROUP BY location
)
SELECT
    f.state,
    f.total_txns,
    f.fraud_count,
    f.fraud_rate,
    COALESCE(s.schemes_available, 0) AS state_specific_schemes,
    CASE
        WHEN f.fraud_rate > 0.5 AND COALESCE(s.schemes_available, 0) < 3 THEN 'HIGH RISK - UNDERSERVED'
        WHEN f.fraud_rate > 0.5 THEN 'HIGH RISK'
        WHEN COALESCE(s.schemes_available, 0) < 3 THEN 'UNDERSERVED'
        ELSE 'ADEQUATE'
    END AS vulnerability_status
FROM state_fraud f
LEFT JOIN state_schemes s ON LOWER(f.state) = LOWER(s.state)
ORDER BY f.fraud_rate DESC
""")

cross_analysis.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.state_vulnerability_analysis")
print("State Vulnerability Analysis (Fraud Rate vs Scheme Coverage):")
display(cross_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Impact Metrics — What Digital-Artha Enables

# COMMAND ----------

# Calculate the potential impact of the platform
impact = spark.sql(f"""
SELECT
    -- Fraud Detection Impact
    (SELECT COUNT(*) FROM {catalog}.{schema}.gold_transactions_enriched) AS total_transactions_analyzed,
    (SELECT SUM(CASE WHEN ensemble_flag THEN 1 ELSE 0 END) FROM {catalog}.{schema}.gold_transactions_enriched) AS frauds_detected,
    (SELECT ROUND(SUM(CASE WHEN ensemble_flag THEN amount ELSE 0 END), 2) FROM {catalog}.{schema}.gold_transactions_enriched) AS fraud_amount_prevented_inr,

    -- Financial Inclusion Impact
    (SELECT COUNT(*) FROM {catalog}.{schema}.gold_schemes) AS schemes_indexed,
    (SELECT COUNT(DISTINCT ministry) FROM {catalog}.{schema}.gold_schemes) AS ministries_covered,
    (SELECT COUNT(DISTINCT state) FROM {catalog}.{schema}.gold_schemes WHERE state != 'All India') AS states_with_specific_schemes,

    -- Regulatory Intelligence
    (SELECT COUNT(*) FROM {catalog}.{schema}.gold_circular_chunks) AS rbi_circular_chunks_searchable,
    (SELECT COUNT(DISTINCT circular_id) FROM {catalog}.{schema}.gold_circular_chunks) AS rbi_circulars_indexed,

    -- Graph Intelligence
    (SELECT COUNT(*) FROM {catalog}.{schema}.platinum_fraud_rings) AS fraud_rings_detected,
    (SELECT SUM(size) FROM {catalog}.{schema}.platinum_fraud_rings) AS accounts_in_fraud_rings,
    (SELECT SUM(CASE WHEN is_hub THEN 1 ELSE 0 END) FROM {catalog}.{schema}.platinum_sender_profiles) AS money_mule_hubs_identified
""")

print("=" * 60)
print("DIGITAL-ARTHA IMPACT SUMMARY")
print("=" * 60)
display(impact)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `fraud_recovery_guide` — 6 fraud types with RBI-mandated recovery steps
# MAGIC - `state_fraud_analysis` — Fraud rate by Indian state
# MAGIC - `state_vulnerability_analysis` — Cross-reference fraud risk vs scheme coverage
# MAGIC - `data_quality_metrics` — Pipeline health (from notebook 12)
# MAGIC
# MAGIC **Next:** Expand dashboard with Fraud Rings + Human Impact pages
