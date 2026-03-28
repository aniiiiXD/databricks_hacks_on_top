-- Databricks DLT SQL
-- Gold Transactions: Business-ready UPI transactions with anomaly flags
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH MATERIALIZED VIEW gold_transactions (
  CONSTRAINT valid_risk_label EXPECT (ai_risk_label IN ('low', 'medium', 'high', 'critical')) ON VIOLATION DROP ROW
)
COMMENT 'Business-ready UPI transactions with AI risk classification and anomaly flags. Filtered for valid risk labels only.'
AS
SELECT
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
    hour_of_day,
    day_of_week,
    day_of_month,
    month,
    is_weekend,
    time_slot,
    amount_bucket,
    ai_risk_label,

    -- Derived anomaly flag from AI classification
    CASE
      WHEN ai_risk_label IN ('high', 'critical') THEN true
      ELSE false
    END AS anomaly_flag,

    -- Numeric risk score for ML ensemble blending
    CASE
      WHEN ai_risk_label = 'critical' THEN 1.0
      WHEN ai_risk_label = 'high' THEN 0.75
      WHEN ai_risk_label = 'medium' THEN 0.4
      WHEN ai_risk_label = 'low' THEN 0.1
      ELSE 0.0
    END AS ai_risk_score,

    -- Date for partitioning / dashboard filters
    DATE(transaction_time) AS transaction_date,

    ingested_at

FROM silver_transactions;
