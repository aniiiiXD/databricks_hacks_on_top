-- Databricks DLT SQL
-- Gold Fraud Alerts: High-risk flagged transactions for monitoring
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH MATERIALIZED VIEW gold_fraud_alerts
COMMENT 'Transactions flagged as high/critical risk by AI classification. Used for fraud monitoring dashboard and alert feeds.'
AS
SELECT
    transaction_id,
    amount,
    transaction_time,
    transaction_date,
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

    -- Alert metadata
    CONCAT(
      'Risk: ', UPPER(ai_risk_label),
      ' | Amount: ₹', FORMAT_NUMBER(amount, 2),
      ' | Time: ', time_slot,
      ' | Category: ', COALESCE(category, 'unknown'),
      ' | ', CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END
    ) AS alert_summary,

    CURRENT_TIMESTAMP() AS flagged_at

FROM gold_transactions
WHERE anomaly_flag = true
ORDER BY ai_risk_score DESC, amount DESC;
