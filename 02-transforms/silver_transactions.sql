-- Databricks DLT SQL
-- Silver Transactions: Cleaned, deduplicated, with AI risk classification
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH STREAMING TABLE silver_transactions (
  -- Data quality constraints — violations are dropped and tracked in DLT quality metrics
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL AND LENGTH(TRIM(transaction_id)) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0 AND amount < 10000000) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (transaction_time IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sender EXPECT (sender_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_receiver EXPECT (receiver_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sender_not_receiver EXPECT (sender_id != receiver_id) ON VIOLATION DROP ROW
)
TBLPROPERTIES('pipelines.channel' = 'PREVIEW')
COMMENT 'Cleaned UPI transactions with inline AI risk classification. Deduplicated, typed, quality-checked via DLT EXPECT constraints.'
AS
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
    hour_of_day,
    day_of_week,
    day_of_month,
    month,

    -- Derived features for downstream ML
    CASE WHEN day_of_week IN (1, 7) THEN true ELSE false END AS is_weekend,
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

    -- Inline LLM risk classification via ai_query()
    -- This runs the LLM INSIDE the DLT pipeline — no Python needed
    CASE
      WHEN amount > 0 THEN
        LOWER(TRIM(ai_query(
          'databricks-meta-llama-3-1-70b-instruct',
          CONCAT(
            'You are a UPI fraud detection classifier. Classify this transaction risk as exactly one of: low, medium, high, critical. Output ONLY the single label word, nothing else.',
            '\n\nTransaction details:',
            '\n- Amount: ₹', CAST(amount AS STRING),
            '\n- Category: ', COALESCE(category, 'unknown'),
            '\n- Hour: ', CAST(hour_of_day AS STRING),
            '\n- Day of week: ', CAST(day_of_week AS STRING),
            '\n- Weekend: ', CASE WHEN day_of_week IN (1, 7) THEN 'yes' ELSE 'no' END,
            '\n- Payment mode: ', COALESCE(payment_mode, 'unknown'),
            '\n- Device: ', COALESCE(device_type, 'unknown')
          ),
          'STRING'
        )))
      ELSE 'low'
    END AS ai_risk_label,

    ingested_at,
    source_file

FROM STREAM(bronze_transactions)
WHERE
    -- Filter out clearly invalid records
    transaction_id IS NOT NULL
    AND amount > 0
    AND transaction_time IS NOT NULL;
