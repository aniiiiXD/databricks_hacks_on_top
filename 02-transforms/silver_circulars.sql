-- Databricks DLT SQL
-- Silver Circulars: Cleaned RBI circulars with AI topic classification
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH STREAMING TABLE silver_circulars (
  CONSTRAINT valid_circular_id EXPECT (circular_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_text EXPECT (full_text IS NOT NULL AND LENGTH(TRIM(full_text)) > 50) ON VIOLATION DROP ROW,
  CONSTRAINT has_title EXPECT (title IS NOT NULL AND LENGTH(TRIM(title)) > 0) ON VIOLATION DROP ROW
)
TBLPROPERTIES('pipelines.channel' = 'PREVIEW')
COMMENT 'Cleaned RBI circulars with AI-powered topic classification. Quality-checked for non-empty meaningful text.'
AS
SELECT DISTINCT
    circular_id,
    TRIM(title) AS title,
    date,
    circular_date,
    COALESCE(department, 'Unknown') AS department,
    COALESCE(circular_category, 'Uncategorized') AS circular_category,
    TRIM(full_text) AS full_text,
    url,
    LENGTH(TRIM(full_text)) AS text_length,

    -- Word count for analytics
    SIZE(SPLIT(TRIM(full_text), '\\s+')) AS word_count,

    -- Topic classification via keyword matching (ai_query not available on Free Edition)
    CASE
      WHEN LOWER(COALESCE(title, '') || ' ' || SUBSTRING(COALESCE(full_text, ''), 1, 500))
           LIKE '%fraud%' OR LOWER(title) LIKE '%cyber%' OR LOWER(title) LIKE '%unauthori%' THEN 'fraud_prevention'
      WHEN LOWER(title) LIKE '%upi%' OR LOWER(title) LIKE '%digital payment%' OR LOWER(title) LIKE '%payment aggregat%'
           OR LOWER(title) LIKE '%ppi%' OR LOWER(title) LIKE '%mobile banking%' THEN 'digital_payments'
      WHEN LOWER(title) LIKE '%kyc%' OR LOWER(title) LIKE '%aml%' OR LOWER(title) LIKE '%money laundering%'
           OR LOWER(title) LIKE '%uapa%' OR LOWER(title) LIKE '%sanction%' THEN 'compliance'
      WHEN LOWER(title) LIKE '%lend%' OR LOWER(title) LIKE '%credit%' OR LOWER(title) LIKE '%loan%'
           OR LOWER(title) LIKE '%nbfc%' OR LOWER(title) LIKE '%interest rate%' THEN 'lending_regulations'
      WHEN LOWER(title) LIKE '%consumer%' OR LOWER(title) LIKE '%grievance%' OR LOWER(title) LIKE '%ombudsman%' THEN 'consumer_protection'
      WHEN LOWER(title) LIKE '%rtgs%' OR LOWER(title) LIKE '%neft%' OR LOWER(title) LIKE '%nach%' THEN 'payment_systems'
      ELSE 'banking_operations'
    END AS topic_label,

    ingested_at

FROM STREAM(bronze_circulars)
WHERE
    circular_id IS NOT NULL
    AND full_text IS NOT NULL
    AND LENGTH(TRIM(full_text)) > 50;
