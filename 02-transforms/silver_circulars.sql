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

    -- AI topic classification — inline in the pipeline
    LOWER(TRIM(ai_query(
      'databricks-meta-llama-3-1-70b-instruct',
      CONCAT(
        'You are an RBI regulatory document classifier. Classify this circular into exactly one of these categories: fraud_prevention, monetary_policy, lending_regulations, digital_payments, consumer_protection, banking_operations, forex, investment, compliance. Output ONLY the label, nothing else.',
        '\n\nTitle: ', COALESCE(title, ''),
        '\nDepartment: ', COALESCE(department, ''),
        '\nFirst 500 chars: ', SUBSTRING(COALESCE(full_text, ''), 1, 500)
      ),
      'STRING'
    ))) AS topic_label,

    ingested_at

FROM STREAM(bronze_circulars)
WHERE
    circular_id IS NOT NULL
    AND full_text IS NOT NULL
    AND LENGTH(TRIM(full_text)) > 50;
