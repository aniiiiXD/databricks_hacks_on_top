-- Databricks DLT SQL
-- Silver Schemes: Cleaned government financial inclusion schemes
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH MATERIALIZED VIEW silver_schemes (
  CONSTRAINT valid_scheme_id EXPECT (scheme_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_name EXPECT (scheme_name IS NOT NULL AND LENGTH(TRIM(scheme_name)) > 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned government financial inclusion schemes with normalized eligibility criteria.'
AS
SELECT
    scheme_id,
    TRIM(scheme_name) AS scheme_name,
    COALESCE(TRIM(ministry), 'Unknown Ministry') AS ministry,
    COALESCE(TRIM(description), '') AS description,
    COALESCE(TRIM(eligibility_criteria), '') AS eligibility_criteria,
    COALESCE(TRIM(benefits), '') AS benefits,
    COALESCE(TRIM(target_group), 'All') AS target_group,
    COALESCE(income_limit, 0) AS income_limit,
    COALESCE(age_min, 0) AS age_min,
    COALESCE(age_max, 999) AS age_max,
    COALESCE(LOWER(TRIM(gender)), 'all') AS gender,
    COALESCE(LOWER(TRIM(occupation)), 'all') AS occupation,
    COALESCE(TRIM(state), 'All India') AS state,
    url,

    -- Structured eligibility as JSON for programmatic matching
    TO_JSON(NAMED_STRUCT(
      'income_limit', COALESCE(income_limit, 0),
      'age_min', COALESCE(age_min, 0),
      'age_max', COALESCE(age_max, 999),
      'gender', COALESCE(LOWER(TRIM(gender)), 'all'),
      'occupation', COALESCE(LOWER(TRIM(occupation)), 'all'),
      'target_group', COALESCE(TRIM(target_group), 'All'),
      'state', COALESCE(TRIM(state), 'All India')
    )) AS eligibility_json,

    ingested_at

FROM bronze_schemes
WHERE scheme_id IS NOT NULL
  AND scheme_name IS NOT NULL;
