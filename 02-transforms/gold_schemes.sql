-- Databricks DLT SQL
-- Gold Schemes: Business-ready government financial inclusion schemes
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH MATERIALIZED VIEW gold_schemes
COMMENT 'Business-ready government financial inclusion schemes with structured eligibility criteria for programmatic matching.'
AS
SELECT
    scheme_id,
    scheme_name,
    ministry,
    description,
    eligibility_criteria,
    benefits,
    target_group,
    income_limit,
    age_min,
    age_max,
    gender,
    occupation,
    state,
    url,
    eligibility_json,

    -- Plain-language summary (concatenated from available fields)
    CONCAT(scheme_name, ': ', LEFT(COALESCE(benefits, description, ''), 200)) AS plain_summary,

    ingested_at

FROM silver_schemes;
