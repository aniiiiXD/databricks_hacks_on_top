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

    -- AI-generated plain-language summary for display
    ai_query(
      'databricks-meta-llama-3-1-70b-instruct',
      CONCAT(
        'Summarize this Indian government scheme in 2 simple sentences that a rural citizen can understand. Include who is eligible and what benefit they get.',
        '\n\nScheme: ', scheme_name,
        '\nMinistry: ', ministry,
        '\nDescription: ', COALESCE(description, ''),
        '\nEligibility: ', COALESCE(eligibility_criteria, ''),
        '\nBenefits: ', COALESCE(benefits, '')
      ),
      'STRING'
    ) AS plain_summary,

    ingested_at

FROM silver_schemes;
