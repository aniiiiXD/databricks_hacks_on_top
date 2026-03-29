# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Loan Eligibility Engine
# MAGIC
# MAGIC Matches users to government financial inclusion schemes:
# MAGIC 1. **Rule-based matching** — PySpark filter on eligibility criteria
# MAGIC 2. **LLM explanation** — Foundation Model API (best-effort)

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Scheme Data

# COMMAND ----------

schemes_df = spark.table(f"{catalog}.{schema}.gold_schemes")
print(f"Total schemes: {schemes_df.count()}")
display(schemes_df.select("scheme_name", "ministry", "target_group", "income_limit", "age_min", "age_max", "occupation", "state").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Eligibility Matching Engine

# COMMAND ----------

def find_eligible_schemes(age, income, occupation, state, gender="all"):
    """Find government schemes matching a user's profile."""
    schemes = spark.table(f"{catalog}.{schema}.gold_schemes")

    matched = (schemes
        .filter((F.col("age_min") <= age) & (F.col("age_max") >= age))
        .filter((F.col("income_limit") == 0) | (F.col("income_limit") >= income))
        .filter((F.col("gender") == "all") | (F.col("gender") == gender.lower()))
        .filter((F.col("occupation") == "all") | (F.col("occupation").contains(occupation.lower())))
        .filter((F.col("state") == "All India") | (F.lower(F.col("state")).contains(state.lower())))
    )

    results = matched.select(
        "scheme_id", "scheme_name", "ministry", "description",
        "eligibility_criteria", "benefits", "target_group",
        "income_limit", "age_min", "age_max", "gender", "occupation", "state",
        "plain_summary", "url"
    ).collect()

    return [row.asDict() for row in results]

# Test
test = find_eligible_schemes(age=25, income=150000, occupation="street_vendor", state="Maharashtra", gender="male")
print(f"Found {len(test)} matching schemes")
for s in test[:5]:
    print(f"  - {s['scheme_name']} ({s['ministry']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. LLM Explanation (best-effort)

# COMMAND ----------

def explain_eligibility(user_profile, schemes, language="english"):
    """Generate explanation using Foundation Model API. Falls back to plain text."""
    if not schemes:
        return "No matching schemes found. Try broadening your criteria."

    scheme_text = "\n".join([
        f"- {s['scheme_name']} ({s['ministry']}): {s.get('benefits', 'N/A')[:200]}"
        for s in schemes[:10]
    ])

    lang_instr = ""
    if language.lower() in ["hindi", "hi"]:
        lang_instr = "Respond in Hindi. "

    prompt = f"""{lang_instr}A {user_profile['age']}-year-old {user_profile['occupation']} from {user_profile['state']} earning ₹{user_profile['income']:,.0f}/year qualifies for:
{scheme_text}

For each scheme, explain briefly what it offers and why this person qualifies."""

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        response = w.serving_endpoints.query(
            name="databricks-llama-4-maverick",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
            temperature=0.2
        )
        return response.choices[0].message.content
    except Exception as e:
        # Fallback: return scheme list without LLM
        fallback = f"Found {len(schemes)} matching schemes:\n\n"
        for s in schemes[:10]:
            fallback += f"**{s['scheme_name']}** ({s['ministry']})\n"
            fallback += f"  Benefits: {s.get('benefits', 'N/A')[:200]}\n\n"
        fallback += f"\n(LLM explanation unavailable: {e})"
        return fallback

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Full Pipeline

# COMMAND ----------

def check_eligibility(age, income, occupation, state, gender="all", language="english"):
    """End-to-end: match schemes + generate explanation."""
    profile = {"age": age, "income": income, "occupation": occupation, "state": state, "gender": gender}
    matched = find_eligible_schemes(age, income, occupation, state, gender)
    explanation = explain_eligibility(profile, matched, language)

    return {
        "user_profile": profile,
        "matched_schemes": [{"name": s["scheme_name"], "ministry": s["ministry"], "benefits": s.get("benefits", ""), "url": s.get("url", "")} for s in matched],
        "total_matches": len(matched),
        "explanation": explanation,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test

# COMMAND ----------

result = check_eligibility(age=25, income=150000, occupation="street_vendor", state="Maharashtra", gender="male")
print(f"Matched {result['total_matches']} schemes:")
for s in result["matched_schemes"][:5]:
    print(f"  - {s['name']}")
print(f"\n{result['explanation']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Log to MLflow

# COMMAND ----------

import mlflow
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks")

schemes_count = spark.table(f"{catalog}.{schema}.gold_schemes").count()
ministries = spark.sql(f"SELECT COUNT(DISTINCT ministry) FROM {catalog}.{schema}.gold_schemes").collect()[0][0]

with mlflow.start_run(run_name="loan_eligibility_engine"):
    mlflow.log_param("matching_method", "PySpark_filter_rules")
    mlflow.log_param("explanation_model", "databricks-llama-4-maverick")
    mlflow.log_param("languages_supported", "english,hindi,marathi,tamil,telugu")
    mlflow.log_metric("schemes_indexed", schemes_count)
    mlflow.log_metric("ministries_covered", ministries)
    mlflow.log_metric("test_matches", result["total_matches"])
print("✅ MLflow logged: loan_eligibility_engine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC **Next:** Run `06-metric-view.py`
